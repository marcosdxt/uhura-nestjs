//! Servidor RPC: descobre `@UhuraFunction` e responde requisições.

import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
  Optional,
} from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import type * as amqp from 'amqplib';

import { UhuraAmqp } from './amqp';
import type { UhuraModuleOptions } from './config';
import { UHURA_FUNCTION_METADATA, UHURA_OPTIONS } from './constants';
import { UhuraMetrics } from './metrics';
import type { UhuraFunctionOptions } from './decorators/function.decorator';
import { RpcError, type RpcRequest, type RpcResult, type UhuraRpcContext } from './rpc';
import { rpcQueueName } from './transport';

interface FnHandler {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  instance: any;
  methodName: string;
}

@Injectable()
export class UhuraRpcServer implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger('Uhura');
  private channel?: amqp.Channel;
  private byDomain = new Map<string, Map<string, FnHandler>>();

  constructor(
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly amqp: UhuraAmqp,
    @Inject(UHURA_OPTIONS) private readonly options: UhuraModuleOptions,
    @Optional() private readonly metrics?: UhuraMetrics,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    this.byDomain = this.discover();
    if (this.byDomain.size === 0) {
      return;
    }
    await this.servir();

    // O RPC cai junto com o consumidor, e em silencio igual: quem chamar passa
    // a receber timeout em vez de erro, o que parece lentidao do outro servico.
    this.amqp.onReconnect(() => this.servir());
  }

  /** Abre o canal e serve cada dominio. Roda no bootstrap e na reconexao. */
  private async servir(): Promise<void> {
    const channel = await this.amqp.createChannel();
    await channel.prefetch(this.options.prefetch ?? 16);
    this.channel = channel;

    for (const [domain, methods] of this.byDomain) {
      const queue = rpcQueueName(domain);
      await channel.assertQueue(queue, {
        durable: true,
        arguments: { 'x-queue-type': 'quorum' },
      });
      await channel.consume(
        queue,
        (msg) => {
          // Canal fechado no callback: ver o comentario gemeo no consumer.
          void this.onRequest(channel, methods, msg);
        },
        { noAck: false },
      );
      this.logger.log(
        `RPC servindo '${domain}' (${queue}) métodos: ${[...methods.keys()].join(', ')}`,
      );
    }
  }

  private discover(): Map<string, Map<string, FnHandler>> {
    const result = new Map<string, Map<string, FnHandler>>();
    for (const wrapper of this.discovery.getProviders()) {
      const instance = wrapper.instance;
      if (!instance || typeof instance !== 'object') {
        continue;
      }
      const prototype = Object.getPrototypeOf(instance);
      if (!prototype) {
        continue;
      }
      for (const methodName of this.scanner.getAllMethodNames(prototype)) {
        const method = instance[methodName];
        const options = Reflect.getMetadata(UHURA_FUNCTION_METADATA, method) as
          | UhuraFunctionOptions
          | undefined;
        if (options) {
          const methods = result.get(options.domain) ?? new Map<string, FnHandler>();
          methods.set(options.method, { instance, methodName });
          result.set(options.domain, methods);
        }
      }
    }
    return result;
  }

  private async onRequest(
    channel: amqp.Channel,
    methods: Map<string, FnHandler>,
    msg: amqp.ConsumeMessage | null,
  ): Promise<void> {
    if (!msg) {
      return;
    }

    let result: RpcResult;
    // Rotulado depois do parse; requisição ilegível conta como `?` / `?`.
    let labels = { domain: '?', method: '?' };
    const inicio = process.hrtime.bigint();
    try {
      const request = JSON.parse(msg.content.toString()) as RpcRequest;
      labels = { domain: String(request.domain ?? '?'), method: String(request.method ?? '?') };
      const handler = methods.get(request.method);
      if (!handler) {
        result = {
          data: null,
          resCode: 'error',
          errorCode: 'UNKNOWN_METHOD',
          errorMessage: `método desconhecido: ${request.method}`,
          errorStack: { code: 'UNKNOWN_METHOD' },
        };
      } else {
        const ctx: UhuraRpcContext = {
          id: request.id,
          domain: request.domain,
          method: request.method,
          correlationId: msg.properties.correlationId as string | undefined,
          redelivered: msg.fields?.redelivered === true,
          ...(msg.properties.userId ? { callerUser: msg.properties.userId as string } : {}),
        };
        const data = await handler.instance[handler.methodName](request.data, ctx);
        result = { data: data ?? null, resCode: 'ok' };
      }
    } catch (err) {
      result = RpcError.is(err)
        ? {
            // Erro de negocio: codigo e mensagem em campos proprios, e o codigo
            // tambem em `errorStack.code`, onde o driver Rust do device o poe.
            data: null,
            resCode: 'error',
            errorCode: err.code,
            errorMessage: err.message,
            errorStack: { ...(err.details ?? {}), code: err.code },
          }
        : {
            data: null,
            resCode: 'exception',
            errorMessage: (err as Error)?.message ?? String(err),
            errorStack: this.options.debug ? (err as Error)?.stack : undefined,
          };
    }

    if (this.metrics) {
      this.metrics.rpcServerDuration.observe(labels, Number(process.hrtime.bigint() - inicio) / 1e9);
      this.metrics.rpcServer.inc({ ...labels, result: result.resCode === 'ok' ? 'ok' : result.resCode === 'error' ? 'error' : 'exception' });
    }

    const replyTo = msg.properties.replyTo as string | undefined;
    if (replyTo) {
      channel.sendToQueue(replyTo, Buffer.from(JSON.stringify(result)), {
        correlationId: msg.properties.correlationId,
        contentType: 'application/json',
      });
    }
    channel.ack(msg);
  }

  async onModuleDestroy(): Promise<void> {
    await this.channel?.close().catch(() => undefined);
  }
}
