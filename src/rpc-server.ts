//! Servidor RPC: descobre `@UhuraFunction` e responde requisições.

import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
} from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import type * as amqp from 'amqplib';

import { UhuraAmqp } from './amqp';
import type { UhuraModuleOptions } from './config';
import { UHURA_FUNCTION_METADATA, UHURA_OPTIONS } from './constants';
import type { UhuraFunctionOptions } from './decorators/function.decorator';
import type { RpcRequest, RpcResult } from './rpc';
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

  constructor(
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly amqp: UhuraAmqp,
    @Inject(UHURA_OPTIONS) private readonly options: UhuraModuleOptions,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    const byDomain = this.discover();
    if (byDomain.size === 0) {
      return;
    }
    this.channel = await this.amqp.createChannel();
    await this.channel.prefetch(this.options.prefetch ?? 16);

    for (const [domain, methods] of byDomain) {
      const queue = rpcQueueName(domain);
      await this.channel.assertQueue(queue, {
        durable: true,
        arguments: { 'x-queue-type': 'quorum' },
      });
      await this.channel.consume(
        queue,
        (msg) => {
          void this.onRequest(methods, msg);
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
    methods: Map<string, FnHandler>,
    msg: amqp.ConsumeMessage | null,
  ): Promise<void> {
    const channel = this.channel;
    if (!msg || !channel) {
      return;
    }

    let result: RpcResult;
    try {
      const request = JSON.parse(msg.content.toString()) as RpcRequest;
      const handler = methods.get(request.method);
      if (!handler) {
        result = {
          data: null,
          resCode: 'error',
          errorMessage: `método desconhecido: ${request.method}`,
        };
      } else {
        const data = await handler.instance[handler.methodName](request.data);
        result = { data: data ?? null, resCode: 'ok' };
      }
    } catch (err) {
      const error = err as Error;
      result = {
        data: null,
        resCode: 'exception',
        errorMessage: error?.message ?? String(err),
        errorStack: this.options.debug ? error?.stack : undefined,
      };
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
