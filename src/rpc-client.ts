//! Cliente RPC: publica requisições e correlaciona respostas (direct reply-to).

import { randomUUID } from 'node:crypto';

import {
  Inject,
  Injectable,
  Optional,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
} from '@nestjs/common';
import type * as amqp from 'amqplib';

import { UhuraAmqp } from './amqp';
import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS } from './constants';
import { type RpcClientResult, UhuraMetrics } from './metrics';
import { parseErrorCode, type RpcResult } from './rpc';
import { rpcQueueName } from './transport';

const DIRECT_REPLY_TO = 'amq.rabbitmq.reply-to';

/** Opções de uma chamada RPC. */
export interface CallOptions {
  /** Timeout em ms (default 30000). */
  timeoutMs?: number;
}

/**
 * Usuário da URL AMQP (`amqp://usuario:senha@host`), ou `undefined` sem
 * usuário na URL — aí o amqplib conecta como `guest` e nada vai em `user-id`.
 */
export const amqpUser = (url: string | undefined): string | undefined => {
  if (!url) return undefined;
  try {
    const user = decodeURIComponent(new URL(url).username);
    return user || undefined;
  } catch {
    return undefined;
  }
};

@Injectable()
export class UhuraRpcClient implements OnApplicationBootstrap, OnModuleDestroy {
  private channel?: amqp.Channel;
  private readonly pending = new Map<string, (res: RpcResult) => void>();
  private readonly declared = new Set<string>();

  /** Usuário da conexão AMQP, que vai como `user-id` em cada requisição. */
  private readonly user: string | undefined;

  constructor(
    private readonly amqp: UhuraAmqp,
    @Inject(UHURA_OPTIONS) private readonly options: UhuraModuleOptions,
    @Optional() private readonly metrics?: UhuraMetrics,
  ) {
    this.user = amqpUser(options.amqpUrl);
  }

  async onApplicationBootstrap(): Promise<void> {
    await this.abrirRespostas();

    // Tres coisas morrem com a conexao aqui, e cada uma calaria o cliente de um
    // jeito diferente: o canal, a fila de respostas (direct reply-to, que e por
    // canal) e o cache `declared`. Sem reabrir, toda chamada seguinte espera
    // uma resposta que nunca chega e vira timeout — que o chamador le como
    // lentidao do OUTRO servico, e nao como conexao perdida aqui.
    this.amqp.onReconnect(() => this.abrirRespostas());
  }

  private async abrirRespostas(): Promise<void> {
    // As pendentes nao sobrevivem: a resposta delas vinha pelo canal que
    // morreu. Resolver agora, com erro, e melhor que deixar o chamador no
    // timeout — ele fica sabendo a causa.
    for (const [id, resolver] of this.pending) {
      this.pending.delete(id);
      resolver({
        data: null,
        resCode: 'exception',
        errorCode: 'DISCONNECTED',
        errorMessage: 'conexão AMQP caiu antes da resposta',
      });
    }
    // O redeclare de fila e por canal; o canal novo precisa refaze-lo.
    this.declared.clear();

    this.channel = await this.amqp.createChannel();
    await this.channel.consume(
      DIRECT_REPLY_TO,
      (msg) => {
        if (!msg) {
          return;
        }
        const id = msg.properties.correlationId as string | undefined;
        if (!id) {
          return;
        }
        const resolver = this.pending.get(id);
        if (resolver) {
          this.pending.delete(id);
          try {
            resolver(JSON.parse(msg.content.toString()) as RpcResult);
          } catch (err) {
            resolver({ data: null, resCode: 'exception', errorMessage: String(err) });
          }
        }
      },
      { noAck: true },
    );
  }

  /**
   * Chama um método RPC e devolve o `RpcResult` (nunca lança; erros viram
   * `exception`). Em erro, `errorCode` vem preenchido sempre que houver um
   * código — no campo (servidor 0.4+), em `errorStack.code` (driver Rust) ou
   * no prefixo `"CODE: mensagem"` (servidor até a 0.3).
   */
  async call<T = unknown>(
    domain: string,
    method: string,
    data: unknown,
    opts: CallOptions = {},
  ): Promise<RpcResult<T>> {
    const fim = this.metrics?.rpcClientDuration.startTimer({ domain, method });
    const result = await this.chamar<T>(domain, method, data, opts);
    fim?.();
    const errorCode = parseErrorCode(result);
    if (errorCode !== undefined) {
      result.errorCode = errorCode;
    }
    let label: RpcClientResult = result.resCode;
    if (result.resCode === 'exception' && errorCode === 'TIMEOUT') {
      label = 'timeout';
    }
    this.metrics?.rpcClient.inc({ domain, method, result: label });
    return result;
  }

  private async chamar<T>(
    domain: string,
    method: string,
    data: unknown,
    opts: CallOptions,
  ): Promise<RpcResult<T>> {
    const channel = this.channel;
    if (!channel) {
      return {
        data: null,
        resCode: 'exception',
        errorCode: 'DISCONNECTED',
        errorMessage: 'cliente RPC não inicializado',
      };
    }

    const queue = rpcQueueName(domain);
    if (!this.declared.has(domain)) {
      await channel.assertQueue(queue, {
        durable: true,
        arguments: { 'x-queue-type': 'quorum' },
      });
      this.declared.add(domain);
    }

    const correlationId = randomUUID();
    const timeoutMs = opts.timeoutMs ?? 30000;
    return new Promise<RpcResult<T>>((resolve) => {
      const timer = setTimeout(() => {
        if (this.pending.delete(correlationId)) {
          resolve({
            data: null,
            resCode: 'exception',
            errorCode: 'TIMEOUT',
            errorMessage: `timeout após ${timeoutMs}ms`,
          });
        }
      }, timeoutMs);
      this.pending.set(correlationId, (res) => {
        clearTimeout(timer);
        resolve(res as RpcResult<T>);
      });
      channel.sendToQueue(
        queue,
        Buffer.from(JSON.stringify({ id: correlationId, domain, method, data })),
        {
          correlationId,
          replyTo: DIRECT_REPLY_TO,
          contentType: 'application/json',
          // Quem chama, validado pelo broker: o RabbitMQ recusa `user-id`
          // diferente do usuário da conexão. O servidor lê em `ctx.callerUser`.
          ...(this.user ? { userId: this.user } : {}),
        },
      );
    });
  }

  async onModuleDestroy(): Promise<void> {
    await this.channel?.close().catch(() => undefined);
  }
}
