//! Cliente RPC: publica requisições e correlaciona respostas (direct reply-to).

import { randomUUID } from 'node:crypto';

import {
  Inject,
  Injectable,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
} from '@nestjs/common';
import type * as amqp from 'amqplib';

import { UhuraAmqp } from './amqp';
import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS } from './constants';
import type { RpcResult } from './rpc';
import { rpcQueueName } from './transport';

const DIRECT_REPLY_TO = 'amq.rabbitmq.reply-to';

/** Opções de uma chamada RPC. */
export interface CallOptions {
  /** Timeout em ms (default 30000). */
  timeoutMs?: number;
}

@Injectable()
export class UhuraRpcClient implements OnApplicationBootstrap, OnModuleDestroy {
  private channel?: amqp.Channel;
  private readonly pending = new Map<string, (res: RpcResult) => void>();
  private readonly declared = new Set<string>();

  constructor(
    private readonly amqp: UhuraAmqp,
    @Inject(UHURA_OPTIONS) private readonly options: UhuraModuleOptions,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
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

  /** Chama um método RPC e devolve o `RpcResult` (nunca lança; erros viram `exception`). */
  async call<T = unknown>(
    domain: string,
    method: string,
    data: unknown,
    opts: CallOptions = {},
  ): Promise<RpcResult<T>> {
    const channel = this.channel;
    if (!channel) {
      return { data: null, resCode: 'exception', errorMessage: 'cliente RPC não inicializado' };
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
          resolve({ data: null, resCode: 'exception', errorMessage: `timeout após ${timeoutMs}ms` });
        }
      }, timeoutMs);
      this.pending.set(correlationId, (res) => {
        clearTimeout(timer);
        resolve(res as RpcResult<T>);
      });
      channel.sendToQueue(
        queue,
        Buffer.from(JSON.stringify({ id: correlationId, domain, method, data })),
        { correlationId, replyTo: DIRECT_REPLY_TO, contentType: 'application/json' },
      );
    });
  }

  async onModuleDestroy(): Promise<void> {
    await this.channel?.close().catch(() => undefined);
  }
}
