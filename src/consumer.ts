//! Descobre handlers `@UhuraSubscribe` e consome os domínios com idempotência.

import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
} from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import type * as amqp from 'amqplib';
import type { Pool } from 'pg';

import { UhuraAmqp } from './amqp';
import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS, UHURA_PG, UHURA_SUBSCRIBE_METADATA } from './constants';
import type { UhuraSubscribeOptions } from './decorators/subscribe.decorator';
import type { Envelope } from './envelope';
import { markProcessed } from './storage';
import { ensureTopology, queueName } from './transport';

interface Handler {
  options: UhuraSubscribeOptions;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  instance: any;
  methodName: string;
}

@Injectable()
export class UhuraConsumer implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger('Uhura');
  private channel?: amqp.Channel;

  constructor(
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly amqp: UhuraAmqp,
    @Inject(UHURA_OPTIONS) private readonly options: UhuraModuleOptions,
    @Inject(UHURA_PG) private readonly pool: Pool,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    const handlers = this.discover();
    if (handlers.length === 0) {
      return;
    }

    this.channel = await this.amqp.createChannel();
    await this.channel.prefetch(this.options.prefetch ?? 16);

    const byDomain = new Map<string, Handler[]>();
    for (const handler of handlers) {
      const list = byDomain.get(handler.options.domain) ?? [];
      list.push(handler);
      byDomain.set(handler.options.domain, list);
    }

    for (const [domain, domainHandlers] of byDomain) {
      await ensureTopology(this.channel, domain);
      const queue = queueName(domain);
      await this.channel.consume(
        queue,
        (msg) => {
          void this.onMessage(domain, domainHandlers, msg);
        },
        { noAck: false },
      );
      this.logger.log(`assinando '${domain}' (${queue})`);
    }
  }

  private discover(): Handler[] {
    const result: Handler[] = [];
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
        const options = Reflect.getMetadata(UHURA_SUBSCRIBE_METADATA, method) as
          | UhuraSubscribeOptions
          | undefined;
        if (options) {
          result.push({ options, instance, methodName });
        }
      }
    }
    return result;
  }

  private async onMessage(
    domain: string,
    handlers: Handler[],
    msg: amqp.ConsumeMessage | null,
  ): Promise<void> {
    const channel = this.channel;
    if (!msg || !channel) {
      return;
    }
    try {
      const envelope = JSON.parse(msg.content.toString()) as Envelope;
      const event = envelope.type.startsWith(`${domain}.`)
        ? envelope.type.slice(domain.length + 1)
        : envelope.type;

      const matched = handlers.filter((h) => h.options.events.includes(event));
      if (matched.length === 0) {
        channel.ack(msg);
        return;
      }

      // Transactional inbox (SPEC §12.1): dedup + handlers na MESMA transação.
      // Se um handler falhar, o ROLLBACK desfaz a marca do inbox e a redelivery
      // reexecuta do zero; poison messages genuínas atingem o x-delivery-limit
      // e vão ao parking. Handlers recebem o client transacional como 3º
      // argumento para que seus writes Postgres sejam atômicos com o dedup.
      const client = await this.pool.connect();
      try {
        await client.query('BEGIN');
        const isNew = await markProcessed(
          client,
          envelope.id,
          domain,
          envelope.partitionkey ?? null,
        );
        if (isNew) {
          for (const handler of matched) {
            await handler.instance[handler.methodName](
              envelope.data,
              envelope,
              client,
            );
          }
        } else if (this.options.debug) {
          this.logger.debug(`duplicado ignorado ${envelope.id}`);
        }
        await client.query('COMMIT');
      } catch (err) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw err;
      } finally {
        client.release();
      }
      // Multi-pod: ack SOMENTE após o COMMIT — nunca antes.
      channel.ack(msg);
    } catch (err) {
      this.logger.error(`falha ao processar: ${String(err)}`);
      // requeue → retry; após x-delivery-limit vai ao parking.
      channel.nack(msg, false, true);
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.channel?.close().catch(() => undefined);
  }
}
