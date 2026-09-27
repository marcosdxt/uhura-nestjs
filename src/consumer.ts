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
import { markProcessed, wasProcessed } from './storage';
import { ensureGroupTopology, queueName, resolveGroup } from './transport';

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
  private byDomain = new Map<string, Handler[]>();
  private group = '';

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
    // Antes de abrir canal: sem grupo não há fila para assinar, e o serviço
    // não pode subir parecendo saudável sem consumir nada.
    this.group = resolveGroup(this.options.group);

    this.byDomain = new Map<string, Handler[]>();
    for (const handler of handlers) {
      const list = this.byDomain.get(handler.options.domain) ?? [];
      list.push(handler);
      this.byDomain.set(handler.options.domain, list);
    }

    await this.assinar();

    // Sem isto, a reconexao devolveria uma conexao viva e uma fila SEM
    // consumidor — pior que continuar caido, porque parece resolvido: o pod
    // fica Ready, o broker aceita publicacao, e as mensagens se empilham sem
    // ninguem para retira-las.
    this.amqp.onReconnect(() => this.assinar());
  }

  /**
   * Abre o canal e consome cada dominio. Idempotente de proposito: roda no
   * bootstrap E depois de cada reconexao.
   *
   * O canal antigo nao e fechado aqui — quando a conexao cai, ele ja morreu
   * com ela, e chamar `close()` num canal orfao levanta. A referencia e
   * simplesmente substituida.
   */
  private async assinar(): Promise<void> {
    const channel = await this.amqp.createChannel();
    await channel.prefetch(this.options.prefetch ?? 16);
    this.channel = channel;

    for (const [domain, domainHandlers] of this.byDomain) {
      await ensureGroupTopology(channel, domain, this.group);
      const queue = queueName(domain, this.group);
      await channel.consume(
        queue,
        (msg) => {
          // O canal vai FECHADO no callback, e nao lido de `this` na hora do
          // ack: depois de uma reconexao, `this.channel` ja e outro, e dar ack
          // no canal novo para uma mensagem entregue no antigo levanta
          // "unknown delivery tag" — a mensagem volta e o ciclo se repete.
          void this.onMessage(channel, domain, domainHandlers, msg);
        },
        { noAck: false },
      );
      this.logger.log(`assinando '${domain}' como '${this.group}' (${queue})`);
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
    channel: amqp.Channel,
    domain: string,
    handlers: Handler[],
    msg: amqp.ConsumeMessage | null,
  ): Promise<void> {
    if (!msg) {
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

      // Idempotência: dedup por envelope.id no inbox do banco DESTE serviço
      // (um grupo, um banco), consultando ANTES e marcando
      // DEPOIS. Marcar antes de o handler cumprir transformava a primeira falha
      // em perda: o nack reentregava, a linha do inbox já existia, e a
      // reentrega era descartada como duplicata — sem retry e sem parking, que
      // é o contrário do que o inbox existe para garantir.
      if (await wasProcessed(this.pool, envelope.id)) {
        if (this.options.debug) {
          this.logger.debug(`duplicado ignorado ${envelope.id}`);
        }
        channel.ack(msg);
        return;
      }

      for (const handler of matched) {
        await handler.instance[handler.methodName](envelope.data, envelope);
      }

      // Só agora: o inbox registra o que foi feito, não o que se pretendia.
      await markProcessed(this.pool, envelope.id, domain, envelope.partitionkey ?? null);
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
