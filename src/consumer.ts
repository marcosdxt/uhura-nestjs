//! Descobre handlers `@UhuraSubscribe` e consome os domínios com idempotência.

import {
  Inject,
  Injectable,
  Logger,
  Optional,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
} from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import type * as amqp from 'amqplib';
import type { Pool } from 'pg';

import { UhuraAmqp } from './amqp';
import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS, UHURA_PG, UHURA_SUBSCRIBE_METADATA } from './constants';
import { PauseControl } from './control';
import type { UhuraSubscribeOptions } from './decorators/subscribe.decorator';
import type { Envelope, UhuraEventContext } from './envelope';
import { type ConsumerResult, UhuraMetrics } from './metrics';
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
  /** Tag do consumidor ativo por domínio (ausente = não consumindo). */
  private readonly tags = new Map<string, string>();
  /** Domínios pausados pelo controle da station. */
  private readonly paused = new Set<string>();
  private control?: PauseControl;
  /** Pausar/retomar em fila: dois comandos seguidos não podem se cruzar. */
  private serial: Promise<void> = Promise.resolve();

  constructor(
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly amqp: UhuraAmqp,
    @Inject(UHURA_OPTIONS) private readonly options: UhuraModuleOptions,
    @Inject(UHURA_PG) private readonly pool: Pool,
    @Optional() private readonly metrics?: UhuraMetrics,
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
   *
   * Antes de assinar, aprende o estado desejado de pausa (controle da
   * station): um dominio pausado tem a topologia garantida, mas nao ganha
   * consumidor.
   */
  private async assinar(): Promise<void> {
    if (this.options.control !== false) {
      this.control = new PauseControl(this.logger, this.options.controlTimeoutMs ?? 3000);
      const desejado = await this.control.start(
        await this.amqp.createChannel(),
        this.group,
        [...this.byDomain.keys()],
        (domain, paused) => this.setPaused(domain, paused),
      );
      this.paused.clear();
      for (const domain of desejado) {
        this.paused.add(domain);
      }
    }

    const channel = await this.amqp.createChannel();
    await channel.prefetch(this.options.prefetch ?? 16);
    this.channel = channel;
    // As tags eram do canal antigo, que morreu com a conexao.
    this.tags.clear();

    for (const domain of this.byDomain.keys()) {
      await ensureGroupTopology(channel, domain, this.group);
      this.metrics?.consumerPaused.set({ domain, group: this.group }, this.paused.has(domain) ? 1 : 0);
      if (this.paused.has(domain)) {
        this.logger.warn(
          `'${domain}' PAUSADO para '${this.group}' pelo controle da station; nao assinado`,
        );
        continue;
      }
      await this.consumir(channel, domain);
    }
  }

  /** `basic.consume` de um dominio no canal dado. */
  private async consumir(channel: amqp.Channel, domain: string): Promise<void> {
    const handlers = this.byDomain.get(domain) ?? [];
    const queue = queueName(domain, this.group);
    const { consumerTag } = await channel.consume(
      queue,
      (msg) => {
        // O canal vai FECHADO no callback, e nao lido de `this` na hora do
        // ack: depois de uma reconexao, `this.channel` ja e outro, e dar ack
        // no canal novo para uma mensagem entregue no antigo levanta
        // "unknown delivery tag" — a mensagem volta e o ciclo se repete.
        void this.onMessage(channel, domain, handlers, msg);
      },
      { noAck: false },
    );
    if (consumerTag) {
      this.tags.set(domain, consumerTag);
    }
    this.logger.log(`assinando '${domain}' como '${this.group}' (${queue})`);
  }

  /**
   * Pausa (`basic.cancel`) ou retoma (`basic.consume`) um dominio deste grupo.
   *
   * Pausar para de RECEBER: o que ja foi entregue ao processo termina e recebe
   * ack normalmente, e o resto fica na fila. Idempotente, e em serie com os
   * outros comandos.
   */
  setPaused(domain: string, paused: boolean): Promise<void> {
    const run = async (): Promise<void> => {
      if (!this.byDomain.has(domain)) {
        return;
      }
      const channel = this.channel;
      if (paused) {
        this.paused.add(domain);
        const tag = this.tags.get(domain);
        if (channel && tag) {
          await channel.cancel(tag);
          this.tags.delete(domain);
          this.logger.warn(`'${domain}' PAUSADO para '${this.group}' pelo controle da station`);
        }
      } else {
        this.paused.delete(domain);
        if (channel && !this.tags.has(domain)) {
          await this.consumir(channel, domain);
          this.logger.log(`'${domain}' retomado para '${this.group}' pelo controle da station`);
        }
      }
      this.metrics?.consumerPaused.set({ domain, group: this.group }, paused ? 1 : 0);
    };
    this.serial = this.serial.then(run, run);
    return this.serial;
  }

  /** Dominios pausados agora (para readiness/diagnostico). */
  pausedDomains(): string[] {
    return [...this.paused];
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
    const labels = { domain, group: this.group };
    const fim = this.metrics?.consumerDuration.startTimer(labels);
    const conta = (result: ConsumerResult): void => {
      fim?.();
      this.metrics?.consumerHandled.inc({ ...labels, result });
    };
    try {
      const envelope = JSON.parse(msg.content.toString()) as Envelope;
      const event = envelope.type.startsWith(`${domain}.`)
        ? envelope.type.slice(domain.length + 1)
        : envelope.type;

      const matched = handlers.filter((h) => h.options.events.includes(event));
      if (matched.length === 0) {
        channel.ack(msg);
        conta('ignored');
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
        conta('duplicate');
        return;
      }

      this.observarAtraso(labels, envelope);
      const ctx: UhuraEventContext = {
        ...envelope,
        domain,
        event,
        group: this.group,
        redelivered: msg.fields?.redelivered === true,
        envelope,
      };
      for (const handler of matched) {
        await handler.instance[handler.methodName](envelope.data, ctx);
      }

      // Só agora: o inbox registra o que foi feito, não o que se pretendia.
      await markProcessed(this.pool, envelope.id, domain, envelope.partitionkey ?? null);
      channel.ack(msg);
      conta('ok');
    } catch (err) {
      conta('error');
      this.logger.error(`falha ao processar: ${String(err)}`);
      // requeue → retry; após x-delivery-limit vai ao parking.
      channel.nack(msg, false, true);
    }
  }

  /**
   * Publicação → consumo: agora − `time` do envelope (CloudEvents), medido
   * quando o handler começa. Inclui outbox, station, broker e fila parada
   * (pausa, retry); depende dos relógios em NTP — atraso negativo vira 0.
   * Envelope sem `time` (ou ilegível) não entra na série.
   */
  private observarAtraso(labels: { domain: string; group: string }, envelope: Envelope): void {
    if (!this.metrics || typeof envelope.time !== 'string') {
      return;
    }
    const publicado = Date.parse(envelope.time);
    if (Number.isNaN(publicado)) {
      return;
    }
    this.metrics.consumerLag.observe(labels, Math.max(0, (Date.now() - publicado) / 1000));
  }

  async onModuleDestroy(): Promise<void> {
    await this.control?.close();
    await this.channel?.close().catch(() => undefined);
  }
}
