//! Controle de pausa do consumo (protocolo `control` do uhura-core 0.3).
//
// A station pausa um domínio × grupo quando alguém precisa parar de TIRAR
// mensagens da fila sem derrubar o serviço (investigar um handler que está
// quebrando, segurar um grupo durante uma migração). A fila continua
// recebendo; nada se perde.
//
// - exchange `topic` `uhura.control`: cada RÉPLICA liga uma fila exclusiva,
//   auto-delete e sem nome, com `#` — todas recebem todos os comandos;
// - `consumer-pause {domain, group, paused}`: o grupo para (basic.cancel) ou
//   volta (basic.consume) naquele domínio;
// - `consumer-snapshot {paused: [{domain, group}]}`: o estado completo; o que
//   não está na lista consome. A station autoridade o publica ao subir e a
//   cada minuto, e é o que conserta a réplica que perdeu um comando;
// - RPC `uhura.control.rpc` `getPaused {group}`: a réplica pergunta ANTES de
//   assinar as filas, para não consumir nem um instante um domínio pausado. O
//   estado desejado mora no banco da station autoridade, que o SDK não enxerga.
//
// Sem resposta (autoridade fora), a réplica consome e avisa no log: travar o
// boot do serviço porque o painel está fora seria trocar um problema pequeno
// por um grande. O próximo retrato corrige.

import { randomUUID } from 'node:crypto';

import type { Logger } from '@nestjs/common';
import type * as amqp from 'amqplib';

export const CONTROL_EXCHANGE = 'uhura.control';
export const CONTROL_RPC_QUEUE = 'uhura.control.rpc';
const DIRECT_REPLY_TO = 'amq.rabbitmq.reply-to';

/** Um domínio × grupo pausado. */
export interface PausedEntry {
  domain: string;
  group: string;
  since?: string;
  by?: string;
  reason?: string;
}

/** Mensagem da exchange de controle. */
export type ControlMessage =
  | {
      kind: 'consumer-pause';
      domain: string;
      group: string;
      paused: boolean;
      at: string;
      by?: string;
      reason?: string;
    }
  | { kind: 'consumer-snapshot'; paused: PausedEntry[]; at: string };

/** Quem aplica a pausa (o consumidor). */
export type ApplyPause = (domain: string, paused: boolean) => Promise<void>;

/**
 * Traduz uma mensagem de controle em (domínio → pausado) para os domínios
 * deste grupo. `null` quando a mensagem não diz respeito a ele.
 */
export function pausesFor(
  message: ControlMessage,
  group: string,
  domains: readonly string[],
): Map<string, boolean> | null {
  if (message.kind === 'consumer-pause') {
    if (message.group !== group || !domains.includes(message.domain)) {
      return null;
    }
    return new Map([[message.domain, message.paused === true]]);
  }
  if (message.kind === 'consumer-snapshot' && Array.isArray(message.paused)) {
    const mine = new Set(
      message.paused.filter((e) => e.group === group).map((e) => e.domain),
    );
    return new Map(domains.map((d) => [d, mine.has(d)]));
  }
  return null;
}

/** Escuta o controle de um grupo e pergunta o estado desejado ao subir. */
export class PauseControl {
  private channel?: amqp.Channel;

  constructor(
    private readonly logger: Logger,
    private readonly timeoutMs: number,
  ) {}

  /**
   * Liga a fila de controle no canal dado e pergunta o estado desejado.
   * Devolve os domínios pausados (vazio se a autoridade não respondeu).
   *
   * A fila é ligada ANTES da pergunta: um comando que chegue entre a resposta
   * e a assinatura não se perde.
   */
  async start(
    channel: amqp.Channel,
    group: string,
    domains: readonly string[],
    apply: ApplyPause,
  ): Promise<Set<string>> {
    this.channel = channel;
    await channel.assertExchange(CONTROL_EXCHANGE, 'topic', { durable: true });
    const { queue } = await channel.assertQueue('', {
      exclusive: true,
      autoDelete: true,
      durable: false,
    });
    await channel.bindQueue(queue, CONTROL_EXCHANGE, '#');
    await channel.consume(
      queue,
      (msg) => {
        if (!msg) {
          return;
        }
        let message: ControlMessage;
        try {
          message = JSON.parse(msg.content.toString()) as ControlMessage;
        } catch {
          return;
        }
        const pauses = pausesFor(message, group, domains);
        if (!pauses) {
          return;
        }
        for (const [domain, paused] of pauses) {
          void apply(domain, paused).catch((err: unknown) =>
            this.logger.error(`controle: falha ao aplicar pausa em '${domain}': ${String(err)}`),
          );
        }
      },
      { noAck: true },
    );

    return this.desired(channel, group, domains);
  }

  /** `getPaused {group}` pelo RPC da autoridade, no próprio canal de controle. */
  private async desired(
    channel: amqp.Channel,
    group: string,
    domains: readonly string[],
  ): Promise<Set<string>> {
    await channel.assertQueue(CONTROL_RPC_QUEUE, {
      durable: true,
      arguments: { 'x-queue-type': 'quorum' },
    });
    const correlationId = randomUUID();
    const answer = new Promise<Set<string> | null>((resolve) => {
      const timer = setTimeout(() => resolve(null), this.timeoutMs);
      void channel
        .consume(
          DIRECT_REPLY_TO,
          (msg) => {
            if (!msg || msg.properties.correlationId !== correlationId) {
              return;
            }
            clearTimeout(timer);
            try {
              const res = JSON.parse(msg.content.toString()) as {
                resCode?: string;
                data?: { paused?: PausedEntry[] } | null;
                errorMessage?: string;
              };
              if (res.resCode !== 'ok') {
                this.logger.warn(`controle: getPaused recusado: ${res.errorMessage ?? res.resCode}`);
                resolve(null);
                return;
              }
              const paused = (res.data?.paused ?? [])
                .filter((e) => e.group === group && domains.includes(e.domain))
                .map((e) => e.domain);
              resolve(new Set(paused));
            } catch (err) {
              this.logger.warn(`controle: resposta ilegível do getPaused: ${String(err)}`);
              resolve(null);
            }
          },
          { noAck: true },
        )
        .then(() => {
          channel.sendToQueue(
            CONTROL_RPC_QUEUE,
            Buffer.from(
              JSON.stringify({ id: correlationId, domain: 'control', method: 'getPaused', data: { group } }),
            ),
            {
              correlationId,
              replyTo: DIRECT_REPLY_TO,
              contentType: 'application/json',
              // Sem autoridade de pé, o pedido não pode ficar parado na fila
              // para sempre: expira junto com a nossa espera.
              expiration: String(this.timeoutMs),
            },
          );
        })
        .catch((err: unknown) => {
          clearTimeout(timer);
          this.logger.warn(`controle: getPaused não saiu: ${String(err)}`);
          resolve(null);
        });
    });

    const paused = await answer;
    if (paused === null) {
      this.logger.warn(
        `controle: a station autoridade não respondeu em ${this.timeoutMs}ms; ` +
          `'${group}' consome tudo até o próximo retrato do controle`,
      );
      return new Set();
    }
    return paused;
  }

  async close(): Promise<void> {
    await this.channel?.close().catch(() => undefined);
  }
}
