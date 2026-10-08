//! `UhuraService` — API de publicação (grava no outbox).

import { randomUUID } from 'node:crypto';

import { Inject, Injectable, Optional } from '@nestjs/common';
import type { Pool } from 'pg';

import { UhuraAmqp } from './amqp';
import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS, UHURA_PG } from './constants';
import { newEnvelope } from './envelope';
import type { CallOptions } from './rpc-client';
import { UhuraRpcClient } from './rpc-client';
import type { RpcResult } from './rpc';
import { UhuraMetrics } from './metrics';
import { insertOutbox } from './storage';

/** Opções de publicação. */
export interface PublishOptions {
  /** Chave de partição (ordenação). */
  partition?: string;
  /** Origem do evento (`source`). Default: `uhura-nestjs`. */
  source?: string;
}

@Injectable()
export class UhuraService {
  constructor(
    @Inject(UHURA_PG) private readonly pool: Pool,
    @Inject(UHURA_OPTIONS) private readonly options: UhuraModuleOptions,
    private readonly rpc: UhuraRpcClient,
    private readonly amqp: UhuraAmqp,
    @Optional() private readonly metrics?: UhuraMetrics,
  ) {}

  /**
   * `true` quando ha conexao viva com o broker.
   *
   * Existe para a READINESS do servico, e a distincao importa: sem consultar
   * isto, um pod que perdeu a conexao continua respondendo HTTP, continua
   * passando no health check e continua sendo enviado trafego — enquanto nao
   * consome mensagem nenhuma. Foi assim que em 2026-09-22 o
   * `dextrolabs-notification` ficou horas saudavel e mudo.
   *
   * Publicar NAO depende disto: `publish` grava no outbox do Postgres, e quem
   * entrega ao broker e a station. Um servico desconectado ainda aceita
   * trabalho sem perde-lo — o que ele nao faz e CONSUMIR.
   */
  isBrokerConnected(): boolean {
    return this.amqp.isConnected();
  }

  /** Chama um método RPC (`@UhuraFunction`) e devolve o `RpcResult`. */
  call<T = unknown>(
    domain: string,
    method: string,
    args: unknown,
    opts?: CallOptions,
  ): Promise<RpcResult<T>> {
    return this.rpc.call<T>(domain, method, args, opts);
  }

  /**
   * Publica um evento de contrato: grava o envelope CloudEvents no outbox.
   * O dispatcher (uhura-station) o entrega ao broker com publisher confirms.
   */
  async publish(
    domain: string,
    event: string,
    data: unknown,
    opts: PublishOptions = {},
  ): Promise<string> {
    const envelope = newEnvelope(
      randomUUID(),
      opts.source ?? 'uhura-nestjs',
      `${domain}.${event}`,
    );
    envelope.time = new Date().toISOString();
    envelope.facttype = 'EVENT';
    if (opts.partition !== undefined) {
      envelope.subject = opts.partition;
      envelope.partitionkey = opts.partition;
    }
    envelope.data = data;

    const id = await insertOutbox(
      this.pool,
      domain,
      event,
      opts.partition ?? null,
      envelope,
    );
    this.metrics?.published.inc({ domain, event });
    if (this.options.debug) {
      // eslint-disable-next-line no-console
      console.debug(`[uhura] outbox id=${id} type=${envelope.type}`);
    }
    return id;
  }
}
