//! `UhuraService` — API de publicação (grava no outbox).

import { randomUUID } from 'node:crypto';

import { Inject, Injectable } from '@nestjs/common';
import type { Pool } from 'pg';

import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS, UHURA_PG } from './constants';
import { newEnvelope } from './envelope';
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
  ) {}

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
    if (this.options.debug) {
      // eslint-disable-next-line no-console
      console.debug(`[uhura] outbox id=${id} type=${envelope.type}`);
    }
    return id;
  }
}
