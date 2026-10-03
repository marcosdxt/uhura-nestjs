//! Métricas Prometheus do SDK (prom-client), num registro PRÓPRIO.
//
// Registro próprio, e não o global do prom-client: o serviço pode ter o seu
// (ou outra lib pode registrar no global), e dois registros com o mesmo nome
// de métrica no global derrubam o boot com "already registered". Quem já tem
// um /metrics mescla com `Registry.merge([seu, uhura.registry])`; quem não tem
// usa o endpoint que o `UhuraModule` monta (ver `metrics` nas opções).

import { Inject, Injectable, Optional } from '@nestjs/common';
import type { Pool } from 'pg';
import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry } from 'prom-client';

import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS, UHURA_PG } from './constants';
import { outboxBacklog } from './storage';

/** Resultado de uma mensagem no consumidor. */
export type ConsumerResult = 'ok' | 'duplicate' | 'ignored' | 'error';

/** Resultado de uma chamada RPC do cliente. */
export type RpcClientResult = 'ok' | 'error' | 'exception' | 'timeout';

/** Resultado de uma requisição no servidor RPC. */
export type RpcServerResult = 'ok' | 'error' | 'exception';

/** Buckets de duração (s): handler e RPC vivem entre milissegundos e o timeout de 30 s. */
const BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30];

/**
 * Buckets do atraso publicação → consumo (s). Vai além da duração: uma fila
 * pausada ou em retry segura o evento por minutos, e o p95 precisa enxergar.
 */
const LAG_BUCKETS = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600];

@Injectable()
export class UhuraMetrics {
  /** O registro do SDK: monte-o no `/metrics` do serviço. */
  readonly registry = new Registry();

  readonly consumerHandled: Counter<'domain' | 'group' | 'result'>;
  readonly consumerDuration: Histogram<'domain' | 'group'>;
  readonly consumerPaused: Gauge<'domain' | 'group'>;
  readonly consumerLag: Histogram<'domain' | 'group'>;
  readonly rpcClient: Counter<'domain' | 'method' | 'result'>;
  readonly rpcClientDuration: Histogram<'domain' | 'method'>;
  readonly reconnects: Counter<never>;
  readonly published: Counter<'domain' | 'event'>;
  readonly rpcServer: Counter<'domain' | 'method' | 'result'>;
  readonly rpcServerDuration: Histogram<'domain' | 'method'>;
  readonly consumerRedelivered: Counter<'domain' | 'group'>;
  readonly outboxPending: Gauge<never>;
  readonly outboxOldestAge: Gauge<never>;

  constructor(
    @Optional() @Inject(UHURA_OPTIONS) options?: UhuraModuleOptions,
    @Optional() @Inject(UHURA_PG) pool?: Pool,
  ) {
    const registers = [this.registry];
    this.consumerHandled = new Counter({
      name: 'uhura_consumer_handled_total',
      help: 'Mensagens tratadas pelo consumidor, por resultado (ok, duplicate, ignored, error).',
      labelNames: ['domain', 'group', 'result'],
      registers,
    });
    this.consumerDuration = new Histogram({
      name: 'uhura_consumer_handler_duration_seconds',
      help: 'Duração do tratamento de uma mensagem (inbox + handlers + ack).',
      labelNames: ['domain', 'group'],
      buckets: BUCKETS,
      registers,
    });
    this.consumerPaused = new Gauge({
      name: 'uhura_consumer_paused',
      help: '1 quando o consumo do domínio × grupo está pausado pelo controle da station.',
      labelNames: ['domain', 'group'],
      registers,
    });
    this.consumerLag = new Histogram({
      name: 'uhura_consumer_lag_seconds',
      help: 'Atraso publicação → consumo: agora − time do envelope, quando o handler começa.',
      labelNames: ['domain', 'group'],
      buckets: LAG_BUCKETS,
      registers,
    });
    this.rpcClient = new Counter({
      name: 'uhura_rpc_client_total',
      help: 'Chamadas RPC feitas por este serviço, por resultado (ok, error, exception, timeout).',
      labelNames: ['domain', 'method', 'result'],
      registers,
    });
    this.rpcClientDuration = new Histogram({
      name: 'uhura_rpc_client_duration_seconds',
      help: 'Duração de uma chamada RPC até a resposta (ou o timeout).',
      labelNames: ['domain', 'method'],
      buckets: BUCKETS,
      registers,
    });
    this.reconnects = new Counter({
      name: 'uhura_amqp_reconnects_total',
      help: 'Reconexões AMQP bem-sucedidas desde o boot.',
      registers,
    });

    this.published = new Counter({
      name: 'uhura_publish_total',
      help: 'Eventos gravados no outbox por UhuraService.publish, por domínio e evento.',
      labelNames: ['domain', 'event'],
      registers,
    });
    this.rpcServer = new Counter({
      name: 'uhura_rpc_server_total',
      help: 'Requisições RPC atendidas por este serviço (@UhuraFunction), por resultado (ok, error, exception).',
      labelNames: ['domain', 'method', 'result'],
      registers,
    });
    this.rpcServerDuration = new Histogram({
      name: 'uhura_rpc_server_duration_seconds',
      help: 'Duração do handler RPC (@UhuraFunction) até a resposta.',
      labelNames: ['domain', 'method'],
      buckets: BUCKETS,
      registers,
    });
    this.consumerRedelivered = new Counter({
      name: 'uhura_consumer_redelivered_total',
      help: 'Mensagens que chegaram reentregues pelo broker (falha ou queda antes do ack).',
      labelNames: ['domain', 'group'],
      registers,
    });

    // O backlog do outbox é lido do banco NA HORA DO SCRAPE: cobre também o
    // que entra por INSERT direto na transação de negócio (writers dos
    // serviços), e não só o `publish` deste SDK. Sem pool (teste, métricas
    // sem banco), os gauges ficam em zero. Falha de consulta não derruba o
    // scrape: o gauge fica com o último valor.
    let backlog: Promise<{ pending: number; oldestAgeSeconds: number }> | null = null;
    const readBacklog = () => {
      if (!pool) return Promise.resolve(null);
      backlog ??= outboxBacklog(pool).finally(() => {
        backlog = null;
      });
      return backlog.catch(() => null);
    };
    this.outboxPending = new Gauge({
      name: 'uhura_outbox_pending',
      help: 'Eventos no uhura_outbox ainda não publicados pela station.',
      registers,
      async collect() {
        const b = await readBacklog();
        if (b) this.set(b.pending);
      },
    });
    this.outboxOldestAge = new Gauge({
      name: 'uhura_outbox_oldest_pending_age_seconds',
      help: 'Idade (s) do evento mais antigo do outbox ainda não publicado; 0 sem pendência.',
      registers,
      async collect() {
        const b = await readBacklog();
        if (b) this.set(b.oldestAgeSeconds);
      },
    });

    const metrics = options?.metrics;
    const defaults = metrics === false ? false : (metrics?.defaultMetrics ?? true);
    if (defaults) {
      // Processo (CPU, heap, event loop) no mesmo registro: o serviço que usa o
      // endpoint do SDK não precisa de outro só para isso.
      collectDefaultMetrics({ register: this.registry });
    }
  }

  /** Texto de exposição do registro do SDK. */
  metrics(): Promise<string> {
    return this.registry.metrics();
  }

  /** `Content-Type` do texto de exposição. */
  get contentType(): string {
    return this.registry.contentType;
  }
}
