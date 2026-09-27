//! Métricas Prometheus do SDK (prom-client), num registro PRÓPRIO.
//
// Registro próprio, e não o global do prom-client: o serviço pode ter o seu
// (ou outra lib pode registrar no global), e dois registros com o mesmo nome
// de métrica no global derrubam o boot com "already registered". Quem já tem
// um /metrics mescla com `Registry.merge([seu, uhura.registry])`; quem não tem
// usa o endpoint que o `UhuraModule` monta (ver `metrics` nas opções).

import { Inject, Injectable, Optional } from '@nestjs/common';
import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry } from 'prom-client';

import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS } from './constants';

/** Resultado de uma mensagem no consumidor. */
export type ConsumerResult = 'ok' | 'duplicate' | 'ignored' | 'error';

/** Resultado de uma chamada RPC do cliente. */
export type RpcClientResult = 'ok' | 'error' | 'exception' | 'timeout';

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

  constructor(@Optional() @Inject(UHURA_OPTIONS) options?: UhuraModuleOptions) {
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
