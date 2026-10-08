//! Endpoint `/metrics` montado pelo `UhuraModule` (opção `metrics`).

import { Controller, Get, Header, Inject, VERSION_NEUTRAL, type Type } from '@nestjs/common';
import { Registry } from 'prom-client';

import { UhuraMetrics } from './metrics';

/** Caminho padrão do endpoint de métricas. */
export const DEFAULT_METRICS_PATH = 'metrics';

/**
 * Cria o controller no caminho pedido. `VERSION_NEUTRAL`: os serviços ligam o
 * versionamento por URI com default `1`, e sem isto o endpoint viraria
 * `/v1/metrics`, que não é onde o scrape procura.
 *
 * O Kong só roteia os prefixos de API de cada serviço, então `/metrics` fica
 * visível dentro do cluster e não na borda.
 */
export function createMetricsController(path: string): Type<unknown> {
  @Controller({ path: path.replace(/^\/+/, ''), version: VERSION_NEUTRAL })
  class UhuraMetricsController {
    constructor(@Inject(UhuraMetrics) private readonly metrics: UhuraMetrics) {}

    @Get()
    @Header('Content-Type', Registry.PROMETHEUS_CONTENT_TYPE)
    @Header('Cache-Control', 'no-store')
    scrape(): Promise<string> {
      return this.metrics.metrics();
    }
  }
  return UhuraMetricsController;
}
