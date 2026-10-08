//! Configuração do módulo.

/** Opções de `UhuraModule.forRoot`. */
export interface UhuraModuleOptions {
  /** URL AMQP do RabbitMQ (`amqp://` em cluster privado na v1). */
  amqpUrl: string;
  /** URL do PostgreSQL — fonte de verdade (outbox/inbox). */
  postgresUrl: string;
  /**
   * Grupo de consumo: o nome do serviço. Cada grupo tem a sua fila por domínio
   * (`uhura.<domínio>.<grupo>.q`) e recebe todos os eventos dele; as réplicas
   * do mesmo serviço dividem a fila do grupo.
   *
   * Default: `UHURA_GROUP`, depois `SERVICE_NAME` (os charts ews não injetam
   * nenhum dos dois). Obrigatório para quem assina (`@UhuraSubscribe`/
   * `@UhuraEntityChange`): sem ele, o bootstrap falha. Formato
   * `^[a-z0-9][a-z0-9-]{1,62}$`.
   */
  group?: string;
  /** Nome do mesh (reservado para prefixo de domínio). */
  mesh?: string;
  /** Logging detalhado. */
  debug?: boolean;
  /** Prefetch do consumidor (default 16). */
  prefetch?: number;
  /**
   * Endpoint Prometheus do SDK. Default: `GET /metrics` (sem versão de URI)
   * com as métricas do Uhura e as do processo.
   *
   * - `false`: não monta o endpoint (as métricas continuam sendo coletadas em
   *   `UhuraMetrics.registry`, para o serviço que já tem o seu `/metrics`);
   * - `{ path }`: outro caminho; `{ defaultMetrics: false }`: sem as do processo.
   */
  metrics?: false | UhuraMetricsOptions;
  /**
   * Controle de pausa da station (`uhura.control`). Default `true`: cada réplica
   * escuta os comandos de pausa e, ao subir, pergunta o estado desejado do seu
   * grupo antes de assinar as filas. `false` desliga (o grupo não pode ser
   * pausado pelo painel).
   */
  control?: boolean;
  /** Espera pela resposta do `getPaused` no boot (ms, default 3000). */
  controlTimeoutMs?: number;
}

/** Opções do endpoint de métricas. */
export interface UhuraMetricsOptions {
  /** Caminho (default `metrics`). */
  path?: string;
  /** Inclui as métricas do processo (`collectDefaultMetrics`). Default `true`. */
  defaultMetrics?: boolean;
}
