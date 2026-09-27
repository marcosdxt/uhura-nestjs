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
   * Default: `UHURA_GROUP`, depois `SERVICE_NAME` (injetado pelo chart
   * `dextro-service`). Obrigatório para quem assina (`@UhuraSubscribe`/
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
}
