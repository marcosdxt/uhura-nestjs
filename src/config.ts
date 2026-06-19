//! Configuração do módulo.

/** Opções de `UhuraModule.forRoot`. */
export interface UhuraModuleOptions {
  /** URL AMQP do RabbitMQ (`amqp://` em cluster privado na v1). */
  amqpUrl: string;
  /** URL do PostgreSQL — fonte de verdade (outbox/inbox). */
  postgresUrl: string;
  /** Nome do mesh (reservado para prefixo de domínio). */
  mesh?: string;
  /** Logging detalhado. */
  debug?: boolean;
  /** Prefetch do consumidor (default 16). */
  prefetch?: number;
}
