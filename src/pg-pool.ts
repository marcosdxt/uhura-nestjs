//! Pool PostgreSQL do SDK: criado pelo módulo e encerrado com a aplicação.

import { Inject, Injectable, Logger, type OnApplicationShutdown } from '@nestjs/common';
import { Pool } from 'pg';

import { UHURA_PG } from './constants';

/**
 * Cria o pool com listener de `error`.
 *
 * Sem o listener, o erro de uma conexão OCIOSA (o Postgres reiniciou, o RDS fez
 * failover, um admin derrubou a sessão) sai como evento `error` não tratado do
 * `Pool` — e o Node encerra o processo inteiro. O pool descarta a conexão
 * morta sozinho; basta registrar.
 */
export function createUhuraPool(connectionString: string): Pool {
  const pool = new Pool({ connectionString });
  const logger = new Logger('Uhura');
  pool.on('error', (err) => logger.warn(`conexão ociosa do Postgres caiu: ${err.message}`));
  return pool;
}

/** Fecha o pool no shutdown, para não deixar conexões abertas para trás. */
@Injectable()
export class UhuraPgLifecycle implements OnApplicationShutdown {
  constructor(@Inject(UHURA_PG) private readonly pool: Pool) {}

  async onApplicationShutdown(): Promise<void> {
    await this.pool.end().catch(() => undefined);
  }
}
