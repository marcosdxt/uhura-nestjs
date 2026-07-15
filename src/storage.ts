//! Acesso ao outbox/inbox no PostgreSQL — mesmas tabelas/colunas do SDK Rust.

import type { Pool, PoolClient } from 'pg';

import type { Envelope } from './envelope';

/**
 * Executor de queries: `Pool` (autocommit) ou `PoolClient` (dentro de uma
 * transação aberta pelo chamador — padrão *transactional inbox*, SPEC §12.1).
 */
export type PgExecutor = Pool | PoolClient;

/** Insere um evento no `uhura_outbox` e devolve o id gerado. */
export async function insertOutbox(
  pool: Pool,
  domain: string,
  event: string,
  partitionkey: string | null,
  envelope: Envelope,
): Promise<string> {
  const res = await pool.query<{ id: string }>(
    'INSERT INTO uhura_outbox (domain, event, partitionkey, envelope) ' +
      'VALUES ($1, $2, $3, $4) RETURNING id',
    [domain, event, partitionkey, JSON.stringify(envelope)],
  );
  return String(res.rows[0].id);
}

/**
 * Marca o envelope como processado no `uhura_inbox`.
 * Retorna `true` se é novo (deve processar) e `false` se duplicado.
 *
 * Aceita um `PoolClient` transacional para que o INSERT participe da mesma
 * transação dos efeitos de negócio do handler (SPEC §12.1) — igual ao lado
 * Rust (`uhura-pg`). Mesma query/schema; nada muda para o Rust.
 */
export async function markProcessed(
  executor: PgExecutor,
  envelopeId: string,
  domain: string,
  partitionkey: string | null,
): Promise<boolean> {
  const res = await executor.query(
    'INSERT INTO uhura_inbox (envelope_id, domain, partitionkey) ' +
      'VALUES ($1, $2, $3) ON CONFLICT (envelope_id) DO NOTHING',
    [envelopeId, domain, partitionkey],
  );
  return res.rowCount === 1;
}
