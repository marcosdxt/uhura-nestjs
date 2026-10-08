//! Acesso ao outbox/inbox no PostgreSQL — mesmas tabelas/colunas do SDK Rust.

import type { Pool, PoolClient } from 'pg';

import type { Envelope } from './envelope';

/** Insere um evento no `uhura_outbox` e devolve o id gerado. */
export async function insertOutbox(
  pool: Pool | PoolClient,
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
 * Reivindica o envelope no `uhura_inbox` dentro da transação do consumidor.
 * Retorna `true` se é novo e `false` se já foi processado.
 *
 * Roda na MESMA transação dos handlers: a linha só se torna visível no COMMIT,
 * junto com o que o handler gravou por `ctx.tx`. Se o handler falha, o
 * ROLLBACK desfaz a reivindicação e a reentrega processa de novo. Uma
 * reentrega concorrente do mesmo envelope espera no índice único e, depois do
 * COMMIT da primeira, cai no `DO NOTHING`.
 */
export async function claimInbox(
  tx: PoolClient,
  envelopeId: string,
  domain: string,
  partitionkey: string | null,
): Promise<boolean> {
  const res = await tx.query(
    'INSERT INTO uhura_inbox (envelope_id, domain, partitionkey) ' +
      'VALUES ($1, $2, $3) ON CONFLICT (envelope_id) DO NOTHING',
    [envelopeId, domain, partitionkey],
  );
  return res.rowCount === 1;
}

/**
 * O backlog do `uhura_outbox`: quantos eventos esperam a station e a idade do
 * mais antigo. Usa o índice parcial `WHERE published_at IS NULL`.
 */
export async function outboxBacklog(pool: Pool): Promise<{ pending: number; oldestAgeSeconds: number }> {
  const res = await pool.query(
    'SELECT count(*)::int AS pending, ' +
      'COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::float8 AS age ' +
      'FROM uhura_outbox WHERE published_at IS NULL',
  );
  const row = res.rows[0] ?? { pending: 0, age: 0 };
  return { pending: Number(row.pending), oldestAgeSeconds: Number(row.age) };
}
