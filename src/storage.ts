//! Acesso ao outbox/inbox no PostgreSQL — mesmas tabelas/colunas do SDK Rust.

import type { Pool } from 'pg';

import type { Envelope } from './envelope';

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
 * O envelope já foi processado com sucesso antes?
 *
 * Consulta e marcação são separadas de propósito: marcar antes de o handler
 * cumprir transforma a primeira falha em perda — o nack reentrega, a linha do
 * inbox já existe, e a reentrega é descartada como duplicata, sem retry e sem
 * parking. Ver `markProcessed`.
 */
export async function wasProcessed(pool: Pool, envelopeId: string): Promise<boolean> {
  const res = await pool.query('SELECT 1 FROM uhura_inbox WHERE envelope_id = $1', [envelopeId]);
  return (res.rowCount ?? 0) > 0;
}

/**
 * Marca o envelope como processado no `uhura_inbox`.
 * Retorna `true` se é novo e `false` se outro consumidor chegou antes.
 *
 * Chamado DEPOIS de o handler cumprir: o inbox registra o que já foi feito, e
 * não o que se pretende fazer.
 */
export async function markProcessed(
  pool: Pool,
  envelopeId: string,
  domain: string,
  partitionkey: string | null,
): Promise<boolean> {
  const res = await pool.query(
    'INSERT INTO uhura_inbox (envelope_id, domain, partitionkey) ' +
      'VALUES ($1, $2, $3) ON CONFLICT (envelope_id) DO NOTHING',
    [envelopeId, domain, partitionkey],
  );
  return res.rowCount === 1;
}
