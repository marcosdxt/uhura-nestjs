//! Topologia RabbitMQ — DEVE espelhar exatamente o driver Rust (uhura-transport).
//
// Mesmos nomes e mesmos argumentos de fila/exchange; caso contrário um redeclare
// entre os dois SDKs gera conflito no broker.
//
// - `uhura.<domínio>`: exchange `topic` do domínio (quem publica e quem consome);
// - `uhura.<domínio>.<grupo>.q`: quorum queue do grupo de consumo, binding `#`,
//   DLX → parking do grupo, `x-delivery-limit`;
// - `uhura.<domínio>.<grupo>.parking` / `.parking.q`: parking do grupo;
// - `uhura.<domínio>.rpc`: fila de RPC (ponto a ponto, sem grupo).
//
// Até a 0.1 a fila era uma só por domínio (`uhura.<domínio>.q`), e serviços
// diferentes disputavam o mesmo evento. Com a fila por grupo, cada serviço
// recebe todos os eventos do domínio e as réplicas do mesmo serviço dividem a
// fila do grupo.

import type { Channel } from 'amqplib';

/** Limite de entregas antes do parking (poison-message handling). */
const DELIVERY_LIMIT = 5;

/** Formato do grupo: sem ponto, para o nome da fila não ficar ambíguo. */
const GROUP_PATTERN = /^[a-z0-9][a-z0-9-]{1,62}$/;

/** Colidiriam com os sufixos da própria topologia. */
const RESERVED_GROUPS = new Set(['parking', 'rpc']);

export function exchangeName(domain: string): string {
  return `uhura.${domain}`;
}
export function queueName(domain: string, group: string): string {
  return `uhura.${domain}.${group}.q`;
}
export function parkingExchange(domain: string, group: string): string {
  return `uhura.${domain}.${group}.parking`;
}
export function parkingQueue(domain: string, group: string): string {
  return `uhura.${domain}.${group}.parking.q`;
}
export function rpcQueueName(domain: string): string {
  return `uhura.${domain}.rpc`;
}

/** Valida o nome do grupo de consumo e o devolve; lança se inválido. */
export function validateGroup(group: string): string {
  if (!GROUP_PATTERN.test(group)) {
    throw new Error(
      `grupo de consumo inválido '${group}': use ^[a-z0-9][a-z0-9-]{1,62}$`,
    );
  }
  if (RESERVED_GROUPS.has(group)) {
    throw new Error(`grupo de consumo '${group}' é reservado (colide com a topologia)`);
  }
  return group;
}

/**
 * Resolve o grupo de consumo: opção explícita → `UHURA_GROUP` → `SERVICE_NAME`
 * (o chart `dextro-service` já injeta o nome do release nele). Mesma ordem do
 * `ConsumerGroup::resolve` do Rust.
 *
 * Sem nenhum dos três, lança: adivinhar (hostname, pod) faria cada réplica
 * virar um grupo, e cada evento seria processado uma vez por réplica.
 */
export function resolveGroup(
  explicit: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): string {
  // Env vazia no k8s chega como string vazia, não como ausente.
  const pick = (v: string | undefined): string | undefined =>
    v !== undefined && v.trim() !== '' ? v.trim() : undefined;
  const group = pick(explicit) ?? pick(env.UHURA_GROUP) ?? pick(env.SERVICE_NAME);
  if (group === undefined) {
    throw new Error(
      'grupo de consumo ausente: defina `group` no UhuraModule.forRoot, UHURA_GROUP ou ' +
        'SERVICE_NAME com o nome do serviço',
    );
  }
  return validateGroup(group);
}

/** Declara (idempotente) a exchange do domínio. Lado de quem publica. */
export async function ensureExchange(channel: Channel, domain: string): Promise<void> {
  await channel.assertExchange(exchangeName(domain), 'topic', { durable: true });
}

/**
 * Declara (idempotente) exchange + quorum queue do grupo + DLX/parking do
 * grupo. Lado de quem consome.
 */
export async function ensureGroupTopology(
  channel: Channel,
  domain: string,
  group: string,
): Promise<void> {
  const exchange = exchangeName(domain);
  const parkingEx = parkingExchange(domain, group);
  const parkingQ = parkingQueue(domain, group);
  const mainQ = queueName(domain, group);

  await ensureExchange(channel, domain);

  await channel.assertExchange(parkingEx, 'fanout', { durable: true });
  await channel.assertQueue(parkingQ, {
    durable: true,
    arguments: { 'x-queue-type': 'quorum' },
  });
  await channel.bindQueue(parkingQ, parkingEx, '');

  await channel.assertQueue(mainQ, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum',
      'x-dead-letter-exchange': parkingEx,
      'x-delivery-limit': DELIVERY_LIMIT,
    },
  });
  await channel.bindQueue(mainQ, exchange, '#');
}
