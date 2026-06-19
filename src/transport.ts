//! Topologia RabbitMQ — DEVE espelhar exatamente o driver Rust (uhura-transport).
//
// Mesmos nomes e mesmos argumentos de fila/exchange; caso contrário um redeclare
// entre os dois SDKs gera conflito no broker.

import type { Channel } from 'amqplib';

/** Limite de entregas antes do parking (poison-message handling). */
const DELIVERY_LIMIT = 5;

export function exchangeName(domain: string): string {
  return `uhura.${domain}`;
}
export function queueName(domain: string): string {
  return `uhura.${domain}.q`;
}
export function parkingExchange(domain: string): string {
  return `uhura.${domain}.parking`;
}
export function parkingQueue(domain: string): string {
  return `uhura.${domain}.parking.q`;
}

/** Declara (idempotente) exchange + quorum queue + DLX/parking do domínio. */
export async function ensureTopology(channel: Channel, domain: string): Promise<void> {
  const exchange = exchangeName(domain);
  const parkingEx = parkingExchange(domain);
  const parkingQ = parkingQueue(domain);
  const mainQ = queueName(domain);

  await channel.assertExchange(exchange, 'topic', { durable: true });

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
