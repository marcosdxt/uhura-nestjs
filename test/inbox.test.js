//! Inbox transacional: dedup, handlers e COMMIT numa transação só; ack depois.
//
// Sem broker e sem banco: canal falso e o pool de `pg-falso`.

const { test } = require('node:test');
const assert = require('node:assert');
const { poolFalso } = require('./pg-falso');

require('reflect-metadata');
const uhura = require('../dist');
const { UhuraConsumer } = require('../dist/consumer');
const { UhuraService } = require('../dist/uhura.service');

const GRUPO = 'ews003';
const esperar = (ms) => new Promise((r) => setTimeout(r, ms));
const scanner = {
  getAllMethodNames: (p) => Object.getOwnPropertyNames(p).filter((n) => n !== 'constructor'),
};

/** Consumidor de `audit.recorded`; devolve a função de entrega e o que o canal respondeu. */
const consumidor = async (handler, pool) => {
  let cb;
  const respostas = [];
  const canal = {
    prefetch: async () => {},
    assertExchange: async () => {},
    assertQueue: async (n) => ({ queue: n }),
    bindQueue: async () => {},
    consume: async (_f, fn) => {
      cb = fn;
      return { consumerTag: 't' };
    },
    ack: () => respostas.push('ack'),
    nack: (_m, _all, requeue) => respostas.push(requeue ? 'nack-requeue' : 'nack'),
    close: async () => {},
  };
  class Assinante {
    async onRecorded(data, ctx) {
      return handler(data, ctx);
    }
  }
  const proto = Assinante.prototype;
  uhura.UhuraSubscribe({ domain: 'audit', events: ['recorded'] })(
    proto,
    'onRecorded',
    Object.getOwnPropertyDescriptor(proto, 'onRecorded'),
  );
  const c = new UhuraConsumer(
    { getProviders: () => [{ instance: new Assinante() }] },
    scanner,
    { onReconnect: () => {}, createChannel: async () => canal },
    { amqpUrl: '', postgresUrl: '', group: GRUPO, control: false },
    pool,
  );
  await c.onApplicationBootstrap();
  const entregar = async (envelope) => {
    cb({ content: Buffer.from(JSON.stringify(envelope)), fields: { redelivered: false }, properties: {} });
    await esperar(10);
  };
  return { entregar, respostas };
};

const env = { id: 'ev-1', type: 'audit.recorded', source: 's', specversion: '1.0', data: {} };

test('handler que falha não marca o inbox: a reentrega processa de novo', async () => {
  const pool = poolFalso();
  let tentativas = 0;
  const { entregar, respostas } = await consumidor(() => {
    tentativas += 1;
    if (tentativas === 1) throw new Error('transitória');
  }, pool);

  await entregar(env);
  assert.deepStrictEqual(respostas, ['nack-requeue']);
  assert.ok(!pool.inbox.has('ev-1'));
  assert.ok(pool.comandos.includes('ROLLBACK'));

  await entregar(env);
  assert.strictEqual(tentativas, 2);
  assert.deepStrictEqual(respostas, ['nack-requeue', 'ack']);
  assert.ok(pool.inbox.has('ev-1'));
  assert.strictEqual(pool.liberados(), 2);
});

test('envelope já processado: ack sem chamar o handler', async () => {
  const pool = poolFalso();
  let chamadas = 0;
  const { entregar, respostas } = await consumidor(() => {
    chamadas += 1;
  }, pool);

  await entregar(env);
  await entregar(env);
  assert.strictEqual(chamadas, 1);
  assert.deepStrictEqual(respostas, ['ack', 'ack']);
  assert.strictEqual(pool.liberados(), 2);
});

test('o ack sai depois do COMMIT, e o handler recebe a transação em ctx.tx', async () => {
  const pool = poolFalso();
  let recebida;
  const { entregar, respostas } = await consumidor(async (_data, ctx) => {
    recebida = ctx.tx;
    await ctx.tx.query('UPDATE negocio SET x = 1');
  }, pool);

  await entregar(env);
  assert.ok(recebida && typeof recebida.query === 'function');
  assert.deepStrictEqual(
    pool.comandos.map((c) => c.split(' ')[0]),
    ['BEGIN', 'INSERT', 'UPDATE', 'COMMIT'],
  );
  assert.deepStrictEqual(respostas, ['ack']);
});

test('publish com tx grava o outbox na transação dada, e não no pool', async () => {
  const noPool = [];
  const naTx = [];
  const pool = { query: async (sql) => (noPool.push(sql), { rows: [{ id: '1' }] }) };
  const tx = { query: async (sql) => (naTx.push(sql), { rows: [{ id: '2' }] }) };
  const svc = new UhuraService(pool, {}, {}, {});

  assert.strictEqual(await svc.publish('notification.inapp', 'requested', {}, { tx }), '2');
  assert.strictEqual(noPool.length, 0);
  assert.match(naTx[0], /INSERT INTO uhura_outbox/);

  assert.strictEqual(await svc.publish('notification.inapp', 'requested', {}), '1');
  assert.strictEqual(noPool.length, 1);
});
