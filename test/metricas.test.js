//! 0.6: métricas de publicação, servidor RPC, reentrega e backlog do outbox.
//
// Sem broker e sem banco: canais e pool falsos.

const { test } = require('node:test');
const assert = require('node:assert');

require('reflect-metadata');
const uhura = require('../dist');
const { UhuraConsumer } = require('../dist/consumer');
const { UhuraRpcServer } = require('../dist/rpc-server');
const { UhuraService } = require('../dist/uhura.service');

const esperar = (ms) => new Promise((r) => setTimeout(r, ms));
const scanner = {
  getAllMethodNames: (p) => Object.getOwnPropertyNames(p).filter((n) => n !== 'constructor'),
};
const decorar = (Classe, metodo, decorator) => {
  const proto = Classe.prototype;
  decorator(proto, metodo, Object.getOwnPropertyDescriptor(proto, metodo));
};
const novasMetricas = (pool) => new uhura.UhuraMetrics({ metrics: { defaultMetrics: false } }, pool);

test('uhura_publish_total conta o que o publish grava no outbox', async () => {
  const metrics = novasMetricas();
  const pool = { query: async () => ({ rows: [{ id: '1' }] }) };
  const svc = new UhuraService(pool, {}, {}, {}, metrics);
  await svc.publish('dash.display', 'paired', { a: 1 }, { partition: 'x' });
  await svc.publish('dash.display', 'paired', { a: 2 }, { partition: 'y' });
  await svc.publish('dash.display', 'unpaired', { a: 3 }, { partition: 'x' });
  const texto = await metrics.metrics();
  assert.match(texto, /uhura_publish_total\{domain="dash.display",event="paired"\} 2/);
  assert.match(texto, /uhura_publish_total\{domain="dash.display",event="unpaired"\} 1/);
});

test('uhura_rpc_server_total e duração por domínio, método e resultado', async () => {
  const metrics = novasMetricas();
  class Realtime {
    async issueDisplayToken() {
      return { token: 't' };
    }
    async negado() {
      throw new uhura.RpcError('FORBIDDEN', 'não');
    }
    async boom() {
      throw new Error('caiu');
    }
  }
  for (const m of ['issueDisplayToken', 'negado', 'boom']) {
    decorar(Realtime, m, uhura.UhuraFunction({ domain: 'notification.realtime', method: m }));
  }
  let cb;
  const canal = {
    prefetch: async () => {},
    assertQueue: async () => ({}),
    consume: async (_f, fn) => {
      cb = fn;
      return { consumerTag: 't' };
    },
    sendToQueue: () => {},
    ack: () => {},
    close: async () => {},
  };
  const amqp = { onReconnect: () => {}, createChannel: async () => canal };
  const server = new UhuraRpcServer({ getProviders: () => [{ instance: new Realtime() }] }, scanner, amqp, {}, metrics);
  await server.onApplicationBootstrap();
  const pedir = async (method) => {
    cb({
      content: Buffer.from(JSON.stringify({ id: method, domain: 'notification.realtime', method, data: {} })),
      fields: {},
      properties: { replyTo: 'r', correlationId: method },
    });
    await esperar(5);
  };
  await pedir('issueDisplayToken');
  await pedir('issueDisplayToken');
  await pedir('negado');
  await pedir('boom');
  const texto = await metrics.metrics();
  const l = (m, r) => `uhura_rpc_server_total\\{domain="notification.realtime",method="${m}",result="${r}"\\}`;
  assert.match(texto, new RegExp(`${l('issueDisplayToken', 'ok')} 2`));
  assert.match(texto, new RegExp(`${l('negado', 'error')} 1`));
  assert.match(texto, new RegExp(`${l('boom', 'exception')} 1`));
  assert.match(texto, /uhura_rpc_server_duration_seconds_count\{domain="notification.realtime",method="issueDisplayToken"\} 2/);
});

test('uhura_consumer_redelivered_total conta só a reentrega', async () => {
  const metrics = novasMetricas();
  let cb;
  const canal = {
    prefetch: async () => {},
    assertExchange: async () => {},
    assertQueue: async (n) => ({ queue: n }),
    bindQueue: async () => {},
    consume: async (_f, fn) => {
      cb = fn;
      return { consumerTag: 't' };
    },
    ack: () => {},
    nack: () => {},
    close: async () => {},
  };
  class Assinante {
    async on() {}
  }
  decorar(Assinante, 'on', uhura.UhuraSubscribe({ domain: 'user-account.client', events: ['upserted'] }));
  const pool = { query: async () => ({ rowCount: 0, rows: [] }) };
  const amqp = { onReconnect: () => {}, createChannel: async () => canal };
  const c = new UhuraConsumer(
    { getProviders: () => [{ instance: new Assinante() }] },
    scanner,
    amqp,
    { amqpUrl: '', postgresUrl: '', group: 'dextrolabs-dashplatform', control: false },
    pool,
    metrics,
  );
  await c.onApplicationBootstrap();
  const env = (id) => ({ id, type: 'user-account.client.upserted', source: 's', specversion: '1.0', data: {} });
  cb({ content: Buffer.from(JSON.stringify(env('a'))), fields: { redelivered: false }, properties: {} });
  cb({ content: Buffer.from(JSON.stringify(env('b'))), fields: { redelivered: true }, properties: {} });
  await esperar(10);
  const texto = await metrics.metrics();
  assert.match(texto, /uhura_consumer_redelivered_total\{domain="user-account.client",group="dextrolabs-dashplatform"\} 1/);
});

test('backlog do outbox lido no scrape; sem pool fica zero; falha não derruba o scrape', async () => {
  const consultas = [];
  const pool = {
    query: async (sql) => {
      consultas.push(sql);
      return { rows: [{ pending: 7, age: 42.5 }] };
    },
  };
  const metrics = novasMetricas(pool);
  const texto = await metrics.metrics();
  assert.match(texto, /uhura_outbox_pending 7/);
  assert.match(texto, /uhura_outbox_oldest_pending_age_seconds 42.5/);
  // Os dois gauges dividem UMA consulta por scrape.
  assert.strictEqual(consultas.length, 1);
  assert.match(consultas[0], /published_at IS NULL/);

  const semPool = await novasMetricas().metrics();
  assert.match(semPool, /uhura_outbox_pending 0/);

  const quebrado = novasMetricas({ query: async () => { throw new Error('sem tabela'); } });
  const texto2 = await quebrado.metrics();
  assert.match(texto2, /uhura_outbox_pending 0/);
});
