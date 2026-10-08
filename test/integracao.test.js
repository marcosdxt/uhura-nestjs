//! Integração com uma station de verdade (uhura-engine 0.2, autoridade do
//! controle) e um RabbitMQ de verdade. Pulado sem as variáveis:
//
//   UHURA_IT_AMQP_URL=amqp://guest:guest@127.0.0.1:5672
//   UHURA_IT_STATION_URL=http://127.0.0.1:18080
//   UHURA_IT_ADMIN_TOKEN=...
//   UHURA_IT_PG_URL=postgres://...   (o banco da station, com o schema do uhura)
//
// Prova o protocolo entre os dois lados: pausar pelo painel para o consumo do
// SDK, retomar volta, e uma réplica que SOBE com o grupo pausado não consome.

const { test } = require('node:test');
const assert = require('node:assert');

require('reflect-metadata');
const uhura = require('../dist');
const { UhuraAmqp } = require('../dist/amqp');
const { UhuraConsumer } = require('../dist/consumer');

const AMQP = process.env.UHURA_IT_AMQP_URL;
const STATION = process.env.UHURA_IT_STATION_URL;
const TOKEN = process.env.UHURA_IT_ADMIN_TOKEN;
const PG = process.env.UHURA_IT_PG_URL;
const pular = !(AMQP && STATION && TOKEN && PG);
const { Pool } = require('pg');
const { insertOutbox } = require('../dist/storage');
let pool;

const GRUPO = 'svc-integracao';
const DOMINIO = 'it.evento';
const esperar = (ms) => new Promise((r) => setTimeout(r, ms));

async function station(method, path, body) {
  const r = await fetch(`${STATION}${path}`, {
    method,
    headers: { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  return { status: r.status, json: await r.json().catch(() => null) };
}

async function ate(oque, fn) {
  for (let i = 0; i < 50; i += 1) {
    if (await fn()) return;
    await esperar(200);
  }
  assert.fail(`não aconteceu: ${oque}`);
}

/** Um "serviço" com o SDK: conexão real, um handler em `it.evento`. */
async function subir(recebidos) {
  class Assinante {
    async onCriado(data) {
      recebidos.push(data);
    }
  }
  const proto = Assinante.prototype;
  uhura.UhuraSubscribe({ domain: DOMINIO, events: ['criado'] })(
    proto,
    'onCriado',
    Object.getOwnPropertyDescriptor(proto, 'onCriado'),
  );
  const options = { amqpUrl: AMQP, postgresUrl: '', group: GRUPO };
  const amqp = new UhuraAmqp(options);
  await amqp.onModuleInit();
  const consumer = new UhuraConsumer(
    { getProviders: () => [{ instance: new Assinante() }] },
    { getAllMethodNames: (p) => Object.getOwnPropertyNames(p).filter((n) => n !== 'constructor') },
    amqp,
    options,
    pool,
  );
  await consumer.onApplicationBootstrap();
  return {
    consumer,
    parar: async () => {
      await consumer.onModuleDestroy();
      await amqp.onApplicationShutdown();
    },
  };
}

/** Grava no outbox; quem publica é o relay da station. */
async function publicar(id) {
  const env = { id, source: 'it', specversion: '1.0', type: `${DOMINIO}.criado`, data: { id } };
  await insertOutbox(pool, DOMINIO, 'criado', null, env);
}

const linha = async () => {
  const r = await fetch(`${STATION}/api/overview`);
  const ov = await r.json();
  return (ov.domains ?? []).find((d) => d.domain === DOMINIO && d.group === GRUPO);
};

test('pausa pelo painel para o SDK, retomar volta, e o boot respeita a pausa', { skip: pular }, async () => {
  pool = new Pool({ connectionString: PG });
  const recebidos = [];
  const a = await subir(recebidos);
  try {
    await publicar('m1');
    await ate('m1 consumida', () => recebidos.length === 1);

    let r = await station('POST', '/api/consumers/pause', { domain: DOMINIO, group: GRUPO });
    assert.strictEqual(r.status, 200, JSON.stringify(r.json));
    await ate('consumidor cancelado', async () => (await linha())?.consumers === 0);
    assert.deepStrictEqual(a.consumer.pausedDomains(), [DOMINIO]);

    await publicar('m2');
    await ate('m2 parada na fila', async () => (await linha())?.main === 1);
    await esperar(500);
    assert.strictEqual(recebidos.length, 1, 'pausado não consome');
    assert.strictEqual((await linha()).paused, true);

    // Réplica nova sobe com o grupo pausado: pergunta e não assina.
    const recebidosB = [];
    const b = await subir(recebidosB);
    try {
      assert.deepStrictEqual(b.consumer.pausedDomains(), [DOMINIO]);
      await esperar(500);
      assert.strictEqual(recebidosB.length, 0);

      r = await station('POST', '/api/consumers/resume', { domain: DOMINIO, group: GRUPO });
      assert.strictEqual(r.status, 200, JSON.stringify(r.json));
      await ate('m2 consumida depois de retomar', () => recebidos.length + recebidosB.length === 2);
      await ate('as duas réplicas de volta', async () => (await linha())?.consumers === 2);
    } finally {
      await b.parar();
    }
  } finally {
    await a.parar();
    await pool.end();
  }
});
