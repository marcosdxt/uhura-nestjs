//! Inbox transacional contra Postgres e RabbitMQ de verdade, sem station: o
//! teste publica direto no exchange do domínio. Pulado sem as variáveis:
//
//   UHURA_IT_AMQP_URL=amqp://guest:guest@127.0.0.1:5672
//   UHURA_IT_PG_URL=postgres://...   (com o schema do uhura: uhura_inbox)

const { test } = require('node:test');
const assert = require('node:assert');
const amqplib = require('amqplib');
const { Pool } = require('pg');

require('reflect-metadata');
const uhura = require('../dist');
const { UhuraAmqp } = require('../dist/amqp');
const { UhuraConsumer } = require('../dist/consumer');

const AMQP = process.env.UHURA_IT_AMQP_URL;
const PG = process.env.UHURA_IT_PG_URL;
const pular = !(AMQP && PG);

const GRUPO = 'svc-inbox-it';
const esperar = (ms) => new Promise((r) => setTimeout(r, ms));

async function ate(oque, fn) {
  for (let i = 0; i < 75; i += 1) {
    if (await fn()) return;
    await esperar(200);
  }
  assert.fail(`não aconteceu: ${oque}`);
}

async function subir(pool, dominio, handler) {
  class Assinante {
    async on(data, ctx) {
      return handler(data, ctx);
    }
  }
  const proto = Assinante.prototype;
  uhura.UhuraSubscribe({ domain: dominio, events: ['criado'] })(
    proto,
    'on',
    Object.getOwnPropertyDescriptor(proto, 'on'),
  );
  const options = { amqpUrl: AMQP, postgresUrl: PG, group: GRUPO, control: false };
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
  return async () => {
    await consumer.onModuleDestroy();
    await amqp.onApplicationShutdown();
  };
}

async function publicar(dominio, id) {
  const conn = await amqplib.connect(AMQP);
  const ch = await conn.createConfirmChannel();
  const env = { id, source: 'it', specversion: '1.0', type: `${dominio}.criado`, data: { id } };
  ch.publish(`uhura.${dominio}`, 'default', Buffer.from(JSON.stringify(env)), { persistent: true });
  await ch.waitForConfirms();
  await conn.close();
}

async function profundidade(fila) {
  const conn = await amqplib.connect(AMQP);
  const ch = await conn.createChannel();
  const { messageCount } = await ch.checkQueue(fila);
  await conn.close();
  return messageCount;
}

test('falha transitória: o que o handler gravou em ctx.tx some no rollback, e a reentrega grava uma vez', { skip: pular }, async () => {
  const pool = new Pool({ connectionString: PG });
  const dominio = `it.inbox.${Date.now()}`;
  await pool.query('CREATE TABLE IF NOT EXISTS it_negocio (envelope_id TEXT, tentativa INT)');
  let tentativas = 0;
  const parar = await subir(pool, dominio, async (_data, ctx) => {
    tentativas += 1;
    await ctx.tx.query('INSERT INTO it_negocio VALUES ($1, $2)', [ctx.id, tentativas]);
    if (tentativas === 1) throw new Error('transitória');
  });
  try {
    await publicar(dominio, `${dominio}-e1`);
    await ate('processada na segunda tentativa', () => tentativas === 2);
    await esperar(300);
    const negocio = await pool.query('SELECT tentativa FROM it_negocio WHERE envelope_id = $1', [`${dominio}-e1`]);
    assert.deepStrictEqual(negocio.rows, [{ tentativa: 2 }]);
    const inbox = await pool.query('SELECT 1 FROM uhura_inbox WHERE envelope_id = $1', [`${dominio}-e1`]);
    assert.strictEqual(inbox.rowCount, 1);

    // Mesmo envelope de novo: duplicado, o handler não roda.
    await publicar(dominio, `${dominio}-e1`);
    await esperar(800);
    assert.strictEqual(tentativas, 2);
  } finally {
    await parar();
    await pool.end();
  }
});

test('falha permanente: depois do x-delivery-limit a mensagem vai ao parking e o inbox fica limpo', { skip: pular }, async () => {
  const pool = new Pool({ connectionString: PG });
  const dominio = `it.parking.${Date.now()}`;
  let tentativas = 0;
  const parar = await subir(pool, dominio, async () => {
    tentativas += 1;
    throw new Error('venenosa');
  });
  try {
    await publicar(dominio, `${dominio}-p1`);
    await ate('estacionada', async () => (await profundidade(`uhura.${dominio}.${GRUPO}.parking.q`)) === 1);
    assert.ok(tentativas >= 5, `tentativas: ${tentativas}`);
    const inbox = await pool.query('SELECT 1 FROM uhura_inbox WHERE envelope_id = $1', [`${dominio}-p1`]);
    assert.strictEqual(inbox.rowCount, 0);
  } finally {
    await parar();
    await pool.end();
  }
});
