//! Controle de pausa e métricas do SDK 0.3, contra o `dist`.
//
// Sem broker: o canal falso guarda os callbacks de consumo e faz o papel da
// station autoridade quando alguém manda o `getPaused` para `uhura.control.rpc`.

const { test } = require('node:test');
const { poolFalso } = require('./pg-falso');
const assert = require('node:assert');

require('reflect-metadata');
const uhura = require('../dist');
const { UhuraConsumer } = require('../dist/consumer');
const { UhuraRpcClient } = require('../dist/rpc-client');
const { pausesFor } = require('../dist/control');

const GRUPO = 'dextrolabs-audit';
const esperar = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Canal falso. `autoridade(pedido)` devolve o `RpcResult` do getPaused, ou
 * `undefined` para ficar calado (autoridade fora).
 */
const canalFalso = (autoridade) => {
  const chamadas = [];
  const consumidores = new Map(); // fila -> callback
  let tag = 0;
  const canal = {
    chamadas,
    consumidores,
    prefetch: async (...a) => chamadas.push(['prefetch', ...a]),
    assertExchange: async (...a) => chamadas.push(['assertExchange', ...a]),
    assertQueue: async (nome, ...a) => {
      chamadas.push(['assertQueue', nome, ...a]);
      return { queue: nome || 'amq.gen-controle' };
    },
    bindQueue: async (...a) => chamadas.push(['bindQueue', ...a]),
    consume: async (fila, cb, opts) => {
      chamadas.push(['consume', fila, opts]);
      consumidores.set(fila, cb);
      tag += 1;
      return { consumerTag: `tag-${tag}` };
    },
    cancel: async (t) => {
      chamadas.push(['cancel', t]);
      for (const [fila, _] of consumidores) {
        if (fila.endsWith('.q') && t) consumidores.delete(fila);
      }
    },
    sendToQueue: (fila, conteudo, props) => {
      chamadas.push(['sendToQueue', fila, props]);
      if (fila !== 'uhura.control.rpc') return true;
      const pedido = JSON.parse(conteudo.toString());
      const resposta = autoridade?.(pedido);
      if (resposta !== undefined) {
        const cb = consumidores.get('amq.rabbitmq.reply-to');
        setImmediate(() =>
          cb({
            content: Buffer.from(JSON.stringify(resposta)),
            properties: { correlationId: props.correlationId },
          }),
        );
      }
      return true;
    },
    ack: (...a) => chamadas.push(['ack', ...a]),
    nack: (...a) => chamadas.push(['nack', ...a]),
    close: async () => {},
  };
  return canal;
};

/** Consumidor que assina `audit` e `billing`. */
const montar = (options, canal, metrics, pool = {}) => {
  class Assinante {
    async onRecorded() {}
    async onBilled() {}
  }
  const proto = Assinante.prototype;
  uhura.UhuraSubscribe({ domain: 'audit', events: ['recorded'] })(
    proto,
    'onRecorded',
    Object.getOwnPropertyDescriptor(proto, 'onRecorded'),
  );
  uhura.UhuraSubscribe({ domain: 'billing', events: ['billed'] })(
    proto,
    'onBilled',
    Object.getOwnPropertyDescriptor(proto, 'onBilled'),
  );
  const discovery = { getProviders: () => [{ instance: new Assinante() }] };
  const scanner = {
    getAllMethodNames: (p) => Object.getOwnPropertyNames(p).filter((n) => n !== 'constructor'),
  };
  const amqp = { onReconnect: () => {}, createChannel: async () => canal };
  return new UhuraConsumer(discovery, scanner, amqp, options, pool, metrics);
};

const consumiu = (canal, fila) =>
  canal.chamadas.filter(([n, q]) => n === 'consume' && q === fila).length;

const controle = (canal, msg) =>
  canal.consumidores.get('amq.gen-controle')({ content: Buffer.from(JSON.stringify(msg)) });

test('pausesFor: comando do grupo, de outro grupo e retrato', () => {
  const doms = ['audit', 'billing'];
  assert.deepStrictEqual(
    [...pausesFor({ kind: 'consumer-pause', domain: 'audit', group: GRUPO, paused: true }, GRUPO, doms)],
    [['audit', true]],
  );
  assert.strictEqual(
    pausesFor({ kind: 'consumer-pause', domain: 'audit', group: 'outro', paused: true }, GRUPO, doms),
    null,
  );
  assert.strictEqual(
    pausesFor({ kind: 'consumer-pause', domain: 'x', group: GRUPO, paused: true }, GRUPO, doms),
    null,
  );
  const snap = pausesFor(
    { kind: 'consumer-snapshot', paused: [{ domain: 'billing', group: GRUPO }, { domain: 'audit', group: 'outro' }] },
    GRUPO,
    doms,
  );
  assert.deepStrictEqual([...snap], [['audit', false], ['billing', true]]);
});

test('no boot pergunta o estado e nao assina o dominio pausado', async () => {
  const pedidos = [];
  const canal = canalFalso((p) => {
    pedidos.push(p);
    return { resCode: 'ok', data: { group: GRUPO, paused: [{ domain: 'audit', group: GRUPO }] } };
  });
  const metrics = new uhura.UhuraMetrics({ metrics: { defaultMetrics: false } });
  const c = montar({ amqpUrl: '', postgresUrl: '', group: GRUPO }, canal, metrics);
  await c.onApplicationBootstrap();

  assert.deepStrictEqual(pedidos.map((p) => [p.domain, p.method, p.data]), [
    ['control', 'getPaused', { group: GRUPO }],
  ]);
  // A fila de controle e exclusiva, auto-delete, ligada com `#`.
  assert.ok(
    canal.chamadas.some(([n, q, o]) => n === 'assertQueue' && q === '' && o.exclusive && o.autoDelete),
  );
  assert.ok(
    canal.chamadas.some(([n, q, ex, rk]) => n === 'bindQueue' && q === 'amq.gen-controle' && ex === 'uhura.control' && rk === '#'),
  );
  // Topologia do pausado garantida, sem consumidor; o outro consome.
  assert.ok(canal.chamadas.some(([n, q]) => n === 'assertQueue' && q === `uhura.audit.${GRUPO}.q`));
  assert.strictEqual(consumiu(canal, `uhura.audit.${GRUPO}.q`), 0);
  assert.strictEqual(consumiu(canal, `uhura.billing.${GRUPO}.q`), 1);
  assert.deepStrictEqual(c.pausedDomains(), ['audit']);
  assert.match(await metrics.metrics(), /uhura_consumer_paused\{domain="audit",group="dextrolabs-audit"\} 1/);

  // Retomar pelo controle: passa a consumir.
  controle(canal, { kind: 'consumer-pause', domain: 'audit', group: GRUPO, paused: false, at: 'x' });
  await esperar(10);
  assert.strictEqual(consumiu(canal, `uhura.audit.${GRUPO}.q`), 1);
  assert.deepStrictEqual(c.pausedDomains(), []);

  // Pausar de novo: basic.cancel com a tag daquele dominio, e so dele.
  controle(canal, { kind: 'consumer-pause', domain: 'billing', group: GRUPO, paused: true, at: 'x' });
  await esperar(10);
  assert.deepStrictEqual(
    canal.chamadas.filter(([n]) => n === 'cancel').map(([, t]) => t),
    ['tag-3'].slice(0, 1).map(() => canal.chamadas.find(([n, q]) => n === 'consume' && q === `uhura.billing.${GRUPO}.q`) && 'tag-3'),
  );
  assert.deepStrictEqual(c.pausedDomains(), ['billing']);

  // Comando de outro grupo nao mexe em nada.
  controle(canal, { kind: 'consumer-pause', domain: 'audit', group: 'outro', paused: true, at: 'x' });
  await esperar(10);
  assert.deepStrictEqual(c.pausedDomains(), ['billing']);

  // Retrato vazio: tudo volta.
  controle(canal, { kind: 'consumer-snapshot', paused: [], at: 'x' });
  await esperar(10);
  assert.deepStrictEqual(c.pausedDomains(), []);
  assert.strictEqual(consumiu(canal, `uhura.billing.${GRUPO}.q`), 2);
});

test('autoridade calada: consome tudo, sem travar o boot', async () => {
  const canal = canalFalso(() => undefined);
  const c = montar({ amqpUrl: '', postgresUrl: '', group: GRUPO, controlTimeoutMs: 30 }, canal);
  const inicio = Date.now();
  await c.onApplicationBootstrap();
  assert.ok(Date.now() - inicio < 1_000);
  assert.strictEqual(consumiu(canal, `uhura.audit.${GRUPO}.q`), 1);
  assert.strictEqual(consumiu(canal, `uhura.billing.${GRUPO}.q`), 1);
  // O pedido expira junto com a espera: nao fica parado na fila do RPC.
  const envio = canal.chamadas.find(([n, q]) => n === 'sendToQueue' && q === 'uhura.control.rpc');
  assert.strictEqual(envio[2].expiration, '30');
});

test('control: false nao liga o controle (comportamento da 0.2)', async () => {
  const canal = canalFalso(() => {
    throw new Error('nao deveria perguntar');
  });
  const c = montar({ amqpUrl: '', postgresUrl: '', group: GRUPO, control: false }, canal);
  await c.onApplicationBootstrap();
  assert.ok(!canal.chamadas.some(([n, ex]) => n === 'assertExchange' && ex === 'uhura.control'));
  assert.strictEqual(consumiu(canal, `uhura.audit.${GRUPO}.q`), 1);
});

test('metricas do consumidor: ok, ignorado, duplicado e erro', async () => {
  const canal = canalFalso(() => undefined);
  const metrics = new uhura.UhuraMetrics({ metrics: { defaultMetrics: false } });
  const pool = poolFalso();
  const c = montar({ amqpUrl: '', postgresUrl: '', group: GRUPO, control: false }, canal, metrics, pool);
  await c.onApplicationBootstrap();
  const entrega = (type, id) => ({
    content: Buffer.from(JSON.stringify({ id, type, source: 's', specversion: '1.0', data: {} })),
    properties: {},
  });
  const cb = canal.consumidores.get(`uhura.audit.${GRUPO}.q`);
  cb(entrega('audit.recorded', 'e1'));
  await esperar(10);
  cb(entrega('audit.recorded', 'e1'));
  await esperar(10);
  cb(entrega('audit.outro', 'e2'));
  await esperar(10);
  cb({ content: Buffer.from('nao-e-json'), properties: {} });
  await esperar(10);

  const texto = await metrics.metrics();
  for (const result of ['ok', 'duplicate', 'ignored', 'error']) {
    assert.match(
      texto,
      new RegExp(`uhura_consumer_handled_total\\{domain="audit",group="${GRUPO}",result="${result}"\\} 1`),
      result,
    );
  }
  assert.match(texto, /uhura_consumer_handler_duration_seconds_count\{domain="audit",group="dextrolabs-audit"\} 4/);
});

test('metricas do cliente RPC: ok e timeout', async () => {
  let responder = true;
  const canal = canalFalso();
  canal.sendToQueue = (fila, conteudo, props) => {
    if (!responder) return true;
    const cb = canal.consumidores.get('amq.rabbitmq.reply-to');
    setImmediate(() =>
      cb({
        content: Buffer.from(JSON.stringify({ resCode: 'ok', data: 1 })),
        properties: { correlationId: props.correlationId },
      }),
    );
    return true;
  };
  const amqp = { onReconnect: () => {}, createChannel: async () => canal };
  const metrics = new uhura.UhuraMetrics({ metrics: { defaultMetrics: false } });
  const client = new UhuraRpcClient(amqp, {}, metrics);
  await client.onApplicationBootstrap();

  assert.strictEqual((await client.call('user.rpc', 'GetUser', {})).resCode, 'ok');
  responder = false;
  const r = await client.call('user.rpc', 'GetUser', {}, { timeoutMs: 20 });
  assert.match(r.errorMessage, /timeout/);

  const texto = await metrics.metrics();
  assert.match(texto, /uhura_rpc_client_total\{domain="user.rpc",method="GetUser",result="ok"\} 1/);
  assert.match(texto, /uhura_rpc_client_total\{domain="user.rpc",method="GetUser",result="timeout"\} 1/);
  assert.match(texto, /uhura_rpc_client_duration_seconds_count\{domain="user.rpc",method="GetUser"\} 2/);
});

test('endpoint /metrics: sem versao de URI, desligavel, com as do processo', async () => {
  const { VERSION_NEUTRAL } = require('@nestjs/common');
  const base = { amqpUrl: 'amqp://x', postgresUrl: 'postgres://x' };
  const padrao = uhura.UhuraModule.forRoot(base);
  assert.strictEqual(padrao.controllers.length, 1);
  const ctrl = padrao.controllers[0];
  assert.strictEqual(Reflect.getMetadata('path', ctrl), 'metrics');
  assert.strictEqual(Reflect.getMetadata('__version__', ctrl), VERSION_NEUTRAL);
  assert.ok(padrao.exports.includes(uhura.UhuraMetrics));

  assert.strictEqual(uhura.UhuraModule.forRoot({ ...base, metrics: false }).controllers.length, 0);
  const outro = uhura.UhuraModule.forRoot({ ...base, metrics: { path: '/interno/metricas' } });
  assert.strictEqual(Reflect.getMetadata('path', outro.controllers[0]), 'interno/metricas');

  const metrics = new uhura.UhuraMetrics(base);
  const texto = await new ctrl(metrics).scrape();
  assert.match(texto, /# TYPE uhura_amqp_reconnects_total counter/);
  assert.match(texto, /process_cpu_user_seconds_total/);
});
