//! 0.4: RpcError, contexto com id para idempotência e atraso publicação → consumo.
//
// Sem broker: canais falsos guardam os callbacks de consumo e as respostas.

const { test } = require('node:test');
const { poolFalso } = require('./pg-falso');
const assert = require('node:assert');

require('reflect-metadata');
const uhura = require('../dist');
const { UhuraConsumer } = require('../dist/consumer');
const { UhuraRpcServer } = require('../dist/rpc-server');
const { UhuraRpcClient } = require('../dist/rpc-client');

const GRUPO = 'dextrolabs-audit';
const esperar = (ms) => new Promise((r) => setTimeout(r, ms));
const scanner = {
  getAllMethodNames: (p) => Object.getOwnPropertyNames(p).filter((n) => n !== 'constructor'),
};

const decorar = (Classe, metodo, decorator) => {
  const proto = Classe.prototype;
  decorator(proto, metodo, Object.getOwnPropertyDescriptor(proto, metodo));
};

/** Servidor RPC de `user.rpc` com canal falso; `pedir` devolve a resposta publicada. */
const servidor = async (Classe) => {
  const respostas = [];
  let cb;
  const canal = {
    prefetch: async () => {},
    assertQueue: async () => ({}),
    consume: async (_fila, fn) => {
      cb = fn;
      return { consumerTag: 't' };
    },
    sendToQueue: (fila, conteudo, props) => respostas.push({ fila, props, body: JSON.parse(conteudo.toString()) }),
    ack: () => {},
    close: async () => {},
  };
  const amqp = { onReconnect: () => {}, createChannel: async () => canal };
  const server = new UhuraRpcServer(
    { getProviders: () => [{ instance: new Classe() }] },
    scanner,
    amqp,
    {},
  );
  await server.onApplicationBootstrap();
  const pedir = async (method, data, { id = 'req-1', redelivered = false } = {}) => {
    cb({
      content: Buffer.from(JSON.stringify({ id, domain: 'user.rpc', method, data })),
      fields: { redelivered },
      properties: { replyTo: 'amq.rabbitmq.reply-to', correlationId: id },
    });
    await esperar(5);
    return respostas.pop().body;
  };
  return pedir;
};

test('RpcError vira resCode error com errorCode e errorMessage separados', async () => {
  class Usuarios {
    async getUser() {
      throw new uhura.RpcError('USER_NOT_FOUND', 'Usuário não encontrado.', { detail: 'X' });
    }
    async boom() {
      throw new Error('conexao caiu');
    }
  }
  decorar(Usuarios, 'getUser', uhura.UhuraFunction({ domain: 'user.rpc', method: 'getUser' }));
  decorar(Usuarios, 'boom', uhura.UhuraFunction({ domain: 'user.rpc', method: 'boom' }));
  const pedir = await servidor(Usuarios);

  assert.deepStrictEqual(await pedir('getUser', {}), {
    data: null,
    resCode: 'error',
    errorCode: 'USER_NOT_FOUND',
    errorMessage: 'Usuário não encontrado.',
    errorStack: { detail: 'X', code: 'USER_NOT_FOUND' },
  });

  const exc = await pedir('boom', {});
  assert.strictEqual(exc.resCode, 'exception');
  assert.strictEqual(exc.errorCode, undefined);
  assert.strictEqual(exc.errorMessage, 'conexao caiu');

  const desconhecido = await pedir('nada', {});
  assert.strictEqual(desconhecido.resCode, 'error');
  assert.strictEqual(desconhecido.errorCode, 'UNKNOWN_METHOD');
});

test('RpcError de outra copia do pacote tambem e reconhecido (marca, nao instanceof)', () => {
  const fake = new Error('x');
  fake[Symbol.for('uhura.RpcError')] = true;
  assert.ok(uhura.RpcError.is(fake));
  assert.ok(uhura.RpcError.is(new uhura.RpcError('A', 'b')));
  assert.ok(!uhura.RpcError.is(new Error('x')));
  assert.ok(!uhura.RpcError.is(null));
});

test('handler RPC recebe o id da requisicao no contexto', async () => {
  const vistos = [];
  class Usuarios {
    async resetUser(input, ctx) {
      vistos.push(ctx);
      return { ok: input.n };
    }
  }
  decorar(Usuarios, 'resetUser', uhura.UhuraFunction({ domain: 'user.rpc', method: 'resetUser' }));
  const pedir = await servidor(Usuarios);
  const res = await pedir('resetUser', { n: 1 }, { id: 'abc', redelivered: true });
  assert.deepStrictEqual(res, { data: { ok: 1 }, resCode: 'ok' });
  assert.deepStrictEqual(vistos, [
    { id: 'abc', domain: 'user.rpc', method: 'resetUser', correlationId: 'abc', redelivered: true },
  ]);
});

test('cliente: errorCode do campo, do errorStack (Rust) e do prefixo antigo', async () => {
  let resposta;
  let cbResp;
  const canal = {
    assertQueue: async () => ({}),
    consume: async (_f, fn) => {
      cbResp = fn;
      return { consumerTag: 'r' };
    },
    sendToQueue: (_fila, _c, props) => {
      if (resposta === undefined) return true;
      setImmediate(() =>
        cbResp({ content: Buffer.from(JSON.stringify(resposta)), properties: { correlationId: props.correlationId } }),
      );
      return true;
    },
    close: async () => {},
  };
  const amqp = { onReconnect: () => {}, createChannel: async () => canal };
  const metrics = new uhura.UhuraMetrics({ metrics: { defaultMetrics: false } });
  const client = new UhuraRpcClient(amqp, {}, metrics);
  await client.onApplicationBootstrap();

  resposta = { data: null, resCode: 'error', errorCode: 'FORBIDDEN', errorMessage: 'Não.' };
  let r = await client.call('user.rpc', 'getUser', {});
  assert.strictEqual(r.errorCode, 'FORBIDDEN');
  assert.strictEqual(r.errorMessage, 'Não.');

  resposta = { data: null, resCode: 'error', errorMessage: 'NOT_FOUND: sem device', errorStack: { code: 'NOT_FOUND' } };
  r = await client.call('iot-devices.rpc', 'get', {});
  assert.strictEqual(r.errorCode, 'NOT_FOUND');

  // Servidor NestJS ate a 0.3: excecao comum com "CODE: mensagem".
  resposta = { data: null, resCode: 'exception', errorMessage: 'USER_NOT_FOUND: Usuário não encontrado.' };
  r = await client.call('user.rpc', 'getUser', {});
  assert.strictEqual(r.errorCode, 'USER_NOT_FOUND');
  assert.strictEqual(r.errorMessage, 'USER_NOT_FOUND: Usuário não encontrado.', 'mensagem intacta');

  resposta = { data: null, resCode: 'exception', errorMessage: 'Cannot read properties of undefined' };
  r = await client.call('user.rpc', 'getUser', {});
  assert.strictEqual(r.errorCode, undefined);

  resposta = { data: 1, resCode: 'ok', errorMessage: 'X: y' };
  r = await client.call('user.rpc', 'getUser', {});
  assert.strictEqual(r.errorCode, undefined);

  resposta = undefined;
  r = await client.call('user.rpc', 'getUser', {}, { timeoutMs: 10 });
  assert.strictEqual(r.errorCode, 'TIMEOUT');
  const texto = await metrics.metrics();
  assert.match(texto, /uhura_rpc_client_total\{domain="user.rpc",method="getUser",result="timeout"\} 1/);
  assert.match(texto, /uhura_rpc_client_total\{domain="user.rpc",method="getUser",result="error"\} 1/);
});

test('parseErrorCode direto', () => {
  assert.strictEqual(uhura.parseErrorCode({ resCode: 'error', errorMessage: 'RATE_LIMITED: Muitos.' }), 'RATE_LIMITED');
  assert.strictEqual(uhura.parseErrorCode({ resCode: 'error', errorMessage: 'timeout após 10ms' }), undefined);
  assert.strictEqual(uhura.parseErrorCode({ resCode: 'exception', errorStack: 'Error: x\n  at' }), undefined);
});

/** Consumidor de `audit.recorded` com canal falso e inbox vazio. */
const consumidor = async (handler, metrics) => {
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
    async onRecorded(data, ctx) {
      return handler(data, ctx);
    }
  }
  decorar(Assinante, 'onRecorded', uhura.UhuraSubscribe({ domain: 'audit', events: ['recorded'] }));
  const pool = poolFalso();
  const amqp = { onReconnect: () => {}, createChannel: async () => canal };
  const c = new UhuraConsumer(
    { getProviders: () => [{ instance: new Assinante() }] },
    scanner,
    amqp,
    { amqpUrl: '', postgresUrl: '', group: GRUPO, control: false },
    pool,
    metrics,
  );
  await c.onApplicationBootstrap();
  return async (envelope, redelivered = false) => {
    cb({ content: Buffer.from(JSON.stringify(envelope)), fields: { redelivered }, properties: {} });
    await esperar(10);
  };
};

test('handler de evento recebe o envelope como contexto, com id, grupo e reentrega', async () => {
  const vistos = [];
  const entregar = await consumidor((data, ctx) => vistos.push({ data, ctx }));
  const env = { id: 'ev-1', type: 'audit.recorded', source: 's', specversion: '1.0', data: { a: 1 } };
  await entregar(env, true);
  assert.strictEqual(vistos.length, 1);
  const { data, ctx } = vistos[0];
  assert.deepStrictEqual(data, { a: 1 });
  // Compativel com a 0.3: os campos do envelope continuam no segundo argumento.
  assert.strictEqual(ctx.id, 'ev-1');
  assert.strictEqual(ctx.type, 'audit.recorded');
  assert.strictEqual(ctx.domain, 'audit');
  assert.strictEqual(ctx.event, 'recorded');
  assert.strictEqual(ctx.group, GRUPO);
  assert.strictEqual(ctx.redelivered, true);
  assert.deepStrictEqual(ctx.envelope, env);
});

test('uhura_consumer_lag_seconds: agora menos o time do envelope', async () => {
  const metrics = new uhura.UhuraMetrics({ metrics: { defaultMetrics: false } });
  const entregar = await consumidor(() => undefined, metrics);
  const base = { type: 'audit.recorded', source: 's', specversion: '1.0', data: {} };
  await entregar({ ...base, id: 'a', time: new Date(Date.now() - 2000).toISOString() });
  // Relogio do publicador adiantado: conta 0, nao negativo.
  await entregar({ ...base, id: 'b', time: new Date(Date.now() + 60000).toISOString() });
  // Sem time, ou ilegivel: fora da serie.
  await entregar({ ...base, id: 'c' });
  await entregar({ ...base, id: 'd', time: 'ontem' });

  const texto = await metrics.metrics();
  const labels = `domain="audit",group="${GRUPO}"`;
  assert.match(texto, new RegExp(`uhura_consumer_lag_seconds_count\\{${labels}\\} 2`));
  const soma = Number(texto.match(new RegExp(`uhura_consumer_lag_seconds_sum\\{${labels}\\} ([0-9.]+)`))[1]);
  assert.ok(soma >= 2 && soma < 3, `soma ${soma}`);
  assert.match(texto, new RegExp(`uhura_consumer_lag_seconds_bucket\\{le="1",${labels}\\} 1`));
  assert.match(texto, new RegExp(`uhura_consumer_lag_seconds_bucket\\{le="2.5",${labels}\\} 2`));
});
