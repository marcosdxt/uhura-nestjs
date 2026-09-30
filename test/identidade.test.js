//! 0.5: identidade do chamador no RPC, garantida pelo broker (`user-id` AMQP).
//
// Os testes de unidade rodam sempre. O de integração precisa de um RabbitMQ
// com dois usuários (pulado sem as variáveis):
//
//   UHURA_IT_ID_ALICE_URL=amqp://alice:senha@127.0.0.1:5672
//   UHURA_IT_ID_BOB_URL=amqp://bob:senha@127.0.0.1:5672

const { test } = require('node:test');
const assert = require('node:assert');

require('reflect-metadata');
const amqplib = require('amqplib');
const uhura = require('../dist');
const { UhuraAmqp } = require('../dist/amqp');
const { UhuraRpcServer } = require('../dist/rpc-server');
const { UhuraRpcClient, amqpUser } = require('../dist/rpc-client');
const { rpcQueueName } = require('../dist/transport');

const ALICE = process.env.UHURA_IT_ID_ALICE_URL;
const BOB = process.env.UHURA_IT_ID_BOB_URL;
const pular = !(ALICE && BOB);
const esperar = (ms) => new Promise((r) => setTimeout(r, ms));
const scanner = {
  getAllMethodNames: (p) => Object.getOwnPropertyNames(p).filter((n) => n !== 'constructor'),
};

test('amqpUser: usuário da URL, decodificado; sem usuário, undefined', () => {
  assert.strictEqual(amqpUser('amqp://dextrolabs-terminal:s%40nha@rabbit:5672'), 'dextrolabs-terminal');
  assert.strictEqual(amqpUser('amqp://us%C3%A9r:x@h'), 'usér');
  assert.strictEqual(amqpUser('amqp://rabbit:5672'), undefined);
  assert.strictEqual(amqpUser(undefined), undefined);
  assert.strictEqual(amqpUser('não é url'), undefined);
});

test('cliente manda o usuário da conexão como user-id', async () => {
  const enviados = [];
  const canal = {
    assertQueue: async () => ({}),
    consume: async () => ({ consumerTag: 't' }),
    sendToQueue: (_fila, _conteudo, props) => enviados.push(props),
    close: async () => {},
  };
  const amqp = { onReconnect: () => {}, createChannel: async () => canal };
  const cliente = new UhuraRpcClient(amqp, { amqpUrl: 'amqp://dextrolabs-terminal:x@h:5672' });
  await cliente.onApplicationBootstrap();
  void cliente.call('user.rpc', 'getUser', {}, { timeoutMs: 20 });
  await esperar(5);
  assert.strictEqual(enviados[0].userId, 'dextrolabs-terminal');
});

test('integração: o handler recebe quem publicou, e o broker recusa user-id forjado', { skip: pular }, async () => {
  const DOMINIO = `it.identidade.${Date.now()}`;
  class Eco {
    async quem(_data, ctx) {
      return { callerUser: ctx.callerUser };
    }
  }
  const proto = Eco.prototype;
  uhura.UhuraFunction({ domain: DOMINIO, method: 'quem' })(proto, 'quem', Object.getOwnPropertyDescriptor(proto, 'quem'));

  const optsServidor = { amqpUrl: ALICE, postgresUrl: '', group: 'it-identidade' };
  const amqpServidor = new UhuraAmqp(optsServidor);
  await amqpServidor.onModuleInit();
  const server = new UhuraRpcServer({ getProviders: () => [{ instance: new Eco() }] }, scanner, amqpServidor, optsServidor);
  await server.onApplicationBootstrap();

  const optsCliente = { amqpUrl: BOB, postgresUrl: '', group: 'it-identidade-cli' };
  const amqpCliente = new UhuraAmqp(optsCliente);
  await amqpCliente.onModuleInit();
  const cliente = new UhuraRpcClient(amqpCliente, optsCliente);
  await cliente.onApplicationBootstrap();

  try {
    const r = await cliente.call(DOMINIO, 'quem', {}, { timeoutMs: 5000 });
    assert.strictEqual(r.resCode, 'ok');
    assert.deepStrictEqual(r.data, { callerUser: 'bob' });

    // Bob tentando se passar por Alice: o broker fecha o canal.
    const conn = await amqplib.connect(BOB);
    const ch = await conn.createChannel();
    const fechado = new Promise((resolve) => ch.on('error', (err) => resolve(err)));
    ch.sendToQueue(rpcQueueName(DOMINIO), Buffer.from('{}'), { userId: 'alice' });
    const err = await Promise.race([fechado, esperar(3000).then(() => null)]);
    assert.ok(err, 'o broker devia recusar user-id diferente do usuário da conexão');
    assert.match(String(err.message), /PRECONDITION_FAILED|user_id/i);
    await conn.close().catch(() => undefined);
  } finally {
    await cliente.onModuleDestroy();
    await server.onModuleDestroy?.();
    await amqpCliente.onApplicationShutdown();
    await amqpServidor.onApplicationShutdown();
  }
});
