//! Testes do grupo de consumo, contra o `dist` — o mesmo codigo que vai para os
//! servicos.
//
// Sem framework: o runner de teste ja vem no Node 20, e acrescentar jest a um
// SDK que os servicos instalam custaria arvore de dependencia em todos eles.
// Os nomes esperados sao os mesmos do teste de integracao do Rust
// (uhura-core/crates/uhura-transport/tests): a topologia e o contrato entre os
// dois SDKs.

const { test } = require('node:test');
const assert = require('node:assert');

const uhura = require('../dist');
const { UhuraConsumer } = require('../dist/consumer');
const transport = require('../dist/transport');

test('nomes identicos ao driver Rust', () => {
  assert.strictEqual(transport.exchangeName('teste.evento'), 'uhura.teste.evento');
  assert.strictEqual(
    transport.queueName('teste.evento', 'svc-audit'),
    'uhura.teste.evento.svc-audit.q',
  );
  assert.strictEqual(
    transport.parkingExchange('teste.evento', 'svc-audit'),
    'uhura.teste.evento.svc-audit.parking',
  );
  assert.strictEqual(
    transport.parkingQueue('teste.evento', 'svc-audit'),
    'uhura.teste.evento.svc-audit.parking.q',
  );
  // RPC continua ponto a ponto, sem grupo.
  assert.strictEqual(transport.rpcQueueName('iot-devices.rpc'), 'uhura.iot-devices.rpc.rpc');
});

test('valida o formato do grupo', () => {
  for (const ok of ['dextrolabs-audit', 'ab', 'a1', '9svc', 'a'.repeat(63)]) {
    assert.strictEqual(transport.validateGroup(ok), ok);
  }
  for (const bad of ['', 'a', '-svc', 'Svc', 'svc.audit', 'svc_audit', 'svc audit', 'ação', 'a'.repeat(64)]) {
    assert.throws(() => transport.validateGroup(bad), /inválido/, JSON.stringify(bad));
  }
  for (const reservado of ['parking', 'rpc']) {
    assert.throws(() => transport.validateGroup(reservado), /reservado/);
  }
});

test('resolve: explicito -> UHURA_GROUP -> SERVICE_NAME', () => {
  const env = { UHURA_GROUP: 'do-env', SERVICE_NAME: 'do-chart' };
  assert.strictEqual(transport.resolveGroup('explicito', env), 'explicito');
  assert.strictEqual(transport.resolveGroup(undefined, env), 'do-env');
  assert.strictEqual(transport.resolveGroup('  ', { UHURA_GROUP: '', SERVICE_NAME: 'do-chart' }), 'do-chart');
  assert.throws(() => transport.resolveGroup(undefined, {}), /ausente/);
  assert.throws(() => transport.resolveGroup(undefined, { UHURA_GROUP: 'Nome.Ruim' }), /inválido/);
});

test('forRoot recusa grupo explicito invalido', () => {
  assert.throws(
    () => uhura.UhuraModule.forRoot({ amqpUrl: 'amqp://x', postgresUrl: 'postgres://x', group: 'a.b' }),
    /inválido/,
  );
});

/** Canal falso: registra o que foi declarado e consumido. */
const canalFalso = () => {
  const chamadas = [];
  const registra = (nome) => async (...args) => {
    chamadas.push([nome, ...args]);
    return {};
  };
  return {
    chamadas,
    prefetch: registra('prefetch'),
    assertExchange: registra('assertExchange'),
    assertQueue: registra('assertQueue'),
    bindQueue: registra('bindQueue'),
    consume: registra('consume'),
    close: registra('close'),
  };
};

/** Monta o consumidor com um provider que assina `audit`. */
const consumidor = (options, canal) => {
  class Assinante {
    async onRecorded() {}
  }
  const proto = Assinante.prototype;
  uhura.UhuraSubscribe({ domain: 'audit', events: ['recorded'] })(
    proto,
    'onRecorded',
    Object.getOwnPropertyDescriptor(proto, 'onRecorded'),
  );
  const discovery = { getProviders: () => [{ instance: new Assinante() }] };
  const scanner = {
    getAllMethodNames: (p) => Object.getOwnPropertyNames(p).filter((n) => n !== 'constructor'),
  };
  let canaisAbertos = 0;
  const amqp = {
    onReconnect: () => {},
    createChannel: async () => {
      canaisAbertos += 1;
      return canal;
    },
  };
  const consumer = new UhuraConsumer(discovery, scanner, amqp, options, {});
  return { consumer, canaisAbertos: () => canaisAbertos };
};

test('consumidor sem grupo falha no bootstrap, antes de abrir canal', async () => {
  const antes = { UHURA_GROUP: process.env.UHURA_GROUP, SERVICE_NAME: process.env.SERVICE_NAME };
  delete process.env.UHURA_GROUP;
  delete process.env.SERVICE_NAME;
  try {
    const { consumer, canaisAbertos } = consumidor({ amqpUrl: '', postgresUrl: '' }, canalFalso());
    await assert.rejects(consumer.onApplicationBootstrap(), /grupo de consumo ausente/);
    assert.strictEqual(canaisAbertos(), 0);
  } finally {
    for (const [k, v] of Object.entries(antes)) {
      if (v !== undefined) process.env[k] = v;
    }
  }
});

test('consumidor assina a fila do grupo, com DLX para o parking do grupo', async () => {
  const canal = canalFalso();
  const { consumer } = consumidor({ amqpUrl: '', postgresUrl: '', group: 'dextrolabs-audit' }, canal);
  await consumer.onApplicationBootstrap();

  const fila = 'uhura.audit.dextrolabs-audit.q';
  const parkingEx = 'uhura.audit.dextrolabs-audit.parking';
  const declaradas = canal.chamadas.filter(([n]) => n === 'assertQueue');
  assert.deepStrictEqual(
    declaradas.map(([, nome, opts]) => [nome, opts]),
    [
      ['uhura.audit.dextrolabs-audit.parking.q', { durable: true, arguments: { 'x-queue-type': 'quorum' } }],
      [
        fila,
        {
          durable: true,
          arguments: {
            'x-queue-type': 'quorum',
            'x-dead-letter-exchange': parkingEx,
            'x-delivery-limit': 5,
          },
        },
      ],
    ],
  );
  assert.ok(canal.chamadas.some(([n, q, ex, rk]) => n === 'bindQueue' && q === fila && ex === 'uhura.audit' && rk === '#'));
  assert.ok(canal.chamadas.some(([n, q]) => n === 'consume' && q === fila));
  // A fila unica antiga nao e mais declarada por ninguem.
  assert.ok(!canal.chamadas.some(([, nome]) => nome === 'uhura.audit.q'));
});
