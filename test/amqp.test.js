//! Testes do supervisor de conexao, contra o `dist` — o mesmo codigo que vai
//! para os servicos.
//
// Sem framework: o runner de teste ja vem no Node 20. Acrescentar jest a um SDK
// que quatro servicos instalam custaria arvore de dependencia em todos eles, e
// o que precisa ser testado aqui cabe no que o runtime ja oferece.
//
// O `amqplib` e injetado no cache do require ANTES de carregar o modulo sob
// teste: e a unica forma de simular uma queda de conexao sem um broker.

const { test, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert');
const { EventEmitter } = require('node:events');
const path = require('node:path');

const CAMINHO_AMQPLIB = require.resolve('amqplib');
const CAMINHO_MODULO = path.join(__dirname, '..', 'dist', 'amqp.js');

/** Uma conexao falsa: emite `close` quando a gente mandar. */
class ConexaoFalsa extends EventEmitter {
  async createChannel() {
    return {
      prefetch: async () => {},
      consume: async () => {},
      close: async () => {},
    };
  }
  async close() {
    this.emit('close');
  }
  derrubar() {
    this.emit('close');
  }
}

let conexoes;
let falharProximas;

const instalarAmqplibFalso = () => {
  conexoes = [];
  falharProximas = 0;
  require.cache[CAMINHO_AMQPLIB] = {
    id: CAMINHO_AMQPLIB,
    filename: CAMINHO_AMQPLIB,
    loaded: true,
    exports: {
      connect: async () => {
        if (falharProximas > 0) {
          falharProximas -= 1;
          throw new Error('ECONNREFUSED');
        }
        const c = new ConexaoFalsa();
        conexoes.push(c);
        return c;
      },
    },
  };
  delete require.cache[CAMINHO_MODULO];
};

const novoSupervisor = () => {
  const { UhuraAmqp } = require(CAMINHO_MODULO);
  return new UhuraAmqp({ amqpUrl: 'amqp://teste', postgresUrl: 'postgres://x' });
};

/** Deixa o event loop girar o suficiente para o backoff disparar. */
const esperar = (ms) => new Promise((r) => setTimeout(r, ms));

beforeEach(instalarAmqplibFalso);
afterEach(() => {
  delete require.cache[CAMINHO_AMQPLIB];
});

test('conecta no init e se declara conectado', async () => {
  const amqp = novoSupervisor();
  await amqp.onModuleInit();

  assert.equal(amqp.isConnected(), true);
  assert.equal(conexoes.length, 1);

  await amqp.onApplicationShutdown();
});

test('queda de conexao reconecta E reassina', async () => {
  // O ponto do teste nao e reconectar: e REASSINAR. Uma conexao viva com a
  // fila sem consumidor e pior que ficar caido, porque parece resolvido.
  const amqp = novoSupervisor();
  await amqp.onModuleInit();

  let reassinou = 0;
  amqp.onReconnect(async () => {
    reassinou += 1;
  });

  conexoes[0].derrubar();
  assert.equal(amqp.isConnected(), false, 'deve se declarar caido na hora');

  await esperar(1_400);

  assert.equal(amqp.isConnected(), true, 'deve ter reconectado');
  assert.equal(conexoes.length, 2, 'deve ser uma conexao NOVA');
  assert.equal(reassinou, 1, 'deve ter reassinado exatamente uma vez');

  await amqp.onApplicationShutdown();
});

test('nao reassina na primeira conexao', async () => {
  // Quem registra ja fez a propria inscricao no bootstrap; chamar aqui
  // duplicaria o consumidor e cada mensagem seria processada duas vezes.
  const amqp = novoSupervisor();
  let chamadas = 0;
  amqp.onReconnect(async () => {
    chamadas += 1;
  });

  await amqp.onModuleInit();
  await esperar(50);

  assert.equal(chamadas, 0);
  await amqp.onApplicationShutdown();
});

test('tentativa que falha nao desiste: tenta de novo', async () => {
  const amqp = novoSupervisor();
  await amqp.onModuleInit();

  falharProximas = 1;
  conexoes[0].derrubar();

  await esperar(3_600);

  assert.equal(amqp.isConnected(), true, 'a segunda tentativa deve ter pegado');
  await amqp.onApplicationShutdown();
});

test('shutdown nao dispara reconexao', async () => {
  // O proprio `close()` emite `close`. Sem a guarda, o processo ficaria
  // tentando reconectar enquanto termina, e o pod demoraria a morrer.
  const amqp = novoSupervisor();
  await amqp.onModuleInit();

  await amqp.onApplicationShutdown();
  await esperar(1_400);

  assert.equal(conexoes.length, 1, 'nao deve ter aberto conexao nova');
  assert.equal(amqp.isConnected(), false);
});

test('createChannel recusa enquanto desconectado', async () => {
  // Devolver um canal de uma conexao morta daria erro obscuro la na frente;
  // recusar aqui diz a causa.
  const amqp = novoSupervisor();
  await amqp.onModuleInit();
  conexoes[0].derrubar();

  await assert.rejects(() => amqp.createChannel(), /indispon/);

  await amqp.onApplicationShutdown();
});
