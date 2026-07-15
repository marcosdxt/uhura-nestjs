//! Testes do fluxo transactional inbox do `UhuraConsumer.onMessage`.
//
// Sem framework de teste no pacote, usa o runner nativo do Node (`node:test`):
//   npx tsc tests/consumer.spec.ts --outDir <tmp> --module commonjs \
//     --target ES2021 --experimentalDecorators --emitDecoratorMetadata \
//     --esModuleInterop --skipLibCheck --strict --moduleResolution node --types node
//   NODE_PATH=./node_modules node --test <tmp>/tests/consumer.spec.js
// (tsconfig.build exclui `tests/`, então nada disso entra no pacote publicado.)

import 'reflect-metadata';

import * as assert from 'node:assert/strict';
import { test } from 'node:test';

import { UhuraConsumer } from '../src/consumer';
import type { Envelope } from '../src/envelope';

function makeEnvelope(): Envelope {
  return {
    id: 'env-1',
    type: 'usuario.info.started',
    data: { id: '42' },
    partitionkey: '42',
  } as unknown as Envelope;
}

interface Case {
  /** Ordem global de operações: BEGIN | inbox-insert | handler | COMMIT | ROLLBACK | ack | nack(...). */
  order: string[];
  /** Argumentos recebidos pelo handler em cada invocação. */
  handlerArgs: unknown[][];
  /** Client transacional mockado (para comparar com o 3º arg do handler). */
  client: unknown;
  released: boolean;
  /** Entrega a mensagem ao onMessage (pode ser chamado de novo p/ redelivery). */
  run: () => Promise<void>;
}

/**
 * Monta consumer + mocks de pool/client/channel. `insertResults` controla o
 * `rowCount` devolvido por cada INSERT no inbox (1 = novo, 0 = duplicado);
 * `handlerImpl` é o corpo do handler assinante.
 */
function makeCase(
  insertResults: number[],
  handlerImpl: (...args: unknown[]) => Promise<void>,
): Case {
  const order: string[] = [];
  const handlerArgs: unknown[][] = [];
  let insertCall = 0;

  const c: Case = {
    order,
    handlerArgs,
    client: undefined,
    released: false,
    run: async () => undefined,
  };

  const client = {
    query: async (sql: string): Promise<unknown> => {
      if (sql.startsWith('INSERT INTO uhura_inbox')) {
        order.push('inbox-insert');
        return { rowCount: insertResults[insertCall++] ?? 0 };
      }
      order.push(sql); // BEGIN | COMMIT | ROLLBACK
      return {};
    },
    release: (): void => {
      c.released = true;
    },
  };
  c.client = client;

  const pool = {
    connect: async () => client,
    // Se onMessage regredir para autocommit direto no Pool, o teste quebra.
    query: async () => {
      throw new Error('onMessage não deve consultar o Pool diretamente');
    },
  };

  const channel = {
    ack: (): void => {
      order.push('ack');
    },
    nack: (_msg: unknown, allUpTo: boolean, requeue: boolean): void => {
      order.push(`nack(${String(allUpTo)},${String(requeue)})`);
    },
  };

  const instance = {
    async handle(...args: unknown[]): Promise<void> {
      handlerArgs.push(args);
      order.push('handler');
      await handlerImpl(...args);
    },
  };

  const handlers = [
    {
      options: { domain: 'usuario.info', events: ['started'] },
      instance,
      methodName: 'handle',
    },
  ];

  // Dependências não usadas por onMessage viram stubs.
  const consumer = new UhuraConsumer(
    {} as never,
    {} as never,
    {} as never,
    { amqpUrl: '', postgresUrl: '', mesh: 'test', debug: false } as never,
    pool as never,
  );
  (consumer as unknown as { channel: unknown }).channel = channel;

  const msg = { content: Buffer.from(JSON.stringify(makeEnvelope())) };

  c.run = async (): Promise<void> => {
    await (
      consumer as unknown as {
        onMessage: (d: string, h: unknown[], m: unknown) => Promise<void>;
      }
    ).onMessage('usuario.info', handlers, msg);
  };

  return c;
}

test('sucesso: BEGIN → markProcessed → handler → COMMIT → ack, nessa ordem', async () => {
  const c = makeCase([1], async () => undefined);
  await c.run();

  assert.deepEqual(c.order, ['BEGIN', 'inbox-insert', 'handler', 'COMMIT', 'ack']);
  assert.equal(c.released, true);

  // Handler recebe (data, envelope, client transacional).
  assert.equal(c.handlerArgs.length, 1);
  const [data, envelope, tx] = c.handlerArgs[0] as [
    { id: string },
    Envelope,
    unknown,
  ];
  assert.deepEqual(data, { id: '42' });
  assert.equal(envelope.id, 'env-1');
  assert.equal(tx, c.client);
});

test('handler lança: ROLLBACK + nack(requeue), SEM ack nem COMMIT', async () => {
  const c = makeCase([1], async () => {
    throw new Error('boom');
  });
  await c.run();

  assert.deepEqual(c.order, [
    'BEGIN',
    'inbox-insert',
    'handler',
    'ROLLBACK',
    'nack(false,true)',
  ]);
  assert.equal(c.released, true);
  assert.ok(!c.order.includes('ack'));
  assert.ok(!c.order.includes('COMMIT'));
});

test('redelivery após rollback reexecuta o handler e faz ack', async () => {
  let attempts = 0;
  // 1ª entrega falha; o ROLLBACK desfaz a marca do inbox, então a redelivery
  // vê isNew=true de novo (insertResults = [1, 1]).
  const c = makeCase([1, 1], async () => {
    attempts += 1;
    if (attempts === 1) {
      throw new Error('falha transitória');
    }
  });

  await c.run(); // 1ª entrega → rollback + nack
  await c.run(); // redelivery → sucesso

  assert.equal(attempts, 2);
  assert.deepEqual(c.order, [
    'BEGIN',
    'inbox-insert',
    'handler',
    'ROLLBACK',
    'nack(false,true)',
    'BEGIN',
    'inbox-insert',
    'handler',
    'COMMIT',
    'ack',
  ]);
});

test('duplicata genuína (isNew=false): COMMIT + ack sem invocar handler', async () => {
  const c = makeCase([0], async () => undefined);
  await c.run();

  assert.deepEqual(c.order, ['BEGIN', 'inbox-insert', 'COMMIT', 'ack']);
  assert.equal(c.handlerArgs.length, 0);
  assert.equal(c.released, true);
});
