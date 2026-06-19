# uhura-nestjs

SDK NestJS do **Uhura** — message bus para mesh de microserviços, *contract-first*, sobre RabbitMQ + PostgreSQL.

> Pacote: `@uhura/nestjs` (GitHub Packages npm). Parte do projeto Uhura. A especificação formal completa vive em [`dextro-message-bus/SPEC.md`](../dextro-message-bus/SPEC.md).

## O que este pacote faz

Expõe o Uhura ao código NestJS por **decorators** e um módulo. Mesma semântica do SDK Rust (`uhura-rust`). Cobre:

- **Publicar** eventos de contrato (gravados no *outbox* dentro da transação de negócio).
- **Assinar** eventos (`@UhuraSubscribe`), com ordem por partição e idempotência (Inbox).
- **RPC** sobre mensageria (`@UhuraFunction` + `RpcResult<T>`).
- **CDC** de entidades (`@UhuraEntityChange` / `@UhuraEntityNotify`).

## Instalação

```bash
npm install @uhura/nestjs
```

```ts
// app.module.ts
@Module({
  imports: [
    UhuraModule.forRoot({
      amqpUrl: process.env.UHURA_AMQP_URL,   // amqp:// em cluster privado (v1)
      postgresUrl: process.env.UHURA_PG_URL, // fonte de verdade (outbox/inbox)
      mesh: 'acme',
      debug: false,
    }),
  ],
})
export class AppModule {}
```

## API (resumo)

```ts
// Contrato (no repositório de contratos, incluído como submódulo)
@UhuraContract({ domain: 'usuario.info', events: ['started','stopped','removed'], partitionId: 'id' })
export interface UsuarioInfo { id: string; description: string; date: Date }

// Publicar
await uhura.publish(UsuarioInfo, contract, 'started');

// Assinar
@UhuraSubscribe({ domain: 'uhura.acme.usuario.info', events: ['started'] })
async handle(entity: UsuarioInfo, ctx: UhuraContext) {}

// RPC
@UhuraFunction({ domain: 'usuario.info', method: 'hydrate' })
async hydrate(input: HydrateDTO, ctx: UhuraContext): Promise<UsuarioInfo> {}
const res = await uhura.method(UsuarioInfo, 'hydrate', { id: '42' });

// CDC
@UhuraEntityChange({ domain: 'usuario.info', events: ['inserted','updated'] })
async onChange(entity: UsuarioInfo, ctx: UhuraCdcContext) {}
```

## Garantias

- Envelope **CloudEvents 1.0** + *trace context* **W3C/OpenTelemetry** propagado em todos os saltos.
- Entrega **at-least-once + Inbox idempotente = effectively-once** (não é *exactly-once*).
- Ordem por partição via *consistent-hash exchange* + *Single Active Consumer*.

## Status

🚧 Em implementação a partir da `SPEC.md`. Veja a spec para arquitetura, topologia e garantias.

## Desenvolvimento

```bash
npm install
npm test            # unitários
npm run test:int    # integração (Testcontainers: Postgres + RabbitMQ)
```
