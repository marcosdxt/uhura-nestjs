# uhura-nestjs

NestJS SDK for **Uhura** — a *contract-first* message bus for microservice meshes, built on RabbitMQ + PostgreSQL.

> Package: `@dextro/uhura-nestjs` (private Dextro registry — Verdaccio, `npm.dextrolabs.com.br`). Part of the Uhura project. The full formal specification lives in [`dextro-message-bus/SPEC.md`](../dextro-message-bus/SPEC.md).

## What this package does

Exposes Uhura to NestJS code through **decorators** and a module. Same semantics as the Rust SDK (`uhura-rust`). It covers:

- **Publish** contract events (written to the *outbox* inside the business transaction).
- **Subscribe** to events (`@UhuraSubscribe`), with per-partition ordering and idempotency (Inbox).
- **RPC** over messaging (`@UhuraFunction` + `RpcResult<T>`).
- **CDC** of entities (`@UhuraEntityChange` / `@UhuraEntityNotify`).

## Installation

The package lives in the `@dextro` scope of the Dextro Verdaccio. Point the
scope at it in the project's `.npmrc` (the `dextro-service` pipeline writes this
itself, with the in-cluster address):

```
@dextro:registry=https://npm.dextrolabs.com.br/
```

```bash
npm install @dextro/uhura-nestjs
```

```ts
// app.module.ts
@Module({
  imports: [
    UhuraModule.forRoot({
      amqpUrl: process.env.UHURA_AMQP_URL,   // amqp:// on a private cluster (v1)
      postgresUrl: process.env.UHURA_PG_URL, // source of truth (outbox/inbox)
      mesh: 'acme',
      // Consumer group = the service name. Optional here when UHURA_GROUP or
      // SERVICE_NAME is set; required (boot fails) if the service subscribes.
      group: 'acme-billing',
      debug: false,
    }),
  ],
})
export class AppModule {}
```

## API (overview)

```ts
// Contract (kept in the contracts repository, included as a submodule)
@UhuraContract({ domain: 'usuario.info', events: ['started','stopped','removed'], partitionId: 'id' })
export interface UsuarioInfo { id: string; description: string; date: Date }

// Publish
await uhura.publish(UsuarioInfo, contract, 'started');

// Subscribe
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

## Consumer groups (0.2)

Every service that subscribes to a domain gets **its own copy** of every event:

| Resource | Name |
|----------|------|
| Domain exchange (`topic`) | `uhura.<domain>` |
| Group queue (quorum, DLX, `x-delivery-limit=5`, bound `#`) | `uhura.<domain>.<group>.q` |
| Group parking (fanout exchange + quorum queue) | `uhura.<domain>.<group>.parking` / `.parking.q` |
| RPC (point-to-point, no group) | `uhura.<domain>.rpc` |

Names and arguments are byte-identical to the Rust driver (`uhura-core`,
`uhura-transport`); a mismatch would make the broker reject the redeclare.

- **Group = service name.** Resolved as `group` option → `UHURA_GROUP` →
  `SERVICE_NAME` (the `dextro-service` chart already injects the release name).
  Format `^[a-z0-9][a-z0-9-]{1,62}$` (no dots); `parking` and `rpc` are
  reserved. A service with `@UhuraSubscribe`/`@UhuraEntityChange` handlers and
  no group **fails at bootstrap**; an invalid explicit `group` fails in
  `forRoot`.
- Replicas of the same service share the group queue (competing consumers).
- Up to 0.1 there was a single queue per domain (`uhura.<domain>.q`), so
  different services competed for the same event and each event reached only
  one of them. CDC (`@UhuraEntityChange`) used the same path and had the same
  defect.
- The Inbox dedupes by `envelope.id` in the **service's own database** — one
  group, one database. Two groups sharing a database would dedupe each other.

### Migrating from 0.1

1. Deploy **every consumer** of the domain with 0.2 (group resolved). During
   the rollout each event lands in the old queue (still bound) and in every
   migrated group's queue; the Inbox drops the repeat.
2. Deploy the stations (`uhura-engine`) built on `uhura-core` 0.2 — the old
   station re-declares `uhura.<domain>.q` on publish.
3. Per domain: `uhura queues retire <domain>` (Rust CLI). It republishes what is
   left in the old queue and old parking to the domain exchange (each group gets
   a copy; the Inbox dedupes) and deletes the old topology. It refuses while the
   old queue still has consumers, and puts everything back if no group is bound.

## Guarantees

- **CloudEvents 1.0** envelope + **W3C/OpenTelemetry** *trace context* propagated across every hop.
- **At-least-once delivery + idempotent Inbox = effectively-once** (not *exactly-once*).
  The Inbox is written **after** the handler succeeds, so a failing handler is
  retried and eventually parked instead of being swallowed as a duplicate. Two
  concurrent deliveries of the same envelope can therefore both reach a handler
  before either records it — the Inbox dedupes what was **done**, it is not a
  lock.
- Per-partition ordering: the routing key is the `partitionkey`. The spec's
  *consistent-hash exchange* + *Single Active Consumer* are not implemented yet
  in either SDK; when they land they will be per group (shards inside the group).

## Status

**Functional MVP** — events and RPC, **verified in bidirectional interop with the
Rust engine** (`uhura-cli`), sharing the same outbox/inbox in Postgres and the same
RabbitMQ topology:

- `UhuraService.publish(domain, event, data, {partition})` — writes to the outbox.
- `@UhuraSubscribe({domain, events})` — consumer on the service's group queue,
  with idempotency (Inbox), ack/nack→group parking.
- `@UhuraFunction({domain, method})` — RPC endpoint (server).
- `UhuraService.call(domain, method, args)` — RPC client → `RpcResult<T>`.
- `@UhuraEntityChange({domain, events})` — CDC handler (`inserted`/`updated`/
  `removed` events generated by triggers via `uhura db sync`).

Verified interop: NestJS↔Rust events (both directions) and NestJS↔Rust RPC
(Rust client `uhura call` → `@UhuraFunction` server; NestJS client → NestJS
server).

Not yet implemented: domain mesh-prefixing. Contract codegen comes from the CLI
(`uhura sync`).

## Layout

```
src/
  envelope.ts     # CloudEvents 1.0 (names identical to the Rust SDK)
  transport.ts    # RabbitMQ topology (mirrors the Rust driver)
  storage.ts      # outbox/inbox (same tables/columns)
  uhura.service.ts# publish() -> outbox + call() RPC
  consumer.ts     # @UhuraSubscribe discovery + idempotent consumption
  rpc-server.ts   # @UhuraFunction discovery + RPC responses
  rpc-client.ts   # RPC client (direct reply-to + correlationId)
  amqp.ts         # shared AMQP connection
  uhura.module.ts # UhuraModule.forRoot
  decorators/     # @UhuraContract, @UhuraSubscribe, @UhuraFunction
```

## Development

```bash
npm install
npm run typecheck
npm run build
npm test        # node:test against dist, one file at a time (no broker needed)
```

## Publishing

Publishing is done by **Jenkins** (`dextro-pipeline`, `dextroLib(stack: 'node')`
in the `Jenkinsfile`), never from a workstation. On `main`, after lint/build/test,
the pipeline publishes `@dextro/uhura-nestjs` to the Verdaccio **if the version in
`package.json` is not there yet** — bumping `version` is the release request; a
repeated version is a no-op with a warning. Prereleases (`x.y.z-rc.N`) go out
under the `next` dist-tag.

The package was `@marcosaquino/uhura-nestjs` on npmjs up to 0.1.0; from 0.2.0 on
it is only `@dextro/uhura-nestjs`.
