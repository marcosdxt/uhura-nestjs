# uhura-nestjs

NestJS SDK for **Uhura** — a *contract-first* message bus for microservice meshes, built on RabbitMQ + PostgreSQL.

> Package: `@modulocker/uhura-nestjs` (Modulocker internal Verdaccio, `verdaccio.modulocker.app.br`). Part of the Uhura project; the original design notes live in [`doc/uhura-spec.md`](./doc/uhura-spec.md). Brought from `Dextro-Labs/uhura-nestjs` 0.6 (`5ef3868`) plus the transactional Inbox (0.7).

## What this package does

Exposes Uhura to NestJS code through **decorators** and a module. Same semantics as the Rust SDK (`uhura-rust`). It covers:

- **Publish** contract events (written to the *outbox* inside the business transaction).
- **Subscribe** to events (`@UhuraSubscribe`), with per-partition ordering and idempotency (Inbox).
- **RPC** over messaging (`@UhuraFunction` + `RpcResult<T>`).
- **CDC** of entities (`@UhuraEntityChange` / `@UhuraEntityNotify`).

## Installation

The package lives in the `@modulocker` scope of the Modulocker Verdaccio. Point
the scope at it in the project's `.npmrc` (the `useVerdaccio` option of
`template-node-ci` does the same in the pipeline):

```
@modulocker:registry=https://verdaccio.modulocker.app.br/
```

```bash
npm install @modulocker/uhura-nestjs
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
  `SERVICE_NAME`. The ews charts do not inject either: set `UHURA_GROUP` (or
  pass `group`) with the service name.
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

## Metrics and pause control (0.3)

### Prometheus metrics

`UhuraModule.forRoot` now also provides `UhuraMetrics`, a prom-client
**private registry** (never the global one, so it cannot clash with the
service's own metrics):

| Metric | Type | Labels |
|---|---|---|
| `uhura_consumer_handled_total` | counter | `domain`, `group`, `result` = `ok`\|`duplicate`\|`ignored`\|`error` |
| `uhura_consumer_handler_duration_seconds` | histogram | `domain`, `group` |
| `uhura_consumer_paused` | gauge | `domain`, `group` (1 = paused by the station) |
| `uhura_consumer_lag_seconds` (0.4) | histogram | `domain`, `group` — publish → consume |
| `uhura_rpc_client_total` | counter | `domain`, `method`, `result` = `ok`\|`error`\|`exception`\|`timeout` |
| `uhura_rpc_client_duration_seconds` | histogram | `domain`, `method` |
| `uhura_amqp_reconnects_total` | counter | — |
| `uhura_publish_total` (0.6) | counter | `domain`, `event` — events written by `UhuraService.publish` |
| `uhura_rpc_server_total` (0.6) | counter | `domain`, `method`, `result` = `ok`\|`error`\|`exception` — requests served by `@UhuraFunction` |
| `uhura_rpc_server_duration_seconds` (0.6) | histogram | `domain`, `method` |
| `uhura_consumer_redelivered_total` (0.6) | counter | `domain`, `group` — messages the broker redelivered |
| `uhura_outbox_pending` (0.6) | gauge | — rows in `uhura_outbox` not yet published by the station (read at scrape time, covers direct `INSERT`s too) |
| `uhura_outbox_oldest_pending_age_seconds` (0.6) | gauge | — age of the oldest unpublished row; 0 when empty |

plus the process metrics (`collectDefaultMetrics`) in the same registry.

By default the module mounts **`GET /metrics`** (`VERSION_NEUTRAL`, so it is
not turned into `/v1/metrics` by URI versioning). Kong only routes each
service's API prefixes, so it is reachable inside the cluster only. Options:

```ts
UhuraModule.forRoot({ ..., metrics: false });                    // no endpoint
UhuraModule.forRoot({ ..., metrics: { path: 'internal/metrics' } });
UhuraModule.forRoot({ ..., metrics: { defaultMetrics: false } }); // SDK metrics only
```

A service that already has a `/metrics` sets `metrics: false` and merges:
`Registry.merge([own, uhuraMetrics.registry])`.

### Pausing a consumer group

The station can pause a domain × group from the panel (Ambiente › Uhura):
replicas stop **taking** messages (`basic.cancel`); the queue keeps receiving
and nothing is lost. Protocol (`uhura-core` `control`):

- each replica binds an exclusive, auto-delete, server-named queue to the
  `uhura.control` topic exchange (`#`) and applies
  `consumer-pause {domain, group, paused}` and
  `consumer-snapshot {paused: [{domain, group}]}` (full state, every minute);
- **on boot (and after every reconnect)** the replica asks
  `uhura.control.rpc` `getPaused {group}` *before* subscribing, so a paused
  domain is never consumed, not even for an instant. The desired state lives
  in the DB of the station started with `UHURA_CONTROL_AUTHORITY=true`;
- if the authority does not answer within `controlTimeoutMs` (3000), the
  replica consumes everything and logs a warning; the next snapshot fixes it.
  The request carries `expiration`, so it never piles up in the RPC queue;
- messages already delivered when the pause arrives finish and are acked.

`control: false` turns it off (0.2 behaviour). `UhuraConsumer.pausedDomains()`
exposes the current state. 0.2 configs work unchanged.

## Business errors, handler context and consumer lag (0.4)

### `RpcError`

```ts
import { RpcError, UhuraFunction, type UhuraRpcContext } from '@modulocker/uhura-nestjs';

@UhuraFunction({ domain: 'user.rpc', method: 'getUser' })
async getUser(input: GetUserInput, ctx: UhuraRpcContext) {
  throw new RpcError('USER_NOT_FOUND', 'Usuário não encontrado.', { detail: 'X' });
}
```

The server replies

```json
{ "data": null, "resCode": "error", "errorCode": "USER_NOT_FOUND",
  "errorMessage": "Usuário não encontrado.", "errorStack": { "detail": "X", "code": "USER_NOT_FOUND" } }
```

`errorStack.code` is where the Rust driver (`dextrolabs-device`) already puts
the code; `errorCode` is the field proper (also in `uhura-core`'s `RpcResult`).
Any other exception is still `resCode: 'exception'` (unexpected failure).
Detection uses a `Symbol.for` brand, so an `RpcError` from another copy of the
package still counts.

On the client, `UhuraService.call` fills `errorCode` whenever a code exists:
the field (0.4+ servers), `errorStack.code` (Rust driver) or the legacy
`"CODE: message"` prefix (NestJS servers up to 0.3, which threw plain errors).
`errorMessage` is never rewritten. Local failures get codes too: `TIMEOUT`
(counted as `result="timeout"`) and `DISCONNECTED`. `parseErrorCode(result)` is
exported for results obtained elsewhere.

### Handler context (idempotency)

- `@UhuraFunction` handlers get `(data, ctx: UhuraRpcContext)`:
  `{ id, domain, method, correlationId, redelivered }`. `id` is the
  `RpcRequest.id` — one per call — so the server can dedupe retries.
- `@UhuraSubscribe` / `@UhuraEntityChange` handlers get
  `(data, ctx: UhuraEventContext)`. `ctx` **is the envelope** (as in 0.3, so
  `ctx.id`, `ctx.type`, `ctx.time`… keep working) plus `domain`, `event`,
  `group`, `redelivered`, `envelope` (the raw envelope) and `tx` (0.7, the
  Inbox transaction — see Guarantees). `ctx.id` is the envelope id, the
  idempotency key.

### `uhura_consumer_lag_seconds`

Histogram `{domain, group}` = now − envelope `time` (CloudEvents), observed when
the handlers start (after the Inbox check). It covers outbox → station → broker →
queue wait (including pause and retries); buckets go up to 1 h. Envelopes
without a valid `time` are not observed; negative values (publisher clock ahead)
count as 0. p95 for the panel:

```
histogram_quantile(0.95, sum by (le) (rate(uhura_consumer_lag_seconds_bucket[5m])))
```

## Guarantees

- **CloudEvents 1.0** envelope + **W3C/OpenTelemetry** *trace context* propagated across every hop.
- **At-least-once delivery + transactional Inbox = effectively-once** (0.7).
  Each delivery runs `BEGIN` → Inbox claim (`INSERT … ON CONFLICT DO NOTHING`)
  → handlers → `COMMIT` → ack. A failing handler rolls the claim back and is
  nacked with requeue, so it is retried and eventually parked instead of being
  swallowed as a duplicate. A concurrent delivery of the same envelope waits on
  the Inbox unique index and becomes a duplicate after the first commits.
- What the handler writes through `ctx.tx` commits **atomically** with the
  Inbox mark. Writes through another connection (a TypeORM `DataSource`, for
  example) do not: if the process dies between them and the `COMMIT`, the
  redelivery runs the handler again, so those writes must be idempotent (a
  unique key on the envelope id does it).
- Inside a handler, publish with `uhura.publish(domain, event, data, { tx: ctx.tx })`:
  the event then leaves only if the handler commits, and the call does not take
  a second connection from the SDK pool (every in-flight delivery holds one).
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
  rpc.ts          # RpcResult, RpcError, UhuraRpcContext
  amqp.ts         # shared AMQP connection
  control.ts      # pause control (uhura.control) + getPaused on boot
  metrics.ts      # UhuraMetrics (prom-client, private registry)
  metrics.controller.ts # GET /metrics
  uhura.module.ts # UhuraModule.forRoot
  decorators/     # @UhuraContract, @UhuraSubscribe, @UhuraFunction
```

## Development

```bash
npm install
npm run typecheck
npm run build
npm test        # node:test against dist, one file at a time (no broker needed)

# interop with a real station (skipped without the variables):
UHURA_IT_AMQP_URL=amqp://... UHURA_IT_STATION_URL=http://127.0.0.1:18080 \
UHURA_IT_ADMIN_TOKEN=... UHURA_IT_PG_URL=postgres://... \
  node --test --test-concurrency=1 test/integracao.test.js
```

## Publishing

Publishing is done by **GitHub Actions** (`.github/workflows/ci.yml`). Every
push and PR runs typecheck, build and tests. On `main` the workflow publishes
`@modulocker/uhura-nestjs` to the Verdaccio **if the version in `package.json`
is not there yet** — bumping `version` is the release request; a repeated
version is a no-op. Prereleases (`x.y.z-rc.N`) go out under the `next`
dist-tag. The token of the `publisher` user goes in the repository secret
`VERDACCIO_TOKEN`.

`scripts/publish.sh` publishes the same thing from a workstation, with the token
in `.npm_secret` (git-ignored).

Package history: `@marcosaquino/uhura-nestjs` on npmjs (0.1.0),
`@modulocker/uhura-nestjs` 0.1.0 on the Verdaccio, `@dextro/uhura-nestjs`
0.2–0.6 at Dextro, and `@modulocker/uhura-nestjs` again from 0.7.0.

## Caller identity in RPC (0.5)

Every RPC request carries the AMQP `user-id` property set to the user of the
client's connection URL. RabbitMQ **rejects** a publish whose `user-id` differs
from the authenticated connection user, so on the server side
`ctx.callerUser` is an identity guaranteed by the broker — unlike the `ctx`
object inside the request data, which is whatever the caller declares.

```ts
@UhuraFunction({ domain: 'user.rpc', method: 'getUser' })
async getUser(input: GetUserInput, ctx: UhuraRpcContext) {
  // ctx.callerUser === 'ews003' when the terminal connects with
  // its own RabbitMQ user; undefined for clients older than 0.5.
}
```

It only identifies services if each service connects with its **own**
RabbitMQ user.
