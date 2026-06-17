# @uhura/bus

```text
  _   _ _   _ _   _ ____      _
 | | | | | | | | | |  _ \    / \
 | | | | |_| | | | | |_) |  / _ \
 | |_| |  _  | |_| |  _ <  / ___ \
  \___/|_| |_|\___/|_| \_\/_/   \_\
   >> HAILING FREQUENCIES OPEN <<
```

**Uhura** é um message-bus NestJS sobre RabbitMQ para sistemas de missão crítica: semântica
tipada de ponta a ponta, persistência forte (quorum queues + publisher confirms), retry em
dois estágios com parking-lot, ordenação por entidade e envelope **CloudEvents 1.0** —
agnóstico de domínio e pronto para interoperar com clients em outras linguagens.

> **Status: em especificação — pré-implementação.**
> [Especificação](./doc/uhura-spec.md) fechada para análise; implementação inicia após os spikes de
> fundação (SPEC §26). Nada abaixo está publicado ainda.

---

## Por que não usar só o `@nestjs/microservices`?

O transport RMQ do Nest usa uma fila única por aplicação com envelope proprietário
(`{pattern, data}`), sem publisher confirms, sem topic exchange e sem DLQ — inadequado para
mensageria séria entre dezenas de serviços (análise completa: SPEC §2). O Uhura constrói
sobre `@golevelup/nestjs-rabbitmq` (o padrão de facto da comunidade) a camada que não existe
pronta no ecossistema Node: **semântica + contratos + recoverability + governança**.

## Conceitos em 30 segundos

Três semânticas, um client:

| Abstração | Verbo | Consumidores | Resposta | Entrega |
|---|---|---|---|---|
| **Event** | `notify` / `publish` | 0..N | não | persistente (at-least-once) |
| **Command** | `send` | exatamente 1 | não | persistente (at-least-once) |
| **Query (RPC)** | `request` | 1 | sim | efêmera (at-most-once) |

Endereçamento sempre por **contrato versionado** (`order.collected.v1`) — nunca por nome de
classe. Toda mensagem viaja como CloudEvent JSON com rastreabilidade
(`correlationid`/`causationid`/`traceparent`).

## Quickstart

### 1. Instale

```bash
npm i @uhura/bus @org/bus-contracts   # bus + contratos do SEU mesh
```

### 2. Configure o módulo

```ts
MessageBusModule.forRootAsync({
  inject: [ConfigService],
  useFactory: (cfg: ConfigService): MessageBusOptions => ({
    serviceName: 'billing-service',
    connection: { urls: [cfg.get('RABBITMQ_URL')] },
    reliability: {
      retry: { immediate: 2, delayedTiers: [10_000, 60_000, 600_000] },
    },
  }),
})
```

### 3. Declare seu objeto e publique

```ts
@BusObject('order', { version: 1, partitionKey: 'id' })
export class OrderSnapshot {
  id: string;
  status: string;
  total: number; // DTO plano — nunca a entity ORM
}
```

```ts
@Injectable()
export class OrderService {
  constructor(private readonly bus: MessageBus) {}

  async collect(order: OrderEntity) {
    // ...regra de negócio...
    const snapshot: OrderSnapshot = { id: order.id, status: 'COLLECTED', total: order.total };

    await this.bus.notify(snapshot, 'collected', 'COLLECTED');
    // → contrato: order.collected.v1 · routing key: order.collected.v1.COLLECTED
  }
}
```

**Anatomia do `notify(objeto, evento, status?)`** — a tripla é o endereço da mensagem:

| Parâmetro | No exemplo | O que define no wire |
|---|---|---|
| `objeto` | `snapshot` (instância de `OrderSnapshot`) | A classe `@BusObject` dá o **nome** (`order`) e a **versão** (`v1`) do contrato; a instância vira o payload (`data` do CloudEvent) |
| `evento` | `'collected'` | O que aconteceu — compõe o **contrato**: `order.collected.v1` |
| `status` | `'COLLECTED'` | Estado resultante — vira o **último segmento da routing key** (`order.collected.v1.COLLECTED`) e é por ele que consumidores filtram. Opcional: omitido vira `_`; transição aceita `{ from: 'PENDING', to: 'COLLECTED' }` |

### 4. Escute — a mesma tripla, do outro lado

O `@Listen` espelha o `notify` parâmetro a parâmetro: `(Objeto, evento, status?)`. A
diferença é o papel do `status` — no produtor ele **declara** o estado; no consumidor ele
**filtra** (e o filtro é seu: cada serviço escolhe seu recorte, resolvido pelo broker, sem
custo para o produtor):

```ts
@MessageController()
export class OrderListeners {
  // espelho exato do notify acima: só transições para COLLECTED
  @Listen(OrderSnapshot, 'collected', 'COLLECTED')
  async onCollected(@Payload() o: OrderSnapshot, @Ctx() ctx: MessageContext) {}

  // lista: este serviço só se importa com estados terminais
  @Listen(OrderSnapshot, 'state-changed', ['CANCELLED', 'EXPIRED'])
  async onTerminal(@Payload() o: OrderSnapshot) {}

  // sem status: todas as variações do evento (binding order.collected.v1.*)
  @Listen(OrderSnapshot, 'collected')
  async onAnyCollected(@Payload() o: OrderSnapshot) {}
}
```

Regra de bolso: **`notify` e `@Listen` casam quando a tripla casa** — mesmo objeto (nome +
versão), mesmo evento, e status compatível (exato, contido na lista, ou listener sem status).

### 5. RPC e Command tipados

```ts
// no serviço dono do objeto
@BusOperation(OrderSnapshot, 'hydrate')
async hydrate(@Payload() args: { id: string }): Promise<OrderSnapshot> { ... }

// em qualquer serviço
const order = await bus.request(OrderSnapshot, 'hydrate', { id: 'order-123' });

@BusCommand(OrderSnapshot, 'cancel')
async cancel(@Payload() args: { id: string; reason: string }) { ... }
await bus.send(OrderSnapshot, 'cancel', { id, reason });
```

## Confiabilidade (resumo — detalhes na SPEC §8–§10)

- **Publicação**: publisher confirms (a Promise resolve no ack do broker), `mandatory`
  detecta não-roteável, teto de in-flight contra OOM sob flow control.
- **Consumo**: ack manual; erro → retries imediatos in-process → tiers atrasados (filas TTL:
  10s/1m/10m) → **DLQ com diagnóstico**. Nunca loop infinito, nunca mensagem descartada.
- **Ordenação por entidade**: `partitionKey` (consistent-hash encadeado) ou
  `singleActiveConsumer` — com os limites documentados honestamente (SPEC §18.1).
- **Idempotência**: dedup por `CloudEvent.id` (adapter Redis), marcação atômica, fail-closed.
- **Evolução de contratos**: tolerant reader; aditivo = mesma versão; breaking = `vN+1` com
  dual-publish e aposentadoria guiada por telemetria.

## Observabilidade

Métricas prontas para exportar (`uhura_published_total`, `uhura_consumed_total`,
`uhura_retries_total`, profundidade/idade de fila…), health indicator compatível com
Terminus, graceful shutdown com drain de in-flight e propagação W3C `traceparent` →
OpenTelemetry. SPEC §19.

## Testando sua aplicação

`@uhura/bus/testing` traz um bus **in-memory com a mesma API** — seus handlers são testados
sem broker, com entrega síncrona e helpers de asserção (`expectPublished`, `givenEvent`).
Comportamento de broker (confirms, retry, partição) é responsabilidade da suíte do
componente, não da sua.

## Ecossistema

O projeto entrega **dois artefatos**; o repo de contratos pertence ao **mesh** adotante
(o projeto propõe a estrutura via `uhura contracts init` e a mantém via CLI):

| Artefato | Dono | Distribuição |
|---|---|---|
| `@uhura/bus` — componente NestJS | projeto (`uhura-bus`, este) | npm (GHCR) |
| `uhura` — CLI de operação/diagnóstico (Rust) | projeto (`uhura-cli`) | binário (GitHub Releases) |
| Repo de contratos do mesh (ex.: `@org/bus-contracts`) | organização adotante | npm da organização |

**O fluxo da frota:** cada serviço declara contratos por decorators → o build emite
manifesto + JSON Schemas em `.uhura/` (commitado, frescor verificado no CI) → `uhura bump`
coleta a frota e abre PR no repo de contratos do mesh → o CI de lá valida compatibilidade,
regenera tipos e o catálogo **`BUS.md`** (quem publica/consome o quê, grafo, órfãos) e
publica o pacote → consumidores atualizam por dependência versionada. Documentação e tipos
**extraídos, nunca escritos à mão**. Fluxo completo: SPEC §27.

## CLI `uhura` (Rust)

```bash
uhura doctor                                    # pré-requisitos do broker (plugins, permissões)
uhura listen order.collected.v1 --status COLLECTED   # observar sem interferir (tap)
uhura publish order collected --status COLLECTED --data @order.json
uhura request order hydrate --data '{"id":"order-123"}'
uhura topology diff                             # drift spec × broker
uhura dlq ls billing-service.billing.dead       # inspeção e replay de DLQ
uhura docs --out BUS.md                         # catálogo da frota
uhura bump --check                              # auditoria de drift dos contratos
```

Binário único, fala apenas o protocolo (CloudEvents + topologia AMQP) — é também a prova de
conformidade poliglota do barramento. SPEC §19.6.

## Roadmap

- [ ] **Spikes de fundação** (SPEC §26): basic.return no golevelup, direct reply-to,
      round-trip CloudEvents, esqueleto Rust da CLI.
- [ ] **Fase 0:** scaffold, envelope, registry de contratos, decorators, client, transport.
- [ ] **Fase 1:** integração RabbitMQ completa (quorum, retry 2 estágios, RPC), métricas, health.
- [ ] **Fase 2:** idempotência (Redis), OTel, `@uhura/bus/testing`, bench suite,
      JSON Schema + manifesto + AsyncAPI; CLI v0 em paralelo.
- [ ] **Fase 3:** publicação 0.1.0, piloto, CLI v1 (`dlq`, `docs`, `bump`), game day, SLOs.
- [ ] **Fase 4 (pós-0.2):** CDC (Captured Outbox) Postgres/MSSQL — spec própria; crate `uhura-bus-rs`.

## Desenvolvimento

- Node 22, NestJS 11, TypeScript 5.x. Testes: `jest` (unit) + Testcontainers
  `rabbitmq:4-management` (integração).
- **Política de qualidade** (SPEC §14.5): diff coverage 100% em todo PR; gate global ≥95%
  com exclusões auditáveis; mutation testing nos módulos core; golden files de conformidade
  em `conformance/` compartilhados com o CI da CLI.

## 📖 Documentação

A [Especificação (doc/uhura-spec.md)](./doc/uhura-spec.md) é o documento canônico — inclui fundamentos teóricos,
análise build-vs-buy, invariantes de interoperabilidade, histórico de design review e o
critério de prontidão pré-implementação.

---
*Developed with focus on reliability, scalability, and elegance.*
