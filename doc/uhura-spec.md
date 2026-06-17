# `@uhura/bus` — Especificação

Componente NestJS de message-bus sobre RabbitMQ. Fornece três abstrações tipadas —
**Event** (pub/sub), **Command** (send) e **Query** (RPC request/reply) — consumidas por
DI + decorators, com **persistência forte** para events/commands (durable + quorum queues +
publisher confirms + delivery persistente + DLX) e entrega **efêmera** para queries.

O componente é **agnóstico de domínio e de sistema**: não conhece nenhum serviço consumidor,
nenhuma routing key de aplicação e nenhuma topologia preexistente. Tudo que é específico de
uma aplicação entra por configuração ou pelos pontos de extensão (§15).

> Status: especificação em draft (pré-implementação). Alvo: pacote npm `@uhura/bus`.

---

## 1. Identidade do pacote

| Item | Valor |
|---|---|
| Nome npm | `@uhura/bus` |
| Versão inicial | `0.1.0` |
| Runtime | Node.js 22, NestJS 11 |
| Linguagem | TypeScript 5.x (build `tsc`, saída `dist/` + `.d.ts`) |
| Transport | RabbitMQ 3.13+ / 4.x (AMQP 0-9-1) via `@golevelup/nestjs-rabbitmq` |
| Envelope | **CloudEvents 1.0** (JSON, structured mode) |
| Peer deps | `@nestjs/common`, `@nestjs/core`, `reflect-metadata`, `rxjs` |
| Peer deps opcionais | `class-validator`/`class-transformer` (middleware de validação), `ioredis` (store de idempotência) |
| Deps | `@golevelup/nestjs-rabbitmq` ^9 (traz `amqplib` + `amqp-connection-manager`), `cloudevents` ^10, `uuid` |
| Dev deps | `jest`, `ts-jest`, `@types/*`, `testcontainers`, `@testcontainers/rabbitmq` |

### Por que `@golevelup/nestjs-rabbitmq` (e não amqplib cru, nem `@nestjs/microservices`)

Decisão revista na v0.4 após a análise da §2:

- O golevelup **já é** `amqplib` + `amqp-connection-manager` com os problemas difíceis
  resolvidos e testados em produção por uma comunidade grande: reconexão automática,
  **confirm channels por padrão** (publish resolve no ack do broker), assert de topologia no
  boot, **fila por handler**, RPC via **direct reply-to** e discovery de decorators via
  `DiscoveryService`/Reflector. Reimplementar isso é o trecho de maior risco e menor
  diferencial do projeto.
- Tudo que o golevelup **não** cobre (semântica Event/Command/Query, CloudEvents, retry em
  tiers, idempotência, registry de contratos, AsyncAPI) é camada **acima** do canal — seria
  escrito igualmente nas duas opções.
- O `MessageBus` próprio envolve o `AmqpConnection` do golevelup; nenhum tipo do golevelup
  vaza na API pública. Isso isola o acoplamento e mantém o **plano B** barato: se o golevelup
  estagnar ou bloquear algo essencial (ex.: exposição de `mandatory`/`basic.return`), troca-se
  o motor por `rascal` ou amqplib cru sem quebrar consumidores do `@uhura/bus`.
- Riscos residuais da escolha estão registrados em §20 (R1).

---

## 2. Análise de alternativas — build vs buy (ecossistema Node/NestJS)

Registro formal: o que foi avaliado, o que cobre, e por que ainda existe componente a
construir. Requisitos de referência: (R1) semântica tipada Event/Command/Query; (R2) publisher
confirms + mandatory; (R3) quorum queues + fila por handler; (R4) retry em 2 estágios + DLQ;
(R5) CloudEvents + contratos versionados; (R6) particionamento consistent-hash; (R7) pipeline
de middleware publish+consume; (R8) idempotência plugável; (R9) decorators/DI NestJS;
(R10) outbox/CDC; (R11) evolução de contratos; (R12) AsyncAPI.

| Candidato | Estado (06/2026) | Cobre | Veredito |
|---|---|---|---|
| `@nestjs/microservices` (RMQ) | ativo (core Nest) | R9 parcial | **Inadequado.** Fila única "inbox" por app com roteamento por `pattern` (anti-padrão que esta spec evita explicitamente, §8.3), envelope proprietário `{pattern, data}` não-interoperável, `emit()` não espera confirm, sem topic exchange, sem mandatory. |
| `@golevelup/nestjs-rabbitmq` | **ativo — padrão de facto** | R2, R3, R9; R6/R7 parcial | **Escolhido como fundação.** Padrão de facto da comunidade. Cobre o encanamento; não cobre a camada semântica. |
| `nestjs-rabbitmq` (AlariCode) | manutenção esporádica | R9 parcial | Fila única por serviço (viola R3), RPC proprietário. Inferior ao golevelup em todos os eixos. |
| `@nestjstools/messaging` | novo (2024-25), autor único, baixa adoção | R1/R7 parcial | Filosoficamente o mais próximo (command/event bus, middlewares), mas imaturo demais para fundação. Referência de API. |
| `rascal` | maduro, ativo (Onebeyond) | R2, R3; R4 quase (defer in-process, não filas TTL) | Melhor prova de que confirms/retry/parking-lot já foram resolvidos em Node — mas sem NestJS (sem DI/decorators). Plano B como motor. |
| `moleculer` | manutenção desacelerada | — | Framework completo que substitui os padrões do Nest e impõe topologia própria. Lock-in, descartado. |
| MassTransit (.NET) / Watermill (Go) | referências | ~todos | Não existe port JS/TS. MassTransit caminha para licenciamento comercial. São as referências de design desta spec, não opções. |

> **Proveniência dos dados desta tabela:** é um levantamento de mercado com corte de
> conhecimento, **não verificado online** nesta revisão — versões, estado de manutenção
> e licenciamento dos candidatos devem ser reconferidos antes de citar este quadro fora
> do contexto do projeto.

**Conclusão:** o ecossistema Node **não tem um "MassTransit"**. O encanamento AMQP existe
pronto (golevelup); a camada de **semântica + governança de contratos** — R1, R4 (tiers via
TTL), R5, R6 (abstração de partitionKey), R8, R10 (MSSQL não tem nada mantido), R11, R12 —
não existe em nenhuma lib mantida. **Esse é o componente legítimo a construir, em cima do
golevelup.** Peças complementares adotadas: `cloudevents` (SDK CNCF, envelope; o binding
AMQP é nosso, §12), `@asyncapi/*` (render de docs a partir do registry próprio).

### 2.1 Por que RabbitMQ (e não Kafka, NATS ou fila gerenciada)

O broker é premissa do ecossistema atual, mas a justificativa precisa ser explícita
(design review exige a alternativa considerada mesmo quando a resposta é "já decidido"):

| Opção | Avaliação para este workload |
|---|---|
| **RabbitMQ** (escolhido) | Workload = commands + RPC + fan-out moderado + ordem por entidade + retry/DLQ ricos **por mensagem**. Quorum queues (Raft) dão durabilidade replicada; routing key cobre o filtro por status; direct reply-to dá RPC sem topologia extra; plugins cobrem particionamento. Já operado pelo time. |
| Kafka | Otimizado para stream/replay/throughput sequencial. Perde aqui em: RPC (não nativo), retry/DLQ por mensagem (improvisados com tópicos extras), roteamento fino por status, custo operacional. Replay/event-sourcing não é requisito (§4). |
| NATS JetStream | Leve e moderno, mas ecossistema e conhecimento do time menores, e sem interop AMQP com topologias legadas (§15). |
| SQS/SNS, Azure Service Bus | A premissa de cluster privado (§4) descarta dependência de cloud; FIFO/dedup nativos não compensam o lock-in. |

**Consequência teórica que governa o design:** RabbitMQ é **fila, não log** — mensagem
consumida some. O barramento **não é event store**: consumidor novo não recebe o passado.
Estado atual se obtém por Query (`hydrate`, §7.2), não por replay (não-escopo, §4).

---

## 3. Princípio de desacoplamento

1. **Zero conhecimento de consumidores.** Nenhum nome de serviço, contrato ou routing key de
   aplicação aparece no código do componente. Exemplos nesta spec usam domínios fictícios.
2. **Defaults configuráveis.** Nomes de exchange usam o prefixo default `uhura.` mas são
   sobrescritíveis (`topology.exchangePrefix`), permitindo múltiplos barramentos isolados no
   mesmo broker.
3. **Interop com topologias preexistentes é um ponto de extensão** (§15), não uma feature de
   migração de um sistema específico. Estratégias de migração de legados são responsabilidade
   da aplicação que adota o componente.
4. **Stores externos por interface.** Idempotência depende de `IdempotencyStore` (interface);
   o adapter Redis é um subpath opcional (`@uhura/bus/redis`), não uma dependência do core.
5. **O motor AMQP é detalhe interno.** Nenhum tipo do golevelup/amqplib aparece na API
   pública — o acoplamento à fundação fica confinado em `transport/`.

---

## 4. Escopo e NÃO-escopo

**No escopo:** topologia, envelope, serialização, roteamento por tipo, decorators de handler,
client (`notify`/`publish`/`send`/`request`), confirms, retry (imediato + atrasado), DLQ,
pipeline de middleware (validação, idempotência, observabilidade), módulo Nest
`forRoot/forRootAsync`, testes unitários + integração.

**Como um evento é gerado:** simplesmente chamando `bus.notify(...)`/`bus.publish(...)` no
service layer, onde o código decide que algo aconteceu. **Não há "componente de geração de
evento"** — o bus é autossuficiente para publicar. Publicar é só publicar no Rabbit.

**Fora do escopo (composição opcional, NÃO dependência do bus):**
- **Atomicidade DB+publish (dual-write / outbox)** → componente de outbox externo (ex.:
  `@uhura/outbox`), **opt-in**, só para eventos que não podem se perder se o processo cair
  entre o commit e o publish. A maioria dos eventos não precisa (publish direto + consumidor
  idempotente basta).
- **Máquina de estados / transições** → um FSM externo (ex.: `@uhura/fsm`) pode chamar
  `bus.notify` na transição.
- **Persistência de domínio** → cada serviço com seu ORM.
- **Event sourcing / replay histórico** → fora de escopo **por design**: RabbitMQ é fila,
  não log (§2.1). Consumidores novos obtêm estado atual via Query (`hydrate`), nunca
  reprocessando o passado. Se replay virar requisito real, a resposta é outro backbone
  (Kafka/stream), não uma feature deste bus.
- **Sagas / process managers** (transações longas multi-serviço com compensação) → não são
  feature do bus nesta versão; compõem-se externamente (FSM + events/commands). Reavaliar
  demanda real após o piloto (Fase 3).
- **Segurança de transporte e authz** → premissa desta versão: o barramento roda **somente
  dentro de cluster privado**; TLS, credenciais e permissões do broker são responsabilidade
  da infraestrutura. Decisão registrada para revisão quando/se a premissa mudar.

Este componente é o **fio + as abstrações**. Reliability de entrega no nível do broker
(confirms, quorum, retry, DLQ) está no escopo; reliability transacional de negócio (outbox)
é opt-in externo.

---

## 5. Vocabulário e camadas de API

Três semânticas de mensagem:

| Abstração | Verbo client | Consumidores | Resposta | Entrega |
|---|---|---|---|---|
| **Event** | `notify` / `publish` | 0..N | não | persistente |
| **Command** | `send` | exatamente 1 | não | persistente |
| **Query (RPC)** | `request` | 1 | sim | efêmera |

E **duas camadas de API** sobre as mesmas semânticas:

- **Camada objeto-cêntrica (primária, §7.2)** — o uso do dia a dia. Declara-se o *objeto do
  barramento* uma vez (`@BusObject`) e opera-se com a tripla `(objeto, evento, status)` e com
  operações nomeadas (`request(Objeto, 'operacao', args)`). Não exige uma classe por evento.
- **Camada de contrato (avançada, §7.3)** — estilo MassTransit: uma classe por contrato
  (`@Message`/`@Command`/`@Query`), para quando o payload do evento difere do snapshot do
  objeto ou o contrato precisa de forma própria.

A camada objeto-cêntrica **compila para** contratos da camada de baixo: `notify(obj, 'evento',
'STATUS')` gera o contrato `objeto.evento.vN` com o snapshot como payload. As duas camadas são
interoperáveis (um `@Listen` pode receber o que um `publish` de contrato equivalente emitiu).

Endereçamento é sempre **por nome de contrato versionado** (`dominio.evento.vN`) — nunca por
nome de classe TypeScript, nunca por `objeto.método` remoto.

---

## 6. Envelope — CloudEvents 1.0

Toda mensagem (event/command/query/reply) viaja como um CloudEvent JSON (**structured mode**:
o envelope inteiro é o body AMQP):

| Campo CE | Origem | Exemplo |
|---|---|---|
| `id` | uuid v4 gerado | `9f2c…` (chave de idempotência) |
| `source` | `serviceName` da config | `billing-service` |
| `type` | nome do contrato | `order.collected.v1` |
| `subject` | id do objeto (opcional) | `order-123` |
| `time` | ISO-8601 / RFC3339 UTC com `Z` | `2026-06-11T12:00:00Z` |
| `datacontenttype` | fixo | `application/json` |
| `data` | **DTO/snapshot plano** (nunca a entity ORM) | `{ ... }` |

Extensões (prefixadas, CE-compliant, nomes sempre minúsculos):

| Extensão | Uso |
|---|---|
| `status` | estado atual / estado de destino da transição (`COLLECTED`) — entra na routing key |
| `fromstate` | estado de origem da transição (`PENDING`) — opcional |
| `correlationid` | correlation id (RPC + tracing) |
| `causationid` | id da mensagem que causou esta |

> Nota: Extensões de contexto de aplicação (ex: `tenantid`, `userid`) devem ser injetadas
> via Middlewares de Contexto, mantendo o envelope base genérico.

### 6.1 Mapeamento envelope ↔ propriedades AMQP (normativo)

Estas regras são invariantes de wire (§12) — clients em qualquer linguagem devem segui-las:

| Propriedade/header AMQP | Valor |
|---|---|
| `content_type` | `application/cloudevents+json` |
| `message_id` | = CloudEvent `id` |
| `delivery_mode` | `2` (persistente) para event/command; `1` para query/reply |
| `correlation_id` (property) | correlation id do RPC (duplicado da extensão `correlationid`) |
| header `traceparent` / `tracestate` | W3C trace context, como **headers AMQP** (long-string) → OpenTelemetry |
| header `x-uhura-attempts` | contador de tentativas de delayed retry (int 64-bit), ver §8.6 |
| header `x-uhura-partition-key` | valor do campo `partitionKey` do `@BusObject` (long-string) — insumo do exchange de hash (§8.3) |
| header `x-uhura-protocol` | versão do protocolo do barramento (int 64-bit; atual: `1`) — consumidor que receber versão superior à conhecida processa em melhor esforço e loga warning (§12.11) |

Headers customizados: somente **long-string** ou **int 64-bit** (nunca short-string, nunca
float) — tipos de `FieldTable` divergem entre clients de linguagens diferentes.

---

## 7. API pública

### 7.1 Module

```ts
MessageBusModule.forRootAsync({
  inject: [ConfigService],
  useFactory: (cfg: ConfigService): MessageBusOptions => ({
    serviceName: 'billing-service',          // vira CloudEvent.source e prefixo das filas
    connection: {
      urls: [cfg.get('RABBITMQ_URL')],       // array p/ HA
      heartbeatIntervalInSeconds: 15,
      reconnectTimeInSeconds: 5,
    },
    topology: {
      exchangePrefix: 'uhura',               // → uhura.events / uhura.commands / uhura.queries / uhura.dlx
      queueType: 'quorum',                   // events/commands; queries são sempre clássicas efêmeras
      defaultPrefetch: 20,
    },
    reliability: {
      publisherConfirms: true,               // events/commands esperam o broker persistir
      mandatory: true,                       // retorna não-roteável
      maxInFlightPublishes: 1_000,           // teto de publishes aguardando confirm (§8.5.6)
      retry: {
        immediate: 2,                        // re-execuções in-process antes de re-enfileirar
        delayedTiers: [10_000, 60_000, 600_000],  // filas de retry com TTL: 10s / 1m / 10m
      },
    },
    rpc: { defaultTimeoutMs: 10_000 },
  }),
})
```

`forRoot(options)` também disponível (config estática).

### 7.2 Camada objeto-cêntrica (primária)

```ts
// Declara o objeto do barramento UMA vez. Versão default v1.
// partitionKey: garante que mensagens de um mesmo objeto (ex: id)
// caiam sempre no mesmo consumidor (Consistent Hashing), mantendo a ordem.
@BusObject('order', { version: 1, partitionKey: 'id' })
export class OrderSnapshot {
  id: string;
  status: string;
  total: number;
  // DTO plano — nunca a entity ORM
}
```

**Publicar — `notify(objeto, evento, status?)`:**

```ts
// evento simples com estado
await bus.notify(snapshot, 'collected', 'COLLECTED');
//  → type: order.collected.v1
//  → routing key: order.collected.v1.COLLECTED

// transição com origem e destino
await bus.notify(snapshot, 'state-changed', { from: 'PENDING', to: 'COLLECTED' });
//  → fromstate=PENDING, status=COLLECTED, rk: order.state-changed.v1.COLLECTED

// sem status (rk recebe placeholder `_` para manter o nº de segmentos)
await bus.notify(snapshot, 'created');
//  → rk: order.created.v1._
```

**Escutar — `@Listen(Objeto, evento, status?)`:**

```ts
@MessageController()
export class OrderListeners {

  @Listen(OrderSnapshot, 'collected', 'COLLECTED')      // binding: order.collected.v1.COLLECTED
  async onCollected(@Payload() o: OrderSnapshot, @Ctx() ctx: MessageContext) {}

  @Listen(OrderSnapshot, 'collected')                   // binding: order.collected.v1.*  (qualquer status)
  async onAnyCollected(@Payload() o: OrderSnapshot) {}

  @Listen(OrderSnapshot, 'state-changed', ['CANCELLED', 'EXPIRED'])  // lista → 2 bindings na mesma fila
  async onTerminalState(@Payload() o: OrderSnapshot) {}
}
```

**RPC — operação nomeada SOBRE o objeto (não método remoto):**

```ts
// handler no serviço dono do objeto
@BusOperation(OrderSnapshot, 'hydrate')                 // contrato: order.hydrate.v1
async hydrate(@Payload() args: { id: string }): Promise<OrderSnapshot> { ... }

// caller em qualquer serviço — tipado de ponta a ponta
const order = await bus.request(OrderSnapshot, 'hydrate', { id: 'order-123' });
// Promise<OrderSnapshot>
```

**Command sobre o objeto:**

```ts
@BusCommand(OrderSnapshot, 'cancel')                    // contrato: order.cancel.v1
async cancel(@Payload() args: { id: string; reason: string }) { ... }

await bus.send(OrderSnapshot, 'cancel', { id, reason });
```

O contrato é sempre o par `(nome do BusObject, evento/operação, versão)` — renomear a classe
TypeScript não quebra o wire; mudar o payload de forma incompatível exige `version: 2` (§11).

### 7.3 Camada de contrato (avançada)

Para payloads que não são o snapshot do objeto, ou contratos com forma própria:

```ts
@Message('order.invoice-ready.v1')                      // Event
export class InvoiceReady { orderId: string; invoiceUrl: string; }

@Command('payment.capture.v1')                          // Command
export class CapturePayment { paymentId: string; }

@Query('order.totals.v1', { reply: OrderTotals })       // Query com reply tipado
export class GetOrderTotals { from: string; to: string; }
```

```ts
@MessageController()
export class Handlers {
  @OnEvent(InvoiceReady)                async onInvoice(@Payload() e: InvoiceReady) {}
  @OnCommand(CapturePayment)            async capture(@Payload() c: CapturePayment) {}
  @OnQuery(GetOrderTotals)              async totals(@Payload() q: GetOrderTotals): Promise<OrderTotals> {}
}

await bus.publish(new InvoiceReady(...));
await bus.send(new CapturePayment(...));
const t = await bus.request(GetOrderTotals, { from, to });
```

### 7.4 Opções por handler

Todo decorator de handler aceita opções:

```ts
@Listen(OrderSnapshot, 'collected', 'COLLECTED', {
  group: 'billing',          // agrupa handlers numa mesma fila (default: 1 fila por contrato)
  concurrency: 5,            // consumidores paralelos deste handler
  prefetch: 10,              // sobrepõe o default
  retry: { immediate: 0, delayedTiers: [30_000] },   // sobrepõe a política global
  idempotent: true,          // liga dedup por CloudEvent.id (requer IdempotencyStore)
  shards: 4,                 // nº de shards quando o objeto tem partitionKey (§8.3); default 4
})
```

---

## 8. Topologia RabbitMQ

**Conexões (normativo):** o transport usa **duas conexões** AMQP — uma para publicação,
outra para consumo. Sob pressão de memória/disco o broker **bloqueia conexões publicadoras**
(flow control); se os consumers compartilhassem a conexão, parariam de drenar exatamente
quando o broker mais precisa deles — deadlock operacional clássico.

### 8.1 Exchanges (todos `durable: true`)

| Exchange | Tipo | Uso |
|---|---|---|
| `<prefix>.events` | `topic` | eventos — **sempre** topic; particionamento é encadeado por grupo (§8.3) |
| `<prefix>.commands` | `direct` | commands — particionamento idem, hash encadeado por grupo |
| `<prefix>.queries` | `direct` | queries (RPC) — mensagens efêmeras |
| `<prefix>.dlx` | `topic` | dead-letter central |
| `<prefix>.retry` | `direct` | roteamento para as filas de retry com TTL |

> O tipo `x-consistent-hash` requer o plugin `rabbitmq_consistent_hash_exchange`
> **habilitado no broker** — pré-requisito operacional (risco R11, §20). O bootstrap falha
> com diagnóstico claro se houver `partitionKey` declarado e o plugin estiver ausente.

### 8.2 Routing key canônica

```
rk = <type>.<STATUS>
   = <objeto>.<evento>.v<N>.<STATUS|_>
```

- Sempre **4 segmentos** a partir do objeto — `status` usa placeholder `_` quando ausente,
  mantendo bindings determinísticos.
- Binding de `@Listen` sem status: `<type>.*`. Com status: `<type>.<STATUS>`. Com **lista de
  status**: um binding por status, todos na mesma fila — o broker entrega **uma** cópia por
  fila mesmo quando mais de um binding casa, então lista nunca duplica entrega.
- Negação ("todos menos X") **não** é expressável em binding AMQP — quem precisar assina
  `*` e filtra no handler (ou via middleware próprio); não é feature do componente.
- A **versão fica dentro do type**, então `v1` e `v2` nunca colidem em bindings.

### 8.3 Filas — uma por consumer-group (não por serviço)

> Decisão: **NÃO** usar uma fila "inbox" única por serviço — isso causa head-of-line blocking,
> impede retry por handler e impede escalar um consumidor isoladamente (modelo MassTransit:
> fila por endpoint). É também o motivo de o transport RMQ do `@nestjs/microservices` ter sido
> descartado (§2).

- Nome: `<serviceName>.<group>`, onde `group` default = nome do contrato
  (ex.: `billing-service.order.collected.v1`). Vários handlers podem compartilhar um `group`
  explícito (ex.: `billing-service.billing`).
- Events/Commands: `x-queue-type: quorum`, `durable: true`,
  `x-dead-letter-exchange: <prefix>.dlx`, `x-delivery-limit` explícito (§8.6).
- Instâncias do mesmo serviço **competem** na mesma fila (escala horizontal natural).
- Serviços diferentes têm filas diferentes bindadas ao mesmo evento (fan-out entre serviços,
  competição dentro do serviço).
- DLQ (parking-lot): `<serviceName>.<group>.dead` (quorum, durable), bindada em `<prefix>.dlx`.
- **Ordem total sem particionamento (baixo throughput):** opção por handler
  `singleActiveConsumer: true` declara a fila com `x-single-active-consumer` — o broker
  garante um único consumidor ativo com failover automático (sem paralelismo). Alternativa
  ao `partitionKey` quando a ordem importa mais que a vazão; os dois são mutuamente
  exclusivos no mesmo group.

**Assinatura por status — o filtro é do assinante, não do evento:**

- Cada consumer-group declara **seus próprios bindings** no topic exchange: um status, uma
  lista (`['CANCELLED','EXPIRED']` → 2 bindings na mesma fila) ou todos (`*`). O produtor
  publica uma única vez; serviços diferentes assinam recortes diferentes do mesmo contrato
  sem interferência — o "faz sentido para mim, não para outros" é resolvido pelo broker.
- **Dispatch em fila compartilhada:** handlers que dividem um `group` dividem a fila, e os
  bindings da fila são a **união** dos filtros de todos os handlers do grupo (inclusive o
  caso default: dois `@Listen` do mesmo contrato com statuses diferentes caem na mesma fila
  por terem o mesmo group default). Na entrega, o dispatcher casa `(type, status)` contra os
  metadados de cada handler e invoca **só os que casam**. Mensagem que não casa com nenhum
  handler (possível com união de filtros) é **ack'ada e contada**
  (`uhura_unmatched_total`, §19.1) — nunca nack/retry: reprocessar não muda o resultado.

**Particionamento (`partitionKey`) — topologia encadeada:**

```
<prefix>.events (topic)
   │  bindings: <type>.<STATUS>…   ← filtro de status, resolvido AQUI (antes do hash)
   ▼
<serviceName>.<group>.hash (x-consistent-hash, hash-header: x-uhura-partition-key)
   │  bindings: peso "1" por shard
   ▼
<serviceName>.<group>.0 … <serviceName>.<group>.<N-1>   (quorum; concurrency 1 por shard)
```

- O publisher escreve o valor do campo `partitionKey` no header **`x-uhura-partition-key`**
  (long-string). A routing key permanece a canônica (§8.2) — por isso o filtro de status
  (inclusive lista) continua sendo resolvido pelo topic exchange **antes** do hash, e
  funciona igual com ou sem particionamento.
- Hashear a routing key canônica seria errado: todas as mensagens do mesmo contrato+status
  cairiam num único shard (sem distribuição por entidade) — daí o `hash-header`.
- Número de shards: opção do grupo (`shards`, default **4** — cada shard é uma quorum queue,
  isto é, um grupo Raft com custo real no cluster; 8+ é opt-in consciente, ver §19.5).
  Mudar `shards` rebalanceia o hash-ring — janela transitória de violação de ordem por
  entidade (mesmo limite do scale-in/out, risco R3).

### 8.4 Filas de query (efêmeras)

- `<serviceName>.<group>.q`: clássica, `durable: false`, `autoDelete: true`.
- Mensagens de query/reply publicadas **sem** `persistent` e **sem** confirm — o caller está
  vivo esperando com timeout; persistir o request só adiciona latência sem benefício.
- Reply via `amq.rabbitmq.reply-to` (direct reply-to) + `correlationId`. Invariante de
  protocolo: o consume em `amq.rabbitmq.reply-to` é `no-ack` e acontece **no mesmo channel**
  do publish, iniciado **antes** do publish (§12).
- Sem retry/DLQ para queries: falhou ou estourou o tempo → erro tipado no caller, que decide.
- Requests de query levam **`expiration` AMQP = timeout do RPC**: se o caller já desistiu,
  a mensagem expira na fila em vez de gerar trabalho perdido no handler.

### 8.5 Publicação persistente (events/commands)

1. Canal em **modo confirm** (default do golevelup/`amqp-connection-manager`).
2. `publish(..., { persistent: true, mandatory: true, messageId, headers })`.
3. A Promise do client resolve no **broker ack** (confirm). `nack` → `PublishRejectedError`.
4. `mandatory` + listener de `basic.return` → `UnroutableMessageError` se nenhuma fila casar.
   O golevelup não expõe `basic.return` de primeira classe; o `PublisherService` acessa o
   canal subjacente para registrar o listener (risco R1, §20).
5. **Retry de publicação duplica (at-least-once começa no publish):** se o confirm estourar
   o timeout e o client republicar, o broker pode ficar com as duas cópias. Normativo: o
   republish **reutiliza o mesmo CloudEvent `id`** — o dedup do consumidor (§8.7) cobre.
   Sob flow control do broker (conexão publicadora bloqueada), o publish falha por timeout
   com `PublishTimeoutError` — **sem buffer ilimitado em memória** (a pressão deve subir
   para o chamador, não virar OOM).
6. **Teto de in-flight (normativo):** no máximo `maxInFlightPublishes` (default 1.000)
   publishes aguardando confirm simultaneamente; acima disso o publish falha **imediato**
   com `PublishBufferFullError`. O timeout (item 5) limita o *tempo* de espera; este teto
   limita a *quantidade* — juntos definem a margem de memória sob flow control prolongado.

### 8.6 Consumo e retry (recoverability em dois estágios)

Modelo NServiceBus: *immediate retries* + *delayed retries* + parking-lot.

1. **ack manual** após o handler resolver.
2. Erro → **immediate retries**: re-executa in-process até `retry.immediate` vezes (sem
   round-trip ao broker).
3. Persistindo o erro → **delayed retries**: a mensagem é **republicada** na fila de retry do
   tier corrente (`<serviceName>.<group>.retry.<tierMs>` — clássica, durable, com
   `x-message-ttl` = tier, `x-dead-letter-exchange: ''` (default exchange) e
   `x-dead-letter-routing-key` = nome da fila original), com o header
   `x-uhura-attempts` incrementado e **todos os headers originais copiados**. Tiers default:
   10s → 1m → 10m.
4. Esgotados os tiers → **DLQ** (`.dead`), com headers de diagnóstico (exceção serializada,
   contagem de tentativas, timestamps). Nunca loop infinito, nunca mensagem descartada.

**Contagem de tentativas (normativo):** a fonte de verdade é o header próprio
`x-uhura-attempts` (int 64-bit), incrementado a cada republish de delayed retry. O `x-death`
do broker fica apenas como diagnóstico — seu formato (array agregado por par queue/reason)
é hostil a parse cross-language e não cobre republish manual.

**`x-delivery-limit` (normativo):** quorum queues no RabbitMQ 4.x têm delivery-limit
**default = 20**; redeliveries do broker (crash de consumidor, requeue) competiriam com a
máquina de retry do componente. As filas declaram `x-delivery-limit` explícito (default da
config: `100`) para que o comportamento seja idêntico em qualquer versão do broker.

**Custo de cardinalidade:** o design cria `grupos × tiers` filas de retry. As filas de retry
são criadas **lazy** (só para grupos com delayedTiers configurados). Mitigação adicional
(tiers compartilhados por serviço com rk codificando a fila de retorno) fica no backlog (§21).

### 8.7 Idempotência (opt-in, via middleware)

Dedup por `CloudEvent.id` contra um `IdempotencyStore` (interface do core; adapter Redis em
`@uhura/bus/redis`; in-memory para testes). Habilitada por handler (`{ idempotent: true }`)
ou globalmente.

**Semântica (normativa, anti-race):** a marcação é **atômica** (`SET NX` + TTL no Redis), com
estados `processing` → `done`. Check-then-act em duas operações é proibido — duas instâncias
consumindo o mesmo redelivery fariam dupla execução. Mensagem em `processing` além do TTL de
processamento é tratada como não processada (o handler deve ser idempotente de qualquer forma
— at-least-once, §10). Janela de dedup configurável (default **24h** — decisão registrada, §22.5).

**Indisponibilidade do store (normativo):** handler marcado `idempotent: true` declara que
duplicata é danosa; logo, store fora do ar → **fail-closed**: a mensagem segue o fluxo de
retry (§8.6) em vez de executar sem dedup. Fail-open explícito por config
(`idempotency.failOpen: true`) para handlers onde atraso é pior que duplicata.

### 8.8 Imutabilidade de argumentos e migração de topologia

Argumentos de fila (`x-queue-type`, `x-delivery-limit`, TTL, DLX) são **imutáveis** no
RabbitMQ: redeclarar com argumento diferente falha com `PRECONDITION_FAILED`. Política:

1. O bootstrap **falha rápido** com diagnóstico claro (qual fila, qual argumento divergiu) —
   nunca degrada silenciosamente.
2. Mudança de argumento = **migração de fila**: drenar → deletar → recriar (runbook no
   README), ou nome novo versionado quando a drenagem não for viável.
3. Defaults de argumentos são, por consequência, **decisões quase permanentes** — fixados
   nesta spec e só alterados com migração planejada (motivo de o `x-delivery-limit` ser
   explícito desde o início, §8.6).

---

## 9. Pipeline de middleware

Cross-cutting concerns **não** são hardcoded no transport — são middlewares em duas pipelines
(estilo MassTransit filters / Watermill middleware):

```
publish pipeline:  serialize → compression → [middleware…] → confirm publish
consume pipeline:  parse → decompression → [middleware…] → handler → ack/nack
```

```ts
MessageBusModule.forRootAsync({
  useFactory: ... ,
  middleware: {
    publish: [CompressionMiddleware, OtelPublishMiddleware],
    consume: [DecompressionMiddleware, ValidationMiddleware, IdempotencyMiddleware, OtelConsumeMiddleware],
  },
})
```

Interface:

```ts
interface MessageMiddleware {
  handle(ctx: MessageContext, next: () => Promise<void>): Promise<void>;
}
```

**Built-ins (todos opcionais):**

| Middleware | Função | Dependência |
|---|---|---|
| `CompressionMiddleware` | Comprime payloads grandes (Gzip/Brotli) | `zlib` (nativo) |
| `ValidationMiddleware` | valida `data` com class-validator antes do handler | `class-validator` (peer opcional) |
| `IdempotencyMiddleware` | dedup por `CloudEvent.id` | `IdempotencyStore` |
| `OtelMiddleware` | spans de publish/consume, propaga `traceparent` | `@opentelemetry/api` (peer opcional) |
| `LoggingMiddleware` | log estruturado de entrada/saída/erro | — |

> Nota: o golevelup oferece interceptors/pipes do Nest no consumo, mas **não há pipeline de
> publicação** nele — a pipeline própria é necessária e cobre os dois sentidos com a mesma
> interface.

Aplicações registram middlewares próprios (ex.: extração de tenant para contexto de request).

---

## 10. Semântica de confiabilidade

- **Entrega (events/commands):** at-least-once — confirms + ack manual + quorum. Consumidores
  devem ser idempotentes ou usar o middleware de idempotência.
- **Entrega (queries):** at-most-once — timeout → `RpcTimeoutError`; erro remoto →
  `RpcRemoteError` (com causa serializada); queda de conexão com RPCs pendentes →
  `RpcConnectionLostError` imediato em todos os pendentes (fail-fast; o caller decide se
  re-tenta). Sem retry automático.
- **Ordenação:** não garantida globalmente. Dentro de uma fila com 1 consumidor e
  `concurrency: 1`, FIFO; com competição/retry, não. Com `partitionKey`, ordem por entidade
  **enquanto o conjunto de consumidores está estável** — e com a exceção documentada de
  delayed retry (§18.1). Documentar como limite.
- **Veneno:** após esgotar os tiers → DLQ com diagnóstico; replay manual (ver §21 roadmap —
  admin/replay tooling).

---

## 11. Evolução de contratos

Política explícita (tolerant reader / Spring Cloud Stream):

1. **Mudança aditiva** (novo campo opcional em `data`): **mesmo `vN`**. Consumidores devem
   ignorar campos desconhecidos (o parser nunca falha por campo extra) e tratar campos novos
   como opcionais.
2. **Mudança breaking** (remoção/renomeação/mudança de tipo/semântica): **`vN+1`** — novo
   contrato, novas filas/bindings.
3. **Transição:** o produtor publica em **dual-publish** (`vN` e `vN+1`) até todos os
   consumidores migrarem; depois remove o `vN`. **Critério de remoção verificável (não
   "quando der"):** `uhura_consumed_total{contract="…vN"}` zerado em **todos** os serviços
   por período acordado (sugestão: 14 dias) → o `vN` é removido e suas filas drenadas.
   Sem telemetria central agregando essa métrica, dual-publish vira permanente — é por isso
   que as métricas (§19.1) chegam na Fase 1, antes do primeiro contrato versionado.
4. O componente loga *warning* quando um handler recebe contrato com `source` desconhecido ou
   parse parcial — visibilidade de drift.
5. **Fonte de verdade exportável:** o `contract-registry` sabe emitir **JSON Schema por
   contrato versionado** (`order.collected.v1.json`) — insumo tanto para a geração AsyncAPI
   (roadmap Fase 2) quanto para codegen em outras linguagens (§12). Não é requisito das
   Fases 0–1.

### 11.6 Manifesto por serviço — documentação extraída, nunca escrita

O `contract-registry` de cada serviço emite no build um **manifesto declarativo**
`uhura-manifest.json`, gravado em `.uhura/` no próprio repo do serviço (junto com os
schemas que ele produz) e propagado ao repo `contracts/` via `uhura bump` (§27.3):

```jsonc
{
  "service": "billing-service",
  "busVersion": "0.1.0",
  "generatedAt": "2026-06-12T12:00:00Z",
  "publishes":  [{ "contract": "order.invoice-ready.v1" }],
  "consumes":   [{ "contract": "order.collected.v1", "status": ["COLLECTED"], "group": "billing" }],
  "operations": [{ "contract": "order.totals.v1", "reply": "OrderTotals" }],   // queries que ESTE serviço atende
  "commands":   [{ "contract": "payment.capture.v1" }]                          // commands que ESTE serviço atende
}
```

- **Extraído, não mantido**: o manifesto vem dos decorators via registry — não existe
  documentação manual para desatualizar. Se está no manifesto, está no código.
- É a **fonte única** de três artefatos: AsyncAPI (render por serviço, Fase 2), o catálogo
  da frota `BUS.md` (`uhura docs`, §19.6) e codegen futuro.
- Limite honesto: `publishes` cobre o que é declarável estaticamente (contratos registrados);
  `bus.publishRaw` (§15) não aparece — documentado como ponto cego do modo raw.

---

## 12. Interoperabilidade poliglota — invariantes de wire

A extensão para outras linguagens (**Rust é intenção futura**, não escopo atual) custa zero
agora **se** a spec mantiver a disciplina: tudo que define o comportamento do barramento deve
ser expressável **no protocolo e na topologia**, nunca em convenção TypeScript.

**Invariantes de wire (normativos para qualquer client):**

1. Envelope: CloudEvents 1.0 **structured JSON no body**; `content_type` AMQP =
   `application/cloudevents+json`; extensões minúsculas; tipos de extensão: string/bool/int.
2. `type` = contrato versionado e seu mapeamento determinístico para routing key (gramática
   da §8.2, incluindo o placeholder `_`).
3. Topologia inteiramente derivável por fórmula: nomes/tipos de exchanges, argumentos de fila
   (`x-queue-type`, `x-delivery-limit`, DLX, TTLs de retry) — um client em outra linguagem
   declara topologia **idêntica** a partir da spec, sem ler código TS.
4. Retry: contagem via header `x-uhura-attempts` (int64) com regra de cópia de headers no
   republish (§8.6); sequência de tiers é configuração, não código.
5. Headers customizados: whitelist de tipos — long-string ou int64 (§6.1).
6. RPC: direct reply-to com `correlation_id` na **property** AMQP, consume `no-ack` no mesmo
   channel do publish, iniciado antes do publish; o envelope de erro do reply também é
   contrato: `{ code, message, source, stack }` (decisão registrada, §22.4).
7. Particionamento: o hash do `x-consistent-hash` é calculado **no broker** (agnóstico de
   linguagem); o valor particionador viaja no header **`x-uhura-partition-key`**
   (long-string) e os exchanges de hash são declarados com
   `hash-header: x-uhura-partition-key`; binding keys de hash são pesos numéricos (`"1"`);
   a routing key permanece a canônica (§8.2), preservando o filtro de status no broker.
8. Idempotência: chave = CloudEvent `id` (UUID), janela de dedup é semântica do barramento.
9. Payload JSON cross-language: inteiros fora da faixa segura de 2^53 viajam como **string**;
   timestamps RFC3339 UTC com `Z`; `undefined`/`NaN`/`Infinity` proibidos.
10. Contratos: JSON Schema por versão (§11.5) como fonte neutra para codegen
    (TS: `json-schema-to-typescript`/zod; Rust futuro: `typify`).
11. Evolução do próprio protocolo: header `x-uhura-protocol` (int64, atual `1`) em toda
    mensagem. Mudança incompatível em qualquer invariante desta lista → incremento da
    versão; consumidor que receber versão superior à que conhece processa em melhor
    esforço e **loga warning** (visibilidade de frota desatualizada).

**Açúcar TS-only (livre para divergir em outra linguagem):** decorators NestJS, DI, classes
DTO + class-validator, nomes de métodos do client, pipeline de middleware, config module.

**Nota Rust (registro, não escopo):** não existe framework de bus RabbitMQ maduro em Rust
(2026) — o port seria um crate fino `uhura-bus-rs` sobre `lapin` (cliente AMQP de facto,
com confirms/quorum/direct reply-to) + `cloudevents-sdk` ou structs serde próprias +
`opentelemetry`. Com os invariantes acima, nada no design atual bloqueia esse port.
**O primeiro consumidor Rust do protocolo já está planejado: a CLI `uhura` (§19.6)** — seu
núcleo é o embrião do crate e a prova viva de que estes invariantes bastam para um client
não-TS.

---

## 13. Estrutura de pastas

```
uhura-bus/
├── package.json              # exports: ".", "./redis", "./testing"
├── tsconfig.json / tsconfig.build.json
├── jest.config.ts            # unit
├── test/jest-e2e.config.ts   # integração (testcontainers)
├── conformance/              # golden files dos invariantes de wire (§14.5.4) — consumidos também pelo CI do uhura-cli
├── SPEC.md
├── README.md
└── src/
    ├── index.ts                              # barrel
    ├── messagebus.constants.ts               # tokens, defaults
    ├── module/
    │   ├── messagebus.module.ts              # @Global forRoot/forRootAsync (configura RabbitMQModule do golevelup)
    │   └── messagebus-options.interface.ts
    ├── envelope/
    │   ├── cloud-event.ts                    # build/parse CloudEvents (+ fromstate/status) — wrapper do SDK `cloudevents`
    │   ├── amqp-binding.ts                   # mapeamento envelope ↔ properties/headers AMQP (§6.1)
    │   └── serializer.ts
    ├── decorator/
    │   ├── bus-object.decorator.ts           # @BusObject (camada primária)
    │   ├── listen.decorator.ts               # @Listen
    │   ├── bus-operation.decorator.ts        # @BusOperation / @BusCommand
    │   ├── message.decorator.ts              # @Message/@Command/@Query (camada de contrato)
    │   ├── on-handlers.decorator.ts          # @OnEvent/@OnCommand/@OnQuery
    │   ├── message-controller.decorator.ts
    │   └── params.decorator.ts               # @Payload/@Ctx
    ├── contract/
    │   ├── contract-registry.ts              # nome+versão → metadata (as 2 camadas convergem aqui)
    │   ├── json-schema.emitter.ts            # registry → JSON Schema por contrato (§11.5)
    │   ├── manifest.emitter.ts               # registry → uhura-manifest.json (§11.6)
    │   └── routing.ts                        # type+status → routing key canônica
    ├── transport/                            # ÚNICA camada que vê tipos do golevelup
    │   ├── topology.service.ts               # exchanges/filas/bindings/retry-tiers (declaração via AmqpConnection)
    │   ├── publisher.service.ts              # confirms + persistent + mandatory/basic.return (e modo efêmero)
    │   ├── consumer.service.ts               # ack/nack/immediate+delayed retry/DLQ (x-uhura-attempts)
    │   └── rpc-client.service.ts             # direct reply-to + correlation map + fail-fast em reconexão
    ├── pipeline/
    │   ├── middleware.interface.ts
    │   ├── pipeline.ts
    │   └── builtin/                          # validation, idempotency, otel, logging, compression
    ├── provider/
    │   ├── message-bus.client.ts             # MessageBus (notify/publish/send/request)
    │   ├── handler-registry.service.ts
    │   └── handler-discovery.service.ts      # DiscoveryModule + Reflector
    ├── observability/
    │   ├── metrics.ts                        # contadores/histogramas (§19)
    │   └── health.indicator.ts               # Terminus indicator (§19)
    ├── idempotency/
    │   ├── idempotency-store.interface.ts
    │   └── memory-store.ts                   # default p/ testes
    ├── errors/                               # RpcTimeoutError, RpcRemoteError, RpcConnectionLostError, PublishRejectedError, PublishTimeoutError, PublishBufferFullError, PayloadTooLargeError, UnroutableMessageError, ...
    └── interface/                            # MessageContext, options, etc.

src-redis/  (subpath "./redis")
    └── redis-idempotency-store.ts            # adapter ioredis (peer opcional)

src-testing/  (subpath "./testing")
    ├── in-memory-bus.ts                      # MessageBus in-memory: mesma API, entrega síncrona (§14.4)
    └── assertions.ts                         # expectPublished/expectConsumed/given helpers

```

---

## 14. Estratégia de testes

### 14.1 Unitários (sem broker — `jest.config.ts`)

- **envelope**: build/parse CloudEvents round-trip; extensões `status/fromstate/traceparent`;
  mapeamento AMQP (§6.1): content_type, message_id=id, delivery_mode por semântica.
- **serializer**: DTO plano ok; rejeita objeto com refs circulares (proteção contra entity ORM);
  rejeita `NaN`/`Infinity`/`undefined` (§12.9).
- **contract-registry**: `@BusObject` + evento/operação → contrato `objeto.evento.vN`;
  `@Message/@Command/@Query` registram nome+versão; renomear classe não muda o contrato;
  colisão de contrato → erro no bootstrap; emissão de JSON Schema por contrato.
- **routing**: rk canônica com 4 segmentos; placeholder `_`; binding com/sem status; lista
  de status → um binding por status; versão dentro do type (v1 ≠ v2).
- **dispatcher**: casa `(type, status)` contra metadados dos handlers do group; invoca só os
  que casam; mensagem sem handler casado → ack + incremento de `uhura_unmatched_total`.
- **decorators**: metadata de `@Listen/@BusOperation/@OnEvent/...` extraída; opções por handler
  (group/concurrency/retry) aplicadas; command/query com 2 handlers → erro.
- **camadas interoperam**: `notify(obj,'e','S')` produz envelope idêntico ao
  `publish` do contrato equivalente.
- **pipeline**: ordem de middlewares; short-circuit; erro em middleware → nack path;
  ValidationMiddleware rejeita payload inválido; IdempotencyMiddleware dedup com memory-store;
  marcação atômica processing→done (sem janela check-then-act).
- **rpc-client**: correlation map; timeout → `RpcTimeoutError`; erro remoto → `RpcRemoteError`;
  queda de conexão → `RpcConnectionLostError` em todos os pendentes.
- **publisher**: events/commands com `{persistent:true, mandatory:true}`; queries sem persistent
  e sem confirm; `nack` → `PublishRejectedError`.
- **retry policy**: cálculo de tier por tentativa via `x-uhura-attempts`; cópia de headers no
  republish; esgotou → DLQ path.

### 14.2 Integração — Testcontainers RabbitMQ (`test/jest-e2e.config.ts`)

Container `rabbitmq:4-management` real via `@testcontainers/rabbitmq`, `runInBand`,
tag de imagem fixada.

- **topology**: bootstrap cria exchanges durable, filas quorum por consumer-group (com
  `x-delivery-limit` explícito), filas de retry com TTL (lazy, só p/ grupos com tiers) e DLQs
  (assert via management API).
- **event round-trip**: `notify` → `@Listen` recebe snapshot íntegro; **fan-out**: dois
  serviços (duas filas) recebem o mesmo evento; duas instâncias do mesmo serviço (mesma fila)
  **competem** sem duplicar.
- **status filtering**: `@Listen(..., 'COLLECTED')` não recebe outras transições; `@Listen`
  sem status recebe todas; `@Listen(..., ['A','B'])` recebe A e B, não recebe C, **sem
  duplicar** entrega; `fromstate/status` corretos na transição `{from,to}`.
- **particionamento encadeado**: com `partitionKey`, mensagens da mesma entidade caem sempre
  no mesmo shard (hash por `x-uhura-partition-key`) E o filtro de status continua aplicado
  no broker (shard não recebe status fora do filtro do grupo); entidades distintas
  distribuem entre shards.
- **head-of-line**: handler lento no group A não atrasa entrega no group B (valida fila por grupo).
- **command**: entregue a exatamente 1 handler.
- **rpc**: `request` retorna objeto hidratado tipado; sem handler → `RpcTimeoutError`; erro no
  handler → `RpcRemoteError`; fila de query é efêmera (some após desconectar — assert via mgmt API).
- **publisher confirm**: `publish` resolve só após confirm; routing inexistente + mandatory →
  `UnroutableMessageError`.
- **retry em dois estágios**: handler falha → N immediate retries (sem redelivery do broker) →
  tiers atrasados (mensagem visita a fila de retry com TTL, `x-uhura-attempts` incrementa,
  headers preservados) → **DLQ** com headers de diagnóstico e payload preservado.
- **PERSISTÊNCIA (teste-chave)**: publica com confirm numa fila quorum **sem consumidor**;
  `container.restart()`; sobe consumidor → mensagem **sobrevive** e é entregue. Em contraste,
  fila de query **não** sobrevive (valida a separação efêmero/persistente).
- **reconexão**: derruba/restaura conexão → publishes pendentes drenam após reconectar;
  RPCs pendentes falham com `RpcConnectionLostError` (não ficam pendurados).
- **idempotência**: mesmo `CloudEvent.id` entregue 2× → handler executa 1×; entrega
  concorrente do mesmo id em 2 consumidores → handler executa 1× (atomicidade).
- **graceful shutdown**: SIGTERM com mensagens in-flight → consumo para, in-flight conclui e
  faz ack, processo encerra sem requeue (§19.3).
- **interop legada (§15)**: handler com `{exchange, routingKey, queue}` explícitos consome de
  topologia preexistente criada manualmente no teste.

### 14.3 Infra de teste e supply chain

- Helper `withRabbit()` (sobe container, devolve URL + management client, teardown).
- Timeout de suite ampliado (pull + boot).
- CI: Docker disponível (GitHub Actions `services:` ou DinD).
- **Supply chain:** lockfile commitado; `npm audit` no CI (falha em vulnerabilidade alta);
  atualização de dependências via PR revisado (sem auto-merge); ranges de versão
  conservadores (`^` só em deps com histórico de semver disciplinado).

### 14.4 Testes da aplicação consumidora — `@uhura/bus/testing`

O componente entrega o harness que os serviços usam para testar **seus handlers** sem
broker (gap clássico: cada adotante reinventa mocks do client):

- `InMemoryMessageBus`: mesma API pública (`notify/publish/send/request`), entrega
  **síncrona e determinística** aos handlers registrados no módulo de teste; honra a
  semântica (command exige 1 handler; query retorna reply; status filtering aplicado).
- Helpers: `expectPublished(contract, matcher)`, `givenEvent(obj, evento, status)`
  (injeta direto no pipeline de consumo, incluindo middlewares).
- Não simula: confirms, retry tiers, particionamento — comportamento de broker se testa
  com Testcontainers (§14.2); o in-memory cobre lógica de negócio dos handlers.

### 14.5 Política de cobertura e qualidade — `uhura-bus` E `uhura-cli`

Vale para os dois projetos (TypeScript e Rust), nos dois níveis (unit + integração) — nenhum
dos dois é cidadão de segunda classe:

| Dimensão | `uhura-bus` (TS) | `uhura-cli` (Rust) |
|---|---|---|
| Unit | `jest` (§14.1) | `cargo test` por crate (`uhura-core` testado isolado da camada de comando) |
| Integração | Testcontainers `rabbitmq:4-management` (§14.2) | `testcontainers-rs` contra a mesma imagem: publish+confirm, tap, reply-to, dlq replay |
| Interop | — | **Round-trip TS↔Rust**: mensagem publicada pela suíte TS é consumida/validada pela suíte Rust e vice-versa |
| Cobertura | `istanbul/v8` | `cargo-llvm-cov` |
| Mutação | Stryker (módulos core) | `cargo-mutants` (módulos core) |

**Gates (normativos, CI bloqueante):**

1. **Diff coverage 100%**: todo PR cobre 100% das linhas/branches que **alterou** — é o
   gate duro, e é o que o estado da técnica recomenda (cobertura nova não se negocia;
   cobertura legada se conquista).
2. **Gate global ≥ 95% statements/branches**, subindo a 100% com **exclusões auditáveis**:
   cada `istanbul ignore`/equivalente Rust exige justificativa no código e aprovação em
   review. "100% sem exclusões declaradas" vira teatro de métrica — exclusão explícita e
   revisada é mais honesta que teste vazio para inflar número.
3. **Mutation score mínimo 80%** nos módulos core (envelope, routing, retry, idempotência,
   dispatch) em job noturno — cobertura mede *execução*; mutação mede se os testes
   **verificam** algo. É o que separa cobertura total de verificação total.
4. **Golden files compartilhados**: fixtures canônicas de envelope/headers/routing keys
   versionadas em **`conformance/` no repo `uhura-bus`** (artefato do **projeto** — wire é
   protocolo, não domínio do mesh), consumidas pela suíte local e pelo CI do `uhura-cli`
   (pinadas por tag de release) — o mesmo byte stream deve fazer round-trip em TS e em
   Rust. É a verificação contínua dos invariantes de wire (§12), não apenas pontual no
   spike S8.

---

## 15. Interoperabilidade com topologias preexistentes (escape hatch)

Para integrar com sistemas que já possuem exchanges/filas AMQP fora das convenções deste
componente, os decorators aceitam **endereço explícito**, ignorando a derivação canônica:

```ts
@OnQuery(LegacyUserInfo, { raw: { exchange: 'user_info_exchange', routingKey: 'user_info_rk', queue: 'user_info_queue' } })
async legacyUserInfo(@Payload() req: LegacyUserInfo) { ... }
```

E o client expõe publicação raw:

```ts
await bus.publishRaw({ exchange, routingKey, payload, options });
```

Regras:
- O modo raw **não** participa da topologia gerenciada (sem retry tiers/DLQ automáticos) —
  a aplicação assume a semântica da topologia externa.
- Envelope CloudEvents é opcional no modo raw (`{ envelope: false }` envia o payload cru).
- Dual-publish (canônico + raw) fica a cargo da aplicação, tipicamente durante migrações.

> Este é um ponto de extensão genérico. O componente não conhece nenhuma topologia legada
> específica; migrações de sistemas existentes são planos da aplicação, não features do pacote.

---

## 16. Change Data Capture (CDC) com TypeORM

Extensão opcional via sub-pacote `@uhura/typeorm` para automação de eventos a partir
de alterações no banco de dados (Postgres e MSSQL).

> **Status: Fase 4 (pós-0.2).** Esta seção registra a arquitetura acordada, mas o CDC é um
> produto próprio (geração de DDL, relay, dois bancos) e **não entra no caminho do 0.1** —
> será extraído para spec própria quando a fase iniciar. Os spikes S5/S6 (§26.1) validam
> as hipóteses antes disso.

### 16.1 Arquitetura "Captured Outbox" — captura por banco

Diferente do CDC baseado em log (Debezium), esta implementação captura para uma tabela de
outbox (`uhura_outbox`) e entrega por **polling**, oferecendo resiliência sem infraestrutura
adicional. A **estratégia de captura é específica por banco** (decisão v0.4 — a v0.3 era
contraditória entre triggers e Change Tracking no MSSQL):

| Banco | Captura | Racional |
|---|---|---|
| **Postgres** | **Triggers** gerados pelo `@DbWatch`: gravam snapshot em `uhura_outbox` na **mesma transação** de negócio. `LISTEN/NOTIFY` opcional só como "despertar" do polling (sem payload no evento). | Trigger é barato e transacional no PG. |
| **MSSQL** | **Change Tracking (CT) nativo** + polling por `CHANGE_TRACKING_CURRENT_VERSION()`; sem triggers customizadas. | CT reduz o overhead de escrita vs trigger; o snapshot é lido na consulta de polling. |

Componentes comuns aos dois bancos:

1. **Transporte (Relay Worker)**: worker em background consulta as mudanças pendentes em
   intervalos curtos (polling adaptativo, §18.2).
2. **Garantia de Entrega (At-Least-Once)**: o worker só avança o ponteiro/remove a mensagem
   após o **publisher confirm** do RabbitMQ. Se o serviço cair, as mudanças permanecem no banco.
3. **Deduplicação (Relay Lock)**: lock de banco (`SELECT FOR UPDATE SKIP LOCKED` no Postgres,
   `UPDLOCK` no MSSQL) garante uma instância processando por vez, evitando duplicatas no
   barramento. (Limite de throughput conhecido — risco R6, §20.)

### 16.2 API de Uso

```ts
@Entity('orders')
@BusObject('order', { version: 1 })
@DbWatch({
  events: ['INSERT', 'UPDATE'],
  watchColumns: ['status', 'total'], // opcional: só dispara se estas colunas mudarem
})
export class OrderEntity {
  @PrimaryColumn() id: string;
  @Column() status: string;
}
```

Configuração no módulo:

```ts
UhuraTypeOrmExtension.register({
  dataSource: myDataSource,
  mode: 'postgres', // ou 'mssql'
  pollingIntervalMs: 500,
})
```

---

## 17. Composição opcional com outros componentes

- **Outbox (`@uhura/outbox`)** — essencial para garantir a atomicidade entre a alteração no
  banco de dados e a publicação da mensagem (Transactional Outbox Pattern). O `MessageBus`
  atua como o transport para o worker de outbox, garantindo que eventos de domínio críticos
  nunca sejam perdidos se o processo cair após o commit mas antes do publish.
  (Referência Node existente, nichada: `pg-transactional-outbox` — só Postgres; MSSQL não tem
  nada mantido no ecossistema, será código próprio.)
- **FSM (`@uhura/fsm`)** — handlers de transição podem chamar
  `bus.notify(obj, 'state-changed', { from, to })`.

---

## 18. Trade-offs Arquiteturais e Diretrizes de Operação

Baseado na análise de resiliência e performance para ecossistemas de microsserviços de alta
carga (dezenas de serviços compartilhando o mesmo broker).

### 18.1 Ordenação por entidade × retry atrasado (limite documentado)

Ao usar `partitionKey`, a ordem por entidade vale **enquanto a mensagem não entra em delayed
retry**: quando uma mensagem do "Pedido 1" vai para a fila de retry com TTL, mensagens
subsequentes do "Pedido 1" continuam fluindo pelo shard — a ordem **daquela entidade** é
sacrificada para não bloquear o shard inteiro (head-of-line). Este é o comportamento
normativo das versões 0.x: **disponibilidade do shard > ordem da entidade em pane**.

> **Backlog — Side-Wait-Queue ("afinidade de pane"):** mover mensagens da entidade em erro
> para uma fila de espera vinculada ao ID, preservando a ordem da entidade sem travar o shard.
> Rebaixado de design normativo (v0.3 §16.1) para backlog porque exige decisões ainda não
> tomadas: onde vive o registro "entidade X em pane" (store compartilhado? consistência entre
> instâncias?), como expira, como drena a fila de espera em ordem, e qual o custo de uma fila
> (ou stream) por entidade em pane. Não implementar parcialmente.

Outro limite documentado: **escalar consumidores de um exchange `x-consistent-hash` muda o
mapeamento de hash** (rebalanceamento) — durante scale-in/scale-out a ordem por entidade pode
ser violada transitoriamente. Operação: preferir escalar grupos particionados fora de pico
(risco R3, §20).

### 18.2 Otimização de Polling e Banco de Dados (CDC §16)

- **Adaptive Polling**: o intervalo de polling deve ser dinâmico (ex: 200ms sob carga, até 5s
  em ociosidade).
- **Postgres Wake-up**: opcionalmente, usar `LISTEN/NOTIFY` apenas como gatilho de "despertar"
  do polling, sem carregar payload no evento.
- **Restrição de Uso**: o decorator `@DbWatch` é **proibido** para tabelas de logs, auditoria
  ou eventos de sistema de alto volume. Deve ser restrito a **Entidades de Domínio**.

### 18.3 Controle de Bloat e Performance da Outbox (Postgres)

A tabela `uhura_outbox` deve ser gerenciada para evitar degradação do banco:

- **Particionamento por Turno**: a tabela deve ser particionada por tempo (ex: a cada **4 horas**).
- **Purge via Drop**: em vez de `DELETE` linha a linha (que gera "bloat"), o worker apenas
  avança um ponteiro persistido (transacional com o confirm — recuperável após crash). Um job
  assíncrono executa `DROP TABLE` na partição do turno anterior já processada.
- No MSSQL com Change Tracking, a retenção é gerenciada pelo próprio CT
  (`CHANGE_RETENTION`) — sem tabela de outbox para purgar.

### 18.4 Trade-offs aceitos em design review (registro — não reabrir sem fato novo)

Avaliados no painel multi-perspectiva (§25) e **aceitos conscientemente**:

| Trade-off | Por que aceito |
|---|---|
| Cluster RabbitMQ único como domínio de falha compartilhado | Quorum cobre perda de nó; segregação por vhost/prefix existe e clusters por criticidade são decisão de infra quando a escala justificar (§19.5). Multi-cluster no componente seria complexidade especulativa. |
| JSON/CloudEvents structured (vs encoding binário) | Legibilidade, ferramental e interop valem o overhead nesta escala; porta de saída via `datacontenttype` se um dia for gargalo medido. |
| Duas camadas de API desde o 0.1 | A camada objeto-cêntrica **compila para** a de contrato — a de baixo existe de qualquer forma e o modo raw (§15) depende dela. Não há peça a deletar. |
| Topologia declarada (idempotente) em todo boot de toda instância | Declares AMQP são baratos; reavaliar apenas se o management plane mostrar pressão em deploys de frota. |
| Sampling de spans OTel fora do componente | Política de amostragem pertence ao exporter/aplicação. |

Requisitos mínimos para operar o barramento em produção (sem isto, incidentes de mensageria
são invisíveis até virarem incidentes de negócio).

### 19.1 Métricas (expostas via `metrics.ts`; exporter é da aplicação)

| Métrica | Tipo | Labels |
|---|---|---|
| `uhura_published_total` / `uhura_publish_errors_total` | counter | contract, semantic (event/command/query) |
| `uhura_publish_confirm_duration` | histogram | contract |
| `uhura_consumed_total` | counter | contract, group, outcome (ok/retry/dead) |
| `uhura_handler_duration` | histogram | contract, group |
| `uhura_retries_total` | counter | contract, group, stage (immediate/delayed), tier |
| `uhura_dlq_depth` | gauge (via mgmt API ou no enqueue) | group |
| `uhura_rpc_inflight` / `uhura_rpc_timeouts_total` | gauge / counter | contract |
| `uhura_queue_depth` / `uhura_oldest_message_age_seconds` | gauge (poll opcional da mgmt API) | group |
| `uhura_unmatched_total` | counter — entregue na fila do group sem handler casado (§8.3) | group, contract |

Alertas recomendados (documentar no README): DLQ depth > 0 sustentado; taxa de retry > X%;
p99 de confirm acima do esperado (broker sob pressão).

### 19.2 Health check

`UhuraBusHealthIndicator` (compatível com `@nestjs/terminus`): conexão estabelecida + canais
abertos + (opcional) topologia assertada. Readiness deve falhar enquanto a topologia não foi
declarada (evita consumir/publicar em topologia parcial no boot).

### 19.3 Graceful shutdown

No `onApplicationShutdown` (SIGTERM):
1. Cancela os consumers (para de receber novas mensagens).
2. Aguarda os handlers in-flight concluírem (timeout configurável; default 30s) e faz ack.
3. Drena publishes pendentes de confirm.
4. Fecha canais e conexão.

Mensagens não-acked no momento do kill são redelivered pelo broker (at-least-once cobre);
o objetivo do drain é minimizar redeliveries em deploy normal.

### 19.4 Backpressure

`prefetch` é o mecanismo: o consumidor nunca recebe mais que `prefetch` mensagens não-acked.
Diretriz: dimensionar `prefetch ≈ concurrency × 2`; nunca `prefetch: 0` (ilimitado) em
filas de alto volume.

### 19.5 Orçamento de capacidade do broker

O custo dominante em escala de frota não é throughput — é **cardinalidade de filas**
(cada quorum queue é um grupo Raft com custo fixo de memória/CPU no cluster):

```
filas por consumer-group ≈ 1 (principal) + 1 (DLQ) + tiers (retry, lazy)
grupo particionado       ≈ shards × (principal quorum) + 1 (DLQ) + tiers + 1 exchange hash
```

Diretrizes:
- `partitionKey` **só onde ordem por entidade é requisito real** — cada grupo particionado
  multiplica quorum queues por `shards` (motivo do default 4, §8.3).
- Monitorar **contagem total de filas e de quorum queues** do cluster (mgmt API) com alerta
  de tendência; tratar crescimento como decisão de capacidade, não como efeito colateral.
- Isolamento de blast radius: o cluster é um domínio de falha único para todos os serviços
  que o compartilham. vhost por ambiente + `exchangePrefix` por barramento já permitem
  segregar; quando a escala justificar, barramentos de criticidades distintas podem ir para
  **clusters distintos** — decisão de infra, não feature do componente.
- **Vizinho barulhento:** um serviço pode saturar o cluster compartilhado (conexões, canais,
  taxa de publish, filas profundas). Contenção é **política de broker**, não feature do
  componente: limites per-user/per-vhost do RabbitMQ (conexões/canais) e policies de
  `max-length`/`overflow` nas filas de maior risco. O componente contribui com a telemetria
  por serviço (`source` em toda métrica/mensagem) que permite atribuir o consumo.

### 19.6 CLI `uhura` — operação, diagnóstico e desenvolvimento

Ferramenta de linha de comando própria do barramento (precedente: `nats` CLI, `kcat`).
As ferramentas genéricas do RabbitMQ não falam as convenções do bus — envelope CloudEvents,
contratos versionados, headers `x-uhura-*`, DLQ com diagnóstico. A CLI absorve o tooling de
admin/replay que estava em backlog e é o caminho do runbook O2 (§26.2).

**Princípios (normativos):**

1. **Fala o protocolo, não a lib.** Implementa diretamente os invariantes de wire (§12) —
   zero dependência do pacote TS. Com isso é também a **prova de conformidade poliglota**
   do barramento (primeiro client não-TS) e a incubadora do crate `uhura-bus-rs`.
2. **Observar nunca interfere.** `listen` cria fila **exclusiva/autoDelete própria** bindada
   ao exchange — jamais consome da fila de um grupo real. Tap é sempre seguro.
3. **Tudo que publica é atribuível.** `source` = `uhura-cli/<user>@<host>`, CloudEvent `id`
   próprio, `x-uhura-protocol` — eventos de CLI são rastreáveis em qualquer auditoria.
4. **Scriptável.** Saída humana por default; `-o json` (NDJSON) para pipes; exit codes
   estáveis; sem prompt interativo quando há flag.
5. **Destrutivo é explícito.** `drain`, `dlq replay`, `dlq purge` exigem `--yes`.

**Comandos (v0 → v1):**

| Comando | Função | Versão |
|---|---|---|
| `uhura contracts [--objeto X]` | lista contratos/objetos conhecidos (repo `contracts/`, §22.9) e bindings ativos no broker | v0 |
| `uhura listen <contrato> [--status A,B]` | tap não-intrusivo: imprime CloudEvents formatados em tempo real (fila exclusiva própria) | v0 |
| `uhura publish <objeto> <evento> [--status S] --data @f.json` | monta envelope válido, valida contra JSON Schema se disponível, publica com confirm | v0 |
| `uhura send <objeto> <operacao> --data '{…}'` | command (mesmas garantias do publish) | v0 |
| `uhura request <objeto> <operacao> --data '{…}' [--timeout 10s]` | RPC via direct reply-to; imprime reply ou erro tipado (`code/message/source/stack`) | v0 |
| `uhura topology show/diff [--service X]` | deriva a topologia canônica pela fórmula (§12.3) e **diffa contra o broker real** (drift detection) | v0 |
| `uhura queues [--service X]` | profundidade, oldest age, consumidores, taxas (mgmt API) | v0 |
| `uhura doctor` | valida pré-requisitos operacionais: conexão, permissões, plugin consistent-hash (R11), versão do broker, vhost | v0 |
| `uhura dlq ls/show <fila>` | inspeciona DLQs: payload + headers de diagnóstico (exceção, tentativas, timestamps) | v1 |
| `uhura dlq replay <fila> [--id …] [--reset-attempts] --yes` | devolve mensagens à fila original (republish com headers corretos) | v1 |
| `uhura dlq purge <fila> --yes` / `uhura drain <fila> [--count N] --yes` | descarte/consumo de fila real — destrutivos | v1 |
| `uhura docs [--out BUS.md] [--live]` | agrega os manifestos da frota (§11.6) num catálogo Markdown: por contrato — schema resumido, produtores, consumidores (com filtro de status/group), operações e commands; grafo de fluxo (mermaid); seção de **órfãos**: contrato publicado sem consumidor (candidato a aposentadoria, cruza com §11.3) e consumidor de contrato que ninguém publica (bug de governança). `--live` cruza com bindings/filas reais do broker e anota drift | v1 |
| `uhura contracts init [--dir]` | **scaffold do repo de contratos do mesh**: estrutura de diretórios (schemas/, manifests/), `uhura.fleet.json`, workflows de CI (linter de compat, geração de tipos, `BUS.md`, publish) — o projeto propõe a estrutura; o repo é da organização (§27) | v1 |
| `uhura bump [--config f] [--service X] [--check\|--pr\|--push]` | sincroniza a frota com o repo de contratos do mesh: coleta `.uhura/` dos repos configurados, agrega, valida e abre PR com diff semântico (§27.3); `--check` = auditoria de drift para CI | v1 |

**Configuração:** `RABBITMQ_URL` + `UHURA_MGMT_URL` (ou flags), `--vhost`, `--prefix`
(= `exchangePrefix`), `--contracts <dir>` (repo de JSON Schemas).

**Implementação (decisões §22.11–12): Rust, em repositório próprio (`uhura-cli`).**
Stack: `clap` + `lapin` + `tokio` + `serde`/`serde_json` (envelope com structs próprias) +
`reqwest` (management API). Distribuição: binário único por release (linux x86_64/aarch64) —
máquinas de operação não precisam de toolchain Node. O repo é um workspace Cargo com dois
crates: `uhura-core` (envelope, derivação de topologia, publisher confirm, direct reply-to —
o embrião do `uhura-bus-rs`, §12) e `uhura-cli` (camada de comando). Qualidade: mesma
política dos demais projetos (§14.5).

---

## 20. Riscos e mitigações (registro de design review)

| # | Risco | Impacto | Mitigação |
|---|---|---|---|
| R1 | Acoplamento ao `@golevelup/nestjs-rabbitmq` (cadência de releases por major do Nest; `basic.return`/`mandatory` não exposto de 1ª classe) | médio | Tipos do golevelup confinados em `transport/`; acesso ao canal cru para `basic.return` (se insuficiente, PR upstream pequeno); plano B documentado: motor `rascal` ou amqplib cru sob a mesma API. |
| R2 | Confirms ≠ garantia fim-a-fim: dual-write DB+publish continua possível sem outbox | alto p/ fluxos críticos | Documentado (§4, §17): fluxos que não toleram perda usam `@uhura/outbox`/CDC §16; demais aceitam a janela. |
| R3 | Rebalanceamento do `x-consistent-hash` em scale-in/out viola ordem por entidade transitoriamente | médio | Limite documentado (§18.1); operação fora de pico; sem correção possível no client (hash é do broker). |
| R4 | Delayed retry quebra ordem da entidade em pane (sem Side-Wait-Queue) | médio | Comportamento normativo documentado (§18.1); Side-Wait-Queue no backlog com questões de design explícitas. |
| R5 | Cardinalidade de filas: grupos × tiers de retry | baixo-médio | Criação lazy (§8.6); tiers compartilhados por serviço no backlog; monitorar contagem de filas no broker. |
| R6 | Relay do CDC com lock único = teto de throughput por serviço | médio | Polling adaptativo; restrição de uso do `@DbWatch` (§18.2); sharding do relay no backlog. |
| R7 | `x-delivery-limit` default (20) do RabbitMQ 4.x interferindo na máquina de retry | baixo | Valor explícito nas declarações (§8.6); teste de integração cobre. |
| R8 | Race no dedup de idempotência (check-then-act) | médio | Semântica atômica normativa `SET NX`+TTL com estados processing/done (§8.7); teste de concorrência (§14.2). |
| R9 | SDK `cloudevents` JS não tem binding AMQP | baixo | Binding próprio em `envelope/amqp-binding.ts` (§6.1) — structured mode torna isso ~trivial; porta de saída: structs próprias. |
| R10 | RPCs pendurados em reconexão | médio | Fail-fast `RpcConnectionLostError` em todos os pendentes (§10); teste de integração cobre. |
| R11 | Plugin `rabbitmq_consistent_hash_exchange` ausente no broker | médio (bloqueia `partitionKey`) | Pré-requisito operacional documentado; bootstrap falha com diagnóstico (§8.1); validar no cluster alvo antes da Fase 1 (spike S3, §26.1). |
| R12 | Argumentos de fila imutáveis: mudar um default exige migração de fila em produção | médio | Política e runbook (§8.8); defaults tratados como decisões quase-permanentes, fixados na spec antes do primeiro deploy. |
| R13 | Flow control do broker bloqueia publishers sob pressão | médio | Conexões separadas publish/consume (§8); `PublishTimeoutError` + teto `maxInFlightPublishes` (§8.5.5–6); alerta de p99 de confirm (§19.1). |
| R14 | Direção estratégica do RabbitMQ é AMQP 1.0 (nativo no 4.x); 0-9-1 pode ser rebaixado numa major futura | baixo (horizonte longo) | Protocolo confinado em `transport/`; envelope CloudEvents agnóstico de transporte; monitorar release notes de majors. |
| R15 | Cardinalidade de filas em escala de frota (quorum queues = grupos Raft) degrada o cluster antes do throughput | médio | Orçamento de capacidade e monitoração de contagem (§19.5); `shards` default 4; retry queues lazy e clássicas; partitionKey só com requisito real de ordem. |

---

## 21. Roadmap

- **Fase 0** — scaffold do pacote + module sobre golevelup + envelope/binding AMQP +
  contract-registry/routing + decorators (2 camadas) + client + transport com persistência;
  testes unitários.
- **Fase 1** — integração Testcontainers completa (persistência com restart, retry em 2
  estágios com `x-uhura-attempts`, fan-out/competição, RPC com fail-fast, graceful shutdown)
  + pipeline de middleware (validation, logging) + métricas/health (§19).
- **Fase 2** — idempotência (interface + adapter Redis em subpath) + OTel middleware +
  modo raw (§15) + **`@uhura/bus/testing`** (§14.4) + **bench suite** (throughput e p50/p99
  de confirm/fim-a-fim, no CI contra container local — guarda de regressão de performance)
  + emissão de **JSON Schema por contrato e do manifesto por serviço** (§11.6) +
  **geração de documentação AsyncAPI** a partir do `contract-registry`. **Em paralelo: CLI
  `uhura` v0** (Rust, §19.6 — listen, publish, send, request, topology diff, queues, doctor).
- **Fase 3** — publicar `@uhura/bus@0.1.0`; pilotar em um par de serviços; **CLI v1**
  (dlq ls/show/replay — pré-requisito do runbook O2 virar tooling — e `uhura docs`, o
  catálogo da frota a partir dos manifestos); **game day em staging**
  (cluster 3 nós, kill de líder de quorum sob carga); coletar feedback de API e baseline de
  SLO (§22.7) antes do 0.2.
- **Fase 4 (pós-0.2)** — CDC `@uhura/typeorm` (§16), extraído para spec própria; promoção
  do núcleo da CLI a crate publicado `uhura-bus-rs` quando houver o primeiro serviço Rust.
- **Backlog** — scheduling/delayed messages de aplicação; **Side-Wait-Queue** (§18.1, com as
  questões de design listadas); tiers de retry compartilhados por serviço (R5); sharding do
  relay de CDC (R6); client **Rust** `uhura-bus-rs` sobre `lapin` (§12 — depende só dos
  invariantes de wire, já fixados); **claim-check** para payloads grandes (EIP — ponteiro
  para object store acima do limite); linter de **compatibilidade de contratos** no CI
  (diff de JSON Schema: aditivo × breaking); exporter de **consumer lag / idade de fila**
  via management API.

---

## 22. Decisões registradas

Fechadas em 2026-06 no refinamento pré-implementação (histórico do processo nas §25–§26):

1. **Registry npm**: GHCR (GitHub Packages, npm privado da org).
2. **Onde nasce o código**: repo próprio (este), com CI e release independentes desde o
   início — sem incubação em serviço.
3. **Validação**: `ValidationMiddleware` é **opt-in explícito** (registrado em
   `middleware.consume`); instalar `class-validator` não muda comportamento sozinho.
4. **Replies de query (erro remoto)**: envelope `{ code, message, source, stack }` — **stack
   completa incluída**, decisão deliberada sob a premissa de cluster privado (§4): valor de
   debug entre serviços > risco de vazamento interno. É invariante de wire (§12.6); se a
   premissa de rede mudar, esta decisão deve ser revisitada **antes** de expor o barramento.
5. **Janela de dedup da idempotência**: **24h** (default configurável).
6. **`x-delivery-limit`**: **100** — explícito em todas as quorum queues (quase-permanente,
   §8.8).
7. **SLOs / performance budget**: medir baseline no piloto (Fase 3, 2–4 semanas de produção
   instrumentada) e fixar os SLOs sobre o observado; até lá valem os alertas de estado
   (DLQ > 0 sustentado, `oldest_message_age` crescendo, conexão caída).
8. **Compressão / teto de payload**: comprimir acima de **16 KiB**; rejeitar no publish acima
   de **512 KiB** (descomprimido) com `PayloadTooLargeError`; claim-check permanece em backlog.
9. **JSON Schemas dos contratos**: repositório **`contracts/` neutro compartilhado** —
   fonte única para o CI de compatibilidade e para o futuro codegen Rust (§12.10).
10. **Tier de durabilidade por grupo** (`queueType: 'classic'` opt-in para eventos de baixo
    valor): **rejeitado por ora** (painel de review §25, achado B1) — uniformidade quorum
    elimina a classe de erro "achei que era durável"; o knob só entra se o piloto mostrar
    custo de replicação relevante. Revisitar com dados da Fase 3.
11. **CLI `uhura` em Rust** (§19.6): decidido em 2026-06. Racional: (a) binário único para
    máquinas de operação, sem toolchain Node; (b) falando **somente o protocolo** (§12), a
    CLI é a prova de conformidade dos invariantes de wire — bug de interop aparece numa
    ferramenta de operador, não num serviço de produção; (c) o núcleo incuba o
    `uhura-bus-rs` sem custo dedicado. Condição: a CLI **não pode depender do pacote TS**
    nem de convenção fora da spec — se algo só dá para fazer lendo o código TS, é furo nos
    invariantes (§12) e deve ser corrigido lá.
12. **CLI em repositório próprio** (`uhura-cli`): ciclo de release, CI (cargo + testcontainers-rs)
    e toolchain independentes do pacote npm; workspace com crates `uhura-core` + `uhura-cli`
    (§19.6). Sincronização com o bus se dá **pela spec e pelos golden files** (§14.5), não
    por monorepo.
13. **Repo de contratos pertence ao mesh, não ao projeto** (corrigido na v0.13; refina a
    decisão 9): é componente npm **da organização adotante** (ex.: `@org/bus-contracts`),
    publicando JSON Schemas, manifestos, tipos TS gerados e o `BUS.md` daquela frota. O
    projeto entrega só a **estrutura proposta** (`uhura contracts init`) e as ferramentas
    de manutenção — coerência com o princípio §3 (o projeto não conhece domínio). Serviços
    importam contratos de terceiros como dependência npm versionada, nunca do código uns
    dos outros (§27).
    **Alternativa considerada — git submodule: rejeitada.** Pin por SHA não carrega
    semântica (semver minor/major do pacote comunica aditivo × breaking), permite depender
    de estado que não passou no portão de governança (npm só publica pós-linter), exige
    chore manual de update em toda a frota (vs renovate) e obrigaria cada serviço a gerar
    os tipos localmente (o pacote os entrega pré-gerados). Submodule segue adequado ao
    padrão histórico de **incubar componentes**; contratos têm fan-out e cadência que pedem
    distribuição versionada. Futuro Rust: o repo é git de qualquer forma — Cargo consome
    por tag, sem mudar esta decisão.
14. **Sincronização pull-based por artefato commitado** (`uhura bump`, §27.3): a CLI coleta
    `.uhura/` commitado nos repos dos serviços (config `uhura.fleet.json`) — **lê arquivos,
    nunca builda os serviços** (sem dependência de toolchain Node na CLI Rust). O frescor
    do artefato é gate no CI de cada serviço; a governança (compat/tipos/`BUS.md`/publish)
    permanece no CI do repo de contratos do mesh. O bump **propõe** (PR), não publica.

---

## 23. Fundamentos — teoria de sistemas distribuídos aplicada

Cada tema clássico de mensageria distribuída, como esta spec o endereça, e o status honesto.
Não há tema sem posição; os "⚠️" são itens rastreados (backlog/roadmap), não pontos cegos.

| Tema | O que a teoria diz | Como a spec endereça | Status |
|---|---|---|---|
| **Dual-write** | DB + broker sem transação comum = inconsistência inevitável em falha parcial | Outbox/CDC **opt-in** (§4, §16, §17); fluxos não-críticos aceitam a janela conscientemente | ✅ decidido |
| **Exactly-once é mito fim-a-fim** | Só existe at-least-once + dedup, ou at-most-once | At-least-once + dedup consumidor (§8.7); duplicação **no publish** também coberta (republish com mesmo `id`, §8.5.5) | ✅ |
| **Ordenação total × paralelismo** | Ordem global e escala horizontal são incompatíveis | Ordem **por entidade** via hash encadeado (`partitionKey`) ou `singleActiveConsumer` (§8.3); limites explícitos: delayed retry (§18.1), rebalance (R3) | ✅ com limites documentados |
| **Timeout ≠ falha** | Quem espera não sabe se o remoto falhou, está lento ou a resposta se perdeu | RPC at-most-once com erros tipados; fail-fast em reconexão; `expiration` = timeout no request (§8.4) | ✅ |
| **Backpressure** | Sistema sem limitação de fluxo quebra no pico | Consumo: `prefetch` (§19.4). Publicação: `PublishTimeoutError` + teto `maxInFlightPublishes` (§8.5.5–6); conexões publish/consume separadas (§8) | ✅ |
| **Poison messages** | Não podem bloquear a fila nem ser descartadas | Tiers + parking-lot com diagnóstico (§8.6); tooling de replay em backlog — até lá, runbook O2 (§26.3) | ✅ |
| **Idempotência de consumidor** | Pré-condição do at-least-once | Chave = CloudEvent `id`; marcação atômica; fail-closed com store fora (§8.7) | ✅ |
| **Causalidade e rastreabilidade** | Correlação não é ordenação; cadeias exigem propagação explícita | `correlationid`/`causationid`/`traceparent` (§6); OTel middleware | ✅ |
| **Fila ≠ log (replay)** | Broker de fila apaga o que entrega; histórico é propriedade de log | Não-escopo formal (§2.1, §4); estado atual via Query `hydrate` | ✅ decidido |
| **Evolução de contratos** | Deploy independente; o schema é a fronteira | Tolerant reader + versão no `type` + dual-publish com critério telemétrico de remoção (§11) | ⚠️ política existe; linter CI de compat é backlog |
| **Dependência de relógio** | Relógios mentem; correção não pode depender de timestamp | Nenhuma decisão de correção usa `time`; TTLs são do broker | ✅ |
| **Durabilidade / consenso** | Quorum (Raft) é o que sobrevive a perda de nó | Quorum queues + confirms (§8.5); perda exige perder a maioria | ✅ limite aceito |
| **Transações longas (sagas)** | Compensação > 2PC em microsserviços | Não-escopo (§4); composição externa; reavaliar pós-piloto | ✅ decidido |
| **Membership dinâmico** | Rebalance gera janelas de inconsistência inevitáveis | Rebalance do hash documentado como limite (R3) — sem correção client-side possível | ✅ limite aceito |

---

## 24. Proveniência de padrões e capacidades deliberadamente ausentes

Esta spec não inventa padrão novo; cada mecanismo tem origem rastreável em sistema maduro
(deliberado: o risco de design original em infra de mensageria é alto).

| Padrão na spec | Origem / referência | Onde |
|---|---|---|
| Queue-per-endpoint | NServiceBus, MassTransit | §8.3 |
| Recoverability em estágios (immediate → delayed → parking-lot) | NServiceBus | §8.6 |
| Pipeline de middleware nos dois sentidos | MassTransit (filters), Watermill | §9 |
| Tolerant reader + versionamento de contrato | Spring Cloud Stream, lei de Postel | §11 |
| Transactional outbox | Eventuate, Debezium, microservices.io | §16, §17 |
| Envelope interoperável | CloudEvents 1.0 (CNCF) | §6 |
| Dedup consumidor (inbox pattern) | NServiceBus, SQS FIFO | §8.7 |
| RPC com reply-to + correlation | EIP (Hohpe/Woolf), direct reply-to nativo | §8.4 |
| Single active consumer | RabbitMQ 3.8+ nativo | §8.3 |
| Particionamento por hash de entidade | Kafka partitions (conceito), plugin consistent-hash (mecanismo) | §8.3 |
| Claim-check | EIP | backlog §21 |

**O que o mercado tem e decidimos NÃO ter:**

| Feature | Quem tem | Por que ficou fora |
|---|---|---|
| Sagas/state machines no bus | MassTransit, NServiceBus, Axon | Complexidade alta, demanda não comprovada; composição externa cobre (§4); reavaliar pós-piloto |
| Replay/event store | Kafka, EventStoreDB | Broker errado para isso; requisito inexistente (§2.1) |
| Scheduling nativo | Azure Service Bus, plugin `x-delayed-message` | Plugin com limitações conhecidas em cluster; backlog até caso de uso real |
| Priority queues | RabbitMQ nativo | Conflita com prefetch; o problema real se resolve com filas/grupos separados |
| Schema registry central | Confluent/Kafka | Overhead operacional; JSON Schema em repo + CI cobre nesta escala (§22.9) |
| Multi-broker / transport abstrato | MassTransit | Abstrair o transport custa o controle fino que motivou o componente (§2.1) |

---

## 25. Histórico de design review (painel multi-perspectiva, 2026-06)

Revisão da v0.6 sob dez lentes de avaliação (Google = SRE/10×, AWS = ORR/blast radius,
Microsoft = lifecycle/DX, Tesla = delete-the-part, SpaceX = test-like-you-fly/margens,
Lockheed = mission assurance/supply chain, NVIDIA = performance, Siemens = horizonte de
década, BYD = custo, Meta = telemetria). Resultado consolidado na v0.7:

| Lente | Achado | Disposição |
|---|---|---|
| Google | Cardinalidade de filas (quorum = Raft) é o gargalo em 10×, não throughput | Corrigido → §19.5, R15 |
| Google | Declare de topologia em todo boot (pico em deploy de frota) | Aceito (idempotente/barato) — §18.4 |
| AWS | Invariantes de wire sem mecanismo de evolução | Corrigido → `x-uhura-protocol` (§6.1, §12.11) |
| AWS | Cluster único = domínio de falha compartilhado | Aceito + diretriz de segregação — §18.4, §19.5 |
| AWS | Vizinho barulhento sem contenção | Diretriz operacional → §19.5 |
| Microsoft | Sem harness de teste para a aplicação consumidora | Corrigido → `@uhura/bus/testing` (§14.4) |
| Microsoft | Política N-1 de runtimes não declarada | Aceito (define-se no 0.2) |
| Tesla | CDC é um segundo produto no caminho do 0.1; README divergia da SPEC | Corrigido → Fase 4, README alinhado (§16, §21) |
| Tesla | Duas camadas de API: há peça a deletar? | Aceito (a de baixo existe de qualquer forma) — §18.4 |
| Tesla/Google | `shards: 8` default caro | Corrigido → default 4 (§8.3) |
| SpaceX | Failover de quorum nunca exercitado pré-produção (CI = 1 nó) | Roadmap → game day Fase 3 (§21) |
| SpaceX | Publishes in-flight sem teto de quantidade → OOM sob flow control | Corrigido → `maxInFlightPublishes` (§8.5.6) |
| Lockheed | Supply chain sem postura declarada | Corrigido → §14.3 |
| Lockheed | Matriz formal de rastreabilidade requisito→teste | Aceito (implícita basta; formalizar só sob exigência externa) |
| NVIDIA | Sem benchmark do componente (baseline mediria o piloto, não o bus) | Roadmap → bench suite Fase 2 (§21) |
| NVIDIA | JSON vs binário | Aceito — §18.4 |
| Siemens | Direção estratégica AMQP 1.0 no RabbitMQ 4.x | Registrado → risco R14 |
| BYD | Tier de durabilidade por grupo (classic opt-in) | **Rejeitado** → §22.10 |
| Meta | Fim do dual-publish sem critério verificável | Corrigido → critério telemétrico (§11.3) |
| Meta | Sampling OTel | Aceito (é do exporter) — §18.4 |

**Síntese:** a spec passou bem nas lentes de confiabilidade e teoria; os gaps reais estavam
nas bordas operacionais e de produto (evolução de protocolo, margens de memória, DX do
consumidor, disciplina de aposentadoria de contrato) — o que um ORR ou post-mortem acharia
no primeiro ano de produção.

---

## 26. Prontidão pré-implementação

As decisões de design estão fechadas (§22). O que separa esta spec do código:

### 26.1 Spikes técnicos — validar antes de assumir

| # | Hipótese a validar | Como | Se falhar |
|---|---|---|---|
| S1 | golevelup v9 dá acesso ao canal cru suficiente para `mandatory`/`basic.return` (R1) | Ler código da v9 + POC de 1 dia: publicar não-roteável e capturar o return | PR upstream pequeno; plano B: motor rascal/amqplib sob a mesma API |
| S2 | Direct reply-to via golevelup com correlação própria + fail-fast em reconexão (§8.4, R10) | POC: request/reply + derrubar conexão com RPC pendente | RPC client direto no canal, sem helper do golevelup |
| S3 | Plugin consistent-hash habilitado no cluster alvo; modo `hash-header` na topologia encadeada (§8.3); rebalance ao mudar `shards` medido (R3, R11) | Broker de staging; POC da cadeia completa com 2→3 shards sob carga | `singleActiveConsumer` como única opção de ordem no curto prazo; partitionKey adiado |
| S4 | Testcontainers + RabbitMQ 4 no CI atual (DinD/services) | Pipeline dry-run com teste de fumaça | Broker fixo de CI compartilhado (pior isolamento, aceitável) |
| S5 | Triggers PG via metadata TypeORM produzem DDL idempotente e seguro (§16) | POC com 1 entity: gerar, aplicar 2×, validar same-transaction | Migrations geradas por CLI, não automáticas no boot |
| S6 | MSSQL Change Tracking: latência/custo do polling na carga real (§16) | POC contra base de staging | Voltar a triggers também no MSSQL |
| S7 | `cloudevents@10` round-trip com extensões custom (`status`, `fromstate`) | POC de 2h | Structs próprias (porta de saída prevista, R9) |
| S8 | Esqueleto da CLI Rust (§19.6): `lapin` publica com confirm + consome tap + direct reply-to contra o broker, interoperando com mensagem publicada pelo lado TS | POC de 1–2 dias: `uhura listen` + `uhura publish` mínimos round-trip com o S7 | Reavaliar §22.11 (CLI em TS reusando o pacote, perdendo a prova de conformidade) |

S1 + S2 + S7 são o **caminho crítico** (≈2–3 dias somados) — validam a fundação antes da
Fase 0. S3–S6 paralelizam com a Fase 0 (afetam features das Fases 1–4).

### 26.2 Definições operacionais — documentos, não código

| # | Item | Para quando |
|---|---|---|
| O1 | Runbook de migração de argumentos de fila (§8.8): drenar → deletar → recriar | antes do 1º deploy em produção |
| O2 | Runbook de DLQ: inspeção e replay manual até a CLI v1 (`uhura dlq`, §19.6) chegar na Fase 3 | antes do piloto |
| O3 | Alertas mínimos (§19.1): DLQ > 0 sustentado; retry rate; oldest_message_age; p99 confirm | junto com a baseline de SLO (§22.7) |
| O4 | Convenção de isolamento por ambiente: vhost por ambiente + `exchangePrefix` por barramento | Fase 0 |

### 26.3 Critério "ready to code" (Fase 0)

- [x] Decisões de design fechadas e registradas (§22) — 2026-06
- [ ] **S1** validado (`basic.return` no golevelup)
- [ ] **S2** validado (direct reply-to + fail-fast)
- [ ] **S7** validado (round-trip `cloudevents@10`)

**Resumo executivo:** spec teoricamente fechada (§23), mecanismos com proveniência de
mercado (§24), review em painel absorvido (§25), decisões registradas (§22). O que separa
a spec do código são **3 POCs de fundação — ≈2–3 dias** (+ S8 para a CLI).

---

## 27. Como utilizar — fluxo de adoção ponta a ponta

**Fronteira projeto × mesh (normativa):** o projeto Uhura entrega **dois artefatos** — o
componente e a CLI. O terceiro elemento do fluxo, o **repo de contratos, pertence ao mesh**
de serviços que adota o barramento: o projeto apenas **propõe a estrutura** (scaffold via
`uhura contracts init`) e fornece as ferramentas para mantê-la atualizada em todos os
serviços (`uhura bump`, `uhura docs`, linter de compatibilidade). O componente e a CLI não
conhecem nenhum contrato de domínio (princípio §3); um mesmo broker pode inclusive servir
**múltiplos meshes**, cada um com seu repo de contratos e seu `exchangePrefix`.

| Artefato | Dono | Distribuição | Papel |
|---|---|---|---|
| `@uhura/bus` (repo `uhura-bus`, este) | **projeto** | npm (GHCR) | componente NestJS que cada serviço importa |
| `uhura` CLI (repo `uhura-cli`) | **projeto** | binário (GitHub Releases) | operação, diagnóstico, manutenção do repo de contratos do mesh |
| Repo de contratos do mesh (ex.: `mesh-contracts` → npm `@org/bus-contracts`) | **mesh / organização adotante** | npm da organização | fonte única dos contratos **daquela frota**: JSON Schemas, manifestos, tipos TS gerados e o catálogo `BUS.md` |

### 27.1 O fluxo do serviço (produtor e/ou consumidor)

1. **Instala e configura:** `npm i @uhura/bus @org/bus-contracts` (o pacote de contratos
   **do seu mesh**) + `MessageBusModule.forRootAsync(...)` (§7.1). Pronto para
   publicar/consumir.
2. **Declara o que é seu:** `@BusObject`/`@Listen`/`@BusOperation`/`@BusCommand` (§7.2–7.4)
   no próprio código. **Tipos de contratos de outros serviços vêm do pacote de contratos do
   mesh** (gerados dos JSON Schemas) — nunca se importa classe do código de outro serviço;
   é isso que mantém deploys independentes.
3. **Build emite, não escreve:** o build do serviço gera `uhura-manifest.json` + JSON
   Schemas dos contratos que ele **produz** (§11.5–11.6) em **`.uhura/` commitado no
   próprio repo**; o CI do serviço falha se `.uhura/` estiver desatualizado em relação ao
   código (check de frescor — artefato gerado, versionado e verificado).
   A propagação ao repo de contratos do mesh é feita pelo **`uhura bump`** (§27.3).
4. **O repo de contratos do mesh é o portão de governança:** o CI desse repo (gerado pelo
   scaffold `uhura contracts init`) valida compatibilidade (aditiva = mesma versão;
   breaking = exige `vN+1` — o linter de compat roda aqui), regenera os tipos TS (futuro:
   Rust via `typify`), regenera o **`BUS.md`** com `uhura docs` e publica nova versão do
   pacote de contratos da organização.
5. **Consumidores acompanham por dependência:** atualização de `@org/bus-contracts` (ex.:
   renovate) entrega tipos novos; o `BUS.md` é a documentação viva da frota — quem publica,
   quem consome com qual filtro, grafo de fluxo, órfãos.
6. **Operação no dia a dia:** `uhura doctor` (pré-requisitos), `uhura listen` (observar sem
   interferir), `uhura queues`/`dlq` (incidentes), `uhura topology diff` (drift).

### 27.2 Regras do fluxo (normativas)

- **Dono do contrato é o produtor.** Mudança de contrato nasce no repo do serviço produtor
  e se materializa no repo de contratos do mesh via PR gerado — esse repo não aceita
  edição manual de schema/manifesto/`BUS.md` (CI falha).
- **Consumidor nunca depende do produtor em build** — só do pacote de contratos do mesh.
  A única dependência compartilhada de runtime é o broker.
- **Versionamento:** o pacote de contratos versiona por **minor** em mudança aditiva e por
  **major** quando um contrato `vN` é removido (fim de dual-publish, critério §11.3).
- **O que é do projeto não mora aqui:** golden files de conformidade dos invariantes de
  wire vivem em `conformance/` no repo do projeto (§14.5.4) — o repo de contratos do mesh
  contém só domínio.

### 27.3 Sincronização da frota — `uhura bump`

A mecânica de levar os contratos dos serviços ao repo de contratos do mesh vive em **um**
lugar — a CLI — em vez de duplicada em N pipelines. Configuração via JSON:

```jsonc
// uhura.fleet.json (vive no repo de contratos do mesh)
{
  "contractsRepo": "git@github.com:org/mesh-contracts.git",
  "services": [
    { "name": "billing-service", "repo": "git@github.com:org/billing-service.git", "ref": "main", "path": ".uhura" },
    { "name": "shipping-service", "repo": "git@github.com:org/shipping-service.git", "ref": "main", "path": ".uhura" }
  ]
}
```

```
uhura bump [--config uhura.fleet.json]   # frota inteira
           [--service billing-service]   # só um serviço
           [--check]                     # dry-run: reporta drift, exit code ≠ 0 se houver
           [--pr | --push] --yes         # abre PR (default) ou commita direto
```

**Comportamento (normativo):**

1. **Coleta por artefato, nunca por build:** o bump faz fetch raso de cada repo (`ref`
   configurado) e lê `.uhura/` **commitado** — não instala dependências nem builda nada
   (decisão §22.14). O frescor do artefato é responsabilidade do CI de cada serviço (§27.1.3).
2. **Agrega e valida antes de propor:** schemas válidos, manifesto consistente com os
   schemas, sem colisão de contrato entre serviços (dois produtores do mesmo contrato →
   erro, exceto dual-publish declarado).
3. **Diff semântico, não textual:** o PR gerado descreve o que mudou em termos de contrato
   (novo contrato, campo aditivo, contrato removido, consumidor novo) — é o changelog da
   frota. O linter de compatibilidade e a regeneração de tipos/`BUS.md` continuam no CI do
   repo de contratos do mesh (portão de governança, §27.1.4) — o bump propõe, não publica.
4. **Idempotente:** bump sem mudança = nenhum PR; `--check` é o modo de auditoria contínua
   (job agendado que falha se algum serviço divergiu do catálogo).

**Gatilhos suportados** (os três usam o mesmo comando): job agendado central (ex.: diário,
frota inteira), step no CI de cada serviço (`uhura bump --service <self> --pr` pós-merge),
ou manual durante desenvolvimento.
