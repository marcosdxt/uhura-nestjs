//! `@uhura/nestjs` — superfície pública do SDK.

import 'reflect-metadata';

export { UhuraAmqp } from './amqp';
export type { UhuraModuleOptions, UhuraMetricsOptions } from './config';
export {
  UhuraMetrics,
  type ConsumerResult,
  type RpcClientResult,
} from './metrics';
export { createMetricsController, DEFAULT_METRICS_PATH } from './metrics.controller';
export {
  CONTROL_EXCHANGE,
  CONTROL_RPC_QUEUE,
  pausesFor,
  type ControlMessage,
  type PausedEntry,
} from './control';
export { UhuraConsumer } from './consumer';
export {
  CLOUDEVENTS_SPEC_VERSION,
  newEnvelope,
  type Envelope,
  type FactType,
  type UhuraEventContext,
} from './envelope';
export { UhuraModule } from './uhura.module';
export {
  exchangeName,
  queueName,
  parkingExchange,
  parkingQueue,
  rpcQueueName,
  resolveGroup,
  validateGroup,
} from './transport';
export { UhuraService, type PublishOptions } from './uhura.service';
export type { CallOptions } from './rpc-client';
export {
  RpcError,
  parseErrorCode,
  type ResCode,
  type RpcResult,
  type RpcRequest,
  type UhuraRpcContext,
} from './rpc';
export {
  UhuraContract,
  type UhuraContractOptions,
} from './decorators/contract.decorator';
export {
  UhuraSubscribe,
  type UhuraSubscribeOptions,
} from './decorators/subscribe.decorator';
export {
  UhuraFunction,
  type UhuraFunctionOptions,
} from './decorators/function.decorator';
export {
  UhuraEntityChange,
  type UhuraEntityChangeOptions,
} from './decorators/entity-change.decorator';
