//! `@uhura/nestjs` — superfície pública do SDK.

import 'reflect-metadata';

export type { UhuraModuleOptions } from './config';
export {
  CLOUDEVENTS_SPEC_VERSION,
  newEnvelope,
  type Envelope,
  type FactType,
} from './envelope';
export { UhuraModule } from './uhura.module';
export { UhuraService, type PublishOptions } from './uhura.service';
export type { CallOptions } from './rpc-client';
export type { ResCode, RpcResult, RpcRequest } from './rpc';
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
