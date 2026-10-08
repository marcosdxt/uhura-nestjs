//! `@UhuraFunction` — anota um método como endpoint RPC.

import { UHURA_FUNCTION_METADATA } from '../constants';

/** Opções de `@UhuraFunction`. */
export interface UhuraFunctionOptions {
  /** Domínio do contrato (ex.: `usuario.info`). */
  domain: string;
  /** Nome do método (ex.: `hydrate`). */
  method: string;
}

/** Registra o método como handler RPC de `(domain, method)`. */
export function UhuraFunction(options: UhuraFunctionOptions): MethodDecorator {
  return (_target, _key, descriptor: TypedPropertyDescriptor<any>) => {
    if (descriptor.value) {
      Reflect.defineMetadata(UHURA_FUNCTION_METADATA, options, descriptor.value);
    }
    return descriptor;
  };
}
