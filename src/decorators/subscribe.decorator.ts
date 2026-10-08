//! `@UhuraSubscribe` — anota um método como handler de eventos de um domínio.

import { UHURA_SUBSCRIBE_METADATA } from '../constants';

/** Opções de `@UhuraSubscribe`. */
export interface UhuraSubscribeOptions {
  /** Domínio a assinar (ex.: `usuario.info`). */
  domain: string;
  /** Eventos de interesse (ex.: `['started']`). */
  events: string[];
}

/** Marca o método para receber eventos do domínio. */
export function UhuraSubscribe(options: UhuraSubscribeOptions): MethodDecorator {
  return (_target, _key, descriptor: TypedPropertyDescriptor<any>) => {
    if (descriptor.value) {
      Reflect.defineMetadata(UHURA_SUBSCRIBE_METADATA, options, descriptor.value);
    }
    return descriptor;
  };
}
