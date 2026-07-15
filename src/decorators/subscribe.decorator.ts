//! `@UhuraSubscribe` — anota um método como handler de eventos de um domínio.

import { UHURA_SUBSCRIBE_METADATA } from '../constants';

/** Opções de `@UhuraSubscribe`. */
export interface UhuraSubscribeOptions {
  /** Domínio a assinar (ex.: `usuario.info`). */
  domain: string;
  /** Eventos de interesse (ex.: `['started']`). */
  events: string[];
}

/**
 * Marca o método para receber eventos do domínio.
 *
 * Assinatura do handler: `(data, envelope, tx?)`. O 3º parâmetro (opcional)
 * é o `PoolClient` da transação do *transactional inbox*: dedup + handler
 * rodam na mesma transação Postgres, e o `ack` só acontece após o COMMIT.
 * Use `tx` para os writes do handler ganharem atomicidade com o dedup
 * (effectively-once). Handlers `(data, envelope)` seguem funcionando.
 */
export function UhuraSubscribe(options: UhuraSubscribeOptions): MethodDecorator {
  return (_target, _key, descriptor: TypedPropertyDescriptor<any>) => {
    if (descriptor.value) {
      Reflect.defineMetadata(UHURA_SUBSCRIBE_METADATA, options, descriptor.value);
    }
    return descriptor;
  };
}
