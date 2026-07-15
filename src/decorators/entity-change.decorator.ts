//! `@UhuraEntityChange` — handler de eventos de CDC de uma entidade.
//
// CDC flui pelo mesmo caminho de eventos (outbox → station → fila do domínio),
// então reutiliza o metadado de assinatura; o handler recebe `(data, envelope, tx?)`
// com `envelope.facttype = 'SNAPSHOT'` e `source = 'pg:<tabela>'`. O 3º
// parâmetro opcional é o `PoolClient` da transação do *transactional inbox*
// (dedup + handler na mesma transação; ack só após COMMIT) — use-o para os
// writes do handler serem atômicos com o dedup.

import { UHURA_SUBSCRIBE_METADATA } from '../constants';

/** Opções de `@UhuraEntityChange`. */
export interface UhuraEntityChangeOptions {
  /** Domínio do contrato (ex.: `usuario.info`). */
  domain: string;
  /** Eventos de CDC: `inserted` | `updated` | `removed`. */
  events: string[];
}

/** Marca o método para receber mudanças de entidade (CDC) do domínio. */
export function UhuraEntityChange(options: UhuraEntityChangeOptions): MethodDecorator {
  return (_target, _key, descriptor: TypedPropertyDescriptor<any>) => {
    if (descriptor.value) {
      Reflect.defineMetadata(UHURA_SUBSCRIBE_METADATA, options, descriptor.value);
    }
    return descriptor;
  };
}
