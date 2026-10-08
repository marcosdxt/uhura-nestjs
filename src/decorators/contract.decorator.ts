//! `@UhuraContract` — anota uma interface/classe de contrato.

import { UHURA_CONTRACT_METADATA } from '../constants';

/** Opções de `@UhuraContract`. */
export interface UhuraContractOptions {
  /** Domínio do contrato (ex.: `usuario.info`). */
  domain: string;
  /** Eventos suportados (ex.: `['started','stopped']`). */
  events: string[];
  /** Campo usado como `partitionkey`/`subject`. */
  partitionId?: string;
}

/** Registra os metadados do contrato na classe-alvo. */
export function UhuraContract(options: UhuraContractOptions): ClassDecorator {
  return (target) => {
    Reflect.defineMetadata(UHURA_CONTRACT_METADATA, options, target);
  };
}
