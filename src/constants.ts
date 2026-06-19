//! Tokens de injeção e chaves de metadados.

/** Token: opções do módulo (`UhuraModuleOptions`). */
export const UHURA_OPTIONS = Symbol('UHURA_OPTIONS');

/** Token: pool de conexões PostgreSQL (`pg.Pool`). */
export const UHURA_PG = Symbol('UHURA_PG');

/** Metadado de método anotado com `@UhuraSubscribe`. */
export const UHURA_SUBSCRIBE_METADATA = 'uhura:subscribe';

/** Metadado de classe anotada com `@UhuraContract`. */
export const UHURA_CONTRACT_METADATA = 'uhura:contract';
