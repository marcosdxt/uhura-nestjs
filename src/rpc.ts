//! Tipos de RPC — formato de fio idêntico ao SDK Rust (uhura-core).

/** Código de resultado de um método RPC. */
export type ResCode = 'ok' | 'error' | 'exception';

/** Envelope de resposta de um método RPC. */
export interface RpcResult<T = unknown> {
  data: T | null;
  resCode: ResCode;
  /**
   * Código do erro de negócio (`resCode: 'error'`), ex.: `USER_NOT_FOUND`.
   *
   * Servidores 0.4+ mandam o campo; de servidores antigos (e do driver Rust do
   * device, que manda `errorStack.code`) o cliente o deduz — ver
   * {@link parseErrorCode}. No cliente, falhas locais também ganham código:
   * `TIMEOUT` e `DISCONNECTED`.
   */
  errorCode?: string;
  errorMessage?: string;
  errorStack?: unknown;
}

/** Requisição RPC enviada ao servidor. */
export interface RpcRequest {
  id: string;
  domain: string;
  method: string;
  data: unknown;
}

/**
 * Contexto entregue a um handler `@UhuraFunction` como segundo argumento.
 *
 * `id` é o id da requisição (`RpcRequest.id`): o cliente o gera uma vez por
 * chamada, então é a chave para o servidor implementar idempotência.
 */
export interface UhuraRpcContext {
  /** `RpcRequest.id`. */
  id: string;
  domain: string;
  method: string;
  /** `correlationId` AMQP da requisição (no SDK, igual ao `id`). */
  correlationId?: string;
  /** `true` quando o broker já entregou esta requisição antes (reentrega). */
  redelivered: boolean;
}

const RPC_ERROR: unique symbol = Symbol.for('uhura.RpcError') as never;

/**
 * Erro de negócio de um método RPC.
 *
 * ```ts
 * throw new RpcError('USER_NOT_FOUND', 'Usuário não encontrado.');
 * ```
 *
 * O servidor responde `resCode: 'error'` com `errorCode` e `errorMessage`
 * separados, e `errorStack: {code, ...details}` — o mesmo lugar em que o
 * driver Rust do device põe o código. Qualquer outra exceção continua virando
 * `resCode: 'exception'` (falha inesperada, não regra de negócio).
 */
export class RpcError extends Error {
  /** Marca entre cópias do pacote: `instanceof` falha com dois `node_modules`. */
  readonly [RPC_ERROR] = true;

  constructor(
    readonly code: string,
    message: string,
    readonly details?: Record<string, unknown>,
  ) {
    super(message);
    this.name = 'RpcError';
  }

  /** `true` para um `RpcError` de qualquer cópia do SDK. */
  static is(err: unknown): err is RpcError {
    return (
      typeof err === 'object' &&
      err !== null &&
      (err as Record<symbol, unknown>)[RPC_ERROR] === true
    );
  }
}

/** Prefixo `"CODE: mensagem"` da convenção anterior à 0.4. */
const LEGACY_PREFIX = /^([A-Z][A-Z0-9_]{1,63}): /;

/**
 * Deduz o `errorCode` de uma resposta que não o traz no campo próprio:
 * `errorStack.code` (driver Rust) ou o prefixo `"CODE: mensagem"` (servidores
 * NestJS até a 0.3). A mensagem não é alterada.
 */
export function parseErrorCode(result: RpcResult<unknown>): string | undefined {
  if (result.resCode === 'ok') {
    return undefined;
  }
  if (typeof result.errorCode === 'string' && result.errorCode) {
    return result.errorCode;
  }
  const stack = result.errorStack as { code?: unknown } | undefined;
  if (stack && typeof stack === 'object' && typeof stack.code === 'string' && stack.code) {
    return stack.code;
  }
  return result.errorMessage?.match(LEGACY_PREFIX)?.[1];
}
