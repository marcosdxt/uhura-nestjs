//! Tipos de RPC — formato de fio idêntico ao SDK Rust (uhura-core).

/** Código de resultado de um método RPC. */
export type ResCode = 'ok' | 'error' | 'exception';

/** Envelope de resposta de um método RPC. */
export interface RpcResult<T = unknown> {
  data: T | null;
  resCode: ResCode;
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
