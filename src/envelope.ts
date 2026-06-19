//! Envelope CloudEvents 1.0 + extensões Uhura.
//
// Os nomes de campo seguem EXATAMENTE o SDK Rust (uhura-core) para interop:
// o JSON gravado aqui é desserializado lá e vice-versa.

export const CLOUDEVENTS_SPEC_VERSION = '1.0';

/** Natureza do fato carregado pelo envelope. */
export type FactType = 'EVENT' | 'SNAPSHOT' | 'DELTA';

/** Envelope padrão de toda mensagem do bus. */
export interface Envelope {
  /** Identificador único; chave de deduplicação no Inbox. */
  id: string;
  /** Serviço/instância produtor. */
  source: string;
  /** Versão da spec CloudEvents. */
  specversion: string;
  /** `<domínio>.<evento>`. */
  type: string;
  /** Id da partição/entidade (= partitionkey). */
  subject?: string;
  /** Timestamp do fato (RFC 3339). */
  time?: string;
  /** Tipo de conteúdo de `data`. */
  datacontenttype?: string;
  /** URI/versão do schema do contrato. */
  dataschema?: string;
  /** Chave de ordenação (roteamento consistent-hash). */
  partitionkey?: string;
  /** Sequência monotônica por partição (guarda de ordem). */
  sequence?: number;
  /** Natureza do fato. */
  facttype?: FactType;
  /** Contexto de trace W3C. */
  traceparent?: string;
  /** Estado de trace W3C. */
  tracestate?: string;
  /** Conteúdo do contrato. */
  data?: unknown;
}

/** Cria um envelope mínimo válido (CloudEvents 1.0). */
export function newEnvelope(id: string, source: string, type: string): Envelope {
  return {
    id,
    source,
    specversion: CLOUDEVENTS_SPEC_VERSION,
    type,
    datacontenttype: 'application/json',
  };
}
