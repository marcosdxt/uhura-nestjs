//! Conexão AMQP compartilhada pelo consumer e pelo RPC (client/server).
//!
//! Esta classe é um SUPERVISOR, e não um `connect()`. A diferença não é
//! estilística: sem ela, uma queda de conexão nao produz erro, nao produz log e
//! nao derruba o processo — ela apenas apaga os consumidores. O pod segue
//! `Ready`, o health check passa, e o servico para de consumir para sempre.
//!
//! Aconteceu em producao em 2026-09-22: um restart de no do broker deixou o
//! `dextrolabs-notification` de pe, saudavel e com ZERO consumidores na fila.
//! Nenhuma linha de log. So se descobriu olhando `rabbitmqctl list_queues`.
//!
//! Por isso, tres responsabilidades aqui:
//!
//! 1. reconectar com backoff, para sobreviver a manutencao do broker;
//! 2. avisar quem depende da conexao, para que os consumidores se
//!    RE-INSCREVAM — reconectar sem reassinar devolveria uma conexao viva e uma
//!    fila sem consumidor, que e o pior dos dois mundos porque parece resolvido;
//! 3. expor o estado, para a readiness poder dizer a verdade.

import {
  Inject,
  Injectable,
  Logger,
  Optional,
  type OnApplicationShutdown,
  type OnModuleInit,
} from '@nestjs/common';
import * as amqp from 'amqplib';

import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS } from './constants';
import { UhuraMetrics } from './metrics';

type AmqpConnection = Awaited<ReturnType<typeof amqp.connect>>;

/** Espera entre tentativas: 1s, 2s, 4s… até o teto. */
const BACKOFF_INICIAL_MS = 1_000;
const BACKOFF_MAXIMO_MS = 30_000;

@Injectable()
export class UhuraAmqp implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger('Uhura');
  private connection?: AmqpConnection;
  private conectado = false;
  private encerrando = false;
  private tentativa = 0;
  private timer?: NodeJS.Timeout;
  private readonly aoReconectar: Array<() => Promise<void>> = [];

  constructor(
    @Inject(UHURA_OPTIONS) private readonly options: UhuraModuleOptions,
    @Optional() private readonly metrics?: UhuraMetrics,
  ) {}

  async onModuleInit(): Promise<void> {
    // A primeira conexao e aguardada: subir sem broker esconderia erro de
    // configuracao (URL errada, credencial vencida) atras de uma reconexao
    // silenciosa, e o servico so falharia na primeira mensagem.
    await this.conectar();
  }

  /**
   * `true` quando ha conexao viva. É o que a readiness deve consultar: um pod
   * que nao esta conectado ao broker nao esta pronto para receber trabalho,
   * por mais que o HTTP dele responda.
   */
  isConnected(): boolean {
    return this.conectado;
  }

  /**
   * Registra algo a refazer depois de cada reconexao — tipicamente reabrir o
   * canal e reassinar as filas.
   *
   * Não é chamado na primeira conexão: quem registra já faz a sua própria
   * inscrição inicial no bootstrap. Chamar aqui duplicaria o consumidor.
   */
  onReconnect(fn: () => Promise<void>): void {
    this.aoReconectar.push(fn);
  }

  async createChannel(): Promise<amqp.Channel> {
    if (!this.connection || !this.conectado) {
      throw new Error('conexão AMQP indisponível');
    }
    return this.connection.createChannel();
  }

  async onApplicationShutdown(): Promise<void> {
    // Marca ANTES de fechar: senao o proprio `close` dispara o handler de
    // queda e o processo fica tentando reconectar enquanto termina.
    this.encerrando = true;
    if (this.timer) {
      clearTimeout(this.timer);
    }
    await this.connection?.close().catch(() => undefined);
    this.conectado = false;
  }

  private async conectar(): Promise<void> {
    const connection = await amqp.connect(this.options.amqpUrl);
    this.connection = connection;
    this.conectado = true;
    this.tentativa = 0;

    // `error` e `close` chegam os dois numa queda; `close` sempre vem. Tratar
    // so `error` deixaria passar o fechamento limpo do outro lado, que e
    // exatamente o que um restart de no do broker produz.
    connection.on('error', (err: Error) => {
      this.logger.error(`conexão AMQP com erro: ${err.message}`);
    });
    connection.on('close', () => {
      if (this.encerrando) {
        return;
      }
      this.conectado = false;
      this.logger.warn('conexão AMQP caiu; reconectando');
      this.agendarReconexao();
    });
  }

  private agendarReconexao(): void {
    const espera = Math.min(
      BACKOFF_INICIAL_MS * 2 ** this.tentativa,
      BACKOFF_MAXIMO_MS,
    );
    this.tentativa += 1;
    this.timer = setTimeout(() => {
      void this.tentarReconectar();
    }, espera);
    // `unref` para a espera nao segurar o processo no shutdown.
    this.timer.unref?.();
  }

  private async tentarReconectar(): Promise<void> {
    if (this.encerrando) {
      return;
    }
    try {
      await this.conectar();
      this.metrics?.reconnects.inc();
      this.logger.log('conexão AMQP restabelecida; reassinando');
      for (const fn of this.aoReconectar) {
        await fn();
      }
      this.logger.log('reassinatura concluída');
    } catch (err) {
      this.conectado = false;
      this.logger.error(`reconexão falhou: ${String(err)}`);
      this.agendarReconexao();
    }
  }
}
