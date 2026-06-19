//! Conexão AMQP compartilhada pelo consumer e pelo RPC (client/server).

import {
  Inject,
  Injectable,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import * as amqp from 'amqplib';

import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS } from './constants';

type AmqpConnection = Awaited<ReturnType<typeof amqp.connect>>;

@Injectable()
export class UhuraAmqp implements OnModuleInit, OnModuleDestroy {
  private connection?: AmqpConnection;

  constructor(
    @Inject(UHURA_OPTIONS) private readonly options: UhuraModuleOptions,
  ) {}

  async onModuleInit(): Promise<void> {
    this.connection = await amqp.connect(this.options.amqpUrl);
  }

  async createChannel(): Promise<amqp.Channel> {
    if (!this.connection) {
      throw new Error('conexão AMQP não inicializada');
    }
    return this.connection.createChannel();
  }

  async onModuleDestroy(): Promise<void> {
    await this.connection?.close().catch(() => undefined);
  }
}
