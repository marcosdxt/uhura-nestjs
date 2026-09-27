//! `UhuraModule` — módulo dinâmico do SDK.

import { type DynamicModule, Module, type Provider } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { Pool } from 'pg';

import { UhuraAmqp } from './amqp';
import type { UhuraModuleOptions } from './config';
import { UHURA_OPTIONS, UHURA_PG } from './constants';
import { UhuraConsumer } from './consumer';
import { UhuraRpcClient } from './rpc-client';
import { UhuraRpcServer } from './rpc-server';
import { validateGroup } from './transport';
import { UhuraService } from './uhura.service';

@Module({})
export class UhuraModule {
  /** Registra o Uhura na aplicação (pool PG + publisher + consumidor). */
  static forRoot(options: UhuraModuleOptions): DynamicModule {
    // Grupo explícito e inválido é erro de configuração: falha já aqui, e não
    // só quando o primeiro consumidor subir.
    if (options.group !== undefined && options.group.trim() !== '') {
      validateGroup(options.group.trim());
    }
    const optionsProvider: Provider = {
      provide: UHURA_OPTIONS,
      useValue: options,
    };
    const poolProvider: Provider = {
      provide: UHURA_PG,
      useFactory: (): Pool => new Pool({ connectionString: options.postgresUrl }),
    };

    return {
      module: UhuraModule,
      global: true,
      imports: [DiscoveryModule],
      providers: [
        optionsProvider,
        poolProvider,
        UhuraAmqp,
        UhuraRpcClient,
        UhuraRpcServer,
        UhuraService,
        UhuraConsumer,
      ],
      exports: [UhuraService],
    };
  }
}
