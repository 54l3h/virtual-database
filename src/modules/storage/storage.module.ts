import { Module } from '@nestjs/common';
import { StorageService } from './storage.service';
import { SchemaLogic } from './schema/schema.logic';
import { IndexLogic } from './index/index-logic';
import { RowLogic } from './access_methods/row-logic';
import { ConnectionLogic } from './connection/connection-logic';

@Module({
  providers: [
    StorageService,
    SchemaLogic,
    IndexLogic,
    RowLogic,
    ConnectionLogic,
  ],
  exports: [StorageService, SchemaLogic, ConnectionLogic],
})
export class StorageModule {}
