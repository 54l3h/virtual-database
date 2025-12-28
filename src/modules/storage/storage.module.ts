import { Module } from '@nestjs/common';
import { StorageService } from './storage.service';
import { SchemaLogic } from './schema/schema.logic';
import { IndexLogic } from './index/index-logic';
import { ConnectionLogic } from './connection/connection-logic';
import { SelectHandler } from './dml/select.handler';
import { InsertHandler } from './dml/insert.handler';
import { DeleteHandler } from './dml/delete.handler';
import { UpdateHandler } from './dml/update.handler';

@Module({
  providers: [
    StorageService,
    SchemaLogic,
    IndexLogic,
    ConnectionLogic,
    SelectHandler,
    InsertHandler,
    DeleteHandler,
    UpdateHandler,
  ],
  exports: [StorageService, SchemaLogic, ConnectionLogic, IndexLogic],
})
export class StorageModule {}
