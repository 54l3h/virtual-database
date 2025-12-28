import { Module } from '@nestjs/common';
import { StorageService } from './storage.service';
import { SchemaLogic } from './schema/schema.logic';
import { IndexLogic } from './index/index-logic';
import { ConnectionLogic } from './connection/connection-logic';
import { SelectHandler } from './operations/dml/select.handler';
import { InsertHandler } from './operations/dml/insert.handler';
import { DeleteHandler } from './operations/dml/delete.handler';
import { UpdateHandler } from './operations/dml/update.handler';
import { AlterTableHandler } from './operations/ddl/alter-table.handler';
import { DropDatabaseHandler } from './operations/ddl/drop-database.handler';
import { DropTableHandler } from './operations/ddl/drop-table.handler';
import { CreateDatabaseHandler } from './operations/ddl/create-database.handler';
import { CreateTableHandler } from './operations/ddl/create-table.handler';

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
    AlterTableHandler,
    DropDatabaseHandler,
    DropTableHandler,
    CreateDatabaseHandler,
    CreateTableHandler,
  ],
  exports: [StorageService, SchemaLogic, ConnectionLogic],
})
export class StorageModule {}
