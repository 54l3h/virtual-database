import { Injectable } from '@nestjs/common';
import type {
  AlterTableAST,
  CreateDatabaseAST,
  CreateTableAST,
  DeleteAST,
  DropDatabaseAST,
  DropTableAST,
  InsertAST,
  SelectAST,
  UpdateAST,
} from 'src/common/types/ast.type';
import { ConnectionLogic } from './connection/connection-logic';
import { SelectHandler } from './operations/dml/select.handler';
import { InsertHandler } from './operations/dml/insert.handler';
import { UpdateHandler } from './operations/dml/update.handler';
import { DeleteHandler } from './operations/dml/delete.handler';
import { CreateTableHandler } from './operations/ddl/create-table.handler';
import { CreateDatabaseHandler } from './operations/ddl/create-database.handler';
import { AlterTableHandler } from './operations/ddl/alter-table.handler';
import { DropDatabaseHandler } from './operations/ddl/drop-database.handler';
import { DropTableHandler } from './operations/ddl/drop-table.handler';

@Injectable()
export class StorageService {
  constructor(
    private connectionLogic: ConnectionLogic,
    private readonly selectHandler: SelectHandler,
    private readonly insertHandler: InsertHandler,
    private readonly updateHandler: UpdateHandler,
    private readonly deleteHandler: DeleteHandler,
    private readonly createTableHandler: CreateTableHandler,
    private readonly createDatabaseHandler: CreateDatabaseHandler,
    private readonly alterTableHandler: AlterTableHandler,
    private readonly dropDatbaseHandler: DropDatabaseHandler,
    private readonly dropTableHandler: DropTableHandler,
  ) {}

  async select(AST: SelectAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    return await this.selectHandler.execute(currentDB, AST);
  }

  async insert(AST: InsertAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    return await this.insertHandler.execute(currentDB, AST);
  }

  async update(AST: UpdateAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    return await this.updateHandler.execute(currentDB, AST);
  }

  async delete(AST: DeleteAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    return await this.deleteHandler.execute(currentDB, AST);
  }

  async alterTable(AST: AlterTableAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    return await this.alterTableHandler.execute(currentDB, AST);
  }

  async createTable(AST: CreateTableAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    return await this.createTableHandler.execute(currentDB, AST);
  }

  async dropTable(AST: DropTableAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    await this.dropTableHandler.execute(currentDB, AST);
  }

  async createDatabase(AST: CreateDatabaseAST): Promise<any> {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    await this.createDatabaseHandler.execute(currentDB, AST);
  }

  async dropDatabase(AST: DropDatabaseAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    await this.dropDatbaseHandler.execute(currentDB, AST);
  }
}
