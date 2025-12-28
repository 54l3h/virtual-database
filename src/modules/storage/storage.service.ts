import { Injectable } from '@nestjs/common';
import * as fs from 'fs/promises';
import path from 'path';
import { DataType } from '../../common/enums/data-type.enum';
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
import type { ISchema } from '../../common/types/schema.types';
import { SchemaLogic } from './schema/schema.logic';
import { ConnectionLogic } from './connection/connection-logic';
import { SelectHandler } from './dml/select.handler';
import { InsertHandler } from './dml/insert.handler';
import { UpdateHandler } from './dml/update.handler';
import { DeleteHandler } from './dml/delete.handler';

@Injectable()
export class StorageService {
  constructor(
    private schemaLogic: SchemaLogic,
    private connectionLogic: ConnectionLogic,
    private readonly selectHandler: SelectHandler,
    private readonly insertHandler: InsertHandler,
    private readonly updateHandler: UpdateHandler,
    private readonly deleteHandler: DeleteHandler,
  ) {}

  // databases directory
  private readonly databasesPath = path.join(process.cwd(), 'databases');

  /**
   * ! row operations
   */
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
    await this.schemaLogic.alterTableStructure(currentDB, AST);
    return { message: `Table ${AST.name} altered` };
  }

  async createTable(AST: CreateTableAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    await this.schemaLogic.createNewTable(currentDB, AST);
    return { message: `Table ${AST.name} created successfully` };
  }

  async dropTable(AST: DropTableAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    await this.schemaLogic.removeTable(currentDB, AST.name);
    return { message: `Table ${AST.name} dropped` };
  }

  async createDatabase(AST: CreateDatabaseAST): Promise<any> {
    await this.schemaLogic.createDatabasesDir(); // To ignore errors
    const isExist = await this.schemaLogic.checkDatabaseExistence(AST.name);
    if (isExist) {
      throw new Error(`Database ${AST.name} already exists`);
    }

    await this.createDatabaseDir(AST.name);

    return { message: `Database ${AST.name} created successfully` };
  }

  async dropDatabase(AST: DropDatabaseAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    if (AST.name === currentDB)
      throw new Error(
        `You cannot drop the database because you are connected to this db`,
      );

    await this.schemaLogic.removeDatabase(AST.name);
    return { message: `Database ${AST.name} dropped` };
  }

  /**
   * creates database directory
   * if the database name called 'postgres' => use the predefined schema
   * else you will initaite the database schema without any table
   */
  private async createDatabaseDir(databaseName: string): Promise<void> {
    const databaseDirPath = path.join(this.databasesPath, databaseName);
    await fs.mkdir(databaseDirPath, { recursive: true });

    const databaseSchemaPath = path.join(databaseDirPath, 'schema.json');
    let schema: ISchema = { tables: [] };

    if (databaseName === 'postgres') {
      schema = {
        tables: [
          {
            name: 'users',
            columns: [
              {
                name: 'name',
                type: DataType.VARCHAR,
                length: 255,
              },
              {
                name: 'currentDB',
                type: DataType.VARCHAR,
                length: 255,
              },
            ],
          },
        ],
      };
    }

    await fs.writeFile(databaseSchemaPath, JSON.stringify(schema));
  }
}
