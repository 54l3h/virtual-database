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
import type { ISchema, IColumn } from '../../common/types/schema.types';
import { SchemaLogic } from './schema/schema.logic';
import { RowLogic } from './access_methods/row-logic';
import { ConnectionLogic } from './connection/connection-logic';

@Injectable()
export class StorageService {
  constructor(
    private schemaLogic: SchemaLogic,
    private connectionLogic: ConnectionLogic,
    private rowLogic: RowLogic,
  ) {}

  // databases directory
  private readonly databasesPath = path.join(process.cwd(), 'databases');

  // TODO:
  async checkDatabaseExistence(databaseName: string): Promise<boolean> {
    const databaseDirPath = path.join(this.databasesPath, databaseName);
    try {
      await fs.access(databaseDirPath);
      return true;
    } catch {
      return false;
    }
  }

  async createDatabase(AST: CreateDatabaseAST): Promise<any> {
    await this.schemaLogic.createDatabasesDir(); // To ignore errors
    const isExist = await this.checkDatabaseExistence(AST.name);
    if (isExist) {
      throw new Error(`Database ${AST.name} already exists`);
    }

    await this.createDatabaseDir(AST.name);

    return { message: `Database ${AST.name} created successfully` };
  }

  // check if the table exists into the connected db or not
  // ! should be moved to the schema logic
  async checkTableExistsInCurrentDB(tableName: string): Promise<boolean> {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    const schema = await this.schemaLogic.readCurrentDBSchema(currentDB);
    return schema.tables.some((table) => table.name === tableName);
  }

  /**
   * ! row operations
   */
  async select(AST: SelectAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    return await this.rowLogic.findRows(currentDB, AST);
  }

  async insert(AST: InsertAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    await this.rowLogic.insertRows(currentDB, AST);
    return { success: true, message: 'Rows inserted' };
  }

  async update(AST: UpdateAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    return await this.rowLogic.updateRows(currentDB, AST);
  }

  async delete(AST: DeleteAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    return await this.rowLogic.deleteRows(currentDB, AST);
  }

  // for altering the table
  // TODO:
  // add column => change the structure of the database schema
  // drop column => change the structure of the database schema
  async alterTable(AST: AlterTableAST) {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    await this.schemaLogic.alterTableStructure(currentDB, AST);
    return { message: `Table ${AST.name} altered` };
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
