import { Injectable } from '@nestjs/common';
import * as fs from 'fs/promises';
import * as path from 'path';
import { AlterTableAST, CreateTableAST } from 'src/common/types/ast.type';
import { IColumn, ISchema, ITable } from 'src/common/types/schema.types';
import { IndexLogic } from '../index/index-logic';
import { DataType } from 'src/common/enums/data-type.enum';

@Injectable()
export class SchemaLogic {
  constructor(private readonly indexLogic: IndexLogic) {}
  // Use the path logic from your original code
  private readonly databasesPath = path.join(process.cwd(), 'databases');

  /**
   * ! database operations
   */
  // create the root databases directory if it doesn't exist
  async createDatabasesDir(): Promise<void> {
    await fs.mkdir(this.databasesPath, { recursive: true });
  }

  // create the database directory and the initial schema file
  async createNewDatabase(databaseName: string): Promise<void> {
    const dbPath = path.join(this.databasesPath, databaseName);

    // create the directory
    await fs.mkdir(dbPath, { recursive: true });

    // create the initial empty schema for this database
    const initialSchema: ISchema = { tables: [] };
    await this.updateCurrentDBSchema(databaseName, initialSchema);
  }

  // remove the database directory and everything inside it
  async removeDatabase(databaseName: string): Promise<void> {
    const dbPath = path.join(this.databasesPath, databaseName);
    await fs.rm(dbPath, { recursive: true, force: true });
  }

  /**
   * ! table operations
   */
  // create table folder, data files, and index files
  async createNewTable(currentDB: string, AST: CreateTableAST): Promise<void> {
    // get the db schema to modify it and add the new table
    const schema = await this.readCurrentDBSchema(currentDB);

    // add the new table to the schema
    schema.tables.push({
      name: AST.name,
      columns: AST.columns,
    });

    // Create table directory => databases path(root), current db name, table name
    const tableDirPath = path.join(this.databasesPath, currentDB, AST.name);
    await fs.mkdir(tableDirPath, { recursive: true });

    // create index files for PK and UNIQUE columns
    for (const column of AST.columns) {
      if (column.primaryKey || column.unique) {
        await this.indexLogic.createIndexFile(currentDB, AST.name, column.name);
      }
    }

    // Create metadata file to track the auto incremented column values
    const autoIncrementedColumn = AST.columns.find(
      (col) => col.autoIncrement === true,
    );

    if (autoIncrementedColumn) {
      const metaFilePath = path.join(tableDirPath, `${AST.name}_meta.json`);
      const metadata = { [`${autoIncrementedColumn.name}`]: 0 };
      await fs.writeFile(metaFilePath, JSON.stringify(metadata), 'utf-8');
    }

    // update the schema to add the table
    await this.updateCurrentDBSchema(currentDB, schema);

    // Create NDJSON data file for the table data
    const dataFilePath = path.join(tableDirPath, `${AST.name}.ndjson`);
    await fs.writeFile(dataFilePath, '');
  }

  // remove table entry from schema and delete the table dir
  async removeTable(currentDB: string, tableName: string): Promise<void> {
    const schema = await this.readCurrentDBSchema(currentDB);

    // filter out the table from the schema list
    schema.tables = schema.tables.filter((t) => t.name !== tableName);
    await this.updateCurrentDBSchema(currentDB, schema);

    // delete the physical directory
    const tableDirPath = path.join(this.databasesPath, currentDB, tableName);
    await fs.rm(tableDirPath, { recursive: true, force: true });
  }

  // for altering the table
  // add column => change the structure of the database schema
  // drop column => change the structure of the database schema
  async alterTableStructure(currentDB: string, AST: AlterTableAST) {
    // 2 actions
    // add column
    // drop column

    // you checked the existence
    // get the schema file
    // access the schema file
    // add column to the table
    // create index file if it primary key = true or unique = true

    const schema: ISchema = await this.readCurrentDBSchema(currentDB);

    const table = schema.tables.find((table) => table.name === AST.name);

    if (!table) {
      throw new Error(`Table ${AST.name} not found`);
    }

    // add column
    if (AST.columnName && AST.dataType) {
      const newColumn: IColumn = {
        name: AST.columnName as string,
        type: AST.dataType as DataType,
      };

      table.columns.push(newColumn);
    }

    await this.updateCurrentDBSchema(currentDB, schema);
  }

  /**
   * ! schema helpers
   */
  // get the table data (NAME,COLUMNS,PK)
  async getTableFromCurrentDB(
    currentDB: string,
    tableName: string,
  ): Promise<ITable> {
    const schema = await this.readCurrentDBSchema(currentDB);
    const table: ITable = schema.tables.find(
      (table) => table.name === tableName,
    )!;

    if (!table) {
      throw new Error(`Table ${tableName} not exist in current database`);
    }

    return table;
  }

  // get the connected db schema
  async readCurrentDBSchema(currentDB: string): Promise<ISchema> {
    const schemaFilePath = path.join(
      this.databasesPath,
      currentDB,
      'schema.json',
    );
    try {
      await fs.access(schemaFilePath);
    } catch (error) {
      throw new Error(`Schema file not exist for database ${currentDB}`);
    }
    const data = await fs.readFile(schemaFilePath, 'utf-8');
    return JSON.parse(data);
  }

  // when you are going to connect to another db
  async updateCurrentDBSchema(
    currentDB: string,
    schema: ISchema,
  ): Promise<void> {
    const schemaFilePath = path.join(
      this.databasesPath,
      currentDB,
      'schema.json',
    );
    await fs.writeFile(schemaFilePath, JSON.stringify(schema));
  }

  // check if the table exists into the connected db or not
  async checkTableExistsInCurrentDB(
    schema: ISchema,
    tableName: string,
  ): Promise<boolean> {
    return schema.tables.some((table) => table.name === tableName);
  }
}
