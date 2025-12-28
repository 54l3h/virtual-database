import { Injectable } from '@nestjs/common';
import * as fs from 'fs/promises';
import * as path from 'path';
import { ISchema, ITable } from 'src/common/types/schema.types';
import { IndexLogic } from '../index/index-logic';
import { ConnectionLogic } from '../connection/connection-logic';
import { getPostgresDefinition } from 'src/common/constants/postgres.definition';

@Injectable()
export class SchemaLogic {
  constructor(
    private readonly indexLogic: IndexLogic,
    private readonly connectionLogic: ConnectionLogic,
  ) {}
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
  // async createNewDatabase(databaseName: string): Promise<void> {
  //   const dbPath = path.join(this.databasesPath, databaseName);

  //   // create the directory
  //   await fs.mkdir(dbPath, { recursive: true });

  //   // create the initial empty schema for this database
  //   const initialSchema: ISchema = { tables: [] };
  //   await this.updateCurrentDBSchema(databaseName, initialSchema);
  // }

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
  async checkTableExistsInCurrentDB(tableName: string): Promise<boolean> {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    const schema = await this.readCurrentDBSchema(currentDB);
    return schema.tables.some((table) => table.name === tableName);
  }

  async checkDatabaseExistence(databaseName: string): Promise<boolean> {
    const databaseDirPath = path.join(this.databasesPath, databaseName);
    try {
      await fs.access(databaseDirPath);
      return true;
    } catch {
      return false;
    }
  }

  async getMetadata(currentDB: string, tableName: string) {
    const metaFilePath = path.join(
      this.databasesPath,
      currentDB,
      tableName,
      `${tableName}_meta.json`,
    );
    const data = await fs.readFile(metaFilePath, 'utf-8');
    const metadataContent = JSON.parse(data);

    return { metaFilePath, metadataContent };
  }

  async getNextAutoIncrementValue(
    currentDB: string,
    tableName: string,
    columnName: string,
  ): Promise<number> {
    const { metaFilePath, metadataContent } = await this.getMetadata(
      currentDB,
      tableName,
    );

    metadataContent[columnName] = (metadataContent[columnName] || 0) + 1;
    await fs.writeFile(metaFilePath, JSON.stringify(metadataContent), 'utf-8');
    return metadataContent[columnName];
  }

  async decrementAutoIncrement(
    currentDB: string,
    tableName: string,
    columnName: string,
  ): Promise<void> {
    const { metaFilePath, metadataContent } = await this.getMetadata(
      currentDB,
      tableName,
    );

    metadataContent[columnName] = parseInt(metadataContent[columnName]) - 1;
    await fs.writeFile(metaFilePath, JSON.stringify(metadataContent), 'utf-8');
  }

  async checkColumnExistence(
    tableName: string,
    columnName: string,
  ): Promise<boolean> {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    const table = await this.getTableFromCurrentDB(currentDB, tableName);

    return table.columns.some((column) => column.name === columnName);
  }

  async checkColumnsExistence(
    tableName: string,
    columns: string[],
  ): Promise<boolean> {
    if (columns.includes('*')) {
      return true;
    }

    for (const column of columns) {
      const exists = await this.checkColumnExistence(tableName, column);
      if (!exists) {
        return false;
      }
    }

    return true;
  }

  /**
   * creates database directory
   * if the database name called 'postgres' => use the predefined schema
   * else you will initaite the database schema without any table
   */
  async createDatabaseDir(databaseName: string): Promise<void> {
    const databaseDirPath = path.join(this.databasesPath, databaseName);
    await fs.mkdir(databaseDirPath, { recursive: true });

    const databaseSchemaPath = path.join(databaseDirPath, 'schema.json');
    let schema: ISchema = { tables: [] };

    if (databaseName === 'postgres') {
      schema = getPostgresDefinition();
    }

    await fs.writeFile(databaseSchemaPath, JSON.stringify(schema));
  }
}
