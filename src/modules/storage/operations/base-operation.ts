import { Injectable } from '@nestjs/common';
import * as path from 'path';
import { IColumn, ITable } from 'src/common/types/schema.types';
import * as fs from 'fs/promises';
import { AST } from 'src/common/types/ast.type';

@Injectable()
export abstract class BaseStorageOperations {
  protected readonly databasesPath = path.join(process.cwd(), 'databases');

  abstract execute(currentDB: string, AST: AST): Promise<any>;

  /**
   * Get auto incremented columns
   */
  protected getAutoIncrementColumns(tableSchema: ITable): any[] {
    return tableSchema.columns.filter((col) => col.autoIncrement);
  }

  /**
   * Get the data file path for a table
   */
  protected getDataFilePath(currentDB: string, tableName: string): string {
    return path.join(
      this.databasesPath,
      currentDB,
      tableName,
      `${tableName}.ndjson`,
    );
  }

  /**
   * Get the primary key column name from a table
   */
  protected getPkColumn(tableName: string, tables: ITable[]) {
    const table = tables.find((tbl) => {
      return tbl.name === tableName;
    });

    const pkColumn = table?.columns.find((col) => {
      return col.primaryKey === true;
    });

    return pkColumn ? pkColumn.name : '';
  }

  /**
   * Get all indexed column names (primary key + unique columns)
   */
  protected getIndexedColumns(tableName: string, tables: ITable[]): string[] {
    const table = tables.find((tbl) => tbl.name === tableName);

    if (!table) {
      return [];
    }

    return table.columns
      .filter((col) => col.primaryKey === true || col.unique === true)
      .map((col) => col.name);
  }

  protected getTableSchema(tableName: string, tables: ITable[]): ITable {
    return tables.find((tbl) => tbl.name === tableName)!;
  }

  /**
   * Read a specific row by offset and length from a file
   */
  protected async getRowByOffset(
    buffer: Buffer<ArrayBuffer>,
    fileHandle: fs.FileHandle,
    offset: number,
    length: number,
  ): Promise<any> {
    await fileHandle.read(buffer, 0, length, offset);
    const rowLine = buffer.toString('utf-8').trim();
    const row = JSON.parse(rowLine);
    return row;
  }

  protected getTimestampColumns(tableSchema: ITable): any[] {
    return tableSchema.columns.filter((col) => this.isTimestampColumn(col));
  }

  /**
   * Check if a column is defined as a TIMESTAMP type
   */
  protected isTimestampColumn(column: IColumn): boolean {
    return column.type === 'TIMESTAMP';
  }

  /**
   * Validates and converts a value to an ISO string
   */
  protected coerceTimestamp(
    columnName: string,
    value: any,
    isProvided: boolean,
  ): string {
    if (!isProvided) {
      return new Date().toISOString();
    }

    const date = new Date(value);
    if (isNaN(date.getTime())) {
      throw new Error(
        `Invalid input for column "${columnName}": expected a timestamp but received "${value}"`,
      );
    }

    return date.toISOString();
  }
}
