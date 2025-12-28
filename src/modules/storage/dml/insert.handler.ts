import { Injectable } from '@nestjs/common';
import { BaseStorageOperations } from './base-operation';
import { InsertAST } from 'src/common/types/ast.type';
import { appendFile } from 'src/common/utils/append-file';
import { IndexLogic } from '../index/index-logic';
import { SchemaLogic } from '../schema/schema.logic';
import * as fs from 'node:fs/promises';
import { ITable } from 'src/common/types/schema.types';

@Injectable()
export class InsertHandler extends BaseStorageOperations {
  constructor(
    private readonly indexLogic: IndexLogic,
    private readonly schemaLogic: SchemaLogic,
  ) {
    super();
  }

  async execute(currentDB: string, AST: InsertAST): Promise<any> {
    const schema = await this.schemaLogic.readCurrentDBSchema(currentDB);
    const dataFilePath = this.getDataFilePath(currentDB, AST.table);
    const tableSchema: ITable = this.getTableSchema(AST.table, schema.tables);
    const indexedColumns = this.getIndexedColumns(AST.table, schema.tables);

    // Gather values into rows
    let rows: any[][] = [];
    let currentRow: any[] = [];
    for (let i = 0; i < AST.values.length; i++) {
      currentRow.push(AST.values[i]);
      if (currentRow.length === AST.columns.length) {
        rows.push(currentRow);
        currentRow = [];
      }
    }

    // Get starting offset once
    let { size: currentOffset } = await fs.stat(dataFilePath);

    for (const rowValues of rows) {
      const row: Record<string, any> = { deleted: false };

      // inject column names & values
      for (let i = 0; i < AST.columns.length; i++) {
        row[AST.columns[i]] = rowValues[i];
      }

      // Handle Auto-Increment
      const autoIncrementColumns = this.getAutoIncrementColumns(tableSchema);
      for (const col of autoIncrementColumns) {
        row[col.name] = await this.schemaLogic.getNextAutoIncrementValue(
          currentDB,
          AST.table,
          col.name,
        );
      }

      // Handle Timestamps
      const timestampColumns = this.getTimestampColumns(tableSchema);
      for (const col of timestampColumns) {
        const isProvided = AST.columns.includes(col.name);
        row[col.name] = this.coerceTimestamp(
          col.name,
          row[col.name],
          isProvided,
        );
      }

      // Existence Check
      try {
        for (const colName of indexedColumns) {
          const indexPath = await this.indexLogic.getIndexFilePath(
            currentDB,
            AST.table,
            colName,
          );
          await this.indexLogic.checkExistence(
            indexPath,
            row[colName],
            colName,
          );
        }
      } catch (error) {
        const autoIncrementColumns = this.getAutoIncrementColumns(tableSchema);
        
        if (autoIncrementColumns.length > 0) {
          for (const col of autoIncrementColumns) {
            await this.schemaLogic.decrementAutoIncrement(
              currentDB,
              AST.table,
              col.name,
            );
          }
        }
        
        throw error;
      }

      await appendFile(dataFilePath, row);
      const rowLength = Buffer.byteLength(JSON.stringify(row) + '\n');

      // Update Indexes
      for (const colName of indexedColumns) {
        const indexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
          AST.table,
          colName,
        );
        const indexEntry = {
          value: row[colName],
          offset: currentOffset,
          length: rowLength,
        };
        await appendFile(indexPath, indexEntry);
      }

      // Increment offset for the next row insertion
      currentOffset += rowLength;
    }

    return { success: true, message: `${rows.length} rows inserted` };
  }
}