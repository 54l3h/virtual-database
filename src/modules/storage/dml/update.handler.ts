import { Injectable } from '@nestjs/common';
import { BaseStorageOperations } from './base-operation';
import { UpdateAST } from 'src/common/types/ast.type';
import { SchemaLogic } from '../schema/schema.logic';
import { IndexLogic } from '../index/index-logic';
import * as fs from 'node:fs/promises';
import { TokenType } from 'src/common/enums/token-type.enum';
import { matchesCondition } from 'src/common/utils/comparison.helper';
import { appendFile } from 'src/common/utils/append-file';

@Injectable()
export class UpdateHandler extends BaseStorageOperations {
  constructor(
    private readonly schemaLogic: SchemaLogic,
    private readonly indexLogic: IndexLogic,
  ) {
    super();
  }
  async execute(currentDB: string, AST: UpdateAST): Promise<any> {
    const { table, updates, where } = AST;
    const { criterion, operator, value } = where!;

    const schema = await this.schemaLogic.readCurrentDBSchema(currentDB);
    const dataFilePath = this.getDataFilePath(currentDB, table);

    // indexed columns
    const indexedColumns: string[] = this.getIndexedColumns(
      table,
      schema.tables,
    );

    // check if new value already exists
    for (const [columnName, newValue] of Object.entries(updates)) {
      if (indexedColumns.includes(columnName)) {
        const indexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
          table,
          columnName,
        );
        await this.indexLogic.checkExistence(indexPath, newValue, columnName);
      }
    }

    // find row to update
    const isCriterionIndexed = await this.indexLogic.isIndexed(
      currentDB,
      table,
      criterion,
    );

    let rowsToUpdate: {
      row: Record<string, any>;
      offset: number;
      length: number;
    }[] = [];

    // ! open the file before the loops
    const fileHandle = await fs.open(dataFilePath, 'r');

    try {
      if (isCriterionIndexed) {
        // by index
        const indexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
          table,
          criterion,
        );

        rowsToUpdate = await this.indexLogic.streamIndexFile(
          TokenType.UPDATE,
          indexPath,
          fileHandle,
          value,
          operator,
        );
      } else {
        // slow way
        let pkName = this.getPkColumn(table, schema.tables);

        const pkIndexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
          table,
          pkName,
        );

        const allResults = await this.indexLogic.streamIndexFile(
          TokenType.UPDATE,
          pkIndexPath,
          fileHandle,
        );

        // filter manually by criterion
        rowsToUpdate = allResults.filter(({ row }) =>
          matchesCondition(operator, row[criterion], value),
        );
      }
    } finally {
      // ! close descriptor after finding rows
      await fileHandle.close();
    }

    if (rowsToUpdate.length === 0) {
      throw new Error('No row found');
    }

    // to update the offsets and length in the index files
    const newRowsInfo: {
      oldOffset: number;
      newOffset: number;
      newLength: number;
      updatedRow: any;
    }[] = [];

    // go through all rows to mark as deleted and append updated versions
    for (const item of rowsToUpdate) {
      // mark old as deleted
      // spread the old row data + override the deleted with true
      const deletedRow = { ...item.row, deleted: true };
      // append into the data file
      await appendFile(dataFilePath, deletedRow);

      // get offset BEFORE appending new row (the new version) which should be included into the index file
      const { size: newOffset } = await fs.stat(dataFilePath);

      // add new updated row
      // the old data + updates (override) + by default => deleted: false
      const updatedRow = { ...item.row, ...updates };
      const newLength = Buffer.byteLength(JSON.stringify(updatedRow) + '\n');

      // append the updated version
      await appendFile(dataFilePath, updatedRow);

      // save info for the index
      newRowsInfo.push({
        oldOffset: item.offset,
        newOffset: newOffset,
        newLength: newLength,
        updatedRow: updatedRow,
      });
    }

    // indexes rebuild and rewrite
    await this.indexLogic.rebuildIndexes(currentDB, table, indexedColumns, {
      type: TokenType.UPDATE,
      updateInfo: newRowsInfo,
    });

    return { success: true, message: `${rowsToUpdate.length} rows updated` };
  }
}
