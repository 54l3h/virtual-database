import { Injectable } from '@nestjs/common';
import { DeleteAST } from 'src/common/types/ast.type';
import { SchemaLogic } from '../schema/schema.logic';
import { IndexLogic } from '../index/index-logic';
import { TokenType } from 'src/common/enums/token-type.enum';
import { matchesCondition } from 'src/common/utils/comparison.helper';
import { appendFile } from 'src/common/utils/append-file';
import * as fs from 'node:fs/promises';
import { BaseStorageOperations } from './base-operation';

@Injectable()
export class DeleteHandler extends BaseStorageOperations {
  constructor(
    private readonly schemaLogic: SchemaLogic,
    private readonly indexLogic: IndexLogic,
  ) {
    super();
  }
  async execute(currentDB: string, AST: DeleteAST): Promise<any> {
    const { table, where } = AST;
    const schema = await this.schemaLogic.readCurrentDBSchema(currentDB);
    const dataFilePath = this.getDataFilePath(currentDB, table);

    let rowsToDelete: Record<string, any>[] = [];

    // Get PK
    let pkName = this.getPkColumn(table, schema.tables);

    // ! open file once for deletion search
    const fileHandle = await fs.open(dataFilePath, 'r');

    try {
      if (!where) {
        // Delete all
        const pkIndexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
          table,
          pkName,
        );

        const results = await this.indexLogic.streamIndexFile(
          TokenType.DELETE,
          pkIndexPath,
          fileHandle,
        );

        rowsToDelete = results.map(({ row }) => row);
      } else {
        // delete with WHERE
        const { criterion, operator, value } = where;
        const isIndexed = await this.indexLogic.isIndexed(
          currentDB,
          table,
          criterion,
        );

        if (isIndexed) {
          // fast way
          const indexPath = await this.indexLogic.getIndexFilePath(
            currentDB,
            table,
            criterion,
          );

          const results = await this.indexLogic.streamIndexFile(
            TokenType.DELETE,
            indexPath,
            fileHandle,
            value,
            operator,
          );

          rowsToDelete = results.map(({ row }) => row);
        } else {
          // slow path
          const pkIndexPath = await this.indexLogic.getIndexFilePath(
            currentDB,
            table,
            pkName,
          );

          const allResults = await this.indexLogic.streamIndexFile(
            TokenType.DELETE,
            pkIndexPath,
            fileHandle,
          );

          // filter by criterion

          const matchingResults = allResults.filter(({ row }) =>
            matchesCondition(operator, row[criterion], value),
          );
          rowsToDelete = matchingResults.map(({ row }) => row);
        }
      }
    } finally {
      // ! close descriptor
      await fileHandle.close();
    }

    if (rowsToDelete.length === 0) {
      throw new Error('No rows found');
    }

    // Mark as deleted
    for (const row of rowsToDelete) {
      await appendFile(dataFilePath, { ...row, deleted: true });
    }

    const indexedColumns: string[] = this.getIndexedColumns(
      table,
      schema.tables,
    );

    // rebuild indexes and rewrite index file
    await this.indexLogic.rebuildIndexes(currentDB, AST.table, indexedColumns, {
      type: TokenType.DELETE,
      deleteInfo: rowsToDelete,
    });

    return {
      success: true,
      message: `${rowsToDelete.length} rows Deleted`,
    };
  }
}
