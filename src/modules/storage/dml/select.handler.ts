import { Injectable } from '@nestjs/common';
import { BaseStorageOperations } from './base-operation';
import { IndexLogic } from '../index/index-logic';
import { SchemaLogic } from '../schema/schema.logic';
import { AST, SelectAST } from 'src/common/types/ast.type';
import { ISchema } from 'src/common/types/schema.types';
import * as fs from 'node:fs/promises';
import { Operator } from 'src/common/enums/operator.enum';
import { TokenType } from 'src/common/enums/token-type.enum';
import { matchesCondition } from 'src/common/utils/comparison.helper';
import { projectData } from 'src/common/utils/project-data';

@Injectable()
export class SelectHandler extends BaseStorageOperations {
  constructor(
    private readonly indexLogic: IndexLogic,
    private readonly schemaLogic: SchemaLogic,
  ) {
    super();
  }

  async execute(currentDB: string, AST: SelectAST): Promise<any> {
    // get schema => parsed
    const schema: ISchema =
      await this.schemaLogic.readCurrentDBSchema(currentDB);

    // get data file path
    const dataFilePath = this.getDataFilePath(currentDB, AST.table);

    let pkName = this.getPkColumn(AST.table, schema.tables);

    // THERE IS NO TABLE W/O PK
    if (!pkName) {
      throw new Error(`No primary key found for table ${AST.table}`);
    }

    const indexedColumns: string[] = this.getIndexedColumns(
      AST.table,
      schema.tables,
    );

    let allRows: Record<string, any>[] = [];

    // ! open the file before the for and try
    const fileHandle = await fs.open(dataFilePath, 'r');

    try {
      // If WHERE uses an indexed column with EQUAL operator => use index directly
      if (
        AST.where &&
        AST.where.operator === Operator.EQUAL &&
        indexedColumns.includes(AST.where.criterion) // check if the searching with the indexed column
      ) {
        // get index file path
        const indexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
          AST.table,
          AST.where.criterion,
        );

        const results = await this.indexLogic.streamIndexFile(
          TokenType.SELECT,
          indexPath,
          fileHandle,
          AST.where.value,
          AST.where.operator,
        );

        // destruct the data without deleted
        allRows = this.destructTheData(results);
      }
      // WHERE uses non-indexed => use PK index to scan all rows
      else {
        // get the pk index file path
        const pkIndexFilePath = await this.indexLogic.getIndexFilePath(
          currentDB,
          AST.table,
          pkName,
        );

        const allResults = await this.indexLogic.streamIndexFile(
          TokenType.SELECT,
          pkIndexFilePath,
          fileHandle,
        );

        // If there's a WHERE clause, filter rows
        if (AST.where?.criterion) {
          const { criterion, operator, value } = AST.where;

          const matchingResults = allResults.filter(({ row }) =>
            matchesCondition(operator, row[criterion], value),
          );

          // filter by criterion
          // es2l karim
          allRows = this.destructTheData(matchingResults);
        }
        // No WHERE clause => return all rows
        else {
          allRows = this.destructTheData(allResults);
        }
      }
    } finally {
      // ! close the descriptor after the loops
      await fileHandle.close();
    }

    let projectedData: Record<string, any>[] = projectData(
      AST.columns,
      allRows,
    );

    return { data: projectedData };
  }

  private destructTheData(allResults: Record<string, any>[]) {
    return allResults.map(({ row }) => {
      const { deleted, ...data } = row;
      return data;
    });
  }
}
