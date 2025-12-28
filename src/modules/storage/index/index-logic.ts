import { Injectable } from '@nestjs/common';
import * as fs from 'fs/promises';
import * as path from 'path';
import { createReadStream } from 'fs';
import * as readline from 'readline/promises';
import { Index, ITable } from 'src/common/types/schema.types';
import { DmlOperations } from 'src/common/types/query.types';
import { Operator } from 'src/common/enums/operator.enum';
import { matchesCondition } from 'src/common/utils/comparison.helper';
import { TokenType } from 'src/common/enums/token-type.enum';

@Injectable()
export class IndexLogic {
  protected readonly databasesPath = path.join(process.cwd(), 'databases');

  // to check if the column has an index or not based on this you will know which methodology you will use to searching
  async isIndexed(
    currentDB: string,
    tableName: string,
    columnName: string,
  ): Promise<Boolean> {
    const indexFilePath = await this.getIndexFilePath(
      currentDB,
      tableName,
      columnName,
    );

    try {
      await fs.access(indexFilePath);
      return true;
    } catch (error) {
      return false;
    }
  }

  // based on the columns like => (PK, UNIQUE) => directly create index files
  async createIndexFile(
    currentDB: string,
    tableName: string,
    columnName: string,
  ): Promise<void> {
    const indexFilePath = path.join(
      this.databasesPath,
      currentDB,
      tableName,
      `${columnName}_idx.ndjson`,
    );

    try {
      // empty file you shouldn't add any thing here
      // then you will append
      await fs.writeFile(indexFilePath, '');
    } catch (error) {
      throw new Error(error);
    }
  }

  // get the index file path
  async getIndexFilePath(
    currentDB: string,
    table: string,
    column: string,
  ): Promise<string> {
    // return the index file path
    return path.join(
      this.databasesPath,
      currentDB,
      table,
      `${column}_idx.ndjson`,
    );
  }

  // check existence before insertion and updating in indexed columns
  async checkExistence(
    indexPath: string,
    value: any,
    columnName: string,
  ): Promise<void> {
    const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
    const lines = readline.createInterface({ input: fileStream });

    for await (const line of lines) {
      const { value: existingValue } = JSON.parse(line);
      if (existingValue == value) {
        throw new Error(`${value} already exists in column ${columnName}`);
      }
    }
  }

  async rewriteIndexFile(
    indexFilePath: string,
    newIndex: Index[],
  ): Promise<void> {
    let indexNewContent = '';
    for (const entry of newIndex) {
      indexNewContent += JSON.stringify(entry) + '\n';
    }

    // rewrite the file again
    await fs.writeFile(indexFilePath, indexNewContent, 'utf-8');
  }

  async rebuildIndexes(
    currentDB: string,
    table: string,
    indexedColumns: string[],
    options: {
      type: TokenType.UPDATE | TokenType.DELETE;
      updateInfo?: {
        oldOffset: number;
        newOffset: number;
        newLength: number;
        updatedRow: Record<string, any>;
      }[];
      deleteInfo?: Record<string, any>[];
    },
  ): Promise<void> {
    for (const colName of indexedColumns) {
      const indexPath = await this.getIndexFilePath(currentDB, table, colName);

      const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
      const lines = readline.createInterface({ input: fileStream });

      const newIndex: Index[] = [];

      // handle rebuild indexes for updating
      if (options.type === TokenType.UPDATE) {
        const oldOffsets = options.updateInfo!.map((info) => info.oldOffset);

        // keep lines that aren't going to update
        for await (const line of lines) {
          const entry = JSON.parse(line);
          if (!oldOffsets.includes(entry.offset)) {
            newIndex.push(entry);
          }
        }

        // add new entries for updated rows
        for (const info of options.updateInfo!) {
          newIndex.push({
            value: info.updatedRow[colName],
            offset: info.newOffset,
            length: info.newLength,
          });
        }
      } else {
        // handle rebuild indexes for deleting
        const indexEntries: Index[] = [];
        for await (const line of lines) {
          indexEntries.push(JSON.parse(line));
        }

        // keep lines that aren't going to delete
        for (const entry of indexEntries) {
          const isDeleted = options.deleteInfo?.some(
            (row) => entry.value == row[colName],
          );

          if (!isDeleted) {
            newIndex.push(entry);
          }
        }
      }

      await this.rewriteIndexFile(indexPath, newIndex);
    }
  }

  async streamIndexFile(
    operation: DmlOperations,
    filePath: string,
    fileHandle: fs.FileHandle,
    targetValue?: any,
    operator?: Operator,
  ): Promise<{ row: Record<string, any>; offset: number; length: number }[]> {
    const rowsToChange: any[] = [];
    const fileStream = createReadStream(filePath, { encoding: 'utf-8' });
    const lines = readline.createInterface({ input: fileStream });

    for await (const line of lines) {
      const { value: existingValue, offset, length } = JSON.parse(line);

      if (operation !== TokenType.INSERT) {
        // If no operator is provided => (Full Scan).
        // If operator is provided => matchesCondition.
        const isMatch =
          !operator || matchesCondition(operator, existingValue, targetValue);

        if (isMatch) {
          const buffer = Buffer.alloc(length);
          const row = await this.getRowByOffset(
            buffer,
            fileHandle,
            offset,
            length,
          );

          if (row) {
            rowsToChange.push({ row, offset, length });
          }
        }

        // fast searching
        if (
          operator === Operator.EQUAL &&
          isMatch &&
          targetValue !== undefined
        ) {
          break;
        }
      }
    }

    return rowsToChange;
  }

  async getRowByOffset(
    buffer: Buffer,
    fileHandle: fs.FileHandle,
    offset: number,
    length: number,
  ): Promise<any> {
    await fileHandle.read(buffer, 0, length, offset);
    const rowLine = buffer.toString('utf-8').trim();
    return JSON.parse(rowLine);
  }
}
