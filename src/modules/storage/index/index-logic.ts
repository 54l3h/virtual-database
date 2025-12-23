import { Injectable } from '@nestjs/common';
import * as fs from 'fs/promises';
import * as path from 'path';
import { createReadStream } from 'fs';
import * as readline from 'readline/promises';

@Injectable()
export class IndexLogic {
  private readonly databasesPath = path.join(process.cwd(), 'databases');

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

  // check duplicates before insertion and updating in indexed columns
  async checkDuplicates(
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
}
