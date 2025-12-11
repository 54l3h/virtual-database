import { HttpException, Injectable } from '@nestjs/common';
import { ISchema, StorageService } from 'src/storage/storage.service';

@Injectable()
export class SemanticAnalyzerService {
  constructor(private readonly storageService: StorageService) {}

  async checkTableExistence(tableName: string): Promise<boolean> {
    console.log('here');

    const schemaFile: ISchema = await this.storageService.readSchema();

    const isExist = schemaFile.tables.some((table) => table.name === tableName);

    if (!isExist) {
      throw new Error(`Table ${tableName} does not exist`);
    }

    return true;
  }

  async isTableExist(tableName: string): Promise<boolean> {
    const schemaFile: ISchema = await this.storageService.readSchema();

    return schemaFile.tables.some((table) => table.name === tableName);
  }

  async checkColumnExistence(
    tableName: string,
    columnName: string,
  ): Promise<boolean> {
    const schemaFile: ISchema = await this.storageService.readSchema();

    const table = await this.storageService.checkTableExistence(
      tableName,
      schemaFile,
    );

    const exists = table.columns.some((column) => {
      if (columnName === '*') return true;
      return column.name === columnName;
    });

    if (!exists) {
      throw new Error(`Column ${columnName} does not exist`);
    }

    return true;
  }

  async checkColumnsExistence(
    tableName: string,
    columns: string[],
  ): Promise<boolean> {
    for (const column of columns) {
      await this.checkColumnExistence(tableName, column);
    }

    return true;
  }
}
