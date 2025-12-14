import { Injectable } from '@nestjs/common';
import { StorageService } from '../storage/storage.service';

@Injectable()
export class SemanticAnalyzerService {
  constructor(private readonly storageService: StorageService) {}

  async checkTableExistenceInCurrentDB(tableName: string): Promise<boolean> {
    return await this.storageService.checkTableExistsInCurrentDB(tableName);
  }

  async checkColumnExistence(
    tableName: string,
    columnName: string,
  ): Promise<boolean> {
    const table = await this.storageService.getTableFromCurrentDB(tableName);

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
}
