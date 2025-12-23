import { Injectable } from '@nestjs/common';
import { StorageService } from '../storage/storage.service';
import { SchemaLogic } from '../storage/schema/schema.logic';
import { ConnectionLogic } from '../storage/connection/connection-logic';

@Injectable()
export class SemanticAnalyzerService {
  constructor(
    private readonly storageService: StorageService,
    private readonly schemaLogic: SchemaLogic,
    private readonly connectionLogic: ConnectionLogic,
  ) {}

  async checkTableExistenceInCurrentDB(tableName: string): Promise<boolean> {
    return await this.storageService.checkTableExistsInCurrentDB(tableName);
  }

  async checkColumnExistence(
    tableName: string,
    columnName: string,
  ): Promise<boolean> {
    const currentDB = await this.connectionLogic.getCurrentDatabase();
    const table = await this.schemaLogic.getTableFromCurrentDB(
      currentDB,
      tableName,
    );

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

  async checkDatabaseExistence(databaseName: string): Promise<Boolean> {
    return await this.storageService.checkDatabaseExistence(databaseName);
  }
}
