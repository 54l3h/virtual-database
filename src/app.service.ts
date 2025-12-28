import { Injectable } from '@nestjs/common';
import { SchemaLogic } from './modules/storage/schema/schema.logic';
import { ConnectionLogic } from './modules/storage/connection/connection-logic';

@Injectable()
export class AppService {
  constructor(
    private readonly connectionLogic: ConnectionLogic,
    private readonly schemaLogic: SchemaLogic,
  ) {}
  async connect(database: string) {
    await this.schemaLogic.createDatabasesDir();
    await this.connectionLogic.updateCurrentDatabase(database);
    return { success: true, message: `Connected to ${database}` };
  }
}
