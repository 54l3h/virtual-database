import { Injectable } from '@nestjs/common';
import { StorageService } from './modules/storage/storage.service';
import { SchemaLogic } from './modules/storage/schema/schema.logic';
import { ConnectionLogic } from './modules/storage/connection/connection-logic';

@Injectable()
export class AppService {
  constructor(
    private readonly connectionLogic: ConnectionLogic,
    private readonly schemaLogic: SchemaLogic,
  ) {}
  async connect(database: string) {
    // to make sure that the root dir for the databases exist
    await this.schemaLogic.createDatabasesDir();
    // get the request
    await this.connectionLogic.updateCurrentDatabase(database);
    return { success: true, message: `connected to ${database}` };
  }
}
