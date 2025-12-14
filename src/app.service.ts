import { Injectable } from '@nestjs/common';
import { StorageService } from './modules/storage/storage.service';

@Injectable()
export class AppService {
  constructor(private readonly storageService: StorageService) {}
  async connect(database: string) {
    await this.storageService.createDatabasesDir();
    // get the request
    await this.storageService.updateCurrentDatabase(database)
    return { success: true, message: `connected to ${database}` };
  }
}
