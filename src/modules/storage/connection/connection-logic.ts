import { Injectable } from '@nestjs/common';
import * as fs from 'fs/promises';
import * as path from 'path';
import { IConnection } from '../../../common/types/schema.types';

@Injectable()
export class ConnectionLogic {
  private readonly databasesPath = path.join(process.cwd(), 'databases');
  private readonly dbUsersFilePath = path.join(
    this.databasesPath,
    'postgres',
    'users',
    'users.ndjson',
  );

  /**
   * ! session management
   */
  // the name of connected database
  async getCurrentDatabase(): Promise<string> {
    try {
      const usersDataFile = await fs.readFile(this.dbUsersFilePath, 'utf-8');
      const { currentDB } = JSON.parse(usersDataFile);
      return currentDB;
    } catch (error) {
      throw new Error('Failed to retrieve current database session.');
    }
  }

  // connect to another database by change the value of the current db
  async updateCurrentDatabase(database: string): Promise<void> {
    const databaseDirPath = path.join(this.databasesPath, database);

    // check existence
    try {
      await fs.access(databaseDirPath);
    } catch {
      throw new Error(`Database ${database} doesn't exist`);
    }

    const usersDataFile = await fs.readFile(this.dbUsersFilePath, 'utf-8');
    const data: IConnection = JSON.parse(usersDataFile);
    data.currentDB = database;

    await fs.writeFile(this.dbUsersFilePath, JSON.stringify(data));
  }
}
