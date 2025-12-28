import { Injectable } from '@nestjs/common';
import { DropDatabaseAST } from 'src/common/types/ast.type';
import * as fs from 'node:fs/promises';
import path from 'node:path';
import { BaseStorageOperations } from '../base-operation';

@Injectable()
export class DropDatabaseHandler extends BaseStorageOperations {
  async execute(currentDB: string, AST: DropDatabaseAST): Promise<any> {
    if (AST.name === currentDB)
      throw new Error(
        `You cannot drop the database because you are connected to this db`,
      );
    const dbPath = path.join(this.databasesPath, AST.name);
    await fs.rm(dbPath, { recursive: true, force: true });

    return { message: `Database ${AST.name} dropped` };
  }
}
