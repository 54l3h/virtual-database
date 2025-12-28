import { Injectable } from '@nestjs/common';
import { DropTableAST } from 'src/common/types/ast.type';
import { SchemaLogic } from '../../schema/schema.logic';
import * as fs from 'node:fs/promises';
import { BaseStorageOperations } from '../base-operation';
import path from 'node:path';

@Injectable()
export class DropTableHandler extends BaseStorageOperations {
  constructor(private readonly schemaLogic: SchemaLogic) {
    super();
  }
  async execute(currentDB: string, AST: DropTableAST): Promise<any> {
    const schema = await this.schemaLogic.readCurrentDBSchema(currentDB);

    // filter out the table from the schema list
    schema.tables = schema.tables.filter((t) => t.name !== AST.name);
    await this.schemaLogic.updateCurrentDBSchema(currentDB, schema);

    // delete the physical directory
    const tableDirPath = path.join(this.databasesPath, currentDB, AST.name);
    await fs.rm(tableDirPath, { recursive: true, force: true });

    return { message: `Table ${AST.name} dropped` };
  }
}
