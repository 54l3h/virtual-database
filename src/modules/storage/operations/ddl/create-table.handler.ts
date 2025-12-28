import { Injectable } from '@nestjs/common';
import { CreateTableAST, DeleteAST } from 'src/common/types/ast.type';
import { SchemaLogic } from '../../schema/schema.logic';
import { IndexLogic } from '../../index/index-logic';
import * as fs from 'node:fs/promises';
import { BaseStorageOperations } from '../base-operation';
import path from 'node:path';

@Injectable()
export class CreateTableHandler extends BaseStorageOperations {
  constructor(
    private readonly schemaLogic: SchemaLogic,
    private readonly indexLogic: IndexLogic,
  ) {
    super();
  }
  async execute(currentDB: string, AST: CreateTableAST): Promise<any> {
    // get the db schema to modify it and add the new table
    const schema = await this.schemaLogic.readCurrentDBSchema(currentDB);

    // add the new table to the schema
    schema.tables.push({
      name: AST.name,
      columns: AST.columns,
    });

    // Create table directory => databases path(root), current db name, table name
    const tableDirPath = path.join(this.databasesPath, currentDB, AST.name);
    await fs.mkdir(tableDirPath, { recursive: true });

    // create index files for PK and UNIQUE columns
    for (const column of AST.columns) {
      if (column.primaryKey || column.unique) {
        await this.indexLogic.createIndexFile(currentDB, AST.name, column.name);
      }
    }

    // Create metadata file to track the auto incremented column values
    const autoIncrementedColumn = AST.columns.find(
      (col) => col.autoIncrement === true,
    );

    if (autoIncrementedColumn) {
      const metaFilePath = path.join(tableDirPath, `${AST.name}_meta.json`);
      const metadata = { [`${autoIncrementedColumn.name}`]: 0 };
      await fs.writeFile(metaFilePath, JSON.stringify(metadata), 'utf-8');
    }

    // update the schema to add the table
    await this.schemaLogic.updateCurrentDBSchema(currentDB, schema);

    // Create NDJSON data file for the table data
    const dataFilePath = path.join(tableDirPath, `${AST.name}.ndjson`);
    await fs.writeFile(dataFilePath, '');

    return { message: `Table ${AST.name} created successfully` };
  }
}
