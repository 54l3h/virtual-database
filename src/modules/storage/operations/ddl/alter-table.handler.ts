import { Injectable } from '@nestjs/common';
import { AlterTableAST } from 'src/common/types/ast.type';
import { SchemaLogic } from '../../schema/schema.logic';
import { IndexLogic } from '../../index/index-logic';
import { IColumn, ISchema } from 'src/common/types/schema.types';
import { DataType } from 'src/common/enums/data-type.enum';
import { BaseStorageOperations } from '../base-operation';


@Injectable()
export class AlterTableHandler extends BaseStorageOperations {
  constructor(
    private readonly schemaLogic: SchemaLogic,
    private readonly indexLogic: IndexLogic,
  ) {
    super();
  }
  async execute(currentDB: string, AST: AlterTableAST): Promise<any> {
    const schema: ISchema =
      await this.schemaLogic.readCurrentDBSchema(currentDB);

    const table = schema.tables.find((table) => table.name === AST.name);

    if (!table) {
      throw new Error(`Table ${AST.name} not found`);
    }

    // add column
    if (AST.columnName && AST.dataType) {
      const newColumn: IColumn = {
        name: AST.columnName as string,
        type: AST.dataType as DataType,
      };

      table.columns.push(newColumn);
    }

    await this.schemaLogic.updateCurrentDBSchema(currentDB, schema);

    return { message: `Table ${AST.name} altered` };
  }
}
