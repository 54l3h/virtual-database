import { Injectable } from '@nestjs/common';
import { CreateDatabaseAST } from 'src/common/types/ast.type';
import { SchemaLogic } from '../../schema/schema.logic';
import { BaseStorageOperations } from '../base-operation';

@Injectable()
export class CreateDatabaseHandler extends BaseStorageOperations {
  constructor(private readonly schemaLogic: SchemaLogic) {
    super();
  }
  async execute(currentDB: string, AST: CreateDatabaseAST): Promise<any> {
    await this.schemaLogic.createDatabasesDir(); // To ignore errors
    const isExist = await this.schemaLogic.checkDatabaseExistence(AST.name);
    if (isExist) {
      throw new Error(`Database ${AST.name} already exists`);
    }

    await this.schemaLogic.createDatabaseDir(AST.name);
    return { message: `Database ${AST.name} created successfully` };
  }
}
