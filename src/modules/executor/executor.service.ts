import { Injectable } from '@nestjs/common';
import { StorageService } from 'src/modules/storage/storage.service';
import { ParserService } from '../parser/parser.service';
import { TokenType } from '../../common/enums/token-type.enum';
import {
  SelectAST,
  DeleteAST,
  InsertAST,
  UpdateAST,
  CreateDatabaseAST,
  CreateTableAST,
  DropDatabaseAST,
  DropTableAST,
  AlterTableAST,
  AST,
} from '../../common/types/ast.type';
import { SchemaLogic } from '../storage/schema/schema.logic';
import { ConnectionLogic } from '../storage/connection/connection-logic';

@Injectable()
export class ExecutorService {
  constructor(
    private readonly parser: ParserService,
    private readonly storage: StorageService,
    private readonly schemaLogic: SchemaLogic,
    private readonly connectionLogic: ConnectionLogic,
  ) {}

  /**
   * (SELECT, INSERT, UPDATE, DELETE)
   */
  private readonly dmlHandlers = {
    [TokenType.SELECT]: (ast: SelectAST) => this.storage.select(ast),
    [TokenType.INSERT]: (ast: InsertAST) => this.storage.insert(ast),
    [TokenType.UPDATE]: (ast: UpdateAST) => this.storage.update(ast),
    [TokenType.DELETE]: (ast: DeleteAST) => this.storage.delete(ast),
  };

  /**
   * (CREATE, DROP, ALTER )
   */
  private readonly ddlHandlers = {
    [TokenType.CREATE]: (ast: any) => this.handleCreate(ast),
    [TokenType.DROP]: (ast: any) => this.handleDrop(ast),
    [TokenType.ALTER]: (ast: AlterTableAST) => this.storage.alterTable(ast),
  };

  async executeDML(query: string) {
    const response = await this.executeQuery(query, this.dmlHandlers);
    return { success: true, dml: query, ...response };
  }

  async executeDDL(query: string) {
    const response = await this.executeQuery(query, this.ddlHandlers);
    return { success: true, ddl: query, ...response };
  }

  async executeQuery(query: string, handlers: any) {
    const ast = await this.parser.parse(query);
    const handler = handlers[ast.type];

    if (!handler) {
      throw new Error(`${ast.type} is not a valid operation.`);
    }

    return await handler(ast);
  }

  private async handleCreate(ast: CreateDatabaseAST | CreateTableAST) {
    if (ast.structure === TokenType.DATABASE) {
      return this.storage.createDatabase(ast as CreateDatabaseAST);
    }
    return this.storage.createTable(ast as CreateTableAST);
  }

  private async handleDrop(ast: DropDatabaseAST | DropTableAST) {
    if (ast.structure === TokenType.DATABASE) {
      return this.storage.dropDatabase(ast as DropDatabaseAST);
    }
    return this.storage.dropTable(ast as DropTableAST);
  }
}
