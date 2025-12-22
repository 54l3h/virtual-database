import { forwardRef, Inject, Injectable } from '@nestjs/common';
import { StorageService } from 'src/modules/storage/storage.service';
import { ParserService } from '../parser/parser.service';
import { TokenType } from '../../common/enums/token-type.enum';
import {
  AST,
  SelectAST,
  DeleteAST,
  InsertAST,
  UpdateAST,
  CreateDatabaseAST,
  CreateTableAST,
  DropDatabaseAST,
  DropTableAST,
  AlterTableAST,
} from '../../common/types/ast.type';

@Injectable()
export class ExecutorService {
  constructor(
    @Inject(forwardRef(() => ParserService))
    private readonly parser: ParserService,
    private readonly storage: StorageService,
  ) {}

  async executeDDL(query: string) {
    const AST: AST = await this.parser.parse(query);

    let response;

    switch (AST.type) {
      case TokenType.CREATE:
        if (AST.structure === TokenType.DATABASE) {
          response = await this.storage.createDatabase(
            AST as CreateDatabaseAST,
          );
        } else if (AST.structure === TokenType.TABLE) {
          response = await this.storage.createTable(AST as CreateTableAST);
        }
        break;

      case TokenType.DROP:
        if (AST.structure === TokenType.DATABASE) {
          response = await this.storage.dropDatabase(AST as DropDatabaseAST);
        } else if (AST.structure === TokenType.TABLE) {
          response = await this.storage.dropTable(AST as DropTableAST);
        }
        break;

      case TokenType.ALTER:
        response = await this.storage.alterTable(AST as AlterTableAST);

        break;

      default:
        throw new Error('Unsupported DDL operation');
    }

    return { success: true, ddl: query, ...response };
  }

  async executeDML(query: string) {
    const AST = await this.parser.parse(query);

    let response;

    switch (AST.type) {
      case TokenType.SELECT:
        response = await this.storage.select(AST as SelectAST);
        break;

      case TokenType.INSERT:
        response = await this.storage.insert(AST as InsertAST);
        break;

      case TokenType.UPDATE:
        response = await this.storage.update(AST as UpdateAST);
        break;

      case TokenType.DELETE:
        response = await this.storage.delete(AST as DeleteAST);
        break;

      default:
        throw new Error('Unsupported DML operation');
    }

    return { success: true, dml: query, ...response };
  }
}
