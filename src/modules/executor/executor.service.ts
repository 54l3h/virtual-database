import { Injectable } from '@nestjs/common';
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

  async executeDDL(query: string) {
    // get the AST from the parser which identifies the statement type
    const AST: AST = await this.parser.parse(query);

    // delcare variable called resoponse => will hold the response later
    let response;

    // determine the statment type by get the value of the type(key)
    switch (AST.type) {
      // CREATE STATEMENT
      case TokenType.CREATE:
        // CHECK THE STRUCTURE THAT YOU ARE GONNA CREATE (DATABASE OR TABLE)
        response = await this.handleCreate(AST);
        break;

      // DROP STATEMENT
      case TokenType.DROP:
        // CHECK THE STRUCTURE THAT YOU ARE GONNA DROP (DATABASE OR TABLE)
        response = await this.handleDrop(AST);
        break;

      // ALTER STATEMENT
      // TODO: HANDLE ALTER (DROP) + ALTER DATABASE
      case TokenType.ALTER:
        // ASSIGN THE RESPONSE AFTER GET BACK FROM ALTERING THE TABLE
        response = await this.storage.alterTable(AST as AlterTableAST);
        break;

      default:
        throw new Error('Unsupported DDL operation');
    }

    return { success: true, ddl: query, ...response };
  }

  async executeDML(query: string) {
    // get the AST from the parser which identifies the statement type
    const AST = await this.parser.parse(query);

    // delcare variable called resoponse => will hold the response later
    let response;

    switch (AST.type) {
      // SELECT STATEMENT
      case TokenType.SELECT:
        response = await this.storage.select(AST as SelectAST);
        break;

      // INSERT STATEMENT
      case TokenType.INSERT:
        response = await this.storage.insert(AST as InsertAST);
        break;

      // UPDATE STATEMENT
      case TokenType.UPDATE:
        response = await this.storage.update(AST as UpdateAST);
        break;

      // DELETE STATEMENT
      case TokenType.DELETE:
        response = await this.storage.delete(AST as DeleteAST);
        break;

      default:
        throw new Error('Unsupported DML operation');
    }

    return { success: true, dml: query, ...response };
  }

  // handle create database or table
  private async handleCreate(AST: any) {
    if (AST.structure === TokenType.DATABASE) {
      // RETURN RESPONSE AFTER GET BACK FROM CREATING THE DATABASE
      return await this.storage.createDatabase(AST as CreateDatabaseAST);
    } else if (AST.structure === TokenType.TABLE) {
      const currentDB = await this.connectionLogic.getCurrentDatabase();
      // RETURN RESPONSE AFTER GET BACK FROM CREATING THE TABLE
      return await this.schemaLogic.createNewTable(
        currentDB,
        AST as CreateTableAST,
      );
    }
  }

  // handle drop database or table
  private async handleDrop(AST: DropDatabaseAST | DropTableAST) {
    if (AST.structure === TokenType.DATABASE) {
      // RETURN RESPONSE AFTER GET BACK FROM DROPPING THE DATABASE
      return await this.storage.dropDatabase(AST as DropDatabaseAST);
    } else if (AST.structure === TokenType.TABLE) {
      // RETURN RESPONSE AFTER GET BACK FROM DROPPING THE TABLE
      return await this.storage.dropTable(AST as DropTableAST);
    }
  }
}
