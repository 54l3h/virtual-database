import { forwardRef, Inject, Injectable } from '@nestjs/common';
import { ExecuteSqlDto } from './dto/execute-sql.dto';
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
} from '../../common/types/ast.type';

@Injectable()
export class ExecutorService {
  constructor(
    @Inject(forwardRef(() => ParserService))
    private readonly parser: ParserService,
    private readonly storage: StorageService,
  ) {}

  async executeDDL(dto: ExecuteSqlDto) {
    const { query } = dto;

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
        // Handle DROP operations
        break;

      default:
        throw new Error('Unsupported DDL operation');
    }

    return { success: true, response };
  }

  async executeDML(dto: ExecuteSqlDto) {
    const { query } = dto;

    const AST = await this.parser.parse(query);
    console.log(AST);

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

      // case TokenType.DELETE:
      //   response = await this.storage.delete(AST as DeleteAST);
      //   break;

      default:
        throw new Error('Unsupported DML operation');
    }

    return { success: true, response };
  }

  // async select(AST: SelectAST) {
  //   const response = await this.storage.select(
  //     AST.columns,
  //     AST.table,
  //     AST.where?.criterion,
  //     AST.where?.operator,
  //     AST.where?.value,
  //   );
  //   return response;
  // }

  // async delete(AST: DeleteAST) {
  //   const response = await this.storage.delete(AST);
  //   return response;
  // }

  // async executeSelectStatement(query: string) {
  //   // ! For SELECT statements

  //   // 1. Tokenization
  //   // 2. Parsing
  //   // 3. Executor (Works on the Objects, Criteria, etc)
  //   // 4. Storage Service (Working on the real files)

  //   // TODO: Let's start with the tokenizer
  //   const tokens: IToken[] = this.tokenizer.tokenize(query);
  //   // ! After this you should send the tokens to the parser

  //   // into the
  //   const checkTheFLow = this.parser.parseSelect(tokens);

  //   if (!checkTheFLow) {
  //     throw new Error('SYNTAX ERROR');
  //   }

  //   // Go to execute
  //   // 1. passing => columns, table name => storage service
  //   const fromClauseIndex = tokens.findIndex((token) => {
  //     return token.type === TokenType.FROM;
  //   });
  //   const result = await this.storage.select(
  //     tokens[fromClauseIndex - 1].value,
  //     tokens[fromClauseIndex + 1].value,
  //   );

  //   return result;
  // }
}

//   1    2    3    4
// SELECT id FROM users;
// SELECT id FROM users WHERE id > 5;
// before the FROM claue is the colunn name
// after the FROM claue is the table name
// you could get the FROM clauese index and then work on it to get the index of the column name and then get it
// and you can work FROM clause index to get the index of the table name and then get it
