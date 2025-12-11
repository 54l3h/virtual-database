import { forwardRef, Inject, Injectable } from '@nestjs/common';
import { ExecuteSqlDto } from './dto/execute-sql.dto';
import {
  IToken,
  TokenizerService,
  TokenType,
} from 'src/tokenizer/tokenizer.service';
import { StorageService } from 'src/storage/storage.service';
import { IQuery, ParserService } from 'src/parser/parser.service';

@Injectable()
export class ExecutorService {
  tokens: IToken[] = [];
  idx: number = 0;

  constructor(
    private readonly tokenizer: TokenizerService,
    @Inject(forwardRef(() => ParserService))
    private readonly parser: ParserService,
    private readonly storage: StorageService,
  ) {}

  async executeDDL(dto: ExecuteSqlDto) {
    const { query } = dto;
    // let response;
    console.log('query');
    console.log(query);

    const AST = await this.parser.parse(query);

    console.log('executor');
    console.log(AST);

    let response;

    switch (true) {
      case AST.type === TokenType.CREATE &&
        AST['structure'] === TokenType.DATABASE:
        response = await this.storage.createDatabase(AST);
        break;

      case AST.type === TokenType.CREATE &&
        AST['structure'] === TokenType.TABLE:
        response = await this.storage.createTable(AST);
        break;

      case AST.type === TokenType.UPDATE:
        console.log('switch');

        response = await this.storage.update(AST as any);
        break;

      default:
        break;
    }

    // const response = this.storage.createDatabase(AST);
    // console.log(AST);
    // console.log({ AST });

    // switch (AST.type) {
    //   case TokenType.SELECT:
    //     response = await this.select(AST);
    //     break;

    //   case TokenType.DELETE:
    //     response = await this.delete(AST);
    //     break;

    //   default:
    //     break;
    // }

    return { success: true, response };
  }

  async executeDML(dto: ExecuteSqlDto) {
    const { query } = dto;
    let response;
    const AST = await this.parser.parse(query);
    // console.log(AST);
    console.log({ AST });

    switch (AST.type) {
      case TokenType.SELECT:
        response = await this.select(AST);
        break;

      case TokenType.DELETE:
        response = await this.delete(AST);
        break;

      default:
        break;
    }

    return { success: true, response };
    // const tokens = this.tokenizer.tokenize(query);
    // now you have the tokens
    // const parser =

    // let result;
    // switch (true) {
    //   case query.toUpperCase().startsWith(TokenType.SELECT):
    //     result = await this.executeSelectStatement(query);
    //     break;

    //   default:
    //     break;
    // }

    // return { success: true, dml: query, result };
  }

  async select(AST: IQuery) {
    console.log(AST);

    const response = await this.storage.select(
      AST.columns,
      AST.table,
      AST.where?.criterion,
      AST.where?.operator,
      AST.where?.value,
    );

    return response;
  }

  async delete(AST: IQuery) {
    console.log(AST);

    const response = await this.storage.delete(AST);

    return response;
  }

  async executeSelectStatement(query: string) {
    // ! For SELECT statements

    // 1. Tokenization
    // 2. Parsing
    // 3. Executor (Works on the Objects, Criteria, etc)
    // 4. Storage Service (Working on the real files)

    // TODO: Let's start with the tokenizer
    const tokens: IToken[] = this.tokenizer.tokenize(query);
    // ! After this you should send the tokens to the parser

    // into the
    const checkTheFLow = this.parser.parseSelect(tokens);

    if (!checkTheFLow) {
      throw new Error('SYNTAX ERROR');
    }

    // Go to execute
    // 1. passing => columns, table name => storage service
    const fromClauseIndex = tokens.findIndex((token) => {
      return token.type === TokenType.FROM;
    });
    const result = await this.storage.select(
      tokens[fromClauseIndex - 1].value,
      tokens[fromClauseIndex + 1].value,
    );

    return result;
  }

  async createDatabase(AST) {}
}

//   1    2    3    4
// SELECT id FROM users;
// SELECT id FROM users WHERE id > 5;
// before the FROM claue is the colunn name
// after the FROM claue is the table name
// you could get the FROM clauese index and then work on it to get the index of the column name and then get it
// and you can work FROM clause index to get the index of the table name and then get it
