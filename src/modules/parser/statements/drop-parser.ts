import { Injectable } from '@nestjs/common';
import { BaseParser } from '../base-parser';
import { DropDatabaseAST, DropTableAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { SemanticAnalyzerService } from '../../semantic-analyzer/semantic-analyzer.service';

@Injectable()
export class DropParser extends BaseParser {
  constructor(private readonly semanticAnalyzer: SemanticAnalyzerService) {
    super();
  }

  async parse(tokens: IToken[], pointer: number): Promise<DropDatabaseAST | DropTableAST> {
    this.tokens = tokens;
    this.pointer = pointer;

    // DETERMINATION BASED ON THE CURRENT TOKEN (DATABASE OR TABLE)
    return this.tokens[this.pointer].type === TokenType.DATABASE
      ? await this.parseDropDatabase()
      : await this.parseDropTable();
  }

  // parse the tokens by expect the proper tokens which match the correct flow of the drop database statement and returns the AST which includes the database name
  private async parseDropDatabase(): Promise<DropDatabaseAST> {
    const AST: DropDatabaseAST = {
      type: TokenType.DROP,
      structure: TokenType.DATABASE,
      name: '',
    };

    // expect the DATABASE keyword
    if (!this.expect(TokenType.DATABASE)) {
      throw new Error(`Expected ${TokenType.TABLE}`);
    }

    // expect the database name
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected ${TokenType.IDENTIFIER}`);
    }
    // assign the database name
    AST.name = this.tokens[this.pointer - 1].value as string;

    // semantic analysis
    // check database existence
    const isExist = await this.semanticAnalyzer.checkDatabaseExistence(
      AST.name,
    );

    if (!isExist) {
      throw new Error(`Database ${AST.name} not exist`);
    }

    return AST;
  }

  // parse the tokens by expect the proper tokens which match the correct flow of the drop table statement and returns the AST which includes the table name
  private async parseDropTable(): Promise<DropTableAST> {
    const AST: DropTableAST = {
      type: TokenType.DROP,
      structure: TokenType.TABLE,
      name: '',
    };

    // expect the TABLE keyword after the DROP keyword
    if (!this.expect(TokenType.TABLE)) {
      throw new Error(`Expected ${TokenType.TABLE}`);
    }

    // expect the table name after the TABLE keyword
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected ${TokenType.IDENTIFIER}`);
    }
    // assign the table name into the AST
    AST.name = this.tokens[this.pointer - 1].value as string;

    // semantic analysis
    // check table existence
    const isExist = await this.semanticAnalyzer.checkTableExistenceInCurrentDB(
      AST.name,
    );

    if (!isExist) {
      throw new Error(`Table ${AST.name} is not exist`);
    }

    return AST;
  }
}