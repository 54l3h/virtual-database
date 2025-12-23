import { Injectable } from '@nestjs/common';
import { BaseParser } from '../base-parser';
import { InsertAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { SemanticAnalyzerService } from '../../semantic-analyzer/semantic-analyzer.service';

@Injectable()
export class InsertParser extends BaseParser {
  constructor(private readonly semanticAnalyzer: SemanticAnalyzerService) {
    super();
  }

  // parse the tokens by expect the proper tokens which match the correct flow of the insert statement and returns the AST which includes the columns and values
  async parse(tokens: IToken[], pointer: number): Promise<InsertAST> {
    this.tokens = tokens;
    this.pointer = pointer;

    const AST: InsertAST = {
      type: TokenType.INSERT,
      table: '',
      columns: [],
      values: [],
    };

    // make sure INTO keyword follows INSERT
    if (!this.expect(TokenType.INTO)) {
      throw new Error(`Expected ${TokenType.INTO}`);
    }

    // extract the target table name
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected table name`);
    }
    AST.table = this.tokens[this.pointer - 1].value as string;

    // expect '(' to start the column list
    if (!this.expect(TokenType.LPAREN_OPERATOR)) {
      throw new Error(`Expected '('`);
    }

    // iterate through the list of columns until ')'
    while (!this.expect(TokenType.RPAREN_OPERATOR)) {
      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error('Expected column name');
      }
      AST.columns.push(this.tokens[this.pointer - 1].value as string);

      // consume commas between column names
      if (this.tokens[this.pointer]?.type === TokenType.COMMA) {
        this.pointer++;
      }
    }

    // Expect VALUES keyword after columns list
    if (!this.expect(TokenType.VALUES)) {
      throw new Error(`Expected VALUES keyword`);
    }

    while (this.tokens[this.pointer]?.type === TokenType.LPAREN_OPERATOR) {
      if (!this.expect(TokenType.LPAREN_OPERATOR)) {
        throw new Error(`Expected '('`);
      }

      // get the number of values to compare it to the number of columns => target columns should === expressions
      let valueCount = 0;

      while (!this.expect(TokenType.RPAREN_OPERATOR)) {
        const token = this.tokens[this.pointer];

        if (token.type === TokenType.NUMBER_LITERAL) {
          this.pointer++;
          AST.values.push(parseInt(token.value as string));
        } else if (token.type === TokenType.STRING_LITERAL) {
          this.pointer++;
          AST.values.push(token.value as string);
        } else if (token.type === TokenType.BOOLEAN_LITERAL) {
          this.pointer++;
          AST.values.push(token.value as boolean);
        } else {
          throw new Error(`Unexpected token type ${token.type}`);
        }

        valueCount++;

        if (this.tokens[this.pointer]?.type === TokenType.COMMA) {
          this.pointer++;
        }
      }

      // compare the number of values against the number of columns
      if (valueCount !== AST.columns.length) {
        throw new Error(`Value count doesn't match column count`);
      }

      // for insert many => commas between rows
      if (this.tokens[this.pointer]?.type === TokenType.COMMA) {
        this.pointer++;
      }
    }

    if (!this.expect(TokenType.SEMI_COLON)) {
      throw new Error(`Expected ';'`);
    }

    // semantic analysis
    const tableExists =
      await this.semanticAnalyzer.checkTableExistenceInCurrentDB(AST.table);
    if (!tableExists) {
      throw new Error(`Table ${AST.table} not exist`);
    }

    const columnsExist = await this.semanticAnalyzer.checkColumnsExistence(
      AST.table,
      AST.columns,
    );
    if (!columnsExist) {
      throw new Error(`Invalid columns names`);
    }

    return AST;
  }
}