import { Injectable } from '@nestjs/common';
import { BaseParser } from '../base-parser';
import { DeleteAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { Operator } from '../../../common/enums/operator.enum';
import { SemanticAnalyzerService } from '../../semantic-analyzer/semantic-analyzer.service';

@Injectable()
export class DeleteParser extends BaseParser {
  constructor(private readonly semanticAnalyzer: SemanticAnalyzerService) {
    super();
  }

  // parse the tokens by expect the proper tokens which match the correct flow of the delete statement and returns the AST which includes the table name and WHERE obj => {criterion, operator, value}
  async parse(tokens: IToken[], pointer: number): Promise<DeleteAST> {
    this.tokens = tokens;
    this.pointer = pointer;

    const AST: DeleteAST = {
      type: TokenType.DELETE,
      table: '',
    };

    // expect FROM keyword after DELETE
    if (!this.expect(TokenType.FROM)) {
      throw new Error(`Expected FROM keyword`);
    }

    // expect table name
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected table name`);
    }
    // assign the table name for the semantic validation later
    AST.table = this.tokens[this.pointer - 1].value as string;

    // Parse WHERE clause (optional)
    if (this.expect(TokenType.WHERE)) {
      // expect column name
      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error(`Expected column name after the WHERE clause`);
      }
      // assign column name for the semantic validation later
      const criterion = this.tokens[this.pointer - 1].value as string;

      /**
       * TODO: handle delete by ranges (not yet)
       */
      // expect comparison operator => ( = / LIKE ) => (for now)
      if (!this.expect(TokenType.COMPARISON_OPERATOR)) {
        throw new Error(`Expected comparison operator`);
      }
      // determin the operator
      const operator = this.tokens[this.pointer - 1].value as Operator;

      // get the value type and convert it to the proper type for the comparison later
      let value: string | number | boolean = '';
      if (this.expect(TokenType.NUMBER_LITERAL)) {
        value = Number(this.tokens[this.pointer - 1].value);
      } else if (this.expect(TokenType.STRING_LITERAL)) {
        value = String(this.tokens[this.pointer - 1].value);
      } else if (this.expect(TokenType.BOOLEAN_LITERAL)) {
        value = Boolean(this.tokens[this.pointer - 1].value);
      } else {
        throw new Error(`Expected (number, string, or boolean)`);
      }

      AST.where = { criterion, operator, value };
    }

    // Semantic validation
    const tableExists =
      await this.semanticAnalyzer.checkTableExistenceInCurrentDB(AST.table);
    if (!tableExists) {
      throw new Error(`Table "${AST.table}" doesn't exist`);
    }

    if (AST.where) {
      const columnsExist = await this.semanticAnalyzer.checkColumnsExistence(
        AST.table,
        [AST.where.criterion],
      );
      if (!columnsExist) {
        throw new Error(
          `Column "${AST.where.criterion}" doesn't exist in table "${AST.table}"`,
        );
      }
    }

    return AST;
  }
}