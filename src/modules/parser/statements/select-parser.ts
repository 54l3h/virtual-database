import { Injectable } from '@nestjs/common';
import { BaseParser, ParserState } from '../base-parser';
import { SelectAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { SchemaLogic } from 'src/modules/storage/schema/schema.logic';

@Injectable()
export class SelectParser extends BaseParser {
  constructor(schemaLogic: SchemaLogic) {
    super(schemaLogic);
  }

  async parse(tokens: IToken[], pointer: number): Promise<SelectAST> {
    const state: ParserState = { tokens, pointer };

    const AST: SelectAST = {
      type: TokenType.SELECT,
      columns: [],
      table: '',
    };

    // Parse columns (SELECT * or SELECT col1, col2)
    if (this.expect(state, TokenType.ASTERISK)) {
      AST.columns.push('*');
    } else if (this.expect(state, TokenType.IDENTIFIER)) {
      AST.columns.push(this.getPreviousTokenValue(state));

      while (this.expect(state, TokenType.COMMA)) {
        if (!this.expect(state, TokenType.IDENTIFIER)) {
          throw new Error('Expected column name after comma');
        }
        AST.columns.push(this.getPreviousTokenValue(state));
      }
    } else {
      throw new Error('Expected * or column name(s)');
    }

    // Parse FROM clause
    if (!this.expect(state, TokenType.FROM)) {
      throw new Error('Expected FROM keyword');
    }

    // Parse Table Name
    if (this.expect(state, TokenType.IDENTIFIER)) {
      AST.table = this.getPreviousTokenValue(state);
    } else {
      const currentToken = state.tokens[state.pointer];

      if (
        currentToken &&
        currentToken.type !== TokenType.SEMI_COLON &&
        typeof currentToken.value === 'string'
      ) {
        AST.table = currentToken.value;
        state.pointer++;
      } else {
        throw new Error('Expected table name after FROM');
      }
    }

    // Parse WHERE clause (optional)
    if (this.expect(state, TokenType.WHERE)) {
      AST.where = this.parseWhereClause(state);
    }

    // Parse ORDER BY clause (optional) (NOT YET)
    if (this.expect(state, TokenType.ORDER)) {
      if (!this.expect(state, TokenType.BY)) {
        throw new Error('Expected BY after ORDER');
      }

      if (!this.expect(state, TokenType.IDENTIFIER)) {
        throw new Error('Expected column name for ordering');
      }

      const column = this.getPreviousTokenValue(state);
      let direction: TokenType.ASC | TokenType.DESC = TokenType.ASC;

      if (this.expect(state, TokenType.DESC)) {
        direction = TokenType.DESC;
      } else if (this.expect(state, TokenType.ASC)) {
        direction = TokenType.ASC;
      }

      AST.orderBy = { column, direction };
    }

    // Expect semicolon
    this.expectSemicolon(state);

    // Semantic analysis
    await this.validateTableExists(AST.table);

    if (!AST.columns.includes('*')) {
      const columnsToCheck = [...AST.columns];
      if (AST.where) {
        columnsToCheck.push(AST.where.criterion);
      }
      if (AST.orderBy) {
        columnsToCheck.push(AST.orderBy.column);
      }
      await this.validateColumnsExist(AST.table, columnsToCheck);
    } else {
      const columnsToCheck: string[] = [];
      if (AST.where) {
        columnsToCheck.push(AST.where.criterion);
      }
      if (AST.orderBy) {
        columnsToCheck.push(AST.orderBy.column);
      }
      if (columnsToCheck.length > 0) {
        await this.validateColumnsExist(AST.table, columnsToCheck);
      }
    }

    return AST;
  }
}
