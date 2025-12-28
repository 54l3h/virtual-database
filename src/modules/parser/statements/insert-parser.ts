import { Injectable } from '@nestjs/common';
import { BaseParser, ParserState } from '../base-parser';
import { InsertAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { SchemaLogic } from 'src/modules/storage/schema/schema.logic';
@Injectable()
export class InsertParser extends BaseParser {
  constructor(schemaLogic: SchemaLogic) {
    super(schemaLogic);
  }

  async parse(tokens: IToken[], pointer: number): Promise<InsertAST> {
    const state: ParserState = { tokens, pointer };

    const AST: InsertAST = {
      type: TokenType.INSERT,
      table: '',
      columns: [],
      values: [],
    };

    if (!this.expect(state, TokenType.INTO)) throw new Error('Expected INTO');

    // Get Table Name
    if (!this.expect(state, TokenType.IDENTIFIER))
      throw new Error('Expected table name');
    AST.table = this.getPreviousTokenValue(state);

    // Parse Columns (col1, col2,...)
    if (!this.expect(state, TokenType.LPAREN_OPERATOR))
      throw new Error("Expected '('");

    while (!this.expect(state, TokenType.RPAREN_OPERATOR)) {
      if (!this.expect(state, TokenType.IDENTIFIER))
        throw new Error('Expected column name');
      AST.columns.push(this.getPreviousTokenValue(state));

      // Consume comma
      this.expect(state, TokenType.COMMA);
    }

    // Parse VALUES
    if (!this.expect(state, TokenType.VALUES))
      throw new Error('Expected VALUES');

    // Parse Values (value1, value2, ...)
    while (state.tokens[state.pointer].type === TokenType.LPAREN_OPERATOR) {
      this.expect(state, TokenType.LPAREN_OPERATOR);

      let valuesNumber = 0;
      while (!this.expect(state, TokenType.RPAREN_OPERATOR)) {
        const value = this.parseLiteralValue(state);
        AST.values.push(value);
        valuesNumber++;

        this.expect(state, TokenType.COMMA);
      }

      if (valuesNumber !== AST.columns.length) {
        throw new Error(
          `Value count incorrect, Expected ${AST.columns.length} but got ${valuesNumber}`,
        );
      }

      this.expect(state, TokenType.COMMA);
    }

    this.expectSemicolon(state);

    await this.validateTableExists(AST.table);
    await this.validateColumnsExist(AST.table, AST.columns);

    return AST;
  }
}
