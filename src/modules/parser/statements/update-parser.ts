import { BaseParser, ParserState } from '../base-parser';
import { Injectable } from '@nestjs/common';
import { IToken } from 'src/common/types/token.types';
import { UpdateAST } from 'src/common/types/ast.type';
import { TokenType } from 'src/common/enums/token-type.enum';
import { WhereClause } from 'src/common/types/query.types';
import { SchemaLogic } from 'src/modules/storage/schema/schema.logic';

@Injectable()
export class UpdateParser extends BaseParser {
  constructor(schemaLogic: SchemaLogic) {
    super(schemaLogic);
  }

  async parse(tokens: IToken[], pointer: number): Promise<UpdateAST> {
    const state: ParserState = { tokens, pointer };

    const AST: UpdateAST = {
      type: TokenType.UPDATE,
      table: '',
      updates: {},
      where: {} as WhereClause,
    };

    if (!this.expect(state, TokenType.IDENTIFIER)) {
      throw new Error('Expected table name');
    }
    AST.table = this.getPreviousTokenValue(state);

    if (!this.expect(state, TokenType.SET)) {
      throw new Error('Expected SET keyword');
    }

    const columnsToCheck: string[] = [];

    this.parseSetClause(state, AST, columnsToCheck);

    while (this.expect(state, TokenType.COMMA)) {
      this.parseSetClause(state, AST, columnsToCheck);
    }

    if (!this.expect(state, TokenType.WHERE)) {
      throw new Error('Expected WHERE clause');
    }
    AST.where = this.parseWhereClause(state);
    columnsToCheck.push(AST.where.criterion);

    this.expectSemicolon(state);

    // symantic analysis
    await this.validateTableExists(AST.table);
    await this.validateColumnsExist(AST.table, columnsToCheck);

    return AST;
  }

  private parseSetClause(
    state: ParserState,
    AST: UpdateAST,
    columnsToCheck: string[],
  ): void {
    if (!this.expect(state, TokenType.IDENTIFIER)) {
      throw new Error('Expected column name');
    }
    const columnName = this.getPreviousTokenValue(state);
    columnsToCheck.push(columnName);

    if (!this.expect(state, TokenType.COMPARISON_OPERATOR)) {
      throw new Error("Expected '=' operator");
    }

    const value = this.parseLiteralValue(state);
    AST.updates[columnName] = value;
  }
}
