import { Injectable } from '@nestjs/common';
import { BaseParser, ParserState } from '../base-parser';
import { DeleteAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { SchemaLogic } from 'src/modules/storage/schema/schema.logic';

@Injectable()
export class DeleteParser extends BaseParser {
  constructor(schemaLogic: SchemaLogic) {
    super(schemaLogic);
  }

  async parse(tokens: IToken[], pointer: number): Promise<DeleteAST> {
    const state: ParserState = { tokens, pointer };

    const AST: DeleteAST = {
      type: TokenType.DELETE,
      table: '',
    };

    if (!this.expect(state, TokenType.FROM)) {
      throw new Error('Expected FROM keyword');
    }

    if (!this.expect(state, TokenType.IDENTIFIER)) {
      throw new Error('Expected table name');
    }
    AST.table = this.getPreviousTokenValue(state);

    if (this.expect(state, TokenType.WHERE)) {
      AST.where = this.parseWhereClause(state);
    }

    this.expectSemicolon(state);

    // symantic analysis
    await this.validateTableExists(AST.table);

    if (AST.where) {
      await this.validateColumnsExist(AST.table, [AST.where.criterion]);
    }

    return AST;
  }
}
