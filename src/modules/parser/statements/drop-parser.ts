import { Injectable } from '@nestjs/common';
import { BaseParser, ParserState } from '../base-parser';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { DropDatabaseAST, DropTableAST } from 'src/common/types/ast.type';
import { SchemaLogic } from 'src/modules/storage/schema/schema.logic';

@Injectable()
export class DropParser extends BaseParser {
  constructor(schemaLogic: SchemaLogic) {
    super(schemaLogic);
  }

  async parse(
    tokens: IToken[],
    pointer: number,
  ): Promise<DropDatabaseAST | DropTableAST> {
    const state: ParserState = { tokens, pointer };

    return state.tokens[state.pointer].type === TokenType.DATABASE
      ? await this.parseDropDatabase(state)
      : await this.parseDropTable(state);
  }

  private async parseDropDatabase(
    state: ParserState,
  ): Promise<DropDatabaseAST> {
    const AST: DropDatabaseAST = {
      type: TokenType.DROP,
      structure: TokenType.DATABASE,
      name: '',
    };

    if (!this.expect(state, TokenType.DATABASE)) {
      throw new Error('Expected DATABASE keyword');
    }

    if (!this.expect(state, TokenType.IDENTIFIER)) {
      throw new Error('Expected database name');
    }

    AST.name = this.getPreviousTokenValue(state);
    this.expectSemicolon(state);

    // symantic analysis
    await this.validateDatabaseExists(AST.name);

    return AST;
  }

  private async parseDropTable(state: ParserState): Promise<DropTableAST> {
    const AST: DropTableAST = {
      type: TokenType.DROP,
      structure: TokenType.TABLE,
      name: '',
    };

    if (!this.expect(state, TokenType.TABLE)) {
      throw new Error('Expected TABLE keyword');
    }

    if (!this.expect(state, TokenType.IDENTIFIER)) {
      throw new Error('Expected table name');
    }

    AST.name = this.getPreviousTokenValue(state);
    this.expectSemicolon(state);

    // symantic analysis
    await this.validateTableExists(AST.name);

    return AST;
  }
}
