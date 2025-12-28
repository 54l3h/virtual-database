import { Injectable } from '@nestjs/common';
import { BaseParser, ParserState } from '../base-parser';
import { AlterTableAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { SchemaLogic } from 'src/modules/storage/schema/schema.logic';

@Injectable()
export class AlterParser extends BaseParser {
  constructor(schemaLogic: SchemaLogic) {
    super(schemaLogic);
  }

  async parse(tokens: IToken[], pointer: number): Promise<AlterTableAST> {
    const state: ParserState = { tokens, pointer };

    const AST: AlterTableAST = {
      type: TokenType.ALTER,
      structure: TokenType.TABLE,
      name: '',
      columnName: '',
      action: 'ADD',
    };

    // Expect TABLE keyword
    if (!this.expect(state, TokenType.TABLE)) {
      throw new Error('Expected TABLE keyword');
    }

    // Expect table name
    if (!this.expect(state, TokenType.IDENTIFIER)) {
      throw new Error('Expected table name');
    }
    AST.name = this.getPreviousTokenValue(state);

    // Expect ADD or DROP (Not Yet)
    if (this.expect(state, TokenType.ADD)) {
      AST.action = 'ADD';
    } else if (this.expect(state, TokenType.DROP)) {
      AST.action = 'DROP'; // (Not Yet)
    } else {
      throw new Error('Expected ADD or DROP keyword');
    }

    // Expect column name
    if (!this.expect(state, TokenType.IDENTIFIER)) {
      throw new Error('Expected column name');
    }
    AST.columnName = this.getPreviousTokenValue(state);

    // Only expect datatype for ADD action
    if (AST.action === 'ADD') {
      if (!this.expect(state, TokenType.DATATYPE)) {
        throw new Error('Expected data type');
      }
      AST.dataType = this.getPreviousDataType(state);
    }

    // Expect semicolon
    this.expectSemicolon(state);

    // Semantic analyiss
    await this.validateTableExists(AST.name);

    if (AST.action === 'ADD') {
      await this.validateColumnNotExists(AST.name, AST.columnName);
    } else {
      await this.validateColumnExists(AST.name, AST.columnName);
    }

    return AST;
  }
}
