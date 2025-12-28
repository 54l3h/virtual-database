import { TokenType } from 'src/common/enums/token-type.enum';
import { BaseParser, ParserState } from '../base-parser';
import { IToken } from 'src/common/types/token.types';
import { CreateDatabaseAST, CreateTableAST } from 'src/common/types/ast.type';
import { IColumn } from 'src/common/types/schema.types';
import { DataType } from 'src/common/enums/data-type.enum';
import { Injectable } from '@nestjs/common';
import { SchemaLogic } from 'src/modules/storage/schema/schema.logic';

@Injectable()
export class CreateParser extends BaseParser {
  constructor(schemaLogic: SchemaLogic) {
    super(schemaLogic);
  }

  async parse(
    tokens: IToken[],
    pointer: number,
  ): Promise<CreateDatabaseAST | CreateTableAST> {
    const state: ParserState = { tokens, pointer };

    if (state.tokens[state.pointer].type === TokenType.DATABASE) {
      return await this.parseCreateDatabase(state);
    } else {
      return await this.parseCreateTable(state);
    }
  }

  private async parseCreateDatabase(
    state: ParserState,
  ): Promise<CreateDatabaseAST> {
    const AST: CreateDatabaseAST = {
      type: TokenType.CREATE,
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

    // Semantic analysis
    await this.validateDatabaseNotExists(AST.name);

    return AST;
  }

  private async parseCreateTable(state: ParserState): Promise<CreateTableAST> {
    const AST: CreateTableAST = {
      type: TokenType.CREATE,
      structure: TokenType.TABLE,
      name: '',
      columns: [],
    };

    if (!this.expect(state, TokenType.TABLE)) {
      throw new Error('Expected TABLE keyword');
    }

    if (!this.expect(state, TokenType.IDENTIFIER)) {
      throw new Error('Expected table name');
    }

    AST.name = this.getPreviousTokenValue(state);

    if (!this.expect(state, TokenType.LPAREN_OPERATOR)) {
      throw new Error("Expected '('");
    }

    while (!this.expect(state, TokenType.RPAREN_OPERATOR)) {
      const column = this.parseColumnDefinition(state);
      AST.columns?.push(column);

      if (state.tokens[state.pointer].type === TokenType.COMMA) {
        state.pointer++;
      }
    }

    this.expectSemicolon(state);

    // symantic analysis
    await this.validateTableNotExists(AST.name);

    return AST;
  }

  private parseColumnDefinition(state: ParserState): IColumn {
    if (!this.expect(state, TokenType.IDENTIFIER)) {
      throw new Error('Expected column name');
    }

    const col: IColumn = {
      name: this.getPreviousTokenValue(state),
      type: null as any,
    };

    if (!this.expect(state, TokenType.DATATYPE)) {
      throw new Error('Expected data type');
    }

    const dataType = this.getPreviousDataType(state);

    if (dataType === DataType.SERIAL) {
      col.type = DataType.INTEGER;
      col.autoIncrement = true;
    } else {
      col.type = dataType as DataType;
    }

    if (dataType === 'VARCHAR') {
      if (state.tokens[state.pointer]?.type === TokenType.LPAREN_OPERATOR) {
        state.pointer++;

        if (!this.expect(state, TokenType.NUMBER_LITERAL)) {
          throw new Error('Expected length for VARCHAR');
        }

        col.length = this.getPreviousTokenValueAsNumber(state);

        if (!this.expect(state, TokenType.RPAREN_OPERATOR)) {
          throw new Error("Expected ')' after VARCHAR length");
        }
      }
    }

    while (
      state.tokens[state.pointer].type !== TokenType.COMMA &&
      state.tokens[state.pointer].type !== TokenType.RPAREN_OPERATOR
    ) {
      const currentToken = state.tokens[state.pointer];

      if (currentToken.type === TokenType.PRIMARY) {
        state.pointer++;

        if (!this.expect(state, TokenType.KEY)) {
          throw new Error('Expected KEY after PRIMARY');
        }
        col.primaryKey = true;
      } else if (currentToken.type === TokenType.UNIQUE) {
        state.pointer++;
        col.unique = true;
      } else {
        throw new Error(`Unexpected token: ${currentToken.value}`);
      }
    }

    return col;
  }
}
