import { Injectable } from '@nestjs/common';
import { BaseParser } from '../base-parser';
import { AlterTableAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { DataType } from '../../../common/enums/data-type.enum';
import { SemanticAnalyzerService } from '../../semantic-analyzer/semantic-analyzer.service';

@Injectable()
export class AlterParser extends BaseParser {
  constructor(private readonly semanticAnalyzer: SemanticAnalyzerService) {
    super();
  }

  /**
   * TODO: handle the other action =2> 'DROP
   */
  async parse(tokens: IToken[], pointer: number): Promise<AlterTableAST> {
    this.tokens = tokens;
    this.pointer = pointer;

    const AST: AlterTableAST = {
      type: TokenType.ALTER,
      structure: TokenType.TABLE,
      name: '', // table name
      columnName: '',
      action: 'ADD',
    };

    if (!this.expect(TokenType.TABLE)) {
      throw new Error(`Expected ${TokenType.TABLE}`);
    }
    AST.structure = this.tokens[this.pointer - 1].type as TokenType.TABLE;

    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected table name`);
    }
    AST.name = this.tokens[this.pointer - 1].value as string;

    console.log({ AST });

    if (!this.expect(TokenType.ADD) && !this.expect(TokenType.DROP)) {
      throw new Error(`Expected ${TokenType.ADD} or ${TokenType.DROP}`);
    }

    AST.action = this.tokens[this.pointer - 1].value as 'ADD' | 'DROP';
    console.log(this.tokens);

    if (!this.expect(TokenType.COLUMN)) {
      throw new Error(`Expected ${TokenType.COLUMN}`);
    }

    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected ${TokenType.IDENTIFIER}`);
    }
    AST.columnName = this.tokens[this.pointer - 1].value as string;

    if (!this.expect(TokenType.DATATYPE)) {
      throw new Error(`Expected ${TokenType.DATATYPE}`);
    }
    AST.dataType = this.tokens[this.pointer - 1].value as DataType;

    if (!this.expect(TokenType.SEMI_COLON)) {
      throw new Error(`Expected ${TokenType.SEMI_COLON}`);
    }

    // SEMANTIC ANALYSIS
    // Check the existence of the table & column

    const isTableExist =
      await this.semanticAnalyzer.checkTableExistenceInCurrentDB(AST.name);

    if (!isTableExist) {
      throw new Error(`Table ${AST.name} is not exist`);
    }

    const isColumnExist = await this.semanticAnalyzer.checkColumnExistence(
      AST.name,
      AST.columnName,
    );

    if (isColumnExist) {
      throw new Error(`Column ${AST.columnName} is already exist`);
    }

    return AST;
  }
}
