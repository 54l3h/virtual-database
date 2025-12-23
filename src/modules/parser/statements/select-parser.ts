import { Injectable } from '@nestjs/common';
import { BaseParser } from '../base-parser';
import { SelectAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { Operator } from '../../../common/enums/operator.enum';
import { SemanticAnalyzerService } from '../../semantic-analyzer/semantic-analyzer.service';

@Injectable()
export class SelectParser extends BaseParser {
  constructor(private readonly semanticAnalyzer: SemanticAnalyzerService) {
    super();
  }

  // parse the tokens by expect the proper tokens which match the correct flow of the select statement and returns the AST which includes the table name and the selected columns
  async parse(tokens: IToken[], pointer: number): Promise<SelectAST> {
    this.tokens = tokens;
    this.pointer = pointer;

    const AST: SelectAST = {
      type: TokenType.SELECT,
      columns: [],
      table: '',
    };

    // Parse columns
    if (this.expect(TokenType.ASTERISK)) {
      AST.columns.push('*');
    } else if (this.expect(TokenType.IDENTIFIER)) {
      AST.columns.push(this.tokens[this.pointer - 1].value as string);

      while (this.expect(TokenType.COMMA)) {
        if (!this.expect(TokenType.IDENTIFIER)) {
          throw new Error('Expected column name after comma');
        }
        AST.columns.push(this.tokens[this.pointer - 1].value as string);
      }
    } else {
      throw new Error('Expected * or column/s name/s');
    }

    // Parse FROM clause (mandatory)
    if (!this.expect(TokenType.FROM)) {
      throw new Error(`Expected FROM keyword`);
    }

    // Parse the table name
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected table name`);
    }
    AST.table = this.tokens[this.pointer - 1].value as string;

    // Parse WHERE clause (optional)
    if (this.expect(TokenType.WHERE)) {
      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error(`Expected column name in WHERE clause`);
      }
      const criterion = this.tokens[this.pointer - 1].value as string;

      let operator: Operator;
      if (this.expect(TokenType.LIKE)) {
        operator = Operator.LIKE;
      } else if (this.expect(TokenType.COMPARISON_OPERATOR)) {
        operator = this.tokens[this.pointer - 1].value as Operator;
      } else {
        throw new Error(`Expected comparison operator or LIKE`);
      }

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

    // ORDER BY => optional
    if (this.expect(TokenType.ORDER)) {
      if (!this.expect(TokenType.BY)) {
        throw new Error(`expected BY after ORDER`);
      }

      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error(`expected column name for ordering`);
      }

      const column = this.tokens[this.pointer - 1].value as string;
      let direction: TokenType.ASC | TokenType.DESC = TokenType.ASC; // DEFAULT => ASC

      if (this.expect(TokenType.DESC)) {
        direction = TokenType.DESC;
      } else if (this.expect(TokenType.ASC)) {
        direction = TokenType.ASC;
      }

      // assign ordering info to AST
      AST.orderBy = { column, direction };
    }

    // Semantic validation
    const tableExists =
      await this.semanticAnalyzer.checkTableExistenceInCurrentDB(AST.table);
    if (!tableExists) {
      throw new Error(`Table ${AST.table} doesn't exist`);
    }

    // spread gathered columns names => (selected columns and the condition column(criterion))
    const columnsToCheck = [...AST.columns];

    // if you have WHERE clause you will have condition column so you should push it into the columnsToCheck array
    if (AST.where) {
      columnsToCheck.push(AST.where.criterion);
    }

    // push ordering column into columnsToCheck to verify it exists in the schema
    if (AST.orderBy) {
      columnsToCheck.push(AST.orderBy.column);
    }

    const columnsExist = await this.semanticAnalyzer.checkColumnsExistence(
      AST.table,
      columnsToCheck,
    );
    if (!columnsExist) {
      throw new Error(
        `One or more columns don't exist in table "${AST.table}"`,
      );
    }

    return AST;
  }
}
