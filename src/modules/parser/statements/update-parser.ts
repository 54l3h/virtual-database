import { Injectable } from '@nestjs/common';
import { BaseParser } from '../base-parser';
import { UpdateAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { Operator } from '../../../common/enums/operator.enum';
import { WhereClause } from '../../../common/types/query.types';
import { SemanticAnalyzerService } from '../../semantic-analyzer/semantic-analyzer.service';

@Injectable()
export class UpdateParser extends BaseParser {
  constructor(private readonly semanticAnalyzer: SemanticAnalyzerService) {
    super();
  }

  // parse the tokens by expect the proper tokens which match the correct flow of the update statement and returns the AST which includes the table name, updates => {columnName:newValue}, where => {criterion,operator,value)}
  async parse(tokens: IToken[], pointer: number): Promise<UpdateAST> {
    this.tokens = tokens;
    this.pointer = pointer;

    const AST: UpdateAST = {
      type: TokenType.UPDATE,
      table: '',
      updates: {},
      where: {} as WhereClause,
    };

    // expect table name (identifier) after UPDATE keyword
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected table name`);
    }
    // assign the table namo into the Abstract Syntax Tree object
    AST.table = this.tokens[this.pointer - 1].value as string;

    // expect SET keyword after the table name
    if (!this.expect(TokenType.SET)) {
      throw new Error(`Expected SET keyword`);
    }

    // collect the columns and look for which of them considered as an index
    // if there is any column used as an index you should remove the row which points to the old offset and length
    // and the append the new row
    const columnsToCheck: string[] = [];

    // get column name
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected column name`);
    }
    // get the name of the column from the tokens array
    let updatingColumn = this.tokens[this.pointer - 1].value as string;
    // push the name to the columnsToCheck array to iterate through the columns and check their existence
    columnsToCheck.push(updatingColumn);

    // expect comparison operator => specifically => '='
    if (!this.expect(TokenType.COMPARISON_OPERATOR)) {
      throw new Error(`Expected '='`);
    }

    let token = this.tokens[this.pointer];
    let updatingValue;

    // expect one of these types => number / string / boolean
    if (this.expect(TokenType.NUMBER_LITERAL)) {
      updatingValue = this.tokens[this.pointer - 1].value;
    } else if (this.expect(TokenType.STRING_LITERAL)) {
      updatingValue = this.tokens[this.pointer - 1].value;
    } else if (this.expect(TokenType.BOOLEAN_LITERAL)) {
      updatingValue = this.tokens[this.pointer - 1].value;
    } else {
      throw new Error(`Expected value to update ,but got ${token.type}`);
    }

    // assign the updated column as a key into the updates obj and give it the new value
    AST.updates[updatingColumn] = updatingValue;

    // gather the rest of columns and values if you are going to update multiple columns
    while (this.expect(TokenType.COMMA)) {
      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error(`Expected column name`);
      }
      updatingColumn = this.tokens[this.pointer - 1].value as string;
      // add the another column to the columnsToCheck array
      columnsToCheck.push(updatingColumn);

      // expect comparison operator => specifically => '=' / LIKE
      if (!this.expect(TokenType.COMPARISON_OPERATOR)) {
        throw new Error(`Expected ${TokenType.COMPARISON_OPERATOR}`);
      }

      // GET THE TOKEN
      token = this.tokens[this.pointer];

      // NUMBER / STRING / BOOLEAN
      if (this.expect(TokenType.NUMBER_LITERAL)) {
        updatingValue = this.tokens[this.pointer - 1].value;
      } else if (this.expect(TokenType.STRING_LITERAL)) {
        updatingValue = this.tokens[this.pointer - 1].value;
      } else if (this.expect(TokenType.BOOLEAN_LITERAL)) {
        updatingValue = this.tokens[this.pointer - 1].value;
      } else {
        throw new Error(`Expected value, got ${token.type}`);
      }

      // ASSIGN THE COLUMN NAME INTO THE UPDATES OBJECT WITH THE NEW VALUE
      AST.updates[updatingColumn] = updatingValue;
    }

    // Parse WHERE clause
    if (!this.expect(TokenType.WHERE)) {
      throw new Error(`Expected ${TokenType.WHERE}`);
    }

    // parse the column name (condition column)
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected column name in WHERE clause`);
    }
    // assign the condition column
    const conditionColumn = this.tokens[this.pointer - 1].value as string;

    // expect => = / LIKE
    if (!this.expect(TokenType.COMPARISON_OPERATOR)) {
      throw new Error(`Expected comparison operator`);
    }
    // get the operator to assing it later into the AST
    const operator = this.tokens[this.pointer - 1].value as Operator;

    if (
      !this.expect(TokenType.NUMBER_LITERAL) &&
      !this.expect(TokenType.STRING_LITERAL) &&
      !this.expect(TokenType.BOOLEAN_LITERAL)
    ) {
      throw new Error(`Expected value after the ${operator}`);
    }
    const conditionValue = this.tokens[this.pointer - 1].value;

    AST.where = {
      criterion: conditionColumn, // column name
      operator: operator,
      value: conditionValue,
    };

    columnsToCheck.push(conditionColumn);

    // terminator => ;
    if (!this.expect(TokenType.SEMI_COLON)) {
      throw new Error(`Expected ';'`);
    }

    // semantic analysis
    const tableExists =
      await this.semanticAnalyzer.checkTableExistenceInCurrentDB(AST.table);
    if (!tableExists) {
      throw new Error(`Table ${AST.table} not exist`);
    }

    const columnsExist = await this.semanticAnalyzer.checkColumnsExistence(
      AST.table,
      columnsToCheck,
    );
    if (!columnsExist) {
      throw new Error(`One or more columns don't exist in table ${AST.table}`);
    }

    return AST;
  }
}