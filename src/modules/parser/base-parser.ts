import { IToken } from '../../common/types/token.types';
import { TokenType } from '../../common/enums/token-type.enum';
import { AST } from '../../common/types/ast.type';
import { WhereClause } from '../../common/types/query.types';
import { Operator } from '../../common/enums/operator.enum';
import { DataType } from '../../common/enums/data-type.enum';
import { SchemaLogic } from '../storage/schema/schema.logic';

export interface ParserState {
  tokens: IToken[];
  pointer: number;
}

export abstract class BaseParser {
  constructor(private readonly schemaLogic: SchemaLogic) {}

  abstract parse(tokens: IToken[], pointer: number): Promise<AST>;

  /**
   * Checks if the current token matches the expected element
   */
  protected expect(state: ParserState, element: TokenType): boolean {
    const isMatch = state.tokens[state.pointer]?.type === element;

    if (isMatch) {
      state.pointer++;
    }

    return isMatch;
  }

  /**
   * Gets the value of the previous token (table name, column name, etc) as string
   * then will be coerced
   */
  protected getPreviousTokenValue(state: ParserState): string {
    const index = state.pointer - 1;
    const token = state.tokens[index];

    return String(token.value);
  }

  /**
   * Gets the previous token value as a number
   */
  protected getPreviousTokenValueAsNumber(state: ParserState): number {
    return Number(this.getPreviousTokenValue(state));
  }

  /**
   * Gets the previous token value as a boolean
   */
  protected getPreviousTokenValueAsBoolean(state: ParserState): boolean {
    return Boolean(this.getPreviousTokenValue(state));
  }

  /**
   * Gets the datatype from previous token and converts to uppercase enum
   */
  protected getPreviousDataType(state: ParserState): DataType {
    return this.getPreviousTokenValue(state).toUpperCase() as DataType;
  }

  /**
   * Gets operator from previous token
   */
  protected getPreviousOperator(state: ParserState): Operator {
    return this.getPreviousTokenValue(state) as Operator;
  }

  /**
   * Parses a WHERE clause (SELECT, UPDATE, DELETE)
   */
  protected parseWhereClause(state: ParserState): WhereClause {
    if (!this.expect(state, TokenType.IDENTIFIER)) {
      throw new Error('Expected column name in WHERE clause');
    }
    const criterion = this.getPreviousTokenValue(state);

    let operator: Operator;
    if (this.expect(state, TokenType.LIKE)) {
      operator = Operator.LIKE;
    } else if (this.expect(state, TokenType.COMPARISON_OPERATOR)) {
      operator = this.getPreviousOperator(state);
    } else {
      throw new Error('Expected comparison operator or LIKE');
    }

    let value: string | number | boolean;
    if (this.expect(state, TokenType.NUMBER_LITERAL)) {
      value = this.getPreviousTokenValueAsNumber(state);
    } else if (this.expect(state, TokenType.STRING_LITERAL)) {
      value = this.getPreviousTokenValue(state);
    } else if (this.expect(state, TokenType.BOOLEAN_LITERAL)) {
      value = this.getPreviousTokenValueAsBoolean(state);
    } else {
      throw new Error('Expected value (number, string, or boolean)');
    }

    return { criterion, operator, value };
  }

  /**
   * Expects semicolon
   */
  protected expectSemicolon(state: ParserState): void {
    if (!this.expect(state, TokenType.SEMI_COLON)) {
      throw new Error("Expected ';'");
    }
  }

  /**
   * Parses a literal value (number, string, or boolean)
   */
  protected parseLiteralValue(state: ParserState): string | number | boolean {
    if (this.expect(state, TokenType.NUMBER_LITERAL)) {
      return this.getPreviousTokenValueAsNumber(state);
    }
    if (this.expect(state, TokenType.STRING_LITERAL)) {
      return this.getPreviousTokenValue(state);
    }
    if (this.expect(state, TokenType.BOOLEAN_LITERAL)) {
      const val = this.getPreviousTokenValue(state);
      return val.toLowerCase() === 'true' ? true : false;
    }
    throw new Error('Expected (number, string, or boolean)');
  }

  // Symantic analysis
  /**
   * Validates that a table exists
   */
  protected async validateTableExists(tableName: string): Promise<void> {
    const exists =
      await this.schemaLogic.checkTableExistsInCurrentDB(tableName);

    if (!exists) {
      throw new Error(`Table ${tableName} doesn't exist`);
    }
  }

  /**
   * Validates that a table does NOT exist
   */
  protected async validateTableNotExists(tableName: string): Promise<void> {
    const exists =
      await this.schemaLogic.checkTableExistsInCurrentDB(tableName);

    if (exists) {
      throw new Error(`Table ${tableName} already exists`);
    }
  }

  /**
   * Validates that columns exist in a table
   */
  protected async validateColumnsExist(
    tableName: string,
    columns: string[],
  ): Promise<void> {
    const exist = await this.schemaLogic.checkColumnsExistence(
      tableName,
      columns,
    );

    if (!exist) {
      throw new Error(
        `One or more columns don't exist in table ${tableName}`,
      );
    }
  }

  /**
   * Validates that a column exists in a table
   */
  protected async validateColumnExists(
    tableName: string,
    columnName: string,
  ): Promise<void> {
    const exists = await this.schemaLogic.checkColumnExistence(
      tableName,
      columnName,
    );

    if (!exists) {
      throw new Error(
        `Column ${columnName} does not exist in table ${tableName}`,
      );
    }
  }

  /**
   * Validates that a column does NOT exist in a table
   */
  protected async validateColumnNotExists(
    tableName: string,
    columnName: string,
  ): Promise<void> {
    const exists = await this.schemaLogic.checkColumnExistence(
      tableName,
      columnName,
    );

    if (exists) {
      throw new Error(
        `Column ${columnName} already exists in table "${tableName}"`,
      );
    }
  }

  /**
   * Validates that a database exists
   */
  protected async validateDatabaseExists(dbName: string): Promise<void> {
    const exists = await this.schemaLogic.checkDatabaseExistence(dbName);

    if (!exists) {
      throw new Error(`Database ${dbName} doesn't exist`);
    }
  }

  /**
   * Validates that a database does NOT exist
   */
  protected async validateDatabaseNotExists(dbName: string): Promise<void> {
    const exists = await this.schemaLogic.checkDatabaseExistence(dbName);

    if (exists) {
      throw new Error(`Database ${dbName} already exists`);
    }
  }
}
