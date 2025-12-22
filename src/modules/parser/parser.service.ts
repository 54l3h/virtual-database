import { forwardRef, Inject, Injectable } from '@nestjs/common';
import { TokenizerService } from '../tokenizer/tokenizer.service';
import { Operator } from '../../common/enums/operator.enum';
import { IToken } from '../../common/types/token.types';
import { IColumn } from '../../common/types/schema.types';
import { ExecutorService } from '../executor/executor.service';
import { SemanticAnalyzerService } from '../semantic-analyzer/semantic-analyzer.service';
import { KEYWORDS, TokenType } from '../../common/enums/token-type.enum';
import {
  AST,
  SelectAST,
  DeleteAST,
  UpdateAST,
  InsertAST,
  CreateTableAST,
  CreateDatabaseAST,
  DropDatabaseAST,
  DropTableAST,
  AlterTableAST,
} from '../../common/types/ast.type';
import { DataType } from 'src/common/enums/data-type.enum';

@Injectable()
export class ParserService {
  private tokens: IToken[] = [];
  private pointer: number = 0;

  constructor(
    private readonly tokenizer: TokenizerService,
    private readonly semanticAnalyzer: SemanticAnalyzerService,
    @Inject(forwardRef(() => ExecutorService))
    private readonly executor: ExecutorService,
  ) {}

  // responsibility
  async parse(query: string): Promise<AST> {
    // use the tokenize method from the tokenizer service which is responsible split on the query and identify the Tokens
    this.tokens = this.tokenizer.tokenize(query);

    // pointer to move through the tokens to complete the checking process => (check the value by expect if the parser get what it expected the pointer will go throw the next token if not the parser will throw an error)
    this.pointer = 0;

    // check if the first element of the tokens not a keyword
    // any type is uppercased & all the keywords are uppercased too
    if (this.pointer === 0 && !KEYWORDS.has(this.tokens[0].type)) {
      throw new Error(`INVALID SYNTAX: ${this.tokens[0].value}`);
    }

    // ABSTRACT SYNTAX TREE
    let AST: AST;

    // CHECK THE TYPE OF THE STATEMENT
    // DETERMINATION BASED ON THE FIRST TOKEN WHICH IS THE FIRST MEANINGFUL WORD INTO THE QUERY
    switch (this.tokens[this.pointer].type) {
      // SELECT STATEMENT
      case TokenType.SELECT:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE SELECT METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE SELECT STATEMENT FLOW)
        AST = await this.parseSelect();
        break;

      // DELETE STATEMENT
      case TokenType.DELETE:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE DELETE METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE DELETE STATEMENT FLOW)
        AST = await this.parseDelete();
        break;

      // CREATE STATEMENT => (Create both Databases & Tables)
      case TokenType.CREATE:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE CREATE (DATABASE/TABLE) METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE CREATE STATEMENT FLOW)
        AST =
          // IF THE TOKEN TYPE = DATABASE THEN => PARSE CREATE DATABASE ELSE PARSE CREATE TABLE
          this.tokens[this.pointer].type === TokenType.DATABASE
            ? await this.parseCreateDatabase()
            : await this.parseCreateTable();
        break;

      // DROP STATEMENT => (Drop both Databases & Tables)
      case TokenType.DROP:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE DROP (DATABASE/TABLE) METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE DROP STATEMENT FLOW)
        AST =
          this.tokens[this.pointer].type === TokenType.DATABASE
            ? await this.parseDropDatabase()
            : await this.parseDropTable();
        break;

      // UPDATE STATEMENT
      case TokenType.UPDATE:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE UPDATE METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE UPDATE STATEMENT FLOW)
        AST = await this.parseUpdate();
        break;

      // INSERT STATEMENT
      case TokenType.INSERT:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE INSERT METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE INSERT STATEMENT FLOW)
        AST = await this.parseInsert();
        break;

      // ALTER STATEMENT => (Alter Tables only)
      case TokenType.ALTER:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE ALTER TABLE METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE ALTER TABLE STATEMENT FLOW)
        AST = await this.parseAlterTable();
        break;

      default:
        throw new Error('Unsupported query type');
    }

    // Returns the Abstract Syntax Tree to the executor which will send it to the storage service provider to work on the I/O operations
    return AST;
  }

  // responsibility
  private expect(element: TokenType): boolean {
    // you are passing the expected token => (element)
    // then you will compare it against the token which the pointer on
    // if it matched then this is the expedted token based on the query flow
    // then you should increment the pointer and go through the next token on the array
    // else you should't increment the pointer value and just return the isMatch which will equal false and the statement parser will check the return value and based on it the statement parser will throw an error
    const isMatch = this.tokens[this.pointer].type === element;

    if (isMatch) {
      this.pointer++;
    }

    return isMatch;
  }

  // parse the tokens by expect the proper tokens which match the correct flow of the create database statement and returns the AST which includes the name of the database
  private async parseCreateDatabase(): Promise<CreateDatabaseAST> {
    const AST: CreateDatabaseAST = {
      type: TokenType.CREATE,
      structure: TokenType.DATABASE,
      name: '',
    };

    // expect DATABASE after CREATE
    if (!this.expect(TokenType.DATABASE)) {
      throw new Error(`Expected DATABASE keyword`);
    }

    // expect IDENTIFIER after DATABASE
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected database name`);
    }

    // Assign the Database name into the Abstract syntax tree
    AST.name = this.tokens[this.pointer - 1].value as string;

    // expect semi-colon => ; (after the database name)
    if (!this.expect(TokenType.SEMI_COLON)) {
      throw new Error(`Expected ';'`);
    }

    // semantic analyzer
    // check the existence of the database
    const isExist = await this.semanticAnalyzer.checkDatabaseExistence(
      AST.name,
    );

    if (isExist) {
      throw new Error(`Database ${AST.name} already exists`);
    }

    // returns the AST => will be send to the executor which will be send it to the storage service
    return AST;
  }

  // parse the tokens by expect the proper tokens which match the correct flow of the create table statement and returns the AST which includes the name of the table and the list of columns
  private async parseCreateTable(): Promise<CreateTableAST> {
    const AST: CreateTableAST = {
      type: TokenType.CREATE,
      structure: TokenType.TABLE,
      name: '',
      columns: [],
    };

    // expect TABLE after CREATE
    if (!this.expect(TokenType.TABLE)) {
      throw new Error(`Expected TABLE keyword`);
    }

    // expect table name after TABLE keyword
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected table name`);
    }

    // Assign table name into the AST
    AST.name = this.tokens[this.pointer - 1].value as string;

    // expect the left parantheses operator
    if (!this.expect(TokenType.LPAREN_OPERATOR)) {
      throw new Error(`Expected '('`);
    }

    // if i get the right parantheses then the table will be created but without columns
    // while i didn't get the right parantheses i should expect a column name (IDENTIFIER)
    while (!this.expect(TokenType.RPAREN_OPERATOR)) {
      // expect column name
      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error('Expected column name');
      }
      // expect => pointer + 1
      // to get the name pointer - 1
      const col: IColumn = {
        name: this.tokens[this.pointer - 1].value as string,
        type: null as any, // temporary
      };

      // expect datatype
      if (!this.expect(TokenType.DATATYPE)) {
        throw new Error('Expected data type');
      }

      // get the datatype => pointer - 1
      // to uppercase because DataType enums are uppercased
      const dataType = (
        this.tokens[this.pointer - 1].value as string
      ).toUpperCase();

      // determine the column type
      if (dataType === DataType.SERIAL) {
        col.type = DataType.INTEGER;
        col.autoIncrement = true;
      } else {
        // handle other data types
        col.type = dataType as DataType;
      }

      if (dataType === 'VARCHAR') {
        // you can get length withing the () and you can just get the varchar without the specific size
        if (this.tokens[this.pointer]?.type === TokenType.LPAREN_OPERATOR) {
          this.pointer++;

          // now you should get the length
          if (!this.expect(TokenType.NUMBER_LITERAL)) {
            throw new Error('Expected length for VARCHAR');
          }

          // get the length and assign it into the column object
          col.length = parseInt(this.tokens[this.pointer - 1].value as string);

          // expect the right parenthesis operator
          if (!this.expect(TokenType.RPAREN_OPERATOR)) {
            throw new Error(`Expected ')' after VARCHAR length`);
          }
        }
      }

      // handle the pk and unique and push the column into the AST columns
      while (
        this.tokens[this.pointer].type !== TokenType.COMMA &&
        this.tokens[this.pointer].type !== TokenType.RPAREN_OPERATOR
      ) {
        const currentToken = this.tokens[this.pointer];

        if (currentToken.type === TokenType.PRIMARY) {
          this.pointer++;

          if (!this.expect(TokenType.KEY)) {
            throw new Error('Expected KEY after PRIMARY');
          }
          col.primaryKey = true;
        } else if (currentToken.type === TokenType.UNIQUE) {
          this.pointer++;
          col.unique = true;
        } else {
          throw new Error(`Unexpected token: ${currentToken.value}`);
        }
      }

      AST.columns?.push(col);

      if (this.tokens[this.pointer].type === TokenType.COMMA) {
        this.pointer++;
      }
    }

    // end the create table statement not expect anything, more than the semi-colon
    if (!this.expect(TokenType.SEMI_COLON)) {
      throw new Error(`Expected ';'`);
    }

    // check if the table exist or not
    const tableExists =
      await this.semanticAnalyzer.checkTableExistenceInCurrentDB(AST.name);

    if (tableExists) {
      throw new Error(`Table ${AST.name} already exists`);
    }

    return AST;
  }

  // parse the tokens by expect the proper tokens which match the correct flow of the insert statement and returns the AST which includes the columns and values
  private async parseInsert(): Promise<InsertAST> {
    const AST: InsertAST = {
      type: TokenType.INSERT,
      table: '',
      columns: [],
      values: [],
    };

    // make sure INTO keyword follows INSERT
    if (!this.expect(TokenType.INTO)) {
      throw new Error(`Expected ${TokenType.INTO}`);
    }

    // extract the target table name
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected table name`);
    }
    AST.table = this.tokens[this.pointer - 1].value as string;

    // expect '(' to start the column list
    if (!this.expect(TokenType.LPAREN_OPERATOR)) {
      throw new Error(`Expected '('`);
    }

    // iterate through the list of columns until ')'
    while (!this.expect(TokenType.RPAREN_OPERATOR)) {
      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error('Expected column name');
      }
      AST.columns.push(this.tokens[this.pointer - 1].value as string);

      // consume commas between column names
      if (this.tokens[this.pointer]?.type === TokenType.COMMA) {
        this.pointer++;
      }
    }

    // Expect VALUES keyword after columns list
    if (!this.expect(TokenType.VALUES)) {
      throw new Error(`Expected VALUES keyword`);
    }

    while (this.tokens[this.pointer]?.type === TokenType.LPAREN_OPERATOR) {
      if (!this.expect(TokenType.LPAREN_OPERATOR)) {
        throw new Error(`Expected '('`);
      }

      // get the number of values to compare it to the number of columns => target columns should === expressions
      let valueCount = 0;

      while (!this.expect(TokenType.RPAREN_OPERATOR)) {
        const token = this.tokens[this.pointer];

        if (token.type === TokenType.NUMBER_LITERAL) {
          this.pointer++;
          AST.values.push(parseInt(token.value as string));
        } else if (token.type === TokenType.STRING_LITERAL) {
          this.pointer++;
          AST.values.push(token.value as string);
        } else if (token.type === TokenType.BOOLEAN_LITERAL) {
          this.pointer++;
          AST.values.push(token.value as boolean);
        } else {
          throw new Error(`Unexpected token type: ${token.type}`);
        }

        valueCount++;

        if (this.tokens[this.pointer].type === TokenType.COMMA) {
          this.pointer++;
        }
      }

      // compare the number of values against the number of columns
      if (valueCount !== AST.columns.length) {
        throw new Error(`Value count doesn't match column count`);
      }
      
      // for insert many => commas between rows
      if (this.tokens[this.pointer]?.type === TokenType.COMMA) {
        this.pointer++;
      }
    }

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
      AST.columns,
    );
    if (!columnsExist) {
      throw new Error(`Invalid columns names`);
    }

    return AST;
  }

  //
  private async parseUpdate(): Promise<UpdateAST> {
    const AST: UpdateAST = {
      type: TokenType.UPDATE,
      table: '',
      updates: {},
    };

    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected table name`);
    }
    AST.table = this.tokens[this.pointer - 1].value as string;

    if (!this.expect(TokenType.SET)) {
      throw new Error(`Expected SET keyword`);
    }

    // collect the columns and look for which of them considered as an index
    // if there is any column used as an index you should remove the row which points to the old offset and length
    // and the append the new row
    const columnsToCheck: string[] = [];

    // Parse first column=value (required)
    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected column name`);
    }
    let updatingColumn = this.tokens[this.pointer - 1].value as string;
    columnsToCheck.push(updatingColumn);

    if (!this.expect(TokenType.COMPARISON_OPERATOR)) {
      throw new Error(`Expected '='`);
    }

    let token = this.tokens[this.pointer];
    let updatingValue;

    if (this.expect(TokenType.NUMBER_LITERAL)) {
      updatingValue = this.tokens[this.pointer - 1].value;
    } else if (this.expect(TokenType.STRING_LITERAL)) {
      updatingValue = this.tokens[this.pointer - 1].value;
    } else if (this.expect(TokenType.BOOLEAN_LITERAL)) {
      updatingValue = this.tokens[this.pointer - 1].value;
    } else {
      throw new Error(`Expected value to update ,but got ${token.type}`);
    }

    AST.updates[updatingColumn] = updatingValue;

    // Parse additional columns
    while (this.expect(TokenType.COMMA)) {
      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error(`Expected column name`);
      }
      updatingColumn = this.tokens[this.pointer - 1].value as string;
      columnsToCheck.push(updatingColumn);

      if (!this.expect(TokenType.COMPARISON_OPERATOR)) {
        throw new Error(`Expected '='`);
      }

      token = this.tokens[this.pointer];

      if (this.expect(TokenType.NUMBER_LITERAL)) {
        updatingValue = this.tokens[this.pointer - 1].value;
      } else if (this.expect(TokenType.STRING_LITERAL)) {
        updatingValue = this.tokens[this.pointer - 1].value;
      } else if (this.expect(TokenType.BOOLEAN_LITERAL)) {
        updatingValue = this.tokens[this.pointer - 1].value;
      } else {
        throw new Error(`Expected value, got ${token.type}`);
      }

      AST.updates[updatingColumn] = updatingValue;
    }

    // Parse WHERE clause (optional)
    if (this.expect(TokenType.WHERE)) {
      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error(`Expected column name in WHERE clause`);
      }
      const conditionColumn = this.tokens[this.pointer - 1].value as string;

      if (!this.expect(TokenType.COMPARISON_OPERATOR)) {
        throw new Error(`Expected comparison operator`);
      }
      const operator = this.tokens[this.pointer - 1].value as Operator;

      if (!this.expect(TokenType.NUMBER_LITERAL)) {
        throw new Error(`Expected value`);
      }
      const conditionValue = this.tokens[this.pointer - 1].value;

      AST.where = {
        criterion: conditionColumn,
        operator: operator,
        value: conditionValue,
      };

      columnsToCheck.push(conditionColumn);
    }

    if (!this.expect(TokenType.SEMI_COLON)) {
      throw new Error(`Expected ';'`);
    }

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
      throw new Error(
        `One or more columns don't exist in table "${AST.table}"`,
      );
    }

    return AST;
  }

  private async parseSelect(): Promise<SelectAST> {
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
      throw new Error('Expected * or column name(s)');
    }

    // Parse FROM clause
    if (!this.expect(TokenType.FROM)) {
      throw new Error(`Expected FROM keyword`);
    }

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

      // Check for LIKE or comparison operator
      let operator: Operator;

      if (this.expect(TokenType.LIKE)) {
        operator = Operator.LIKE; // Make sure you have LIKE in your Operator enum
      } else if (this.expect(TokenType.COMPARISON_OPERATOR)) {
        operator = this.tokens[this.pointer - 1].value as Operator;
      } else {
        throw new Error(`Expected comparison operator or LIKE`);
      }

      const token = this.tokens[this.pointer];

      let value: string | number | boolean = '';
      if (token.type === TokenType.NUMBER_LITERAL) {
        if (!this.expect(TokenType.NUMBER_LITERAL)) {
          throw new Error(`Expected value`);
        }
        value = this.tokens[this.pointer - 1].value;
      }

      if (token.type === TokenType.STRING_LITERAL) {
        if (!this.expect(TokenType.STRING_LITERAL)) {
          throw new Error(`Expected value`);
        }
        value = this.tokens[this.pointer - 1].value;
      }

      if (token.type === TokenType.BOOLEAN_LITERAL) {
        if (!this.expect(TokenType.BOOLEAN_LITERAL)) {
          throw new Error(`Expected value`);
        }
        value = this.tokens[this.pointer - 1].value;
      }

      AST.where = { criterion, operator, value };
    }

    // Semantic validation
    const tableExists =
      await this.semanticAnalyzer.checkTableExistenceInCurrentDB(AST.table);
    if (!tableExists) {
      throw new Error(`Table "${AST.table}" doesn't exist`);
    }

    const columnsToCheck = [...AST.columns];
    if (AST.where) {
      columnsToCheck.push(AST.where.criterion);
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

  private async parseDelete(): Promise<DeleteAST> {
    const AST: DeleteAST = {
      type: TokenType.DELETE,
      table: '',
    };

    if (!this.expect(TokenType.FROM)) {
      throw new Error(`Expected FROM keyword`);
    }

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

      if (!this.expect(TokenType.COMPARISON_OPERATOR)) {
        throw new Error(`Expected comparison operator`);
      }
      const operator = this.tokens[this.pointer - 1].value as Operator;

      if (!this.expect(TokenType.NUMBER_LITERAL)) {
        throw new Error(`Expected value`);
      }
      const value = this.tokens[this.pointer - 1].value;

      AST.where = { criterion, operator, value };
    }

    // Semantic validation
    const tableExists =
      await this.semanticAnalyzer.checkTableExistenceInCurrentDB(AST.table);
    if (!tableExists) {
      throw new Error(`Table "${AST.table}" doesn't exist`);
    }

    if (AST.where) {
      const columnsExist = await this.semanticAnalyzer.checkColumnsExistence(
        AST.table,
        [AST.where.criterion],
      );
      if (!columnsExist) {
        throw new Error(
          `Column "${AST.where.criterion}" doesn't exist in table "${AST.table}"`,
        );
      }
    }

    return AST;
  }

  private async parseDropDatabase(): Promise<DropDatabaseAST> {
    const AST: DropDatabaseAST = {
      type: TokenType.DROP,
      structure: TokenType.DATABASE,
      name: '',
    };

    if (!this.expect(TokenType.DATABASE)) {
      throw new Error(`Expected ${TokenType.TABLE}`);
    }

    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected ${TokenType.IDENTIFIER}`);
    }
    AST.name = this.tokens[this.pointer - 1].value as string;

    // semantic analysis
    // check database existence
    const isExist = await this.semanticAnalyzer.checkDatabaseExistence(
      AST.name,
    );

    if (!isExist) {
      throw new Error(`Database ${AST.name} not exist`);
    }

    return AST;
  }

  private async parseDropTable(): Promise<DropTableAST> {
    const AST: DropTableAST = {
      type: TokenType.DROP,
      structure: TokenType.TABLE,
      name: '',
    };

    if (!this.expect(TokenType.TABLE)) {
      throw new Error(`Expected ${TokenType.TABLE}`);
    }

    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected ${TokenType.IDENTIFIER}`);
    }
    AST.name = this.tokens[this.pointer - 1].value as string;

    // semantic analysis
    // check table existence
    const isExist = await this.semanticAnalyzer.checkTableExistenceInCurrentDB(
      AST.name,
    );

    if (!isExist) {
      throw new Error(`Table ${AST.name} is not exist`);
    }

    return AST;
  }

  private async parseAlterTable(): Promise<AlterTableAST> {
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
