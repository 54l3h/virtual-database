import { Injectable } from '@nestjs/common';
import { BaseParser } from '../base-parser';
import { CreateDatabaseAST, CreateTableAST } from '../../../common/types/ast.type';
import { TokenType } from '../../../common/enums/token-type.enum';
import { IToken } from '../../../common/types/token.types';
import { IColumn } from '../../../common/types/schema.types';
import { DataType } from '../../../common/enums/data-type.enum';
import { SemanticAnalyzerService } from '../../semantic-analyzer/semantic-analyzer.service';

@Injectable()
export class CreateParser extends BaseParser {
  constructor(private readonly semanticAnalyzer: SemanticAnalyzerService) {
    super();
  }

  /**
   * Main entry point for CREATE statements.
   * Logic moved from the main ParserService switch case.
   */
  async parse(tokens: IToken[], pointer: number): Promise<CreateDatabaseAST | CreateTableAST> {
    this.tokens = tokens;
    this.pointer = pointer;

    // IF THE TOKEN TYPE = DATABASE THEN => PARSE CREATE DATABASE ELSE PARSE CREATE TABLE
    if (this.tokens[this.pointer].type === TokenType.DATABASE) {
      return await this.parseCreateDatabase();
    } else {
      return await this.parseCreateTable();
    }
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
}