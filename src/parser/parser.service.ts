import { forwardRef, Inject, Injectable } from '@nestjs/common';
import { ExecutorService } from 'src/executor/executor.service';
import { SemanticAnalyzerService } from 'src/semantic-analyzer/semantic-analyzer.service';
import {
  IToken,
  KEYWORDS,
  TokenizerService,
  TokenType,
} from 'src/tokenizer/tokenizer.service';
import { ColumnType, Column } from '../common/types/data-types';

export enum Operators {
  GREATER_THAN = '>',
  LESS_THAN = '<',
  EQUAL = '=',
  GREATER_THAN_OR_EQUAL = '>=',
  LESS_THAN_OR_EQUAL = '<=',
}

export interface IColumn {
  name: string;
  data_type: string;
}

export interface IQuery {
  type: TokenType;
  columns: string[];
  table: string;
  where: {
    criterion: string;
    value: string | number | boolean;
    operator: Operators;
  };
}

export interface IDelQuery {
  type: TokenType.DELETE;
  columns: IColumn[];
  table: string;
  where: {
    criterion: string;
    value: string | number | boolean;
    operator: Operators;
  };
}

@Injectable()
export class ParserService {
  private tokens: IToken[] = [];
  private idx: number = 0;
  constructor(
    private readonly tokenizer: TokenizerService,
    private readonly semanticAnalyzer: SemanticAnalyzerService,
    @Inject(forwardRef(() => ExecutorService))
    private readonly executor: ExecutorService,
  ) {}

  async parse(query: string) {
    this.tokens = this.tokenizer.tokenize(query);
    let AST: IQuery = {} as IQuery;
    console.log(this.tokens);

    this.idx = 0;
    // console.log(KEYWORDS.has(tokens[0].type));

    if (this.idx === 0 && !KEYWORDS.has(this.tokens[0].type)) {
      throw new Error(`INVALID SYNTAX ${this.tokens[0].value}`);
    }

    switch (this.tokens[this.idx].type) {
      case TokenType.SELECT:
        this.idx++;
        AST = this.parseSelectt(this.tokens);
        break;

      case TokenType.DELETE:
        this.idx++;
        AST = this.parseDelete(this.tokens) as any;
        break;

      case TokenType.CREATE:
        this.idx++;
        AST =
          this.tokens[this.idx].type === TokenType.DATABASE
            ? (this.parseCreateDatabase(query) as any)
            : (this.parseCreateTable(query) as any);
        break;

      case TokenType.UPDATE:
        this.idx++;
        AST = (await this.parseUpdate(query)) as any;
        console.log('swit up');
        console.log(AST);

        break;

      default:
        break;
    }

    return AST;
  }

  async parseUpdate(query: string) {
    const AST = {
      type: TokenType.UPDATE,
      tableName: TokenType.TABLE,
      updatingColumn: {} as Column,
      conditionColumn: {} as Column,
      updatingValue: '' as any,
      conditionValue: '' as any,
    };

    if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
      throw new Error(`Expected ${TokenType.IDENTIFIER}`);
    }

    AST.tableName = this.tokens[this.idx - 1].value;

    if (!this.expectt(TokenType.SET, this.tokens)) {
      throw new Error(`Expected ${TokenType.SET}`);
    }

    if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
      throw new Error(`Expected ${TokenType.IDENTIFIER}`);
    }

    AST.updatingColumn = this.tokens[this.idx - 1].value;

    if (!this.expectt(TokenType.COMPARISON_OPERATOR, this.tokens)) {
      throw new Error(`Expected ${TokenType.COMPARISON_OPERATOR}`);
    }

    if (!this.expectt(TokenType.NUMBER_LITERAL, this.tokens)) {
      throw new Error(`Expected ${TokenType.NUMBER_LITERAL}`);
    }

    AST.updatingValue = this.tokens[this.idx - 1].value;

    if (!this.expectt(TokenType.WHERE, this.tokens)) {
      throw new Error(`Expected ${TokenType.WHERE}`);
    }

    if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
      throw new Error(`Expected ${TokenType.IDENTIFIER}`);
    }

    AST.conditionColumn = this.tokens[this.idx - 1].value;

    if (!this.expectt(TokenType.COMPARISON_OPERATOR, this.tokens)) {
      throw new Error(`Expected ${TokenType.COMPARISON_OPERATOR}`);
    }

    if (!this.expectt(TokenType.NUMBER_LITERAL, this.tokens)) {
      throw new Error(`Expected ${TokenType.NUMBER_LITERAL}`);
    }

    AST.conditionValue = this.tokens[this.idx - 1].value;

    if (!this.expectt(TokenType.SEMI_COLON, this.tokens)) {
      throw new Error(`Expected ${TokenType.SEMI_COLON}`);
    }

    console.log('Finally');

    // Check coulmns existense
    await this.semanticAnalyzer.checkColumnsExistence(AST.tableName, [
      AST.conditionColumn as any,
      AST.updatingColumn as any,
    ]);

    console.log(AST);

    return AST;
  }

  parseSelectt(tokens: IToken[]) {
    // You got the tokens
    // Now you should check the flow
    // Then build the ast
    // Send the AST to the semantic analyzer to check tables and columns existence
    // After validate on the existence you should send the ast to the executor to start the execution

    // SELECT * FROM users;
    // SELECT id FROM users;
    // SELECT id, name FROM users;

    // const results: boolean[] = [
    //   this.expectt(TokenType.ASTERISK, tokens) ||
    //     this.expectt(TokenType.IDENTIFIER, tokens) ||
    //     (this.expectt(TokenType.IDENTIFIER, tokens) &&
    //       this.expectt(TokenType.COMMA, tokens) &&
    //       this.expectt(TokenType.IDENTIFIER, tokens)),
    //   this.expectt(TokenType.FROM, tokens),
    //   this.expectt(TokenType.IDENTIFIER, tokens),
    //   this.expectt(TokenType.SEMI_COLON, tokens),
    // ];

    const AST: IQuery = {
      type: TokenType.SELECT,
      columns: [] as string[],
      table: '',
      where: {},
    } as IQuery;

    // AST.where.value = 5;
    // AST.where.criterion = 'id';
    // Validate the syntax / && Pushing the correct values into the AST
    // if (
    //   this.expectt(TokenType.ASTERISK, tokens) &&
    //   this.expectt(TokenType.FROM, tokens) &&
    //   this.expect(TokenType.IDENTIFIER)
    //   // I won't check on the semi-colon existence

    // )

    // if (this.expectt(TokenType.ASTERISK, tokens)) {
    //   AST.columns.push(TokenType.ASTERISK);
    //   if (
    //     this.expectt(TokenType.FROM, tokens) &&
    //     this.expect(TokenType.IDENTIFIER)
    //   ) {
    //     console.log('h');

    //     AST.table = tokens[this.idx - 1].value;
    //     return true;
    //   }
    // } else if (this.expectt(TokenType.IDENTIFIER, tokens)) {
    //   AST.columns.push(tokens[this.idx - 1].value);

    //   while (this.expectt(TokenType.COMMA, tokens)) {
    //     AST.columns.push(tokens[this.idx - 1].value);
    //     if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
    //       throw new Error('Expected column name after the comma');
    //     }
    //   }
    // } else {
    //   throw new Error('Expected * or list of columns');
    // }

    if (this.expectt(TokenType.ASTERISK, tokens)) {
      AST.columns.push('*');
    } else if (this.expectt(TokenType.IDENTIFIER, tokens)) {
      // first column
      AST.columns.push(tokens[this.idx - 1].value);

      while (this.expectt(TokenType.COMMA, tokens)) {
        if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
          throw new Error('Expected column name after the comma');
        }

        AST.columns.push(tokens[this.idx - 1].value);
      }
    } else {
      throw new Error('Expected * or list of columns');
    }

    // after any of the above comes
    // 1- FROM
    // 2- Table name(IDENTIFER)

    if (!this.expectt(TokenType.FROM, tokens)) {
      throw new Error(`Expect ${TokenType.FROM}`);
    }

    if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
      throw new Error(`Expect ${TokenType.IDENTIFIER}`);
    }

    AST.table = tokens[this.idx - 1].value;

    if (this.expectt(TokenType.WHERE, tokens)) {
      if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
        throw new Error(`Expect ${TokenType.IDENTIFIER}`);
      }
      AST.where.criterion = tokens[this.idx - 1].value;

      if (!this.expectt(TokenType.COMPARISON_OPERATOR, tokens)) {
        throw new Error(`Expect ${TokenType.COMPARISON_OPERATOR}`);
      }
      AST.where.operator = tokens[this.idx - 1].value;

      // value
      if (!this.expectt(TokenType.NUMBER_LITERAL, tokens)) {
        throw new Error(`Expect ${TokenType.NUMBER_LITERAL}`);
      }
      AST.where.value = tokens[this.idx - 1].value;
    }

    // if (
    //   this.expectt(TokenType.WHERE, tokens) &&
    //   this.expectt(TokenType.IDENTIFIER, tokens) &&
    //   this.expectt(TokenType.COMPARISON_OPERATOR, tokens)
    // ) {
    //   AST.where.criterion = tokens[this.idx - 3].value;
    //   AST.where.operator = tokens[this.idx - 2].value;
    //   AST.where.value = tokens[this.idx - 1].value;
    // }

    console.log(AST);

    this.semanticAnalyzer.checkTableExistence(AST.table);
    this.semanticAnalyzer.checkColumnsExistence(AST.table, [
      AST.where.criterion,
      ...AST.columns,
    ]);

    // console.log(AST);

    return AST; // Abstract Search Tree

    // send the AST to the semantic analizer to check the existence of the columns and tables

    // Now you should execute the query

    // // Build an AST
    // const AST:IQuery = {
    //     type:'SELECT',
    //     table:`${}`
    // };
  }

  parseDelete(tokens: IToken[]) {
    const AST: IDelQuery = {
      type: TokenType.DELETE,
      columns: [] as IColumn[],
      table: '',
      where: {},
    } as IDelQuery;

    // DELETE FROM users;
    // DELETE FROM users WHERE id > 2;
    // DELETE FROM users WHERE id = 2;
    // DELETE FROM users WHERE role = 'test';

    if (!this.expectt(TokenType.FROM, tokens)) {
      throw new Error(`Expect ${TokenType.FROM}`);
    }

    if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
      throw new Error(`Expect ${TokenType.IDENTIFIER}`);
    }

    AST.table = tokens[this.idx - 1].value;

    if (this.expectt(TokenType.WHERE, tokens)) {
      if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
        throw new Error(`Expect ${TokenType.IDENTIFIER}`);
      }
      AST.where.criterion = tokens[this.idx - 1].value;

      if (!this.expectt(TokenType.COMPARISON_OPERATOR, tokens)) {
        throw new Error(`Expect ${TokenType.COMPARISON_OPERATOR}`);
      }
      AST.where.operator = tokens[this.idx - 1].value;

      // value
      if (!this.expectt(TokenType.NUMBER_LITERAL, tokens)) {
        throw new Error(`Expect ${TokenType.NUMBER_LITERAL}`);
      }
      AST.where.value = tokens[this.idx - 1].value;
    }

    console.log({ AST });

    this.semanticAnalyzer.checkTableExistence(AST.table);
    this.semanticAnalyzer.checkColumnsExistence(AST.table, [
      AST.where.criterion,
    ]);

    return AST;
  }

  parseCreate(query: string) {
    this.idx = 0;
    this.tokens = this.tokenizer.tokenize(query);

    // CREATE TABLE table_name {
    // }

    /**
     * CREATE TABLE table_name {
     *    column1_name datatype (size) [constraints],
     *    column
     * }
     */

    /**
     *     EmployeeID INT PRIMARY KEY AUTO_INCREMENT,
            FirstName VARCHAR(50) NOT NULL,
            LastName VARCHAR(50) NOT NULL,
            DepartmentID INT,
     */

    const AST: IQuery = {
      type: TokenType.CREATE,
      columns: [] as string[],
      table: '',
      where: {},
    } as IQuery;

    switch (this.tokens[this.idx].type) {
      case TokenType.DATABASE:
        break;

      default:
        break;
    }

    this.expectt(TokenType.TABLE, this.tokens);
  }

  async parseCreateTable(query: string) {
    const AST = {
      type: TokenType.CREATE,
      structure: TokenType.TABLE,
      structure_name: '',
      columns: [] as Column[],
    };

    console.log('parseCreateTable');

    if (!this.expectt(TokenType.TABLE, this.tokens)) {
      throw new Error(`Expect ${TokenType.DATABASE}`);
    }
    AST.structure = this.tokens[this.idx - 1].value;

    if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
      throw new Error(`Expect ${TokenType.IDENTIFIER}`);
    }
    AST.structure_name = this.tokens[this.idx - 1].value;

    if (!this.expectt(TokenType.LPAREN_OPERATOR, this.tokens)) {
      throw new Error(`Expect ${TokenType.LPAREN_OPERATOR}`);
    }

    while (!this.expectt(TokenType.RPAREN_OPERATOR, this.tokens)) {
      if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
        throw new Error('Expected column name');
      }
      let col: Column = { name: this.tokens[this.idx - 1].value } as Column;

      if (!this.expectt(TokenType.DATATYPE, this.tokens)) {
        throw new Error('Expected datatype');
      }
      col.type = this.tokens[this.idx - 1].value;

      AST.columns.push(col);

      if (this.tokens[this.idx]?.type === TokenType.COMMA) {
        this.idx++;
      }
    }

    if (!this.expectt(TokenType.SEMI_COLON, this.tokens)) {
      throw new Error(`Expect ${TokenType.SEMI_COLON}`);
    }
    // console.log(AST);

    const isExist = await this.semanticAnalyzer.isTableExist(
      AST.structure_name,
    );
    console.log({ isExist });

    if (isExist) {
      throw new Error(`Relation ${AST.structure_name} is already exist`);
    }

    console.log('finished');

    return AST;
  }

  parseCreateDatabase(query: string) {
    const AST = {
      type: TokenType.CREATE,
      structure: TokenType.DATABASE,
      structure_name: '',
    };

    if (!this.expectt(TokenType.DATABASE, this.tokens)) {
      throw new Error(`Expect ${TokenType.DATABASE}`);
    }
    AST.structure = this.tokens[this.idx - 1].value;

    if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
      throw new Error(`Expect ${TokenType.IDENTIFIER}`);
    }
    AST.structure_name = this.tokens[this.idx - 1].value;

    if (!this.expectt(TokenType.SEMI_COLON, this.tokens)) {
      throw new Error(`Expect ${TokenType.SEMI_COLON}`);
    }

    return AST;
  }

  expectt(element: string, tokens: IToken[]): boolean {
    if (this.idx >= tokens.length) return false;

    const isTheNextElement = tokens[this.idx].type === element;

    if (isTheNextElement) {
      this.idx++;
    }

    return isTheNextElement;
  }

  parseSelect(tokens: IToken[]): boolean {
    // // this.idx = 0;
    // const tokens: IToken[] = this.tokenizer.tokenize(sql);
    this.idx = 0;
    if (tokens[this.idx].value === 'SELECT') {
      const results: boolean[] = [];

      this.tokens = tokens;
      this.idx++;

      //   results.push(
      //     this.expect('*') || this.expect('id'),
      //     this.expect('FROM'),
      //     this.expect('users'),
      //     this.expect(';'),
      //   );

      if (this.expect('*') || this.expect(TokenType.IDENTIFIER)) {
        if (this.expect(TokenType.FROM)) {
        }
      }

      if (!(this.expect(',') && this.expect(TokenType.IDENTIFIER))) {
        throw new Error('SYNTAX ERROR');
      }

      while (this.expect(',')) {}

      results.push(
        this.expect('*') || this.expect(TokenType.IDENTIFIER),
        this.expect(TokenType.FROM) ||
          (this.expect(',') && this.expect(TokenType.IDENTIFIER)),

        this.expect('users'),
        this.expect(';'),
      );

      // true(syntax error)
      if (this.checkTheFlow(results)) {
        return false;
      }
    }

    return true;
  }

  // true(syntax error)
  checkTheFlow(results: boolean[]): boolean {
    return results.includes(false);
  }

  expect(element: string): boolean {
    if (this.idx >= this.tokens.length) return false;

    const isTheNextElement = this.tokens[this.idx].value === element;

    if (isTheNextElement) {
      this.idx++;
    }

    return isTheNextElement;
  }
}
