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
  AlterDatabaseAST,
  AlterTableAST,
} from '../../common/types/ast.type';
import { DataType } from 'src/common/enums/data-type.enum';

// Deprecated
// export enum Operators {
//   GREATER_THAN = '>',
//   LESS_THAN = '<',
//   EQUAL = '=',
//   GREATER_THAN_OR_EQUAL = '>=',
//   LESS_THAN_OR_EQUAL = '<=',
// }

// export interface IQuery {
//   type: TokenType;
//   columns: string[];
//   table: string;
//   where: {
//     criterion: string;
//     value: string | number | boolean;
//     operator: Operators;
//   };
// }

// export interface IDelQuery {
//   type: TokenType.DELETE;
//   columns: IColumn[];
//   table: string;
//   where: {
//     criterion: string;
//     value: string | number | boolean;
//     operator: Operators;
//   };
// }

// @Injectable()
// export class ParserService {
//   private tokens: IToken[] = [];
//   private idx: number = 0;
//   constructor(
//     private readonly tokenizer: TokenizerService,
//     private readonly semanticAnalyzer: SemanticAnalyzerService,
//     @Inject(forwardRef(() => ExecutorService))
//     private readonly executor: ExecutorService,
//   ) {}

//   async parse(query: string) {
//     this.tokens = this.tokenizer.tokenize(query);
//     let AST: IQuery = {} as IQuery;
//     console.log(this.tokens);

//     this.idx = 0;
//     // console.log(KEYWORDS.has(tokens[0].type));

//     if (this.idx === 0 && !KEYWORDS.has(this.tokens[0].type)) {
//       throw new Error(`INVALID SYNTAX ${this.tokens[0].value}`);
//     }

//     switch (this.tokens[this.idx].type) {
//       case TokenType.SELECT:
//         this.idx++;
//         AST = this.parseSelectt(this.tokens);
//         break;

//       case TokenType.DELETE:
//         this.idx++;
//         AST = this.parseDelete(this.tokens) as any;
//         break;

//       case TokenType.CREATE:
//         this.idx++;
//         AST =
//           this.tokens[this.idx].type === TokenType.DATABASE
//             ? (this.parseCreateDatabase(query) as any)
//             : (this.parseCreateTable(query) as any);
//         break;

//       case TokenType.UPDATE:
//         this.idx++;
//         AST = (await this.parseUpdate(query)) as any;
//         console.log('swit up');
//         console.log(AST);

//         break;

//       case TokenType.INSERT:
//         this.idx++;
//         AST = (await this.parseInsert(query)) as any;
//         console.log('swit up');
//         console.log(AST);

//         break;

//       default:
//         break;
//     }

//     return AST;
//   }

//   async parseInsert(query: string) {
//     console.log(query);
//     const AST = {
//       type: TokenType.UPDATE,
//       tableName: '',
//       columns: [] as string[],
//       values: [] as any[],
//     };

//     if (!this.expectt(TokenType.INTO, this.tokens)) {
//       throw new Error(`Expected ${TokenType.INTO}`);
//     }

//     if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
//       throw new Error(`Expected ${TokenType.IDENTIFIER}`);
//     }

//     if (!this.expectt(TokenType.LPAREN_OPERATOR, this.tokens)) {
//       throw new Error(`Expected ${TokenType.LPAREN_OPERATOR}`);
//     }
//     // Continue from here
//   }

//   async parseUpdate(query: string) {
//     const AST = {
//       type: TokenType.UPDATE,
//       tableName: TokenType.TABLE,
//       updatingColumn: {} as Column,
//       conditionColumn: {} as Column,
//       updatingValue: '' as any,
//       conditionValue: '' as any,
//     };

//     if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
//       throw new Error(`Expected ${TokenType.IDENTIFIER}`);
//     }

//     AST.tableName = this.tokens[this.idx - 1].value;

//     if (!this.expectt(TokenType.SET, this.tokens)) {
//       throw new Error(`Expected ${TokenType.SET}`);
//     }

//     if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
//       throw new Error(`Expected ${TokenType.IDENTIFIER}`);
//     }

//     AST.updatingColumn = this.tokens[this.idx - 1].value;

//     if (!this.expectt(TokenType.COMPARISON_OPERATOR, this.tokens)) {
//       throw new Error(`Expected ${TokenType.COMPARISON_OPERATOR}`);
//     }

//     if (!this.expectt(TokenType.NUMBER_LITERAL, this.tokens)) {
//       throw new Error(`Expected ${TokenType.NUMBER_LITERAL}`);
//     }

//     AST.updatingValue = this.tokens[this.idx - 1].value;

//     if (!this.expectt(TokenType.WHERE, this.tokens)) {
//       throw new Error(`Expected ${TokenType.WHERE}`);
//     }

//     if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
//       throw new Error(`Expected ${TokenType.IDENTIFIER}`);
//     }

//     AST.conditionColumn = this.tokens[this.idx - 1].value;

//     if (!this.expectt(TokenType.COMPARISON_OPERATOR, this.tokens)) {
//       throw new Error(`Expected ${TokenType.COMPARISON_OPERATOR}`);
//     }

//     if (!this.expectt(TokenType.NUMBER_LITERAL, this.tokens)) {
//       throw new Error(`Expected ${TokenType.NUMBER_LITERAL}`);
//     }

//     AST.conditionValue = this.tokens[this.idx - 1].value;

//     if (!this.expectt(TokenType.SEMI_COLON, this.tokens)) {
//       throw new Error(`Expected ${TokenType.SEMI_COLON}`);
//     }

//     console.log('Finally');

//     // Check coulmns existense
//     await this.semanticAnalyzer.checkColumnsExistence(AST.tableName, [
//       AST.conditionColumn as any,
//       AST.updatingColumn as any,
//     ]);

//     console.log(AST);

//     return AST;
//   }

//   parseSelectt(tokens: IToken[]) {
//     // You got the tokens
//     // Now you should check the flow
//     // Then build the ast
//     // Send the AST to the semantic analyzer to check tables and columns existence
//     // After validate on the existence you should send the ast to the executor to start the execution

//     // SELECT * FROM users;
//     // SELECT id FROM users;
//     // SELECT id, name FROM users;

//     // const results: boolean[] = [
//     //   this.expectt(TokenType.ASTERISK, tokens) ||
//     //     this.expectt(TokenType.IDENTIFIER, tokens) ||
//     //     (this.expectt(TokenType.IDENTIFIER, tokens) &&
//     //       this.expectt(TokenType.COMMA, tokens) &&
//     //       this.expectt(TokenType.IDENTIFIER, tokens)),
//     //   this.expectt(TokenType.FROM, tokens),
//     //   this.expectt(TokenType.IDENTIFIER, tokens),
//     //   this.expectt(TokenType.SEMI_COLON, tokens),
//     // ];

//     const AST: IQuery = {
//       type: TokenType.SELECT,
//       columns: [] as string[],
//       table: '',
//       where: {},
//     } as IQuery;

//     // AST.where.value = 5;
//     // AST.where.criterion = 'id';
//     // Validate the syntax / && Pushing the correct values into the AST
//     // if (
//     //   this.expectt(TokenType.ASTERISK, tokens) &&
//     //   this.expectt(TokenType.FROM, tokens) &&
//     //   this.expect(TokenType.IDENTIFIER)
//     //   // I won't check on the semi-colon existence

//     // )

//     // if (this.expectt(TokenType.ASTERISK, tokens)) {
//     //   AST.columns.push(TokenType.ASTERISK);
//     //   if (
//     //     this.expectt(TokenType.FROM, tokens) &&
//     //     this.expect(TokenType.IDENTIFIER)
//     //   ) {
//     //     console.log('h');

//     //     AST.table = tokens[this.idx - 1].value;
//     //     return true;
//     //   }
//     // } else if (this.expectt(TokenType.IDENTIFIER, tokens)) {
//     //   AST.columns.push(tokens[this.idx - 1].value);

//     //   while (this.expectt(TokenType.COMMA, tokens)) {
//     //     AST.columns.push(tokens[this.idx - 1].value);
//     //     if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
//     //       throw new Error('Expected column name after the comma');
//     //     }
//     //   }
//     // } else {
//     //   throw new Error('Expected * or list of columns');
//     // }

//     if (this.expectt(TokenType.ASTERISK, tokens)) {
//       AST.columns.push('*');
//     } else if (this.expectt(TokenType.IDENTIFIER, tokens)) {
//       // first column
//       AST.columns.push(tokens[this.idx - 1].value);

//       while (this.expectt(TokenType.COMMA, tokens)) {
//         if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
//           throw new Error('Expected column name after the comma');
//         }

//         AST.columns.push(tokens[this.idx - 1].value);
//       }
//     } else {
//       throw new Error('Expected * or list of columns');
//     }

//     // after any of the above comes
//     // 1- FROM
//     // 2- Table name(IDENTIFER)

//     if (!this.expectt(TokenType.FROM, tokens)) {
//       throw new Error(`Expect ${TokenType.FROM}`);
//     }

//     if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
//       throw new Error(`Expect ${TokenType.IDENTIFIER}`);
//     }

//     AST.table = tokens[this.idx - 1].value;

//     if (this.expectt(TokenType.WHERE, tokens)) {
//       if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
//         throw new Error(`Expect ${TokenType.IDENTIFIER}`);
//       }
//       AST.where.criterion = tokens[this.idx - 1].value;

//       if (!this.expectt(TokenType.COMPARISON_OPERATOR, tokens)) {
//         throw new Error(`Expect ${TokenType.COMPARISON_OPERATOR}`);
//       }
//       AST.where.operator = tokens[this.idx - 1].value;

//       // value
//       if (!this.expectt(TokenType.NUMBER_LITERAL, tokens)) {
//         throw new Error(`Expect ${TokenType.NUMBER_LITERAL}`);
//       }
//       AST.where.value = tokens[this.idx - 1].value;
//     }

//     // if (
//     //   this.expectt(TokenType.WHERE, tokens) &&
//     //   this.expectt(TokenType.IDENTIFIER, tokens) &&
//     //   this.expectt(TokenType.COMPARISON_OPERATOR, tokens)
//     // ) {
//     //   AST.where.criterion = tokens[this.idx - 3].value;
//     //   AST.where.operator = tokens[this.idx - 2].value;
//     //   AST.where.value = tokens[this.idx - 1].value;
//     // }

//     console.log(AST);

//     this.semanticAnalyzer.checkTableExistence(AST.table);
//     this.semanticAnalyzer.checkColumnsExistence(AST.table, [
//       AST.where.criterion,
//       ...AST.columns,
//     ]);

//     // console.log(AST);

//     return AST; // Abstract Search Tree

//     // send the AST to the semantic analizer to check the existence of the columns and tables

//     // Now you should execute the query

//     // // Build an AST
//     // const AST:IQuery = {
//     //     type:'SELECT',
//     //     table:`${}`
//     // };
//   }

//   parseDelete(tokens: IToken[]) {
//     const AST: IDelQuery = {
//       type: TokenType.DELETE,
//       columns: [] as IColumn[],
//       table: '',
//       where: {},
//     } as IDelQuery;

//     // DELETE FROM users;
//     // DELETE FROM users WHERE id > 2;
//     // DELETE FROM users WHERE id = 2;
//     // DELETE FROM users WHERE role = 'test';

//     if (!this.expectt(TokenType.FROM, tokens)) {
//       throw new Error(`Expect ${TokenType.FROM}`);
//     }

//     if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
//       throw new Error(`Expect ${TokenType.IDENTIFIER}`);
//     }

//     AST.table = tokens[this.idx - 1].value;

//     if (this.expectt(TokenType.WHERE, tokens)) {
//       if (!this.expectt(TokenType.IDENTIFIER, tokens)) {
//         throw new Error(`Expect ${TokenType.IDENTIFIER}`);
//       }
//       AST.where.criterion = tokens[this.idx - 1].value;

//       if (!this.expectt(TokenType.COMPARISON_OPERATOR, tokens)) {
//         throw new Error(`Expect ${TokenType.COMPARISON_OPERATOR}`);
//       }
//       AST.where.operator = tokens[this.idx - 1].value;

//       // value
//       if (!this.expectt(TokenType.NUMBER_LITERAL, tokens)) {
//         throw new Error(`Expect ${TokenType.NUMBER_LITERAL}`);
//       }
//       AST.where.value = tokens[this.idx - 1].value;
//     }

//     console.log({ AST });

//     this.semanticAnalyzer.checkTableExistence(AST.table);
//     this.semanticAnalyzer.checkColumnsExistence(AST.table, [
//       AST.where.criterion,
//     ]);

//     return AST;
//   }

//   parseCreate(query: string) {
//     this.idx = 0;
//     this.tokens = this.tokenizer.tokenize(query);

//     // CREATE TABLE table_name {
//     // }

//     /**
//      * CREATE TABLE table_name {
//      *    column1_name datatype (size) [constraints],
//      *    column
//      * }
//      */

//     /**
//      *     EmployeeID INT PRIMARY KEY AUTO_INCREMENT,
//             FirstName VARCHAR(50) NOT NULL,
//             LastName VARCHAR(50) NOT NULL,
//             DepartmentID INT,
//      */

//     const AST: IQuery = {
//       type: TokenType.CREATE,
//       columns: [] as string[],
//       table: '',
//       where: {},
//     } as IQuery;

//     switch (this.tokens[this.idx].type) {
//       case TokenType.DATABASE:
//         break;

//       default:
//         break;
//     }

//     this.expectt(TokenType.TABLE, this.tokens);
//   }

//   async parseCreateTable(query: string) {
//     const AST = {
//       type: TokenType.CREATE,
//       structure: TokenType.TABLE,
//       structure_name: '',
//       columns: [] as Column[],
//     };

//     console.log('parseCreateTable');

//     if (!this.expectt(TokenType.TABLE, this.tokens)) {
//       throw new Error(`Expect ${TokenType.DATABASE}`);
//     }
//     AST.structure = this.tokens[this.idx - 1].value;

//     if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
//       throw new Error(`Expect ${TokenType.IDENTIFIER}`);
//     }
//     AST.structure_name = this.tokens[this.idx - 1].value;

//     if (!this.expectt(TokenType.LPAREN_OPERATOR, this.tokens)) {
//       throw new Error(`Expect ${TokenType.LPAREN_OPERATOR}`);
//     }

//     while (!this.expectt(TokenType.RPAREN_OPERATOR, this.tokens)) {
//       if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
//         throw new Error('Expected column name');
//       }
//       let col: Column = { name: this.tokens[this.idx - 1].value } as Column;

//       if (!this.expectt(TokenType.DATATYPE, this.tokens)) {
//         throw new Error('Expected datatype');
//       }
//       col.type = this.tokens[this.idx - 1].value;

//       AST.columns.push(col);

//       if (this.tokens[this.idx]?.type === TokenType.COMMA) {
//         this.idx++;
//       }
//     }

//     if (!this.expectt(TokenType.SEMI_COLON, this.tokens)) {
//       throw new Error(`Expect ${TokenType.SEMI_COLON}`);
//     }
//     // console.log(AST);

//     const isExist = await this.semanticAnalyzer.isTableExist(
//       AST.structure_name,
//     );
//     console.log({ isExist });

//     if (isExist) {
//       throw new Error(`Relation ${AST.structure_name} is already exist`);
//     }

//     console.log('finished');

//     return AST;
//   }

//   parseCreateDatabase(query: string) {
//     const AST = {
//       type: TokenType.CREATE,
//       structure: TokenType.DATABASE,
//       structure_name: '',
//     };

//     if (!this.expectt(TokenType.DATABASE, this.tokens)) {
//       throw new Error(`Expect ${TokenType.DATABASE}`);
//     }
//     AST.structure = this.tokens[this.idx - 1].value;

//     if (!this.expectt(TokenType.IDENTIFIER, this.tokens)) {
//       throw new Error(`Expect ${TokenType.IDENTIFIER}`);
//     }
//     AST.structure_name = this.tokens[this.idx - 1].value;

//     if (!this.expectt(TokenType.SEMI_COLON, this.tokens)) {
//       throw new Error(`Expect ${TokenType.SEMI_COLON}`);
//     }

//     return AST;
//   }

//   expectt(element: string, tokens: IToken[]): boolean {
//     if (this.idx >= tokens.length) return false;

//     const isTheNextElement = tokens[this.idx].type === element;

//     if (isTheNextElement) {
//       this.idx++;
//     }

//     return isTheNextElement;
//   }

//   parseSelect(tokens: IToken[]): boolean {
//     // // this.idx = 0;
//     // const tokens: IToken[] = this.tokenizer.tokenize(sql);
//     this.idx = 0;
//     if (tokens[this.idx].value === 'SELECT') {
//       const results: boolean[] = [];

//       this.tokens = tokens;
//       this.idx++;

//       //   results.push(
//       //     this.expect('*') || this.expect('id'),
//       //     this.expect('FROM'),
//       //     this.expect('users'),
//       //     this.expect(';'),
//       //   );

//       if (this.expect('*') || this.expect(TokenType.IDENTIFIER)) {
//         if (this.expect(TokenType.FROM)) {
//         }
//       }

//       if (!(this.expect(',') && this.expect(TokenType.IDENTIFIER))) {
//         throw new Error('SYNTAX ERROR');
//       }

//       while (this.expect(',')) {}

//       results.push(
//         this.expect('*') || this.expect(TokenType.IDENTIFIER),
//         this.expect(TokenType.FROM) ||
//           (this.expect(',') && this.expect(TokenType.IDENTIFIER)),

//         this.expect('users'),
//         this.expect(';'),
//       );

//       // true(syntax error)
//       if (this.checkTheFlow(results)) {
//         return false;
//       }
//     }

//     return true;
//   }

//   // true(syntax error)
//   checkTheFlow(results: boolean[]): boolean {
//     return results.includes(false);
//   }

//   expect(element: string): boolean {
//     if (this.idx >= this.tokens.length) return false;

//     const isTheNextElement = this.tokens[this.idx].value === element;

//     if (isTheNextElement) {
//       this.idx++;
//     }

//     return isTheNextElement;
//   }
// }

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

  async parse(query: string): Promise<AST> {
    this.tokens = this.tokenizer.tokenize(query);
    this.pointer = 0;

    if (this.pointer === 0 && !KEYWORDS.has(this.tokens[0].type)) {
      throw new Error(`INVALID SYNTAX: ${this.tokens[0].value}`);
    }

    let AST: AST;

    switch (this.tokens[this.pointer].type) {
      case TokenType.SELECT:
        this.pointer++;
        AST = await this.parseSelect();
        break;

      case TokenType.DELETE:
        this.pointer++;
        AST = await this.parseDelete();
        break;

      case TokenType.CREATE:
        this.pointer++;
        AST =
          this.tokens[this.pointer].type === TokenType.DATABASE
            ? await this.parseCreateDatabase()
            : await this.parseCreateTable();
        break;

      case TokenType.DROP:
        this.pointer++;
        AST =
          this.tokens[this.pointer].type === TokenType.DATABASE
            ? await this.parseDropDatabase()
            : await this.parseDropTable();
        break;

      case TokenType.UPDATE:
        this.pointer++;
        AST = await this.parseUpdate();
        break;

      case TokenType.INSERT:
        this.pointer++;
        AST = await this.parseInsert();
        break;

      case TokenType.ALTER:
        this.pointer++;
        AST =
          this.tokens[this.pointer].type === TokenType.DATABASE
            ? await this.parseAlterDatabase()
            : await this.parseAlterTable();
        break;

      default:
        throw new Error('Unsupported query type');
    }

    return AST;
  }

  // TODO: Use the semantic analyzer here
  async parseCreateDatabase(): Promise<CreateDatabaseAST> {
    const AST: CreateDatabaseAST = {
      type: TokenType.CREATE,
      structure: TokenType.DATABASE,
      name: '',
    };

    if (!this.expect(TokenType.DATABASE)) {
      throw new Error(`Expected DATABASE keyword`);
    }

    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected database name`);
    }
    AST.name = this.tokens[this.pointer - 1].value as string;

    if (!this.expect(TokenType.SEMI_COLON)) {
      throw new Error(`Expected ';'`);
    }

    return AST;
  }

  async parseCreateTable(): Promise<CreateTableAST> {
    const AST: CreateTableAST = {
      type: TokenType.CREATE,
      structure: TokenType.TABLE,
      name: '',
      columns: [],
    };

    if (!this.expect(TokenType.TABLE)) {
      throw new Error(`Expected TABLE keyword`);
    }

    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected table name`);
    }
    AST.name = this.tokens[this.pointer - 1].value as string;

    if (!this.expect(TokenType.LPAREN_OPERATOR)) {
      throw new Error(`Expected '('`);
    }

    while (!this.expect(TokenType.RPAREN_OPERATOR)) {
      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error('Expected column name');
      }
      const col: IColumn = {
        name: this.tokens[this.pointer - 1].value as string,
        type: null as any,
      };

      if (!this.expect(TokenType.DATATYPE)) {
        throw new Error('Expected data type');
      }

      const dataType = (
        this.tokens[this.pointer - 1].value as string
      ).toUpperCase();

      if (dataType === DataType.SERIAL) {
        col.type = DataType.INTEGER;
        col.autoIncrement = true;
      } else {
        col.type = dataType as DataType;
      }

      if (dataType === 'VARCHAR') {
        if (this.tokens[this.pointer]?.type === TokenType.LPAREN_OPERATOR) {
          this.pointer++;

          if (!this.expect(TokenType.NUMBER_LITERAL)) {
            throw new Error('Expected length for VARCHAR');
          }
          col.length = parseInt(this.tokens[this.pointer - 1].value as string);

          if (!this.expect(TokenType.RPAREN_OPERATOR)) {
            throw new Error(`Expected ')' after VARCHAR length`);
          }
        }
      }

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

      AST.columns.push(col);

      if (this.tokens[this.pointer].type === TokenType.COMMA) {
        this.pointer++;
      }
    }

    if (!this.expect(TokenType.SEMI_COLON)) {
      throw new Error(`Expected ';'`);
    }

    const tableExists =
      await this.semanticAnalyzer.checkTableExistenceInCurrentDB(AST.name);

    if (tableExists) {
      throw new Error(`Table ${AST.name} already exists`);
    }

    return AST;
  }

  async parseInsert(): Promise<InsertAST> {
    const AST: InsertAST = {
      type: TokenType.INSERT,
      table: '',
      columns: [],
      values: [],
      rowCount: 0,
    };

    if (!this.expect(TokenType.INTO)) {
      throw new Error(`Expected ${TokenType.INTO}`);
    }

    if (!this.expect(TokenType.IDENTIFIER)) {
      throw new Error(`Expected table name`);
    }
    AST.table = this.tokens[this.pointer - 1].value as string;

    if (!this.expect(TokenType.LPAREN_OPERATOR)) {
      throw new Error(`Expected '('`);
    }

    while (!this.expect(TokenType.RPAREN_OPERATOR)) {
      if (!this.expect(TokenType.IDENTIFIER)) {
        throw new Error('Expected column name');
      }
      AST.columns.push(this.tokens[this.pointer - 1].value as string);

      if (this.tokens[this.pointer]?.type === TokenType.COMMA) {
        this.pointer++;
      }
    }

    if (!this.expect(TokenType.VALUES)) {
      throw new Error(`Expected VALUES keyword`);
    }

    let rowCount = 0;

    while (this.tokens[this.pointer]?.type === TokenType.LPAREN_OPERATOR) {
      if (!this.expect(TokenType.LPAREN_OPERATOR)) {
        throw new Error(`Expected '('`);
      }

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

      if (valueCount !== AST.columns.length) {
        throw new Error(`Value count doesn't match column count`);
      }

      rowCount++;

      if (this.tokens[this.pointer]?.type === TokenType.COMMA) {
        this.pointer++;
      }
    }

    AST.rowCount = rowCount;

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
      AST.columns,
    );
    if (!columnsExist) {
      throw new Error(`Invalid columns names`);
    }

    return AST;
  }

  async parseUpdate(): Promise<UpdateAST> {
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
      throw new Error(`Expected value, got ${token.type}`);
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

  async parseSelect(): Promise<SelectAST> {
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

      if (!this.expect(TokenType.COMPARISON_OPERATOR)) {
        throw new Error(`Expected comparison operator`);
      }
      const operator = this.tokens[this.pointer - 1].value as Operator;

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
        console.log({ str: value });
      }

      if (token.type === TokenType.BOOLEAN_LITERAL) {
        if (!this.expect(TokenType.BOOLEAN_LITERAL)) {
          throw new Error(`Expected value`);
        }
        value = this.tokens[this.pointer - 1].value;
      }
      console.log({ value });
      console.log({ tko: this.tokens });

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

  async parseDelete(): Promise<DeleteAST> {
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

  async parseDropDatabase(): Promise<DropDatabaseAST> {
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

  async parseDropTable(): Promise<DropTableAST> {
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

  async parseAlterTable(): Promise<AlterTableAST> {
    const AST: AlterTableAST = {
      type: TokenType.ALTER,
      structure: TokenType.TABLE,
      name: '',
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

  async parseAlterDatabase(): Promise<AlterDatabaseAST> {
    const AST: AlterDatabaseAST = {} as AlterDatabaseAST;

    return AST;
  }

  expect(element: TokenType): boolean {
    if (this.pointer >= this.tokens.length) {
      return false;
    }

    const isMatch = this.tokens[this.pointer].type === element;

    if (isMatch) {
      this.pointer++;
    }

    return isMatch;
  }
}
