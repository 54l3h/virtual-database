import { TokenType } from '../enums/token-type.enum';
import { IColumn } from './schema.types';
import { WhereClause } from './query.types';
import { DataType } from '../enums/data-type.enum';

export interface BaseAST {
  type: TokenType; // to specify the statement type
}

export interface SelectAST extends BaseAST {
  type: TokenType.SELECT;
  columns: string[]; // column, list of columns, *
  table: string; // table name
  where?: WhereClause; // criterion, opertator, value
  orderBy?: {
    column: string; // column name
    direction: 'ASC' | 'DESC';
  };
}

export interface InsertAST extends BaseAST {
  type: TokenType.INSERT;
  table: string; // table name
  columns: string[]; // column/s name/s
  values: any[]; // inserted values
}

export interface UpdateAST extends BaseAST {
  type: TokenType.UPDATE;
  table: string;
  updates: Record<string, any>; // {column1:value, column2:value}
  where: WhereClause; // criterion, opertator, value
}

export interface DeleteAST extends BaseAST {
  type: TokenType.DELETE;
  table: string;
  where?: WhereClause;
}

export interface CreateTableAST extends BaseAST {
  type: TokenType.CREATE;
  structure: TokenType.TABLE;
  name: string;
  columns: IColumn[];
}

export interface CreateDatabaseAST extends BaseAST {
  type: TokenType.CREATE;
  structure: TokenType.DATABASE;
  name: string;
}

export interface DropTableAST extends BaseAST {
  type: TokenType.DROP;
  structure: TokenType.TABLE;
  name: string;
}

export interface DropDatabaseAST extends BaseAST {
  type: TokenType.DROP;
  structure: TokenType.DATABASE;
  name: string;
}

export interface AlterTableAST extends BaseAST {
  type: TokenType.ALTER;
  structure: TokenType.TABLE;
  name: string; // table name
  columnName: string;
  dataType?: DataType;
  action: 'ADD' | 'DROP';
}

export type AST =
  | SelectAST
  | InsertAST
  | UpdateAST
  | DeleteAST
  | CreateTableAST
  | CreateDatabaseAST
  | DropTableAST
  | DropDatabaseAST
  | AlterTableAST;
