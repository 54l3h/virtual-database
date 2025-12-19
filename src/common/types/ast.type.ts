import { TokenType } from '../enums/token-type.enum';
import { IColumn } from './schema.types';
import { WhereClause } from './query.types';
import { DataType } from '../enums/data-type.enum';

export interface BaseAST {
  type: TokenType;
}

export interface SelectAST extends BaseAST {
  type: TokenType.SELECT;
  columns: string[];
  table: string;
  where?: WhereClause;
}

// ! Check the table if it has indexes before insertion & updating
export interface InsertAST extends BaseAST {
  type: TokenType.INSERT;
  table: string;
  columns: string[];
  values: any[];
  rowCount: number;
}

export interface UpdateAST extends BaseAST {
  type: TokenType.UPDATE;
  table: string;
  updates: Record<string, any>;
  where?: WhereClause;
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

export interface AlterDatabaseAST extends BaseAST {
  type: TokenType.ALTER;
  structure: TokenType.DATABASE;
  name: string;
  action: 'ADD' | 'DROP';
}

export interface AlterTableAST extends BaseAST {
  type: TokenType.ALTER;
  structure: TokenType.TABLE;
  name: string;
  columnName?: string;
  dataType?: DataType;
  // updates?: [];
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
  | AlterDatabaseAST
  | AlterTableAST;
