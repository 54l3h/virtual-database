import { DataType } from '../enums/data-type.enum';

export interface IColumn {
  name: string;
  type: DataType;
  length?: number; // 0 - 255
  primaryKey?: boolean; // Make an INDEX file => ndjson
  autoIncrement?: boolean; // SERIAL
  unique?: boolean; // Make an INDEX file => ndjson
}

export interface ITable {
  name: string;
  columns: IColumn[];
  primaryKey?: string; // Column name
}

export interface Index {
  value: string | number;
  offset: number;
  length: number;
}

export interface ISchema {
  tables: ITable[];
}
