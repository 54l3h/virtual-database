export enum DataType {
  INTEGER = 'INTEGER',
  FLOAT = 'FLOAT',
  TEXT = 'TEXT',
  VARCHAR = 'VARCHAR',
  BOOLEAN = 'BOOLEAN',
  TIMESTAMP = 'TIMESTAMP',
  SERIAL = 'SERIAL',
}

export interface ColumnType {
  type: DataType;
  length?: number; // VARCHAR (0-255)
}

export interface Column {
  name: string;
  type: DataType;
  length?: number; // VARCHAR (0-255)
}
