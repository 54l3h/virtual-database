import { Operator } from '../enums/operator.enum';
import { TokenType } from '../enums/token-type.enum';

export interface WhereClause {
  criterion: string; // column name
  operator: Operator;
  value: string | number | boolean;
}

export type DmlOperations =
  | TokenType.SELECT
  | TokenType.DELETE
  | TokenType.UPDATE
  | TokenType.INSERT;
