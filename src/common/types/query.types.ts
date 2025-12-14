import { Operator } from '../enums/operator.enum';

export interface WhereClause {
  criterion: string; // Condition column name
  operator: Operator;
  value: string | number | boolean;
}
