import { Operator } from '../enums/operator.enum';

export const matchesCondition = (
  operator: Operator,
  rowValue: any,
  targetValue: any,
): boolean => {
  switch (operator) {
    case Operator.EQUAL:
      return rowValue == targetValue;

    case Operator.GREATER_THAN:
      return Number(rowValue) > Number(targetValue);

    case Operator.GREATER_THAN_OR_EQUAL:
      return Number(rowValue) >= Number(targetValue);

    case Operator.LESS_THAN:
      return Number(rowValue) < Number(targetValue);

    case Operator.LESS_THAN_OR_EQUAL:
      return Number(rowValue) <= Number(targetValue);

    case Operator.LIKE: {
      const rowStr = String(rowValue).toUpperCase();
      const searchStr = String(targetValue).toUpperCase();
      return rowStr.includes(searchStr);
    }

    default:
      return false;
  }
};
