import { TokenType } from '../enums/token-type.enum';

export interface IToken {
  type: TokenType;
  value: string | number | boolean;
}
