import { Injectable } from '@nestjs/common';
import { IToken } from '../../common/types/token.types';
import { TokenType, KEYWORDS } from '../../common/enums/token-type.enum';
import { DataType } from '../../common/enums/data-type.enum';

/**
 * @description TokenizerService performs Lexical Analysis
 * Breaks down a SQL query into a sequence of meaningful words called Tokens
 * that the Parser can understand later
 */
@Injectable()
export class TokenizerService {
  // method to identify if a character is a digit (0-9)
  isNumeric(char: string): boolean {
    return /^[0-9]$/.test(char);
  }

  // method to identify if a character is a letter (a-z, A-Z)
  isAlphabetic(char: string): boolean {
    return /^[a-zA-Z]$/.test(char);
  }

  // identifiers can contain letters, numbers, and underscores
  isIdentifier(char: string): boolean {
    return /^[a-zA-Z0-9_]$/.test(char);
  }

  /**
   * Transform a raw string into an array of Tokens.
   */
  tokenize(source: string): IToken[] {
    // array of tokens
    const tokens: IToken[] = [];

    // sequential character consumption
    const chars = source.split('');

    while (chars.length > 0) {
      // ignore spaces, tabs, and newlines
      if ([' ', '\n', '\t'].includes(chars[0])) {
        chars.shift();
      } else if (chars[0] === '*') {
        tokens.push({ type: TokenType.ASTERISK, value: chars.shift()! });
      } else if (chars[0] === ',') {
        tokens.push({ type: TokenType.COMMA, value: chars.shift()! });
      } else if (chars[0] === '(') {
        tokens.push({ type: TokenType.LPAREN_OPERATOR, value: chars.shift()! });
      } else if (chars[0] === ')') {
        tokens.push({ type: TokenType.RPAREN_OPERATOR, value: chars.shift()! });
      } else if (chars[0] === '=') {
        tokens.push({
          type: TokenType.COMPARISON_OPERATOR,
          value: chars.shift()!,
        });
        // Checks if the current single character matches a DataType
      } else if (chars[0] === '>' || chars[0] === '<') {
        let operator = chars.shift()!;
        if (chars.length > 0 && chars[0] === ('=' as string)) {
          operator += chars.shift()!;
        }
        tokens.push({ type: TokenType.COMPARISON_OPERATOR, value: operator });
      } else if (chars[0] === ';' && chars.length === 1) {
        tokens.push({ type: TokenType.SEMI_COLON, value: chars.shift()! });
      } else if (chars[0] === "'") {
        chars.shift();
        let stringifiedIdentifier = '';
        while (chars.length > 0 && chars[0] !== "'") {
          stringifiedIdentifier += chars.shift()!;
        }
        chars.shift();
        tokens.push({
          type: TokenType.STRING_LITERAL,
          value: stringifiedIdentifier,
        });
      } else if (this.isNumeric(chars[0])) {
        let numStr = '';
        while (chars.length > 0 && this.isNumeric(chars[0])) {
          numStr += chars.shift()!;
        }
        tokens.push({ type: TokenType.NUMBER_LITERAL, value: numStr });
      } else if (this.isAlphabetic(chars[0])) {
        let word = '';
        while (chars.length > 0 && this.isIdentifier(chars[0])) {
          word += chars.shift()!;
        }

        const wordUpperCased = word.toUpperCase();

        if (KEYWORDS.has(wordUpperCased)) {
          tokens.push({
            type: wordUpperCased as TokenType,
            value: word,
          });
        } else if (Object.values(DataType).includes(wordUpperCased as any)) {
          tokens.push({
            type: TokenType.DATATYPE,
            value: word,
          });
        } else {
          tokens.push({
            type: TokenType.IDENTIFIER,
            value: word,
          });
        }
      } else {
        throw new Error(`Invalid token ${chars[0]}`);
      } 
    }
    return tokens;
  }
}
