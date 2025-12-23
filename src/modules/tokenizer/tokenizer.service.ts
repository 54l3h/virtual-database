import { Injectable } from '@nestjs/common';
import { IToken } from '../../common/types/token.types';
import { TokenType, KEYWORDS } from '../../common/enums/token-type.enum';
import { DataType } from '../../common/enums/data-type.enum';

/**
 * @description TokenizerService performs Lexical Analysis
 * Breaks down a SQL query into a sequence of meaningful words called Tokens
 * that the Parser can understand later
 * TODO: Handle boolean
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
      }
      // check if the current char is * to handle selecting all the columns later
      else if (chars[0] === '*') {
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
      }
      // to handle the rest operators like => ( > , < , >= , <= )
      else if (chars[0] === '>' || chars[0] === '<') {
        let operator = chars.shift()!; // shift the first portion of the operator
        if (chars.length > 0 && chars[0] === ('=' as string)) {
          // it could be one of (>= , <=)
          operator += chars.shift()!;
        }
        tokens.push({ type: TokenType.COMPARISON_OPERATOR, value: operator });
      }
      // check if the char is semi-colon and the last element in the array
      else if (chars[0] === ';' && chars.length === 1) {
        tokens.push({ type: TokenType.SEMI_COLON, value: chars.shift()! });
      }
      // to gather identifiers
      else if (chars[0] === "'") {
        chars.shift();
        let stringifiedIdentifier = '';
        // if there are characters and i didn't face the terminator => ' i should continue in gathering the chars to get the identifier
        while (chars.length > 0 && chars[0] !== "'") {
          stringifiedIdentifier += chars.shift()!;
        }
        // after i get the sinlge quote the while loop will be broken and then i should consume it with the next line
        chars.shift(); // consume '
        tokens.push({
          type: TokenType.STRING_LITERAL,
          value: stringifiedIdentifier,
        });
      }
      // handle numeric literals
      else if (this.isNumeric(chars[0])) {
        let numStr = '';
        while (chars.length > 0 && this.isNumeric(chars[0])) {
          numStr += chars.shift()!;
        }
        tokens.push({ type: TokenType.NUMBER_LITERAL, value: Number(numStr) });
      }
      // check if the char is alphabetic
      else if (this.isAlphabetic(chars[0])) {
        let word = '';
        while (chars.length > 0 && this.isIdentifier(chars[0])) {
          // gather all characters which the identifier can consists of
          word += chars.shift()!;
        }

        const wordUpperCased = word.toUpperCase();

        // handle boolean literals
        if (wordUpperCased === 'TRUE' || wordUpperCased === 'FALSE') {
          tokens.push({
            type: TokenType.BOOLEAN_LITERAL,
            value: wordUpperCased === 'TRUE' ? true : false,
          });
        } else if (KEYWORDS.has(wordUpperCased)) {
          tokens.push({
            type: wordUpperCased as TokenType,
            value: wordUpperCased,
          });
        }
        // [Datatypes].includes(WORD) => Push into the tokens array and consider it as a datatype
        else if (Object.values(DataType).includes(wordUpperCased as DataType)) {
          tokens.push({
            type: TokenType.DATATYPE,
            value: wordUpperCased,
          });
        }
        // if the word not considered as a Datatype or Keyword then it should be an identifier
        else {
          tokens.push({
            type: TokenType.IDENTIFIER,
            value: word, // wordUpperCased w/o UpperCased
          });
        }
      } else {
        throw new Error(`Invalid token ${chars[0]}`);
      }
    }
    return tokens;
  }
}
