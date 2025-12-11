import { Injectable } from '@nestjs/common';
import { DataType } from 'src/common/types/data-types';
import { Operators } from 'src/parser/parser.service';

export enum TokenType {
  SELECT = 'SELECT',
  FROM = 'FROM',
  WHERE = 'WHERE',
  CREATE = 'CREATE',
  ALTER = 'ALTER',
  DROP = 'DROP',
  DELETE = 'DELETE',
  UPDATE = 'UPDATE',
  TABLE = 'TABLE',
  DATABASE = 'DATABASE',
  COMMA = 'COMMA',
  SEMI_COLON = 'SEMI_COLON',
  NUMBER_LITERAL = 'NUMBER_LITERAL',
  IDENTIFIER = 'IDENTIFIER',
  ASSIGNMENT_OPERATOR = 'ASSIGNMENT_OPERATOR',
  LPAREN_OPERATOR = 'LPAREN_OPERATOR',
  RPAREN_OPERATOR = 'LPAREN_OPERATOR',
  COMPARISON_OPERATOR = 'COMPARISON_OPERATOR',
  ASTERISK = '*',
  GREATER_THAN = '>',
  LESS_THAN = '<',
  EQUAL = '=',
  GREATER_THAN_OR_EQUAL = '>=',
  LESS_THAN_OR_EQUAL = '<=',
  LCURLY_BRACKET = '{',
  RCURLY_BRACKET = '}',
  DATATYPE = 'DATATYPE',
  INSERT = 'INSERT',
  INTO = 'INTO',
  VALUES = 'VALUES',
  SET = 'SET',
}

export interface IToken {
  type: TokenType;
  value: any;
}

export const KEYWORDS = new Set([
  'SELECT',
  'DELETE',
  'CREATE',
  'UPDATE',
  'ALTER',
]);

@Injectable()
export class TokenizerService {
  isNumeric(char: string): boolean {
    return /^[0-9]$/.test(char);
  }

  isAlphabetic(char: string): boolean {
    return /^[a-zA-Z]$/.test(char);
  }

  // the first element only of the identifier should be a alphabetic
  isAlphaNumeric(char: string): boolean {
    return /^[a-z0-9]$/i.test(char);
  }

  tokenize(source: string) {
    const tokens: IToken[] = [];
    const chars = source.split('');

    while (chars.length > 0) {
      if (chars[0] === ' ') {
        chars.shift();
      } else if (chars[0] === '*') {
        tokens.push({ type: TokenType.ASTERISK, value: chars.shift() });
      }

      // because the semi-colon should be at the end
      else if (chars[0] === ',') {
        tokens.push({ type: TokenType.COMMA, value: chars.shift() });
      } else if (chars[0] === '(') {
        tokens.push({ type: TokenType.LPAREN_OPERATOR, value: chars.shift() });
      } else if (chars[0] === ')') {
        tokens.push({ type: TokenType.RPAREN_OPERATOR, value: chars.shift() });
      } else if (chars[0] === '=') {
        tokens.push({
          type: TokenType.COMPARISON_OPERATOR,
          value: chars.shift(),
        });
      } else if (Object.values(DataType).includes(chars[0] as DataType)) {
        tokens.push({
          type: TokenType.DATATYPE,
          value: chars.shift(),
        });
      } else if (chars[0] === '>' || chars[0] === '<') {
        let operator: string = '';
        operator += chars.shift();

        if (chars.length > 0 && chars[0] === ('=' as '<')) {
          operator += chars.shift()!;
        }

        tokens.push({
          type: TokenType.COMPARISON_OPERATOR,
          value: operator,
        });
      }
      // because the semi-colon should be at the end
      else if (chars[0] === ';' && chars.length === 1) {
        tokens.push({ type: TokenType.SEMI_COLON, value: chars.shift() });
      }

      // You should check if it a number literal
      else if (this.isNumeric(chars[0])) {
        let numStr: string = '';

        while (chars.length > 0 && this.isNumeric(chars[0])) {
          numStr += chars.shift();
        }

        tokens.push({ type: TokenType.NUMBER_LITERAL, value: numStr });
      }

      // You should check if it a string in general
      // ( KEYWORD(CLAUSE) or just an IDENTIFIER )
      else if (this.isAlphabetic(chars[0])) {
        let word: string = '';

        // The first iteration it should be an alphabetic if we are going throw a word with more than 1 character, we check on this above and then it's guranteed to be alphanumeric
        while (chars.length > 0 && this.isAlphaNumeric(chars[0])) {
          word += chars.shift();
        }

        // now we have a word
        // could it be an identifier
        // and could it be a keyword(clause)
        // comparison in js is case-sensetive
        // upper case the word
        /**
         * ! send word as the value not the wordUpperCased
         * Because the identifiers are case sensitive
         * (it doesn't matter if we are going to think about clauses)
         * So just send the word as it
         */

        const wordUpperCased = word.toUpperCase();

        switch (wordUpperCased) {
          case TokenType.SELECT:
            tokens.push({ type: TokenType.SELECT, value: word });
            break;

          case TokenType.DELETE:
            tokens.push({ type: TokenType.DELETE, value: word });
            break;

          case 'FROM':
            tokens.push({ type: TokenType.FROM, value: word });
            break;

          case 'WHERE':
            tokens.push({ type: TokenType.WHERE, value: word });
            break;

          case 'CREATE':
            tokens.push({ type: TokenType.CREATE, value: word });
            break;

          case 'ALTER':
            tokens.push({ type: TokenType.ALTER, value: word });
            break;

          case 'DROP':
            tokens.push({ type: TokenType.DROP, value: word });
            break;

          case 'TABLE':
            tokens.push({ type: TokenType.TABLE, value: word });
            break;

          case TokenType.DATABASE:
            tokens.push({ type: TokenType.DATABASE, value: word });
            break;

          case TokenType.UPDATE:
            tokens.push({ type: TokenType.UPDATE, value: word });
            break;

          case TokenType.SET:
            tokens.push({ type: TokenType.SET, value: word });
            break;

          case TokenType.INTO:
            tokens.push({ type: TokenType.INTO, value: word });
            break;

          // it means you are going to send an or datatype identifier
          default:
            if (Object.values(DataType).includes(wordUpperCased as DataType)) {
              tokens.push({
                type: TokenType.DATATYPE,
                value: word,
              });
            } else {
              tokens.push({ type: TokenType.IDENTIFIER, value: word });
            }
            break;
        }
      } else {
        throw new Error(`Invalid token ${chars[0]}`);
      }
    }
    return tokens;
  }
}
