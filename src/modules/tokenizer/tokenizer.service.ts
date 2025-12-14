import { Injectable } from '@nestjs/common';
import { IToken } from '../../common/types/token.types';
import { Operator } from 'src/common/enums/operator.enum';
import { KEYWORDS, TokenType } from 'src/common/enums/token-type.enum';
import { DataType } from 'src/common/enums/data-type.enum';

@Injectable()
export class TokenizerService {
  isNumeric(char: string): boolean {
    return /^[0-9]$/.test(char);
  }

  isAlphabetic(char: string): boolean {
    return /^[a-zA-Z]$/.test(char);
  }

  // the first char only of the identifier should be a alphabetic
  isIdentifier(char: string): boolean {
    return /^[a-zA-Z0-9_]$/.test(char);
  }

  tokenize(source: string): IToken[] {
    const tokens: IToken[] = [];
    const chars = source.split('');

    while (chars.length > 0) {
      if (chars[0] === ' ') {
        chars.shift();
      } else if (chars[0] === '*') {
        tokens.push({ type: TokenType.ASTERISK, value: chars.shift()! });
      }

      // because the semi-colon should be at the end
      else if (chars[0] === ',') {
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
      } else if (Object.values(DataType).includes(chars[0] as DataType)) {
        tokens.push({
          type: TokenType.DATATYPE,
          value: chars.shift()!,
        });
      } else if (chars[0] === '>' || chars[0] === '<') {
        let operator: string = '';
        operator += chars.shift()!;

        if (chars.length > 0 && chars[0] === ('=' as string)) {
          operator += chars.shift()!;
        }

        tokens.push({
          type: TokenType.COMPARISON_OPERATOR,
          value: operator,
        });
      }
      // semi-colon should be at the end
      else if (chars[0] === ';' && chars.length === 1) {
        tokens.push({ type: TokenType.SEMI_COLON, value: chars.shift()! });
      } else if (chars[0] === "'") {
        chars.shift(); // remove opening
        let stringifiedIdentifier = '';

        while (chars.length > 0 && chars[0] !== "'") {
          stringifiedIdentifier += chars.shift()!;
        }

        chars.shift()!; // remove closing

        tokens.push({
          type: TokenType.STRING_LITERAL,
          value: stringifiedIdentifier,
        });
      }

      // You should check if it a number literal
      else if (this.isNumeric(chars[0])) {
        let numStr: string = '';

        while (chars.length > 0 && this.isNumeric(chars[0])) {
          numStr += chars.shift()!;
        }

        tokens.push({ type: TokenType.NUMBER_LITERAL, value: numStr });
      }

      // You should check if it a string in general
      // ( KEYWORD(CLAUSE) or just an IDENTIFIER )
      else if (this.isAlphabetic(chars[0])) {
        let word: string = '';

        // The first iteration it should be an alphabetic if we are going throw a word with more than 1 character, we check on this above and then it's guranteed to be alphanumeric
        while (chars.length > 0 && this.isIdentifier(chars[0])) {
          word += chars.shift()!;
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

          case TokenType.FROM:
            tokens.push({ type: TokenType.FROM, value: word });
            break;

          case TokenType.WHERE:
            tokens.push({ type: TokenType.WHERE, value: word });
            break;

          case TokenType.CREATE:
            tokens.push({ type: TokenType.CREATE, value: word });
            break;

          case TokenType.ALTER:
            tokens.push({ type: TokenType.ALTER, value: word });
            break;

          case TokenType.DROP:
            tokens.push({ type: TokenType.DROP, value: word });
            break;

          case TokenType.TABLE:
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

          case TokenType.INSERT:
            tokens.push({ type: TokenType.INSERT, value: word });
            break;

          case TokenType.INTO:
            tokens.push({ type: TokenType.INTO, value: word });
            break;

          case TokenType.VALUES:
            tokens.push({ type: TokenType.VALUES, value: word });
            break;
          case TokenType.PRIMARY:
            tokens.push({ type: TokenType.PRIMARY, value: word });
            break;
          case TokenType.KEY:
            tokens.push({ type: TokenType.KEY, value: word });
            break;
          case TokenType.UNIQUE:
            tokens.push({ type: TokenType.UNIQUE, value: word });
            break;

          // it means you are going to send an datatype
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
