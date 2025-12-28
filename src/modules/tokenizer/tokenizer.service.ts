import { Injectable } from '@nestjs/common';
import { IToken } from '../../common/types/token.types';
import { TokenType, KEYWORDS } from '../../common/enums/token-type.enum';
import { DataType } from '../../common/enums/data-type.enum';

@Injectable()
export class TokenizerService {
  private readonly WHITE_SPACE = [' ', '\n', '\t'];
  private readonly SINGLE_CHAR = {
    '*': TokenType.ASTERISK,
    ',': TokenType.COMMA,
    '(': TokenType.LPAREN_OPERATOR,
    ')': TokenType.RPAREN_OPERATOR,
  };
  private readonly COMPARISON_OPERATORS = ['=', '>', '<'];

  private isNumeric(char: string): boolean {
    return /^[0-9]$/.test(char);
  }

  private isAlphabetic(char: string): boolean {
    return /^[a-zA-Z]$/.test(char);
  }

  private isIdentifier(char: string): boolean {
    return /^[a-zA-Z0-9_]$/.test(char);
  }

  private handleSingleChar(chars: string[]): IToken {
    const char = chars.shift()!;
    return { type: this.SINGLE_CHAR[char], value: char };
  }

  private handleComparisonOperator(chars: string[]): IToken {
    let operator = chars.shift()!;
    if (chars.length > 0 && chars[0] === '=') {
      operator += chars.shift()!;
    }
    return { type: TokenType.COMPARISON_OPERATOR, value: operator };
  }

  private handleSemicolon(chars: string[]): IToken {
    return { type: TokenType.SEMI_COLON, value: chars.shift()! };
  }

  private handleStringLiteral(chars: string[]): IToken {
    chars.shift(); // consume opening '
    let stringifiedIdentifier = '';

    while (chars.length > 0 && chars[0] !== "'") {
      stringifiedIdentifier += chars.shift()!;
    }

    if (chars.length === 0) {
      throw new Error('Unclosed string literal');
    }

    chars.shift(); // consume closing '
    return {
      type: TokenType.STRING_LITERAL,
      value: stringifiedIdentifier,
    };
  }

  private handleNumeric(chars: string[]): IToken {
    let numStr = '';
    while (chars.length > 0 && this.isNumeric(chars[0])) {
      numStr += chars.shift()!;
    }
    return { type: TokenType.NUMBER_LITERAL, value: Number(numStr) };
  }

  private handleWord(chars: string[]): IToken {
    let word = '';
    while (chars.length > 0 && this.isIdentifier(chars[0])) {
      word += chars.shift()!;
    }

    const wordUpperCased = word.toUpperCase();

    // Boolean
    if (wordUpperCased === 'TRUE' || wordUpperCased === 'FALSE') {
      return {
        type: TokenType.BOOLEAN_LITERAL,
        value: wordUpperCased === 'TRUE',
      };
    }

    // SQL keywords
    if (KEYWORDS.has(wordUpperCased)) {
      return {
        type: wordUpperCased as TokenType,
        value: wordUpperCased,
      };
    }

    // Data types
    if (Object.values(DataType).includes(wordUpperCased as DataType)) {
      return {
        type: TokenType.DATATYPE,
        value: wordUpperCased,
      };
    }

    // Default => Identifier
    return {
      type: TokenType.IDENTIFIER,
      value: word,
    };
  }

  tokenize(source: string): IToken[] {
    const tokens: IToken[] = [];
    const chars = source.split('');

    while (chars.length > 0) {
      if (this.WHITE_SPACE.includes(chars[0])) {
        chars.shift();
        continue;
      }

      if (chars[0] in this.SINGLE_CHAR) {
        tokens.push(this.handleSingleChar(chars));
      } else if (this.COMPARISON_OPERATORS.includes(chars[0])) {
        tokens.push(this.handleComparisonOperator(chars));
      } else if (chars[0] === ';') {
        tokens.push(this.handleSemicolon(chars));
      } else if (chars[0] === "'") {
        tokens.push(this.handleStringLiteral(chars));
      } else if (this.isNumeric(chars[0])) {
        tokens.push(this.handleNumeric(chars));
      } else if (this.isAlphabetic(chars[0])) {
        tokens.push(this.handleWord(chars));
      } else {
        throw new Error(`Invalid token: ${chars[0]}`);
      }
    }

    return tokens;
  }
}
