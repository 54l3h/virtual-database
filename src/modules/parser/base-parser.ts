import { IToken } from '../../common/types/token.types';
import { TokenType } from '../../common/enums/token-type.enum';
import { AST } from '../../common/types/ast.type';

export abstract class BaseParser {
  protected tokens: IToken[] = [];
  protected pointer: number = 0;

  // the child should implement this
  abstract parse(tokens: IToken[], pointer: number): Promise<AST>;

  // the child should able to access this
  protected expect(element: TokenType): boolean {
    const isMatch = this.tokens[this.pointer]?.type === element;

    if (isMatch) {
      this.pointer++;
    }

    return isMatch;
  }
}
