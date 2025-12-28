import { Injectable } from '@nestjs/common';
import { TokenizerService } from '../tokenizer/tokenizer.service';
import { TokenType } from '../../common/enums/token-type.enum';
import { AST } from '../../common/types/ast.type';
import { InsertParser } from './statements/insert-parser';
import { SelectParser } from './statements/select-parser';
import { CreateParser } from './statements/create-parser';
import { UpdateParser } from './statements/update-parser';
import { DeleteParser } from './statements/delete-parser';
import { DropParser } from './statements/drop-parser';
import { AlterParser } from './statements/alter-parser';
import { BaseParser } from './base-parser';

@Injectable()
export class ParserService {
  private parserMap: Map<TokenType, any>;

  constructor(
    private readonly tokenizer: TokenizerService,
    private readonly insertParser: InsertParser,
    private readonly selectParser: SelectParser,
    private readonly createParser: CreateParser,
    private readonly updateParser: UpdateParser,
    private readonly deleteParser: DeleteParser,
    private readonly dropParser: DropParser,
    private readonly alterParser: AlterParser,
  ) {
    // Map the TokenType to the specific parser instance
    this.parserMap = new Map<TokenType, BaseParser>([
      [TokenType.SELECT, this.selectParser],
      [TokenType.INSERT, this.insertParser],
      [TokenType.UPDATE, this.updateParser],
      [TokenType.DELETE, this.deleteParser],
      [TokenType.CREATE, this.createParser],
      [TokenType.DROP, this.dropParser],
      [TokenType.ALTER, this.alterParser],
    ]);
  }

  async parse(query: string): Promise<AST> {
    // use the tokenize method from the tokenizer service which is responsible split on the query and identify the Tokens

    // tokenize the query
    const tokens = this.tokenizer.tokenize(query); // get tokens
    const firstToken = tokens[0].type;

    // find the proper handler from the map
    const handler: BaseParser = this.parserMap.get(firstToken);

    if (!handler) {
      throw new Error('Unsupported query');
    }
    // delegate the work to proper handler
    // pass 1 because we already consumed the first token
    return await handler.parse(tokens, 1);
  }
}
