import { Injectable } from '@nestjs/common';
import { TokenizerService } from '../tokenizer/tokenizer.service';
import { IToken } from '../../common/types/token.types';
import { KEYWORDS, TokenType } from '../../common/enums/token-type.enum';
import { AST } from '../../common/types/ast.type';
import { InsertParser } from './statements/insert-parser';
import { SelectParser } from './statements/select-parser';
import { CreateParser } from './statements/create-parser';
import { UpdateParser } from './statements/update-parser';
import { DeleteParser } from './statements/delete-parser';
import { DropParser } from './statements/drop-parser';
import { AlterParser } from './statements/alter-parser';

@Injectable()
export class ParserService {
  private tokens: IToken[] = [];
  private pointer: number = 0;

  constructor(
    private readonly tokenizer: TokenizerService,
    private readonly insertParser: InsertParser,
    private readonly selectParser: SelectParser,
    private readonly createParser: CreateParser,
    private readonly updateParser: UpdateParser,
    private readonly deleteParser: DeleteParser,
    private readonly dropParser: DropParser,
    private readonly alterParser: AlterParser,
  ) {}

  async parse(query: string): Promise<AST> {
    // use the tokenize method from the tokenizer service which is responsible split on the query and identify the Tokens
    this.tokens = this.tokenizer.tokenize(query);

    // pointer to move through the tokens to complete the checking process => (check the value by expect if the parser get what it expected the pointer will go throw the next token if not the parser will throw an error)
    this.pointer = 0;

    // check if the first element of the tokens not a keyword
    // any type is uppercased & all the keywords are uppercased too
    if (this.pointer === 0 && !KEYWORDS.has(this.tokens[0].type)) {
      throw new Error(`INVALID SYNTAX: ${this.tokens[0].value}`);
    }

    // ABSTRACT SYNTAX TREE
    let AST: AST;

    // CHECK THE TYPE OF THE STATEMENT
    // DETERMINATION BASED ON THE FIRST TOKEN WHICH IS THE FIRST MEANINGFUL WORD INTO THE QUERY
    switch (this.tokens[this.pointer].type) {
      // SELECT STATEMENT
      case TokenType.SELECT:
        // GO THROUGH THE NEXT TOKEN

        this.pointer++;
        // GET THE AST FROM THE PARSE SELECT METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE SELECT STATEMENT FLOW)
        AST = await this.selectParser.parse(this.tokens, this.pointer);
        break;

      // DELETE STATEMENT
      case TokenType.DELETE:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE DELETE METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE DELETE STATEMENT FLOW)
        AST = await this.deleteParser.parse(this.tokens, this.pointer);
        break;

      // CREATE STATEMENT => (Create both Databases & Tables)
      case TokenType.CREATE:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE CREATE (DATABASE/TABLE) METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE CREATE STATEMENT FLOW)
        AST = await this.createParser.parse(this.tokens, this.pointer);
        break;

      // DROP STATEMENT => (Drop both Databases & Tables)
      case TokenType.DROP:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE DROP (DATABASE/TABLE) METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE DROP STATEMENT FLOW)
        AST = await this.dropParser.parse(this.tokens, this.pointer);
        break;

      // UPDATE STATEMENT
      case TokenType.UPDATE:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE UPDATE METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE UPDATE STATEMENT FLOW)
        AST = await this.updateParser.parse(this.tokens, this.pointer);
        break;

      // INSERT STATEMENT
      case TokenType.INSERT:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE INSERT METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE INSERT STATEMENT FLOW
        AST = await this.insertParser.parse(this.tokens, this.pointer);
        break;

      // ALTER STATEMENT => (Alter Tables only)
      case TokenType.ALTER:
        // GO THROUGH THE NEXT TOKEN
        this.pointer++;

        // GET THE AST FROM THE PARSE ALTER TABLE METHOD (WILL CONTINUE EXPECTING THE REMAINING TOKENS BASED ON THE ALTER TABLE STATEMENT FLOW)
        AST = await this.alterParser.parse(this.tokens, this.pointer);
        break;

      default:
        throw new Error('Unsupported query type');
    }

    // Returns the Abstract Syntax Tree to the executor which will send it to the storage service provider to work on the I/O operations
    return AST;
  }
}
