import { Module } from '@nestjs/common';
import { ParserService } from './parser.service';
import { TokenizerModule } from '../tokenizer/tokenizer.module';
import { InsertParser } from './statements/insert-parser';
import { SelectParser } from './statements/select-parser';
import { CreateParser } from './statements/create-parser';
import { UpdateParser } from './statements/update-parser';
import { DeleteParser } from './statements/delete-parser';
import { DropParser } from './statements/drop-parser';
import { AlterParser } from './statements/alter-parser';
import { SchemaLogic } from '../storage/schema/schema.logic';
import { ConnectionLogic } from '../storage/connection/connection-logic';
import { IndexLogic } from '../storage/index/index-logic';

@Module({
  providers: [
    ParserService,
    InsertParser,
    SelectParser,
    CreateParser,
    UpdateParser,
    DeleteParser,
    DropParser,
    AlterParser,
    SchemaLogic,
    ConnectionLogic,
    IndexLogic,
  ],
  imports: [TokenizerModule],
  exports: [ParserService],
})
export class ParserModule {}
