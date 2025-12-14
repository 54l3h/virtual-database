import { forwardRef, Module } from '@nestjs/common';
import { ExecutorService } from './executor.service';
import { ExecutorController } from './executor.controller';
import { ParserModule } from '../parser/parser.module';
import { StorageModule } from '../storage/storage.module';
import { TokenizerModule } from '../tokenizer/tokenizer.module';

@Module({
  controllers: [ExecutorController],
  providers: [ExecutorService],
  exports: [ExecutorService],
  imports: [TokenizerModule, forwardRef(() => ParserModule), StorageModule],
})
export class ExecutorModule {}
