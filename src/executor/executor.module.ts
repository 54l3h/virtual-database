import { forwardRef, Module } from '@nestjs/common';
import { ExecutorService } from './executor.service';
import { ExecutorController } from './executor.controller';
import { TokenizerModule } from 'src/tokenizer/tokenizer.module';
import { ParserModule } from 'src/parser/parser.module';
import { StorageModule } from 'src/storage/storage.module';

@Module({
  controllers: [ExecutorController],
  providers: [ExecutorService],
  exports: [ExecutorService],
  imports: [TokenizerModule, forwardRef(() => ParserModule), StorageModule],
})
export class ExecutorModule {}
