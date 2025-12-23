import { Module } from '@nestjs/common';
import { ExecutorService } from './executor.service';
import { ExecutorController } from './executor.controller';
import { ParserModule } from '../parser/parser.module';
import { StorageModule } from '../storage/storage.module';
import { TokenizerModule } from '../tokenizer/tokenizer.module';
import { SemanticAnalyzerModule } from '../semantic-analyzer/semantic-analyzer.module';

@Module({
  controllers: [ExecutorController],
  providers: [ExecutorService],
  exports: [ExecutorService],
  imports: [
    TokenizerModule,
    ParserModule,
    StorageModule,
    SemanticAnalyzerModule,
  ],
})
export class ExecutorModule {}
