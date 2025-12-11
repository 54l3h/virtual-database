import { forwardRef, Module } from '@nestjs/common';
import { ParserService } from './parser.service';
import { ParserController } from './parser.controller';
import { TokenizerModule } from 'src/tokenizer/tokenizer.module';
import { SemanticAnalyzerModule } from 'src/semantic-analyzer/semantic-analyzer.module';
import { ExecutorModule } from 'src/executor/executor.module';

@Module({
  controllers: [ParserController],
  providers: [ParserService],
  imports: [
    TokenizerModule,
    SemanticAnalyzerModule,
    ExecutorModule,
    forwardRef(() => ExecutorModule),
  ],
  exports: [ParserService],
})
export class ParserModule {}
