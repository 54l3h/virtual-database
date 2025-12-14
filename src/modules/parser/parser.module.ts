import { forwardRef, Module } from '@nestjs/common';
import { ParserService } from './parser.service';
import { ParserController } from './parser.controller';
import { SemanticAnalyzerModule } from 'src/modules/semantic-analyzer/semantic-analyzer.module';
import { TokenizerModule } from '../tokenizer/tokenizer.module';
import { ExecutorModule } from '../executor/executor.module';

@Module({
  controllers: [ParserController],
  providers: [ParserService],
  imports: [
    TokenizerModule,
    SemanticAnalyzerModule,
    forwardRef(() => ExecutorModule),
  ],
  exports: [ParserService],
})
export class ParserModule {}
