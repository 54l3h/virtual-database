import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ParserModule } from './parser/parser.module';
import { TokenizerModule } from './tokenizer/tokenizer.module';
import { ExecutorModule } from './executor/executor.module';
import { StorageModule } from './storage/storage.module';
import { ConfigModule } from '@nestjs/config';
import { AstModule } from './ast/ast.module';
import { SemanticAnalyzerModule } from './semantic-analyzer/semantic-analyzer.module';

@Module({
  imports: [
    ParserModule,
    TokenizerModule,
    ExecutorModule,
    StorageModule,
    ConfigModule.forRoot({ isGlobal: true, envFilePath: '.env' }),
    AstModule,
    SemanticAnalyzerModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
