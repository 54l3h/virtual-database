import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ConfigModule } from '@nestjs/config';
import { ParserModule } from './modules/parser/parser.module';
import { TokenizerModule } from './modules/tokenizer/tokenizer.module';
import { ExecutorModule } from './modules/executor/executor.module';
import { StorageModule } from './modules/storage/storage.module';

@Module({
  imports: [
    ParserModule,
    TokenizerModule,
    ExecutorModule,
    StorageModule,
    ConfigModule.forRoot({ isGlobal: true, envFilePath: '.env' }),
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
