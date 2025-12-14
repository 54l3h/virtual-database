import { Module } from '@nestjs/common';
import { TokenizerService } from './tokenizer.service';
import { TokenizerController } from './tokenizer.controller';

@Module({
  controllers: [TokenizerController],
  providers: [TokenizerService],
  exports: [TokenizerService],
})
export class TokenizerModule {}
