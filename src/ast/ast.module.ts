import { Module } from '@nestjs/common';
import { AstService } from './ast.service';
import { AstController } from './ast.controller';

@Module({
  controllers: [AstController],
  providers: [AstService],
})
export class AstModule {}
