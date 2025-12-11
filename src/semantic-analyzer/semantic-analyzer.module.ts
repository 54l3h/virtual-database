import { Module } from '@nestjs/common';
import { SemanticAnalyzerService } from './semantic-analyzer.service';
import { SemanticAnalyzerController } from './semantic-analyzer.controller';
import { StorageModule } from 'src/storage/storage.module';

@Module({
  controllers: [SemanticAnalyzerController],
  providers: [SemanticAnalyzerService],
  imports: [StorageModule],
  exports: [SemanticAnalyzerService],
})
export class SemanticAnalyzerModule {}
