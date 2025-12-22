import { Module } from '@nestjs/common';
import { SemanticAnalyzerService } from './semantic-analyzer.service';
import { StorageModule } from '../storage/storage.module';

@Module({
  providers: [SemanticAnalyzerService],
  imports: [StorageModule],
  exports: [SemanticAnalyzerService],
})
export class SemanticAnalyzerModule {}
