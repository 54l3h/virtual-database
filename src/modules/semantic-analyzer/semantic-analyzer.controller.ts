import { Controller } from '@nestjs/common';
import { SemanticAnalyzerService } from './semantic-analyzer.service';

@Controller('semantic-analyzer')
export class SemanticAnalyzerController {
  constructor(private readonly semanticAnalyzerService: SemanticAnalyzerService) {}
}
