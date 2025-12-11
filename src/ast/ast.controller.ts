import { Controller } from '@nestjs/common';
import { AstService } from './ast.service';

@Controller('ast')
export class AstController {
  constructor(private readonly astService: AstService) {}
}
