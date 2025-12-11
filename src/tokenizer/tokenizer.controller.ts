import { Controller } from '@nestjs/common';
import { TokenizerService } from './tokenizer.service';

@Controller('tokenizer')
export class TokenizerController {
  constructor(private readonly tokenizerService: TokenizerService) {}
}
