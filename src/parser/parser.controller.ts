import { Body, Controller, Post } from '@nestjs/common';
import { ParserService } from './parser.service';

@Controller('parser')
export class ParserController {
  constructor(private readonly parserService: ParserService) {}

  // @Post()
  // parseSelect(@Body() query: string) {
  //   this.parserService.parseSelect()

  // }
  @Post()
  parser(@Body('query') query: string) {
    return this.parserService.parse(query);
  }
}
