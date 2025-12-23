import { Body, Controller, Post } from '@nestjs/common';
import { ExecutorService } from './executor.service';

@Controller('execute')
export class ExecutorController {
  constructor(private readonly executorService: ExecutorService) {}

  @Post('ddl')
  async executeDDL(@Body() query: string) {
    return this.executorService.executeDDL(query);
  }

  @Post('dml')
  executeDML(@Body() query: string) {
    return this.executorService.executeDML(query);
  }
}
