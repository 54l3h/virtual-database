import { Body, Controller, Post } from '@nestjs/common';
import { ExecutorService } from './executor.service';
import { ExecuteSqlDto } from './dto/execute-sql.dto';

@Controller('execute')
export class ExecutorController {
  constructor(private readonly executorService: ExecutorService) {}

  @Post('ddl')
  executeDDL(@Body() dto: ExecuteSqlDto) {
    return this.executorService.executeDDL(dto);
  }

  @Post('dml')
  executeDML(@Body() dto: ExecuteSqlDto) {
    return this.executorService.executeDML(dto);
  }
}
