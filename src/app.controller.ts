import { Body, Controller, Get, Post } from '@nestjs/common';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Post('connect')
  connect(@Body('database') database: string) {
    return this.appService.connect(database);
  }
}
