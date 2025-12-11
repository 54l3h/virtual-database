import { Body, Controller, Post } from '@nestjs/common';
import { StorageService } from './storage.service';

@Controller('storage')
export class StorageController {
  constructor(private readonly storageService: StorageService) {}

  @Post('create-dir')
  createDir() {
    return this.storageService.createDir();
  }

  @Post('create-schema')
  createSchema() {
    return this.storageService.createSchema();
  }

  @Post('read-schema')
  readSchema() {
    return this.storageService.readSchema();
  }

  // @Post('create-table')
  // createTable() {
  //   return this.storageService.createTable();
  // }

  // @Post('insert-data')
  // insertData() {
  //   return this.storageService.insertData();
  // }
  // @Post('check-dir')
  // checkDirExistence() {
  //   return this.storageService.checkDirExistence();
  // }

  @Post('read-table')
  readTable(@Body('tableName') tableName: string) {
    return this.storageService.readTable(tableName);
  }

  // @Post('select')
  // select(@Body() dto: { columnName: string; tableName: string }) {
  //   console.log(dto);

  //   const { columnName, tableName } = dto;
  //   console.log(columnName, tableName);

  //   return this.storageService.select(columnName, tableName);
  // }

  @Post('delete-row')
  deleteRowByEquality() {
    return this.storageService.deleteRowByEquality();
  }

  @Post('update-row')
  updateRow() {
    return this.storageService.updateRow();
  }
}
