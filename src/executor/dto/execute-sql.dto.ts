import { IsNotEmpty, IsString } from 'class-validator';

export class ExecuteSqlDto {
  @IsString()
  @IsNotEmpty()
  query: string;
}
