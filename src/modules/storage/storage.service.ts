import { Injectable } from '@nestjs/common';
import { createReadStream, read, readFile } from 'fs';
import * as fs from 'fs/promises';
import path from 'path';
import { Operator } from '../../common/enums/operator.enum';
import { DataType } from '../../common/enums/data-type.enum';
import type {
  AlterDatabaseAST,
  AlterTableAST,
  CreateDatabaseAST,
  CreateTableAST,
  DeleteAST,
  DropDatabaseAST,
  DropTableAST,
  InsertAST,
  SelectAST,
  UpdateAST,
} from 'src/common/types/ast.type';
import type {
  ISchema,
  ITable,
  IColumn,
  Index,
} from '../../common/types/schema.types';
import * as readline from 'readline/promises';

export interface IConnection {
  name: string;
  currentDB: string;
}

@Injectable()
export class StorageService {
  private readonly databasesPath = path.join(process.cwd(), 'databases');
  private readonly dbUsersFilePath = path.join(
    this.databasesPath,
    'postgres',
    'users',
    'users.ndjson',
  );

  async createDatabasesDir(): Promise<void> {
    await fs.mkdir(this.databasesPath, { recursive: true });
    await this.createDatabaseDir('postgres');

    const postgresPath = path.join(this.databasesPath, 'postgres');
    const usersDirPath = path.join(postgresPath, 'users');
    await fs.mkdir(usersDirPath, { recursive: true });

    const usersDataPath = path.join(usersDirPath, 'users.ndjson');
    await fs.writeFile(
      usersDataPath,
      JSON.stringify({
        name: 'postgres',
        currentDB: 'postgres',
      }),
    );
  }

  async checkDatabaseExistence(databaseName: string): Promise<boolean> {
    const databaseDirPath = path.join(this.databasesPath, databaseName);
    try {
      await fs.access(databaseDirPath);
      return true;
    } catch {
      return false;
    }
  }

  async createDatabase(AST: CreateDatabaseAST): Promise<void> {
    await this.createDatabasesDir();
    const isExist = await this.checkDatabaseExistence(AST.name);
    if (isExist) {
      throw new Error(`Database ${AST.name} already exists`);
    }

    await this.createDatabaseDir(AST.name);
  }

  async createDatabaseDir(databaseName: string): Promise<void> {
    const databaseDirPath = path.join(this.databasesPath, databaseName);
    await fs.mkdir(databaseDirPath, { recursive: true });

    const databaseSchemaPath = path.join(databaseDirPath, 'schema.json');
    let schema: ISchema = { tables: [] };

    if (databaseName === 'postgres') {
      schema = {
        tables: [
          {
            name: 'users',
            columns: [
              {
                name: 'name',
                type: DataType.VARCHAR,
                length: 255,
              },
              {
                name: 'currentDB',
                type: DataType.VARCHAR,
                length: 255,
              },
            ],
          },
        ],
      };
    }

    await fs.writeFile(databaseSchemaPath, JSON.stringify(schema));
  }

  async getCurrentDatabase(): Promise<string> {
    const usersDataFile = await fs.readFile(this.dbUsersFilePath, 'utf-8');
    const { currentDB } = JSON.parse(usersDataFile);
    return currentDB;
  }

  async updateCurrentDatabase(database: string): Promise<void> {
    const isExist = await this.checkDatabaseExistence(database);
    if (!isExist) {
      throw new Error(`Database ${database} doesn't exist`);
    }

    const usersDataFile = await fs.readFile(this.dbUsersFilePath, 'utf-8');
    const data: IConnection = JSON.parse(usersDataFile);
    data.currentDB = database;

    await fs.writeFile(this.dbUsersFilePath, JSON.stringify(data));
  }

  async readCurrentDBSchema(): Promise<ISchema> {
    const currentDB = await this.getCurrentDatabase();
    const schemaFilePath = path.join(
      this.databasesPath,
      currentDB,
      'schema.json',
    );

    try {
      await fs.access(schemaFilePath);
    } catch (error) {
      throw new Error(`Schema file not exist for database ${currentDB}`);
    }

    const data = await fs.readFile(schemaFilePath, 'utf-8');
    return JSON.parse(data);
  }

  async updateCurrentDBSchema(schema: ISchema): Promise<void> {
    const currentDB = await this.getCurrentDatabase();
    const schemaFilePath = path.join(
      this.databasesPath,
      currentDB,
      'schema.json',
    );
    await fs.writeFile(schemaFilePath, JSON.stringify(schema));
  }

  // TODO: Check from here
  async checkTableExistsInCurrentDB(tableName: string): Promise<boolean> {
    const schema = await this.readCurrentDBSchema();
    return schema.tables.some((table) => table.name === tableName);
  }

  async getTableFromCurrentDB(tableName: string): Promise<ITable> {
    const schema = await this.readCurrentDBSchema();
    const table = schema.tables.find((table) => table.name === tableName);

    if (!table) {
      throw new Error(`Table ${tableName} not exist in current database`);
    }

    return table;
  }

  async createIndexFile(tableName: string, columnName: string) {
    const currentDB = await this.getCurrentDatabase();

    const indexFilePath = path.join(
      this.databasesPath,
      currentDB,
      tableName,
      `${columnName}_idx.ndjson`,
    );

    try {
      await fs.writeFile(indexFilePath, '');
    } catch (error) {
      throw new Error(error);
    }
  }

  async createTable(AST: CreateTableAST): Promise<void> {
    const currentDB = await this.getCurrentDatabase();
    const schema = await this.readCurrentDBSchema();

    schema.tables.push({
      name: AST.name,
      columns: AST.columns,
    });

    // Create table directory
    const tableDirPath = path.join(this.databasesPath, currentDB, AST.name);
    await fs.mkdir(tableDirPath, { recursive: true });

    for (const column of AST.columns) {
      if (column.primaryKey === true || column.unique === true) {
        await this.createIndexFile(AST.name, column.name);
      }
    }

    await this.updateCurrentDBSchema(schema);

    // Create NDJSON file inside table directory
    const dataFilePath = path.join(tableDirPath, `${AST.name}.ndjson`);
    await fs.writeFile(dataFilePath, '');
  }

  async readTableData(tableName: string): Promise<any[]> {
    const currentDB = await this.getCurrentDatabase();
    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      tableName,
      `${tableName}.ndjson`,
    );

    const results: any[] = [];

    try {
      const fileStream = createReadStream(dataFilePath, {
        encoding: 'utf-8',
      });

      const lines = readline.createInterface({ input: fileStream });

      for await (const line of lines) {
        results.push(JSON.parse(line));
      }

      return results;
    } catch (error) {
      throw new Error(`An error occured while reading the data file`);
    }
  }

  // You can select one row (You will try to use index here)
  // You can select many rows
  async select(AST: SelectAST) {
    // look at the pk file like the id_idx.ndjson
    // gather the offsets and lengths into array of objects and then destruct them and itereate with them into the data file
    const currentDB = await this.getCurrentDatabase();
    const schema = await this.readCurrentDBSchema();

    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      AST.table,
      `${AST.table}.ndjson`,
    );

    console.log({ schema });

    let pkName: string = '';

    const table = schema.tables.forEach((table) => {
      if (table.name === AST.table) {
        for (const col of table.columns) {
          if (col.primaryKey) {
            pkName = col.name;
          }
        }
      }
    });

    console.log({ pkName });

    // now you got the pk name so you can access the index file to get the proper offsets

    const pkIndexFilePath = await this.getIndexFilePath(AST.table, pkName);

    const indexes: Record<string, any>[] = [];

    const fileStream = createReadStream(pkIndexFilePath, { encoding: 'utf-8' });

    const lines = readline.createInterface({
      input: fileStream,
    });

    for await (const line of lines) {
      const { offset, length } = JSON.parse(line);
      indexes.push({ offset, length });
    }

    const whatWillBeReturned: Record<string, any>[] = [];

    for (const { offset, length } of indexes) {
      const buffer = Buffer.alloc(length);
      const fileHandle = await fs.open(dataFilePath, 'r');
      await fileHandle.read(buffer, 0, length, offset);
      await fileHandle.close();
      const line = buffer.toString('utf-8').trim();

      const { deleted, ...data } = JSON.parse(line);

      whatWillBeReturned.push(data);
    }

    // ! you finished retrieve all the existed data

    // TODO: filtering the all data to get specific

    // now you got the offsets you should get into the data file and get all the data

    // you should prevent the deleted row from get retrieved

    // const data = await this.readTableData(AST.table);
    const tableData = whatWillBeReturned.filter((row) => {
      return row.deleted === false;
    });

    // const set = new Set(tableData);

    // console.log({ tableData });
    // console.log({ set });

    const columns = AST.columns;
    const where = AST?.where;

    const { criterion, operator, value } = where || {};

    let projectedData: Record<string, any>[] = [];

    if (columns.includes('*')) {
      projectedData = tableData;
    } else {
      projectedData = tableData.map((row) => {
        const obj = {};
        for (const column of columns) {
          obj[`${column}`] = row[`${column}`];
        }
        return obj;
      });
    }

    console.log(projectedData);
    
    let selectedData: {}[] = projectedData;
    console.log({selectedData});
    

    console.log({ criterion });

    if (criterion) {
      switch (operator) {
        case Operator.EQUAL:
          console.log('h');
          
          selectedData = projectedData.filter((row) => {
            return row[`${criterion}`] === value;
          });

          break;
        case Operator.GREATER_THAN_OR_EQUAL:
          selectedData = projectedData.filter((row) => {
            return row[`${criterion}`] >= parseInt(value as string);
          });
          break;
        case Operator.GREATER_THAN:
          selectedData = projectedData.filter((row) => {
            return row[`${criterion}`] > parseInt(value as string);
          });
          break;
        case Operator.LESS_THAN_OR_EQUAL:
          selectedData = projectedData.filter((row) => {
            return row[`${criterion}`] <= parseInt(value as string);
          });
          break;
        case Operator.LESS_THAN:
          selectedData = projectedData.filter((row) => {
            return row[`${criterion}`] < parseInt(value as string);
          });
          break;
      }
    }

    return selectedData;
  }

  async selectAfterIndexing(AST: SelectAST) {}

  async insert(AST: InsertAST) {
    const currentDB = await this.getCurrentDatabase();
    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      AST.table,
      `${AST.table}.ndjson`,
    );

    let rows: any[][] = [];

    console.log(AST.columns.length);

    const columnCount = AST.columns.length;
    console.log({ columnCount });

    for (let i = 0; i < AST.values.length; i += columnCount) {
      rows.push(AST.values.slice(i, i + columnCount));
    }

    // rows => [ [val1,val2], [val1,val2], [val1,val2] ]
    for (const rowValues of rows) {
      if (AST.columns.length !== rowValues.length) {
        throw new Error('INSERT has more target columns than expressions');
      }

      for (let i = 0; i < AST.columns.length; i++) {
        const column = AST.columns[i];
        const value = rowValues[i];

        if (await this.isIndexed(AST.table, column)) {
          const indexPath = await this.getIndexFilePath(AST.table, column);
          const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
          const lines = readline.createInterface({ input: fileStream });

          for await (const line of lines) {
            const { value: existingValue } = JSON.parse(line);
            if (existingValue === value) {
              throw new Error(`Value '${value}' already exist`);
            }
          }
        }
      }

      const row: Record<string, any> = { deleted: false };
      for (let i = 0; i < AST.columns.length; i++) {
        row[AST.columns[i]] = rowValues[i];
      }

      const insertedLine = JSON.stringify(row) + '\n';
      const { size: offset } = await fs.stat(dataFilePath);
      await fs.appendFile(dataFilePath, insertedLine, 'utf-8');

      const length = Buffer.byteLength(insertedLine);
      for (let i = 0; i < AST.columns.length; i++) {
        if (await this.isIndexed(AST.table, AST.columns[i])) {
          const indexPath = await this.getIndexFilePath(
            AST.table,
            AST.columns[i],
          );
          const indexEntry = { value: rowValues[i], offset, length };
          await fs.appendFile(
            indexPath,
            JSON.stringify(indexEntry) + '\n',
            'utf-8',
          );
        }
      }
    }
  }

  async update(AST: UpdateAST) {
    /**
     * TODO: check the new values types and convert them to proper data types by updating the updates columns into the AST
     */

    let newUpdatingObj: Record<string, any> = {};

    // convert the values types
    for (let [column, value] of Object.entries(AST.updates)) {
      newUpdatingObj[column] = !isNaN(value) ? Number(value) : String(value);
    }

    AST.updates = newUpdatingObj;
    if (AST.where) {
      AST.where.value = !isNaN(AST.where.value as any)
        ? Number(AST.where.value)
        : String(AST.where.value);
    }

    console.log({ updates: AST.updates });

    // AST.table => GET TABLE NAME
    // GET THE SCHEMA FILE TO KNOW THE COLUMNS WHICH ARE CONSIDERED AS INDEXES LIKE:
    // (COLUMNS WITH UNIQUE=TRUE) & (COLUMNS WITH PRIMARYKEY=TRUE)
    // THEN GET THE INDEXES FILES TO UPDATE THEM AND REMOVE THE OLD ROWS WHICH POINT TO THE OLD OFFSETS
    // AND INSERT THE NEW ROW WHICH POINTS TO THE NEW ROW IN THE DATAFILE WITH THE CORRECT OFFSET
    const schema = await this.readCurrentDBSchema();
    const currentDB = await this.getCurrentDatabase();
    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      AST.table,
      `${AST.table}.ndjson`,
    );

    const indexes: Record<string, any>[] = [];

    const table = schema.tables.map((table) => {
      for (const column of table.columns) {
        if (column.primaryKey || column.unique) {
          indexes.push(column);
        }
      }
    });

    const indexesNames = indexes.map((index) => {
      return index.name;
    });

    // does the criterion considered as column index or not if yes look at the index file to check if it exists or not
    // you actuall get the indexesNames for the table
    // now you will check if it includes the name of the criterion

    // if it considered you will look up quickly into the index file
    console.log(indexesNames.includes(AST.where?.criterion));

    // desired line which will be updated
    let desiredLine;
    // To lookup quickly
    // ! if it not exist throw an error
    // if it exist take the row offset and length
    if (indexesNames.includes(AST.where?.criterion)) {
      const indexPath = await this.getIndexFilePath(
        AST.table,
        AST.where?.criterion!,
      );

      const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });

      const lines = readline.createInterface({
        input: fileStream,
      });

      let found = false;

      for await (const line of lines) {
        const parsedLine = JSON.parse(line);

        const isMatch =
          typeof parsedLine.value === 'number'
            ? parsedLine.value === Number(AST.where?.value)
            : String(parsedLine.value) === String(AST.where?.value);

        if (isMatch) {
          found = true;
          desiredLine = parsedLine;
          break;
        }
      }

      if (!found) {
        throw new Error('This record is not exist');
      }
    }

    console.log({ desiredLine });

    // now you gonna update
    // you have the indexes files paths
    // and you have the data file
    // you need to get to the row and clone it if exist and then duplicate it with the deleted:true and make another one with the same pk and then mark deleted: false and save the new offset and length
    const { offset, length } = desiredLine;
    // after you get the offset and length you shlould read the line from the data file how? by stream the file

    const buffer = Buffer.alloc(length);
    const fileHandle = await fs.open(dataFilePath, 'r');
    await fileHandle.read(buffer, 0, length, offset);
    await fileHandle.close();

    const line = buffer.toString('utf-8').trim();

    // this line includes the columns names that should be deleted with the values
    // you should iterate into the lines to get the row if the each column with the value into this

    let record = JSON.parse(line);
    record.deleted = true; // mark it as deleted

    //! append the same row as deleted
    await fs.appendFile(dataFilePath, JSON.stringify(record) + '\n');

    // append the new row with the new values
    // then get the offset and length
    const newRecord = { ...record, deleted: false, ...AST.updates };
    // append them into the indexes files

    const insertedRecord = JSON.stringify(newRecord) + '\n';
    // get the length
    const insertedRecordLength = Buffer.byteLength(insertedRecord);

    // you get the record offset after append the row which is marked as deleted
    // and append the new row
    const { size: insertedRecordOffset } = await fs.stat(dataFilePath);
    await fs.appendFile(dataFilePath, insertedRecord);

    // now you got the indexes names
    // after this you should get into the files to update it by remove the old offsets and insert the new row that points to the

    // ! check the code below

    for (const indexName of indexesNames) {
      const newData: Record<string, any>[] = [];
      const indexFilePath = await this.getIndexFilePath(AST.table, indexName);

      console.log(indexFilePath);
      // after you get the index file path
      // you should enter the file and read it and search for the value that equal it
      const fileStream = createReadStream(indexFilePath, { encoding: 'utf-8' });

      const lines = readline.createInterface({
        input: fileStream,
      });

      for await (const line of lines) {
        const parsedLine = JSON.parse(line);

        if (
          parsedLine.value === record[indexName] &&
          parsedLine.offset === offset &&
          parsedLine.length === length
        ) {
          continue;
        }
        newData.push(parsedLine);
      }

      console.log({ newData });

      const newIndex = {
        value: newRecord[indexName],
        offset: insertedRecordOffset,
        length: insertedRecordLength,
      };

      newData.push(newIndex);

      const insertedData = newData
        .map((line) => {
          return JSON.stringify(line);
        })
        .join('\n');

      try {
        await fs.writeFile(indexFilePath, insertedData + '\n');
      } catch (error) {
        throw new Error(
          `An error occured while rewrite the index ${indexName} file`,
        );
      }
    }

    // console.log(indexesFilesPaths);

    // after you get the indexesFilesPaths
    // 1. if the critertion considered as index you will take a look into the index path to get the offset and length (to ckeck if the value existence if)
    // 2. if the value already exist you will not able to use it again and you should throw an error
    // TODO: ...
    console.log(AST);

    // let's talk about the first scenario
    // updating one column
    // const [columnName, newValue] = Object.entries(AST.updates)[0]; // first column and only one

    // chech the indexes
    // get the column name
    // check the index file existence
    // if exists you should lookup for the new value if it exist or not

    // const isIndexed = await this.isIndexed(AST.table, columnName);

    // Check the updating value if it exists or not
    // if exists throw an error
    // if not exist just add it

    // you will use it to read
    // you will update it

    // const currentDB = await this.getCurrentDatabase();

    // const indexFilePath = path.join(
    //   this.databasesPath,
    //   currentDB,
    //   AST.table,
    //   `${columnName}_idx.ndjson`,
    // );

    // const dataFilePath = path.join(
    //   this.databasesPath,
    //   currentDB,
    //   AST.table,
    //   `${AST.table}.ndjson`,
    // );

    // check if the new value already exist or not
    // if (isIndexed === true) {
    //   // read the index file (stream)
    //   // check the line which includes the value
    //   // how index looks like?!
    //   // {value:....,offset:....}

    //   const fileStream = createReadStream(indexFilePath, { encoding: 'utf-8' });

    //   const lines = readline.createInterface({ input: fileStream });

    //   for await (const line of lines) {
    //     const index: Index = JSON.parse(line);
    //     if (index.value === newValue) {
    //       throw new Error(
    //         'This value is already used and can not be used twice',
    //       );
    //     }
    //     // if didn't use before you should get offset of the row that we are going to update
    //   }
    // }

    const isConditionColunmnIndexed = await this.isIndexed(
      AST.table,
      AST.where?.criterion as string,
    );

    // if it indexed you will lookup with the value to serch
    // TODO: you should lookup first
    // ? after you get the offset and length
    // ! you should access the file and go to directly to the row and mark it as deleted
    // * then you should add the new line in the email_idx.ndjson in your situation

    let desiredRecord: Index = {} as Index;
    // if (isConditionColunmnIndexed === true) {
    //   const conditionColumnPath = path.join(
    //     this.databasesPath,
    //     currentDB,
    //     AST.table,
    //     `${AST.where?.criterion}_idx.ndjson`,
    //   );

    //   const fileStream = createReadStream(conditionColumnPath);

    //   const lines = readline.createInterface({ input: fileStream });

    //   for await (const line of lines) {
    //     const record: Index = JSON.parse(line);

    //     const isMatch =
    //       typeof record.value === 'number'
    //         ? record.value === Number(AST.where?.value)
    //         : String(record.value) === String(AST.where?.value);

    //     if (isMatch) {
    //       desiredRecord = record;
    //       break;
    //     }
    //   }

    //   // if (!desiredRecord.value) {
    //   //   throw new Error('This record is not exist to update');
    //   // }
    // }

    // if (desiredRecord.value) {
    //   const { offset, length } = desiredRecord;

    //   // get into the data file to update it
    //   // update the record by marking it as deleted
    //   // append the new record with the new data
    //   // append the new record with the new index with the new offset and length

    //   // here
    //   // const fileStream = createReadStream()

    //   // const fd = fs.open(dataFilePath, 'r+');

    //   // const { size } = await fs.stat(dataFilePath);
    //   // const buffer = Buffer.alloc(size);

    //   // const data = read(fd,buffer);

    //   const fileStream = createReadStream(dataFilePath, {
    //     start: offset - length,
    //     end: offset - 1,
    //     encoding: 'utf-8',
    //   });

    //   const lines = readline.createInterface({ input: fileStream });

    //   let record;
    //   // You will get one line only
    //   for await (const line of lines) {
    //     record = JSON.parse(line);
    //   }

    //   record['deleted'] = true;
    // }

    const insertionData = {};

    const updating = {};

    // const insertedLine = JSON.stringify(insertionData) + '\n';

    // await fs.appendFile(dataFilePath, insertedLine, 'utf-8');

    // if the new value didn't use before
    // you will update the data file & the index
    // if the column (indexed) you can get the offset
    // after this you can cut from the offset till the length and replace it then update the offset and length
    // in the datafile you will get the object offset from the index file and then went to the data file and change the obj directly

    // updates
    // get the table data
  }

  async delete(AST: DeleteAST) {
    const { table, where } = AST;
    const { criterion, operator, value } = where ?? {};

    // criterion is the column name
    // you will check on this column if it considered as an indexed column or not
    // if it conisdered as indexed column

    // TODO: Again
    // after you get the table and appended the new row which is deleted
    // 1- append the new row to the data file mark it as deleted
    // 2- check the schema the pk column and the unique
    // 3- go to the index files of them and then filter the deleted and save the file again

    const tableData = await this.readTableData(table);

    const deletedRows: any[] = [];
    const remainingRows: any[] = [];

    for (const row of tableData) {
      const rowValue = row[criterion!];
      const compareValue = isNaN(value as any) ? value : parseInt(value as any);

      let isMatch = false;

      // matching
      // v2
      switch (operator) {
        case Operator.EQUAL:
          isMatch = rowValue === compareValue!;
          break;

        case Operator.GREATER_THAN:
          isMatch = rowValue > compareValue!;
          break;

        case Operator.GREATER_THAN_OR_EQUAL:
          isMatch = rowValue >= compareValue!;
          break;

        case Operator.LESS_THAN:
          isMatch = rowValue < compareValue!;
          break;

        case Operator.LESS_THAN_OR_EQUAL:
          isMatch = rowValue <= compareValue!;
          break;
      }

      if (isMatch) {
        deletedRows.push(row);
      } else {
        remainingRows.push(row);
      }
    }

    const currentDB = await this.getCurrentDatabase();
    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      AST.table,
      `${AST.table}.ndjson`,
    );

    // !TODO: gather the index files
    // collect the index files
    // you will delete the row from the index files
    // and then mark the row as deleted by append the same row as deleted:true in the data file

    for (const row of deletedRows) {
      await fs.appendFile(
        dataFilePath,
        JSON.stringify({ ...row, deleted: true }) + '\n',
      );
    }

    // after appending into the data file
    // you should deleted the the rows from the index files
    // via lookup at schema find the pk and unique
    const schema: ISchema = await this.readCurrentDBSchema();

    let indexes: Record<string, any>[] = [];

    // you want to find the columns of the table where you are deleting the rows
    const tables = schema.tables.map((table) => {
      for (const column of table.columns) {
        if (column.primaryKey || column.unique) {
          indexes.push(column);
        }
      }
    });

    // now you got the indexes column
    // you should get into the indexes files and then update them by delete the rows which point on the deleted rows

    const indexesPaths: string[] = [];
    for (const index of indexes) {
      indexesPaths.push(await this.getIndexFilePath(AST.table, index.name));
      const indexPath = await this.getIndexFilePath(AST.table, index.name);
      // enter the file search for the value that deleted and remove it and then rewrite the file

      const fileStream = createReadStream(indexPath, {
        encoding: 'utf-8',
      });

      const lines = readline.createInterface({
        input: fileStream,
      });

      const newIndexLines: any[] = [];

      for await (const line of lines) {
        const { value } = JSON.parse(line);
        // console.log({ index });

        let isDeleted = false;

        for (const row of deletedRows) {
          if (row[index.name] === value) {
            isDeleted = true;
            break;
          }

          if (!isDeleted) {
            newIndexLines.push(line);
          }
        }
      }
      // console.log({ newIndexLines });

      try {
        fs.writeFile(indexPath, newIndexLines.join('\n') + '\n');
      } catch (error) {
        throw new Error('An error occured while trying to delete the rows');
      }
    }

    return { success: true, message: 'deleted' };
  }

  async alterTable(AST: AlterTableAST) {
    // you checked the existence
    // get the schema file
    // access the schema file
    // add column to the table
    // create index file if it primary key = true or unique = true

    console.log(AST);

    const currentDB = await this.getCurrentDatabase();
    const schema: ISchema = await this.readCurrentDBSchema();

    const newColumn: IColumn = {
      name: AST.columnName as string,
      type: AST.dataType as DataType,
    };

    const table = schema.tables.find((table) => table.name === AST.name);

    table?.columns.push(newColumn);

    await this.updateCurrentDBSchema(schema);

    // console.log(schema);
  }

  async alterDatabase(AST: AlterDatabaseAST) {}

  async dropDatabase(AST: DropDatabaseAST) {
    const currentDB = await this.getCurrentDatabase();

    if (AST.name === currentDB) {
      throw new Error(
        `You are not able to Drop this db (You are connected to this db now)`,
      );
    }

    // Delete the database directory
    const databaseDir = path.join(this.databasesPath, AST.name);

    try {
      await fs.rm(databaseDir, { recursive: true, force: true });
    } catch (error) {
      throw new Error(error);
    }
  }

  async dropTable(AST: DropTableAST) {
    // you just validated the existence of the table in the parser by using the lexical analyzer
    const currentDB = await this.getCurrentDatabase();

    // will be deleted
    const tablePath = path.join(this.databasesPath, currentDB, AST.name);

    // will be modified
    const schemaPath = path.join(this.databasesPath, currentDB, 'schema.json');

    try {
      await fs.rm(tablePath, { recursive: true, force: true });
    } catch (error) {
      throw new Error(error);
    }

    const schema = await fs.readFile(schemaPath, 'utf-8');
    const parsedSchema: ISchema = JSON.parse(schema);

    const tableIndex = parsedSchema.tables.findIndex((table) => {
      return table.name === AST.name;
    });

    parsedSchema.tables.splice(tableIndex);

    try {
      await fs.writeFile(schemaPath, JSON.stringify(parsedSchema));
    } catch (error) {
      throw new Error(error);
    }
  }

  async getIndexFilePath(tableName: string, columnName: string) {
    const currentDB = await this.getCurrentDatabase();
    const indexFilePath = path.join(
      this.databasesPath,
      currentDB,
      tableName,
      `${columnName}_idx.ndjson`,
    );

    return indexFilePath;
  }

  private async isIndexed(tableName: string, columnName: string) {
    const indexFilePath = await this.getIndexFilePath(tableName, columnName);

    try {
      await fs.access(indexFilePath);
      return true;
    } catch (error) {
      return false;
    }
  }

  private async readIndexFile(tableName: string, columnName: string) {
    const results: any[] = [];
    const indexFilePath = await this.getIndexFilePath(tableName, columnName);
    try {
      const fileStream = createReadStream(indexFilePath, {
        encoding: 'utf-8',
      });

      const lines = readline.createInterface({ input: fileStream });

      for await (const line of lines) {
        results.push(JSON.parse(line));
      }

      return results;
    } catch (error) {
      throw new Error(`An error occured while reading the index file`);
    }
  }

  // async writeTableData(tableName: string, data: any[]): Promise<void> {
  //   const currentDB = await this.getCurrentDatabase();
  //   const dataFilePath = path.join(
  //     this.databasesPath,
  //     currentDB,
  //     `${tableName}.ndjson`,
  //   );

  //   const ndjsonData = data.map((row) => JSON.stringify(row)).join('\n');
  //   await fs.writeFile(
  //     dataFilePath,
  //     ndjsonData + (data.length > 0 ? '\n' : ''),
  //   );
  // }

  // async insertData(
  //   tableName: string,
  //   columns: string[],
  //   values: any[],
  // ): Promise<void> {
  //   const table = await this.getTableFromCurrentDB(tableName);

  //   const columnsNames = table.columns.map((col) => {
  //     return col.name;
  //   });

  //   console.log(columnsNames);

  //   const tableData = await this.readTableData(tableName);

  //   const newRow: any = {};
  //   columns.forEach((col, index) => {
  //     newRow[col] = values[index];
  //   });

  //   tableData.push(newRow);
  //   await this.writeTableData(tableName, tableData);
  // }

  // async update(AST: any): Promise<string> {
  //   await this.getTableFromCurrentDB(AST.tableName);

  //   const tableData = await this.readTableData(AST.tableName);

  //   const updatedRowIndex = tableData.findIndex((row) => {
  //     return row[AST.conditionColumn] === parseInt(AST.conditionValue);
  //   });

  //   if (updatedRowIndex === -1) {
  //     throw new Error('No matching row found');
  //   }

  //   tableData[updatedRowIndex][AST.updatingColumn] = parseInt(
  //     AST.updatingValue,
  //   );
  //   await this.writeTableData(AST.tableName, tableData);

  //   return 'UPDATE 1';
  // }

  // async delete(AST: any): Promise<any[]> {
  //   const { table, where } = AST;
  //   await this.getTableFromCurrentDB(table);

  //   const tableData = await this.readTableData(table);

  //   let dataAfterDeletion = tableData;

  //   if (where?.criterion) {
  //     dataAfterDeletion = this.filterByCondition(
  //       tableData,
  //       where.criterion,
  //       where.operator,
  //       where.value,
  //       true, // invert for deletion
  //     );
  //   }

  //   await this.writeTableData(table, dataAfterDeletion);
  //   return dataAfterDeletion;
  // }

  // private filterByCondition(
  //   data: any[],
  //   criterion: string,
  //   operator: string,
  //   value: any,
  //   invert: boolean = false,
  // ): any[] {
  //   const numericValue = !isNaN(value) ? parseInt(value) : value;

  //   const filterFn = (row: any): boolean => {
  //     const rowValue = row[criterion];
  //     let matches = false;

  //     switch (operator) {
  //       case Operator.EQUAL:
  //         matches = rowValue === numericValue;
  //         break;
  //       case Operator.GREATER_THAN:
  //         matches = rowValue > numericValue;
  //         break;
  //       case Operator.GREATER_THAN_OR_EQUAL:
  //         matches = rowValue >= numericValue;
  //         break;
  //       case Operator.LESS_THAN:
  //         matches = rowValue < numericValue;
  //         break;
  //       case Operator.LESS_THAN_OR_EQUAL:
  //         matches = rowValue <= numericValue;
  //         break;
  //       default:
  //         matches = true;
  //     }

  //     return invert ? !matches : matches;
  //   };

  //   return data.filter(filterFn);
  // }
}

// async insertData(
//   tableName: string = 'users',
//   desiredColumns: string | string[],
//   values: any | any[],
// ) {
//   // You should make sure that the table is actually exists into your schema
//   // How you can do that?
//   const schema: ISchema = await this.readSchema();

//   const isTableExist = schema.tables.findIndex((table) => {
//     return table.name === tableName;
//   });

//   if (isTableExist === -1) {
//     return {
//       success: false,
//       message: `ERROR: relation ${tableName} does not exist`,
//     };
//   }

//   // ! Check existense of the column/s

//   // If it exists => you should go to the data file to insert the new data
//   // read the file
//   // const dataFilePath = path.join(this.dataDir, `${tableName}.json`);
//   // const data = await fs.readFile(dataFilePath, 'utf-8');
//   // instead of reading all the time and parsing use the readFile (class method)
//   const parsedData = await this.readData(tableName); // the table data

//   const dataToInsert = {
//     id: 11,
//     name: 'Mohammed',
//     email: 'mohammedsaleh@outlook.com',
//     city: 'alexandria', // incorrect column name
//   };
//   // imported columns
//   const importedColumns: string[] = Object.keys(dataToInsert);
//   console.log(importedColumns);

//   // You should evaluate the columns name against the schema.json
//   // How you can do this?
//   // You should load the file of the schema.json
//   // Get into the table name
//   // And compare the columns

//   // ! you get the schema in the first lines
//   const theTable = schema.tables.find((table) => table.name === tableName);
//   // TODO: CHECK THE IMPORTED COLUMNS ARE ACTUALLY EXIST OR NOT
//   const columns = theTable?.columns;

//   const columnsNames = columns?.map((column) => {
//     return column.name;
//   });

//   console.log(columnsNames);

//   // * Now you have the correct column names
//   // * Now you have to compare (importedColumns VS. columnsNames(el asas))
//   const areColumnsCorrect = importedColumns.every((importedColumn) => {
//     if (!columnsNames?.includes(importedColumn)) {
//       return false;
//     }

//     return true;
//   });

//   if (!areColumnsCorrect) {
//     return { success: false, message: 'error' };
//   }

//   parsedData.push(dataToInsert);
//   const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);
//   await fs.writeFile(dataFilePath, JSON.stringify(parsedData));

//   // const parsedData = JSON.parse(data);
//   // parsedData.push({});
// }

// // criteria could it be => id, email, name, price
// async deleteRowByEquality(
//   tableName: string = 'users',
//   criteria = 'id',
//   value = 11,
// ) {
//   // You will remove data based on what?
//   // based on the id?
//   // based on the email?
//   // based on the (CRITERIA) => You can got it from the parameter

//   const parsedData: [] = await this.readTable('users');
//   const dataAfterDeletion = parsedData.filter((row) => {
//     return row[criteria] !== value;
//   });

//   if (!dataAfterDeletion.length) {
//     return { success: false, message: 'RECORD NOT FOUND' };
//   }

//   const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);
//   await fs.writeFile(dataFilePath, JSON.stringify(dataAfterDeletion));
// }

// to select
// 1. select by compare with =
// 2. select by compare with another equality operators
// 3. you should get the data type of the column from the AST => so you should add a property called like data type
// 4. data type into the columns like [{column}]

// async select(
//   columns: string[],
//   tableName: string,
//   criterion?: string,
//   operator?: string,
//   value?: any,
// ) {
//   // console.log(columns);
//   // console.log({ tableName });

//   // You will get column/s
//   // you shouldn't check here for the existence of table and columns because you actually checked this into the semantic analyzer before

//   // const schemaFile: ISchema = await this.readSchema();
//   // console.log(schemaFile);

//   // You should check the table existence before trying to reat it from the directory

//   // For now work with find to develop better
//   // then you should work with some(array method)

//   // find the table with find, after this access the table directly adn then use some(array method) => for checking the column existing
//   // const table = schemaFile.tables.find((table) => {
//   //   return table.tableName === tableName;
//   // });

//   // const table = await this.checkTableExistence(tableName, schemaFile);

//   /**
//    * If all the columns requested just return the true
//    * You don't need to check
//    */
//   // const column = table.columns.some((column) => {
//   //   if (columnName === '*') return true;
//   //   return column?.columnName === columnName;
//   // });

//   // this.checkColumnExistence(columnName, table);

//   // you are looking for the id you should check if this column exists or not into the table

//   // read the table file and select only the columns
//   const tableData: [] = await this.readTable(tableName);
//   // console.log(tableData);

//   const projectedData = tableData.map((row) => {
//     const obj = {};
//     // console.log(row);

//     for (const column of columns) {
//       obj[column] = row[column];
//     }

//     return obj;
//   });

//   console.log(projectedData);

//   let selectedData: {}[] = [];

//   if (criterion) {
//     switch (operator) {
//       case Operator.EQUAL:
//         selectedData = projectedData.filter((row) => {
//           if (!isNaN(value)) {
//             return row[`${criterion}`] === parseInt(value);
//           }
//           return row[`${criterion}`] === value;
//         });

//         console.log({ rows: selectedData });

//         break;
//       case Operator.GREATER_THAN_OR_EQUAL:
//         selectedData = projectedData.filter((row) => {
//           return row[`${criterion}`] >= parseInt(value);
//         });
//         break;
//       case Operator.GREATER_THAN:
//         selectedData = projectedData.filter((row) => {
//           return row[`${criterion}`] > parseInt(value);
//         });
//         break;
//       case Operator.LESS_THAN_OR_EQUAL:
//         selectedData = projectedData.filter((row) => {
//           return row[`${criterion}`] <= parseInt(value);
//         });
//         break;
//       case Operator.LESS_THAN:
//         selectedData = projectedData.filter((row) => {
//           return row[`${criterion}`] < parseInt(value);
//         });
//         break;

//       default:
//         selectedData = projectedData;
//         break;
//     }
//   }

//   return selectedData;

//   // console.log(tableData);
//   // you will return an object / array of objects with the specified properties

//   // if (columnName === '*') {
//   //   return tableData;
//   // }

//   // let selectedObjs: {}[] = [];
//   // tableData.map((row) => {
//   //   selectedObjs.push({ [columnName]: row[columnName] });
//   // });
//   // console.log(selectedObjs);

//   // return selectedObjs;
//   // you should read the schema file first
//   // check the table existence
//   // checkk the column/s existence
// }

// async delete(AST) {
//   //  AST.columns,
//   const { table, where } = AST;
//   const { criterion, operator, value } = where;
//   // AST.table,
//   // AST.where?.criterion,
//   // AST.where?.operator,
//   // AST.where?.value,

//   const tableData: [] = await this.readTable(table);
//   console.log(tableData);
//   console.log(where);

//   let dataAfterDeletion;

//   if (criterion) {
//     switch (operator) {
//       case Operator.EQUAL:
//         dataAfterDeletion = tableData.filter((row) => {
//           if (!isNaN(value)) {
//             return row[`${criterion}`] !== parseInt(value);
//           }
//           return row[`${criterion}`] !== value;
//         });

//         // console.log({ dataAfterDeletion });

//         break;
//       case Operator.GREATER_THAN_OR_EQUAL:
//         dataAfterDeletion = tableData.filter((row) => {
//           return row[`${criterion}`] < parseInt(value);
//         });

//         console.log({ dataAfterDeletion });
//         break;
//       case Operator.GREATER_THAN:
//         dataAfterDeletion = tableData.filter((row) => {
//           return row[`${criterion}`] <= parseInt(value);
//         });

//         console.log({ dataAfterDeletion });
//         break;
//       case Operator.LESS_THAN_OR_EQUAL:
//         dataAfterDeletion = tableData.filter((row) => {
//           return row[`${criterion}`] > parseInt(value);
//         });

//         console.log({ dataAfterDeletion });
//         break;
//       case Operator.LESS_THAN:
//         dataAfterDeletion = tableData.filter((row) => {
//           return row[`${criterion}`] >= parseInt(value);
//         });

//         console.log({ dataAfterDeletion });
//         break;

//       default:
//         break;
//     }
//   }

//   const dataFilePath = path.join(this.dataDirPath, `${table}.json`);
//   await fs.writeFile(dataFilePath, JSON.stringify(dataAfterDeletion));

//   return dataAfterDeletion;
// }

// If you are going to delete rows there is no criterion
// async deleteRows(tableName: string) {
//   const schemaFile: ISchema = await this.readSchema();

//   const table = this.checkTableExistence(tableName, schemaFile);
//   const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);

//   await fs.writeFile(dataFilePath, JSON.stringify([]));
// }

// async checkTableExistence(tableName: string, schemaFile) {
//   const table = schemaFile.tables.find((table) => {
//     return table.tableName === tableName;
//   });

//   if (!table) {
//     throw new Error(`Table ${tableName} is not exist`);
//   }

//   return table;
// }

// checkColumnExistence(columnName: string, table) {
//   const column = table.columns.find((column) => {
//     if (columnName === '*') return true;
//     return column?.columnName === columnName;
//   });

//   if (!column) {
//     throw new Error(`Column ${columnName} is not exist`);
//   }
// }

// checkColumnsExistence(columns: string[], table) {
//   for (const column of columns) {
//     this.checkColumnExistence(column, table);
//   }
// }

// async updateRow(
//   tableName: string = 'users',
//   targetColumnName: string = 'name',
//   updatingValue: string = 'Mariem',
//   conditionColumnName: string = 'id',
//   conditionValue = 12,
// ) {
//   const schemaFile: ISchema = await this.readSchema();
//   const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);
//   // table existence
//   // column existence which will be updated
//   // column existence which is used as a criterion

//   // check the table existence and get the table
//   const table = await this.checkTableExistence(tableName, schemaFile);

//   const targetColumn = await this.checkColumnExistence(
//     targetColumnName,
//     table,
//   );

//   const conditionColumn = await this.checkColumnExistence(
//     conditionColumnName,
//     table,
//   );

//   // after checking you will read the file
//   const data: any[] = await this.readData(tableName); // You got the data file

//   const desiredRowIndex = data.findIndex((row) => {
//     return row[`${conditionColumnName}`] === conditionValue;
//   });

//   data[desiredRowIndex][`${targetColumnName}`] = updatingValue;

//   await fs.writeFile(dataFilePath, JSON.stringify(data));

//   // ! You should get the proper column/s to update
// }

// async update(AST) {
//   console.log('update from the storage service');
//   console.log(AST);

//   const tableData: [] = await this.readData(AST.tableName);

//   console.log(tableData);

//   const updatedRowIndex = tableData.findIndex((row) => {
//     return row[`${AST.conditionColumn}`] === parseInt(AST.conditionValue);

//     // obj = row[`${AST.updatingColumn}`] === parseInt(AST.updatingValue);
//   });

//   console.log(updatedRowIndex);

//   tableData[updatedRowIndex][AST.updatingColumn] = parseInt(
//     AST.updatingValue,
//   ) as never;

//   await this.updateDataFile(AST.tableName, tableData);

//   return 'UPDATE1';
// }

// TODO: start again

// Create databases dir
// Connect to postgres => Database
// async createDatabasesDir(): Promise<void> {
//   await fs.mkdir(this.databasesPath, { recursive: true });
//   await this.createDatabaseDir('postgres');

//   const postgresPath = path.join(this.databasesPath, 'postgres');
//   const usersDirPath = path.join(postgresPath, 'users');

//   await fs.mkdir(usersDirPath, { recursive: true });

//   const usersDataPath = path.join(usersDirPath, 'users.ndjson');
//   await fs.writeFile(
//     usersDataPath,
//     JSON.stringify({
//       name: 'postgres',
//       currentDB: 'postgres',
//     }),
//   );
// }

// ! done
// async updateCurrentDatabase(database: string) {
//   const isExit = await this.checkDatabaseExistence(database);

//   if (!isExit) {
//     throw new Error(`Database ${database} is not exist`);
//   }

//   const postgresPath = path.join(this.databasesPath, 'postgres');
//   const usersDirPath = path.join(postgresPath, 'users');

//   const usersDataPath = path.join(usersDirPath, 'users.ndjson');
//   const file = await fs.readFile(usersDataPath, 'utf-8');

//   const data: IConnection = JSON.parse(file);
//   data.currentDB = database;

//   await fs.writeFile(usersDataPath, JSON.stringify(data));
// }

// async createTablee(tableName: string) {}

// ! done
// async createDatabaseDir(databaseName: string) {
//   const databaseDirPath = path.join(this.databasesPath, `${databaseName}`);
//   await fs.mkdir(databaseDirPath, { recursive: true });

//   const databaseSchemaPath = path.join(databaseDirPath, 'schema.json');
//   const usersBaseSchema: ISchema = {
//     tables: [
//       {
//         name: 'users',
//         columns: [
//           {
//             name: 'name',
//             type: DataType.VARCHAR,
//             length: 255,
//           },
//           {
//             name: 'currentDB',
//             type: DataType.VARCHAR,
//             length: 255,
//           },
//         ],
//       },
//     ],
//   };
//   await fs.writeFile(databaseSchemaPath, JSON.stringify(usersBaseSchema));
// }

// ! Done
// async createRootDir(): Promise<void> {
//   await fs.mkdir(this.databasesPath, { recursive: true });
// }

// ! Done
// ! After this you will push new tables
// async createDatabase(AST: CreateDatabaseAST) {
//   // To make sure there is a dir for databases
//   await this.createRootDir();
//   const isExist = await this.checkDatabaseExistence(AST.name);

//   if (isExist) {
//     throw new Error('Database is already exist');
//   }

//   // Create new directory for the new database
//   await this.createDirForDB(AST.name);

//   // Create new schema file for the new database
//   await this.createSchemaFile(AST.name);
// }

// ! Done
// async createDirForDB(databaseName: string): Promise<void> {
//   const databaseDirPath = path.join(this.databasesPath, `${databaseName}`);
//   await fs.mkdir(databaseDirPath, { recursive: true });
// }

// async checkTableExistenceIntoDatabase(database: string, table: string) {
//   const usersDataFile = await fs.readFile(this.dbUsersFilePath, 'utf-8');
//   const usersData = JSON.parse(usersDataFile);
//   // usersData.currentDB
//   console.log(usersData);
// }

// async readSpecificSchema(table: string) {
//   // read users
//   const usersDataFile = await fs.readFile(this.dbUsersFilePath, 'utf-8');
//   const { currentDB } = JSON.parse(usersDataFile);

//   const schemaFilePath = path.join(
//     this.databasesPath,
//     currentDB,
//     table,
//     'schema.json',
//   );
//   try {
//     await fs.access(schemaFilePath);
//     return true;
//     throw new Error('This table is not exist in the connected db');
//   } catch (error) {
//     return false;
//   }

//   const schemaDataFile = await fs.readFile(schemaFilePath, 'utf-8');
//   return JSON.parse(schemaDataFile);
// }

// async createTable(AST: CreateTableAST) {
//   console.log('from the storage');
//   console.log(AST);

//   // Check the table existence into the currentDB

//   // const usersDataPath = path.join(this.databasesPath,)
//   const postgresDir = path.join(this.databasesPath, 'postgres');
//   const usersDirPath = path.join(postgresDir, 'users');
//   const usersDataPath = path.join(usersDirPath, 'users.ndjson');

//   const usersDataFile = await fs.readFile(this.dbUsersFilePath, 'utf-8');
//   const usersData = JSON.parse(usersDataFile);

//   console.log(usersData);
//   // usersData.currentDB;
//   const tablePathIntoDB = path.join(this.databasesPath, usersData.currentDB);

//   await fs.access(tablePathIntoDB);

//   console.log('usersDataPath');
//   console.log(usersDataPath);

//   const parsedData: ISchema = await this.readSchema();
//   console.log(AST);

//   // const table = parsedData.tables.find((table) => {
//   //   return table.name === AST.structure_name;
//   // });

//   // console.log(parsedData);

//   // table?.columns.push(...AST.columns);

//   parsedData.tables.push({
//     name: AST.name,
//     columns: AST.columns,
//   });

//   // const updates: ISchema = {
//   //   tables: [
//   //     {
//   //       tableName: 'users',
//   //       PK: 'id',
//   //       columns: [
//   //         { columnName: 'id', type: 'SERIAL' },
//   //         { columnName: 'name', type: 'VARCHAR(50)' },
//   //         { columnName: 'email', type: 'VARCHAR(50)' },
//   //         { columnName: 'city', type: 'VARCHAR(50)' },
//   //       ],
//   //     },
//   //     {
//   //       tableName: 'products',
//   //       PK: 'id',
//   //       columns: [
//   //         { columnName: 'id', type: 'SERIAL' },
//   //         { columnName: 'title', type: 'VARCHAR(50)' },
//   //         { columnName: 'price', type: 'INTEGER' },
//   //       ],
//   //     },
//   //   ],
//   // };

//   // You will get the parsed data and then update it

//   // parsedData here is
//   // console.log({ oldData: parsedData });

//   // parsedData.tables.push(...updates.tables);
//   // // console.log({ newData: parsedData });

//   await this.createDataFile(AST.name);
//   // // Save the new data
//   await fs.writeFile(this.schemaFilePath, JSON.stringify(parsedData));

//   // // To create the seperated data files for each table

//   // parsedData.push(updates.tables);
// }

// ! Done

// async createSchemaFile(tableName: string): Promise<void> {
//   const dataFilePath = path.join(
//     this.databasesPath,
//     `${tableName}`,
//     `schema.json`,
//   );
//   const data = JSON.stringify({ tables: [] });
//   await fs.writeFile(dataFilePath, data);
// }

// ! Done
// async checkDatabaseExistence(databaseName: string) {
//   const databaseDirPath = path.join(this.databasesPath, `${databaseName}`);
//   try {
//     await fs.access(databaseDirPath);
//     return true;
//   } catch {
//     return false;
//   }
// }

// Creat ndjson file for each table data
// async createDataFileNdjson(tableName: string) {}

// async insertNdjson(databaseName: string, tableName: string) {
//   const data = { id: 1, name: 'ahmed' };
//   const jsonLine = JSON.stringify(data);

//   await fs.appendFile(
//     path.join(this.databasesPath, databaseName, `${tableName}.ndjson`),
//     jsonLine + `\n`,
//   );
// }

// async readDataFileNdjson(databaseName = 'postgres', tableName = 'users') {
//   const tableFilePath = path.join(
//     this.databasesPath,
//     databaseName,
//     `${tableName}.ndjson`,
//   );
//   const fileStream = createReadStream(tableFilePath);

//   // console.log(fileStream);
// }

// async readSchemaFileNdjson() {

// }
