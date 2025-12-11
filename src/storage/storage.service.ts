import { Injectable } from '@nestjs/common';
import * as fs from 'fs/promises';
import path from 'path';
import { Operators } from 'src/parser/parser.service';
import { TokenType } from 'src/tokenizer/tokenizer.service';

export interface ISchema {
  tables: ITable[];
}

export interface ITable {
  name: string;
  PK?: string;
  columns: IColumn[];
}

export interface IColumn {
  name: string;
  type: string;
}

@Injectable()
export class StorageService {
  private readonly dataDirPath = path.join(process.cwd(), 'dataa');
  private readonly databasesPath = path.join(process.cwd(), 'databases');
  private readonly schemaFilePath = path.join(this.dataDirPath, 'schema.json');

  async createDir(): Promise<void> {
    await fs.mkdir(this.dataDirPath, { recursive: true });
  }

  // You will send the schema of the database as an object
  async createSchema(): Promise<void> {
    await this.createDir();
    const data = JSON.stringify({ tables: [] });
    await fs.writeFile(this.schemaFilePath, data);
  }

  // get the actual schema file
  // async getSchemaFile() {}

  // get schema as a parsed data
  async readSchema(): Promise<any> {
    // check if the schema file is exist
    try {
      await fs.access(this.schemaFilePath);
    } catch (error) {
      await this.createSchema();
    }

    const data = await fs.readFile(this.schemaFilePath, 'utf-8');

    const parsedData = JSON.parse(data);
    return parsedData;
  }

  async createDataFile(tableName: string): Promise<void> {
    const data = [];
    const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);
    await fs.writeFile(dataFilePath, JSON.stringify(data));
  }

  async updateDataFile(tableName: string, updatedData): Promise<void> {
    const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);
    await fs.writeFile(dataFilePath, JSON.stringify(updatedData));
  }

  async createTable(AST) {
    console.log('from the storage');
    // console.log(AST);
    const parsedData: ISchema = await this.readSchema();
    console.log(AST);

    // const table = parsedData.tables.find((table) => {
    //   return table.name === AST.structure_name;
    // });

    // console.log(parsedData);

    // table?.columns.push(...AST.columns);

    parsedData.tables.push({
      name: AST.structure_name,
      columns: AST.columns,
    });

    // const updates: ISchema = {
    //   tables: [
    //     {
    //       tableName: 'users',
    //       PK: 'id',
    //       columns: [
    //         { columnName: 'id', type: 'SERIAL' },
    //         { columnName: 'name', type: 'VARCHAR(50)' },
    //         { columnName: 'email', type: 'VARCHAR(50)' },
    //         { columnName: 'city', type: 'VARCHAR(50)' },
    //       ],
    //     },
    //     {
    //       tableName: 'products',
    //       PK: 'id',
    //       columns: [
    //         { columnName: 'id', type: 'SERIAL' },
    //         { columnName: 'title', type: 'VARCHAR(50)' },
    //         { columnName: 'price', type: 'INTEGER' },
    //       ],
    //     },
    //   ],
    // };

    // You will get the parsed data and then update it

    // parsedData here is
    // console.log({ oldData: parsedData });

    // parsedData.tables.push(...updates.tables);
    // // console.log({ newData: parsedData });

    await this.createDataFile(AST.structure_name);
    // // Save the new data
    await fs.writeFile(this.schemaFilePath, JSON.stringify(parsedData));

    // // To create the seperated data files for each table

    // parsedData.push(updates.tables);
  }

  async readData(tableName: string) {
    const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);
    const data = await fs.readFile(dataFilePath, 'utf-8');
    const parsedData = JSON.parse(data);
    return parsedData;
  }

  async readTable(tableName: string) {
    const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);
    const data = await fs.readFile(dataFilePath, 'utf-8');
    const parsedData = JSON.parse(data);
    return parsedData;
  }

  async insertData(
    tableName: string = 'users',
    desiredColumns: string | string[],
    values: any | any[],
  ) {
    // You should make sure that the table is actually exists into your schema
    // How you can do that?
    const schema: ISchema = await this.readSchema();

    const isTableExist = schema.tables.findIndex((table) => {
      return table.name === tableName;
    });

    if (isTableExist === -1) {
      return {
        success: false,
        message: `ERROR: relation ${tableName} does not exist`,
      };
    }

    // ! Check existense of the column/s

    // If it exists => you should go to the data file to insert the new data
    // read the file
    // const dataFilePath = path.join(this.dataDir, `${tableName}.json`);
    // const data = await fs.readFile(dataFilePath, 'utf-8');
    // instead of reading all the time and parsing use the readFile (class method)
    const parsedData = await this.readData(tableName); // the table data

    const dataToInsert = {
      id: 11,
      name: 'Mohammed',
      email: 'mohammedsaleh@outlook.com',
      city: 'alexandria', // incorrect column name
    };
    // imported columns
    const importedColumns: string[] = Object.keys(dataToInsert);
    console.log(importedColumns);

    // You should evaluate the columns name against the schema.json
    // How you can do this?
    // You should load the file of the schema.json
    // Get into the table name
    // And compare the columns

    // ! you get the schema in the first lines
    const theTable = schema.tables.find((table) => table.name === tableName);
    // TODO: CHECK THE IMPORTED COLUMNS ARE ACTUALLY EXIST OR NOT
    const columns = theTable?.columns;

    const columnsNames = columns?.map((column) => {
      return column.name;
    });

    console.log(columnsNames);

    // * Now you have the correct column names
    // * Now you have to compare (importedColumns VS. columnsNames(el asas))
    const areColumnsCorrect = importedColumns.every((importedColumn) => {
      if (!columnsNames?.includes(importedColumn)) {
        return false;
      }

      return true;
    });

    if (!areColumnsCorrect) {
      return { success: false, message: 'error' };
    }

    parsedData.push(dataToInsert);
    const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);
    await fs.writeFile(dataFilePath, JSON.stringify(parsedData));

    // const parsedData = JSON.parse(data);
    // parsedData.push({});
  }

  // criteria could it be => id, email, name, price
  async deleteRowByEquality(
    tableName: string = 'users',
    criteria = 'id',
    value = 11,
  ) {
    // You will remove data based on what?
    // based on the id?
    // based on the email?
    // based on the (CRITERIA) => You can got it from the parameter

    const parsedData: [] = await this.readTable('users');
    const dataAfterDeletion = parsedData.filter((row) => {
      return row[criteria] !== value;
    });

    if (!dataAfterDeletion.length) {
      return { success: false, message: 'RECORD NOT FOUND' };
    }

    const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);
    await fs.writeFile(dataFilePath, JSON.stringify(dataAfterDeletion));
  }

  // to select
  // 1. select by compare with =
  // 2. select by compare with another equality operators
  // 3. you should get the data type of the column from the AST => so you should add a property called like data type
  // 4. data type into the columns like [{column}]

  async select(
    columns: string[],
    tableName: string,
    criterion?: string,
    operator?: string,
    value?: any,
  ) {
    // console.log(columns);
    // console.log({ tableName });

    // You will get column/s
    // you shouldn't check here for the existence of table and columns because you actually checked this into the semantic analyzer before

    // const schemaFile: ISchema = await this.readSchema();
    // console.log(schemaFile);

    // You should check the table existence before trying to reat it from the directory

    // For now work with find to develop better
    // then you should work with some(array method)

    // find the table with find, after this access the table directly adn then use some(array method) => for checking the column existing
    // const table = schemaFile.tables.find((table) => {
    //   return table.tableName === tableName;
    // });

    // const table = await this.checkTableExistence(tableName, schemaFile);

    /**
     * If all the columns requested just return the true
     * You don't need to check
     */
    // const column = table.columns.some((column) => {
    //   if (columnName === '*') return true;
    //   return column?.columnName === columnName;
    // });

    // this.checkColumnExistence(columnName, table);

    // you are looking for the id you should check if this column exists or not into the table

    // read the table file and select only the columns
    const tableData: [] = await this.readTable(tableName);
    // console.log(tableData);

    const projectedData = tableData.map((row) => {
      const obj = {};
      // console.log(row);

      for (const column of columns) {
        obj[column] = row[column];
      }

      return obj;
    });

    console.log(projectedData);

    let selectedData: {}[] = [];

    if (criterion) {
      switch (operator) {
        case Operators.EQUAL:
          selectedData = projectedData.filter((row) => {
            if (!isNaN(value)) {
              return row[`${criterion}`] === parseInt(value);
            }
            return row[`${criterion}`] === value;
          });

          console.log({ rows: selectedData });

          break;
        case Operators.GREATER_THAN_OR_EQUAL:
          selectedData = projectedData.filter((row) => {
            return row[`${criterion}`] >= parseInt(value);
          });
          break;
        case Operators.GREATER_THAN:
          selectedData = projectedData.filter((row) => {
            return row[`${criterion}`] > parseInt(value);
          });
          break;
        case Operators.LESS_THAN_OR_EQUAL:
          selectedData = projectedData.filter((row) => {
            return row[`${criterion}`] <= parseInt(value);
          });
          break;
        case Operators.LESS_THAN:
          selectedData = projectedData.filter((row) => {
            return row[`${criterion}`] < parseInt(value);
          });
          break;

        default:
          selectedData = projectedData;
          break;
      }
    }

    return selectedData;

    // console.log(tableData);
    // you will return an object / array of objects with the specified properties

    // if (columnName === '*') {
    //   return tableData;
    // }

    // let selectedObjs: {}[] = [];
    // tableData.map((row) => {
    //   selectedObjs.push({ [columnName]: row[columnName] });
    // });
    // console.log(selectedObjs);

    // return selectedObjs;
    // you should read the schema file first
    // check the table existence
    // checkk the column/s existence
  }

  async delete(AST) {
    //  AST.columns,
    const { table, where } = AST;
    const { criterion, operator, value } = where;
    // AST.table,
    // AST.where?.criterion,
    // AST.where?.operator,
    // AST.where?.value,

    const tableData: [] = await this.readTable(table);
    console.log(tableData);
    console.log(where);

    let dataAfterDeletion;

    if (criterion) {
      switch (operator) {
        case Operators.EQUAL:
          dataAfterDeletion = tableData.filter((row) => {
            if (!isNaN(value)) {
              return row[`${criterion}`] !== parseInt(value);
            }
            return row[`${criterion}`] !== value;
          });

          // console.log({ dataAfterDeletion });

          break;
        case Operators.GREATER_THAN_OR_EQUAL:
          dataAfterDeletion = tableData.filter((row) => {
            return row[`${criterion}`] < parseInt(value);
          });

          console.log({ dataAfterDeletion });
          break;
        case Operators.GREATER_THAN:
          dataAfterDeletion = tableData.filter((row) => {
            return row[`${criterion}`] <= parseInt(value);
          });

          console.log({ dataAfterDeletion });
          break;
        case Operators.LESS_THAN_OR_EQUAL:
          dataAfterDeletion = tableData.filter((row) => {
            return row[`${criterion}`] > parseInt(value);
          });

          console.log({ dataAfterDeletion });
          break;
        case Operators.LESS_THAN:
          dataAfterDeletion = tableData.filter((row) => {
            return row[`${criterion}`] >= parseInt(value);
          });

          console.log({ dataAfterDeletion });
          break;

        default:
          break;
      }
    }

    const dataFilePath = path.join(this.dataDirPath, `${table}.json`);
    await fs.writeFile(dataFilePath, JSON.stringify(dataAfterDeletion));

    return dataAfterDeletion;
  }

  // If you are going to delete rows there is no criterion
  async deleteRows(tableName: string) {
    const schemaFile: ISchema = await this.readSchema();

    const table = this.checkTableExistence(tableName, schemaFile);
    const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);

    await fs.writeFile(dataFilePath, JSON.stringify([]));
  }

  async checkTableExistence(tableName: string, schemaFile) {
    const table = schemaFile.tables.find((table) => {
      return table.tableName === tableName;
    });

    if (!table) {
      throw new Error(`Table ${tableName} is not exist`);
    }

    return table;
  }

  checkColumnExistence(columnName: string, table) {
    const column = table.columns.find((column) => {
      if (columnName === '*') return true;
      return column?.columnName === columnName;
    });

    if (!column) {
      throw new Error(`Column ${columnName} is not exist`);
    }
  }

  checkColumnsExistence(columns: string[], table) {
    for (const column of columns) {
      this.checkColumnExistence(column, table);
    }
  }

  async updateRow(
    tableName: string = 'users',
    targetColumnName: string = 'name',
    updatingValue: string = 'Mariem',
    conditionColumnName: string = 'id',
    conditionValue = 12,
  ) {
    const schemaFile: ISchema = await this.readSchema();
    const dataFilePath = path.join(this.dataDirPath, `${tableName}.json`);
    // table existence
    // column existence which will be updated
    // column existence which is used as a criterion

    // check the table existence and get the table
    const table = await this.checkTableExistence(tableName, schemaFile);

    const targetColumn = await this.checkColumnExistence(
      targetColumnName,
      table,
    );

    const conditionColumn = await this.checkColumnExistence(
      conditionColumnName,
      table,
    );

    // after checking you will read the file
    const data: any[] = await this.readData(tableName); // You got the data file

    const desiredRowIndex = data.findIndex((row) => {
      return row[`${conditionColumnName}`] === conditionValue;
    });

    data[desiredRowIndex][`${targetColumnName}`] = updatingValue;

    await fs.writeFile(dataFilePath, JSON.stringify(data));

    // ! You should get the proper column/s to update
  }

  async update(AST) {
    console.log('update from the storage service');
    console.log(AST);

    const tableData: [] = await this.readData(AST.tableName);

    console.log(tableData);

    const updatedRowIndex = tableData.findIndex((row) => {
      return row[`${AST.conditionColumn}`] === parseInt(AST.conditionValue);

      // obj = row[`${AST.updatingColumn}`] === parseInt(AST.updatingValue);
    });

    console.log(updatedRowIndex);

    tableData[updatedRowIndex][AST.updatingColumn] = parseInt(
      AST.updatingValue,
    ) as never;

    await this.updateDataFile(AST.tableName, tableData);

    return 'UPDATE1';

    // console.log(updatedData);
  }

  // Will receive AST, but not yet
  async createDatabase(AST) {
    // data/
    //  |___ store-db
    //       |_______ schema.json
    //       |_______ users.json
    //       |_______ products.json
    //  |___ school-db
    //       |_______ schema.json
    //       |_______ students.json
    //       |_______ teachers.json
    // this.databasesPath

    await this.createRootDir();
    const isExist = await this.checkDatabaseExistence(AST.structure_name);

    if (isExist) {
      throw new Error('Database is already exist');
    }

    // Create new directory for the new database
    await this.createDirForDB(AST.structure_name);

    // Create new schema file for the new database
    await this.createNewSchema(AST.structure_name);
  }

  async createRootDir(): Promise<void> {
    await fs.mkdir(this.databasesPath, { recursive: true });
  }

  async createDirForDB(databaseName: string): Promise<void> {
    const databaseDirPath = path.join(this.databasesPath, `${databaseName}`);

    await fs.mkdir(databaseDirPath, { recursive: true });
  }

  async createNewSchema(tableName: string): Promise<void> {
    const dataFilePath = path.join(
      this.databasesPath,
      `${tableName}`,
      `schema.json`,
    );
    const data = JSON.stringify({ tables: [] });
    await fs.writeFile(dataFilePath, data);
  }

  async checkDatabaseExistence(databaseName: string) {
    const dirPath = path.join(this.databasesPath, `${databaseName}`);

    try {
      await fs.access(dirPath);
      return true;
    } catch {
      return false;
    }
  }
}
