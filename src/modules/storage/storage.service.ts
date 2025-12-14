import { Injectable } from '@nestjs/common';
import { createReadStream, readFile } from 'fs';
import * as fs from 'fs/promises';
import path from 'path';
import { Operator } from '../../common/enums/operator.enum';
import { DataType } from '../../common/enums/data-type.enum';
import type {
  CreateDatabaseAST,
  CreateTableAST,
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

  async select(AST: SelectAST) {
    const tableData = await this.readTableData(AST.table);
    const columns = AST.columns;
    const where = AST?.where;

    const { criterion, operator, value } = where || {};

    let projectedData: {}[] = [];

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

    let selectedData: {}[] = projectedData;

    if (criterion) {
      switch (operator) {
        case Operator.EQUAL:
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

  async insert(AST: InsertAST) {
    const data = await this.readTableData(AST.table);

    console.log(AST.columns);
    let i = 0;

    if (AST.columns.length !== AST.values.length) {
      throw new Error('INSERT has more target columns more than the values');
    }

    const indexesPaths: string[] = [];

    for (const column of AST.columns) {
      const isIndexed = await this.isIndexed(AST.table, column);

      if (isIndexed === true) {
        const indexFilePath = await this.getIndexFilePath(AST.table, column);
        indexesPaths.push(indexFilePath); // add the paths here and append on them the new values
        // read the index file and look if the updated value exist or not
        // ! if exists throw an error

        const fileStream = createReadStream(indexFilePath, {
          encoding: 'utf-8',
        });

        const lines = readline.createInterface({ input: fileStream });

        for await (const line of lines) {
          const row = JSON.parse(line);

          if (row.value === AST.values[i]) {
            throw new Error(
              'This value is already used and can not be used twice',
            );
          }
        }

        i++;
      }
    }

    const currentDB = await this.getCurrentDatabase();

    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      AST.table,
      `${AST.table}.ndjson`,
    );

    const insertionData = {};

    for (let i = 0; i < AST.columns.length; i++) {
      insertionData[AST.columns[i]] = AST.values[i];
    }

    const insertedLine = JSON.stringify(insertionData) + '\n';

    try {
      await fs.appendFile(dataFilePath, insertedLine, 'utf-8');
      if (indexesPaths.length !== 0) {
        for (let i = 0; i < indexesPaths.length; i++) {
          const value = AST.values[i];
          const { size: offset } = await fs.stat(dataFilePath);
          const length = Buffer.byteLength(insertedLine);
          const indexObj: Index = { value, offset, length };

          const insertedIndexLine = indexObj;

          await fs.appendFile(
            indexesPaths[i],
            JSON.stringify(insertedIndexLine) + '\n',
            'utf-8',
          );
        }
      }
    } catch (error) {
      throw new Error(`Insertion failed ${AST.table}: ${error.message}`);
    }
  }

  async update(AST: UpdateAST) {
    console.log('update');
    console.log(AST);

    // chech the indexes
    // get the column name
    // check the index file existence
    // if exists you should lookup for the new value if it exist or not
    const [columnName, newValue] = Object.entries(AST.updates)[0]; // one column

    const indexFilePath = path.join(
      this.databasesPath,
      AST.table,
      columnName,
      `${columnName}_idx.ndjson`,
    );

    const isIndexed = await this.isIndexed(AST.table, columnName);

    // Check the updating value if it exists or not
    if (isIndexed === true) {
      // read the index file (stream)
      // check the line which includes the value
      // how index looks like?!
      // {value:....,offset:....}
    }

    console.log(columnName);

    // updates
    // get the table data
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

  async isIndexed(tableName: string, columnName: string) {
    const indexFilePath = await this.getIndexFilePath(tableName, columnName);
    console.log(indexFilePath);

    try {
      await fs.access(indexFilePath);
      return true;
    } catch (error) {
      return false;
    }
  }

  async readIndexFile(tableName: string, columnName: string) {
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

  // ==================== HELPER METHODS ====================

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
