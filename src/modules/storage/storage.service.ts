import { Injectable } from '@nestjs/common';
import { createReadStream, read, readFile } from 'fs';
import * as fs from 'fs/promises';
import path from 'path';
import { Operator } from '../../common/enums/operator.enum';
import { DataType } from '../../common/enums/data-type.enum';
import type {
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
  IConnection,
} from '../../common/types/schema.types';
import * as readline from 'readline/promises';

@Injectable()
export class StorageService {
  // databases directory
  private readonly databasesPath = path.join(process.cwd(), 'databases');

  // path for the database users file
  // includes the username and the current db
  private readonly dbUsersFilePath = path.join(
    this.databasesPath,
    'postgres',
    'users',
    'users.ndjson',
  );

  // create databases root directory
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

  // TODO:
  async checkDatabaseExistence(databaseName: string): Promise<boolean> {
    const databaseDirPath = path.join(this.databasesPath, databaseName);
    try {
      await fs.access(databaseDirPath);
      return true;
    } catch {
      return false;
    }
  }

  async createDatabase(AST: CreateDatabaseAST): Promise<any> {
    await this.createDatabasesDir(); // To ignore errors
    const isExist = await this.checkDatabaseExistence(AST.name);
    if (isExist) {
      throw new Error(`Database ${AST.name} already exists`);
    }

    await this.createDatabaseDir(AST.name);

    return { message: `Database ${AST.name} created successfully` };
  }

  // the name of connected database
  async getCurrentDatabase(): Promise<string> {
    const usersDataFile = await fs.readFile(this.dbUsersFilePath, 'utf-8');
    const { currentDB } = JSON.parse(usersDataFile);
    return currentDB;
  }

  // connect to another database by change the value of the current db
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

  // get the connected db schema
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

  // check if the table exists into the connected db or not
  async checkTableExistsInCurrentDB(tableName: string): Promise<boolean> {
    const schema = await this.readCurrentDBSchema();
    return schema.tables.some((table) => table.name === tableName);
  }

  // get the table data (NAME,COLUMNS,PK)
  async getTableFromCurrentDB(tableName: string): Promise<ITable> {
    const schema = await this.readCurrentDBSchema();
    const table: ITable = schema.tables.find(
      (table) => table.name === tableName,
    )!;

    if (!table) {
      throw new Error(`Table ${tableName} not exist in current database`);
    }

    return table;
  }

  // create data file
  // add the table to the current db schema
  async createTable(AST: CreateTableAST): Promise<void> {
    // get the current db name
    const currentDB = await this.getCurrentDatabase();

    // get the db schema to modify it and add the new table
    const schema = await this.readCurrentDBSchema();

    // add the new table to the schema
    schema.tables.push({
      name: AST.name,
      columns: AST.columns,
    });

    // Create table directory => databases path(root), current db name, table name
    const tableDirPath = path.join(this.databasesPath, currentDB, AST.name);
    await fs.mkdir(tableDirPath, { recursive: true }); // create the directory

    // Create index files for PK and UNIQUE columns
    for (const column of AST.columns) {
      // if column.primaryKey === true
      // if column.unique === true
      // you should create index files
      if (column.primaryKey || column.unique) {
        await this.createIndexFile(AST.name, column.name);
      }
    }

    // Create metadata file to track the auto incremented column values
    const autoIncrementedColumn = AST.columns.find((col) => {
      return col.autoIncrement === true;
    });

    if (autoIncrementedColumn) {
      // create meta file for the table
      const metaFilePath = path.join(tableDirPath, `${AST.name}_meta.json`);

      // append the column name and 0 as an initial value for the counter
      const metadata = { [`${autoIncrementedColumn.name}`]: 0 };
      await fs.writeFile(metaFilePath, JSON.stringify(metadata), 'utf-8');
    }

    // update the schema to add the table
    await this.updateCurrentDBSchema(schema);

    // Create NDJSON data file for the table data
    const dataFilePath = path.join(tableDirPath, `${AST.name}.ndjson`);

    await fs.writeFile(dataFilePath, '');
  }

  // select
  async select(AST: SelectAST) {
    const schema: ISchema = await this.readCurrentDBSchema();
    const currentDB = await this.getCurrentDatabase();

    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      AST.table,
      `${AST.table}.ndjson`,
    );

    // Find indexed columns and primary key
    let pkName: string = '';
    const indexedColumns: string[] = [];

    for (const table of schema.tables) {
      if (table.name === AST.table) {
        for (const col of table.columns) {
          // GATHER INDEXED COLUMNS TO USE THEM TO OPTIMIZE LOOKUP PROCESS
          if (col.primaryKey || col.unique) {
            indexedColumns.push(col.name);
          }
          // FOR FULL SCAN
          if (col.primaryKey) {
            pkName = col.name;
          }
        }
        break;
      }
    }

    // THERE IS NO TABLE W/O PK
    if (!pkName) {
      throw new Error(`No primary key found for table ${AST.table}`);
    }

    let allRows: Record<string, any>[] = [];

    // If WHERE uses an indexed column with EQUAL operator => use index directly
    if (
      AST.where &&
      AST.where.operator === Operator.EQUAL &&
      indexedColumns.includes(AST.where.criterion)
    ) {
      // Use the index file directly
      const indexPath = await this.getIndexFilePath(
        AST.table,
        AST.where.criterion,
      );
      const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
      const lines = readline.createInterface({ input: fileStream });

      for await (const line of lines) {
        const { value: indexValue, offset, length } = JSON.parse(line);

        // Check if this is the value which i am looking for
        // Read this specific row
        // Once i get the value from the index file i should stream the data file and read the row which i am looking for by the offset and length
        if (indexValue == AST.where.value) {
          // Allocate a buffer with the size of the specific row
          const buffer = Buffer.alloc(length);

          // open the file descriptior, 'r' => mode => read-only
          const fileHandle = await fs.open(dataFilePath, 'r');

          // fill the buffer
          // offset the starting point of the data file
          // and length how many bytes to read
          // 0 => where i will start to fill the allocated buffer
          await fileHandle.read(buffer, 0, length, offset);

          // close the descriptor
          await fileHandle.close();

          // read it as a string
          const rowLine = buffer.toString('utf-8').trim();

          // parse it
          const row = JSON.parse(rowLine);

          if (!row.deleted) {
            // desruct the data without deleted key
            // the row can have
            const { deleted, ...data } = row;
            allRows.push(data);
          }

          // break the loop after found the specific row
          break;
        }
      }
    } // WHERE uses any column not indexed
    else if (AST.where?.criterion) {
      // read the pk file index
      const pkIndexFilePath = await this.getIndexFilePath(AST.table, pkName);
      const fileStream = createReadStream(pkIndexFilePath, {
        encoding: 'utf-8',
      });
      const lines = readline.createInterface({ input: fileStream });

      // Get all offsets from PK index
      const indexes: { offset: number; length: number }[] = [];
      for await (const line of lines) {
        const { offset, length } = JSON.parse(line);
        indexes.push({ offset, length });
      }

      // Stream data file and check each row
      for (const { offset, length } of indexes) {
        const buffer = Buffer.alloc(length);
        const fileHandle = await fs.open(dataFilePath, 'r');
        await fileHandle.read(buffer, 0, length, offset);
        await fileHandle.close();

        const rowLine = buffer.toString('utf-8').trim();
        const row = JSON.parse(rowLine);

        const { criterion, operator, value } = AST.where;
        const rowValue = row[criterion]; // get the column value on the row
        let isMatch = false;

        switch (operator) {
          case Operator.EQUAL:
            isMatch = rowValue == value;
            break;
          case Operator.GREATER_THAN:
            isMatch = Number(rowValue) > Number(value);
            break;
          case Operator.GREATER_THAN_OR_EQUAL:
            isMatch = Number(rowValue) >= Number(value);
            break;
          case Operator.LESS_THAN:
            isMatch = Number(rowValue) < Number(value);
            break;
          case Operator.LESS_THAN_OR_EQUAL:
            isMatch = Number(rowValue) <= Number(value);
            break;
          case Operator.LIKE:
            const rowStr = String(rowValue).toLowerCase();
            const searchStr = String(value).toLowerCase();
            isMatch = rowStr.includes(searchStr);
            break;
        }

        if (isMatch) {
          const { deleted, ...data } = row;
          allRows.push(data);
        }
      }
    }
    // Full table scan (without criterion)
    else {
      const pkIndexFilePath = await this.getIndexFilePath(AST.table, pkName);
      const fileStream = createReadStream(pkIndexFilePath, {
        encoding: 'utf-8',
      });
      const lines = readline.createInterface({ input: fileStream });

      // Get all offsets
      const indexes: { offset: number; length: number }[] = [];
      for await (const line of lines) {
        const { offset, length } = JSON.parse(line);
        indexes.push({ offset, length });
      }

      // Read all rows
      for (const { offset, length } of indexes) {
        const buffer = Buffer.alloc(length);
        const fileHandle = await fs.open(dataFilePath, 'r');
        await fileHandle.read(buffer, 0, length, offset);
        await fileHandle.close();

        const rowLine = buffer.toString('utf-8').trim();
        const row = JSON.parse(rowLine);

        const { deleted, ...data } = row;
        allRows.push(data);
      }
    }

    // WHERE
    let filteredRows = allRows;

    // FILTER ROWS: Apply WHERE clause for scenarios that haven't been filtered yet
    // Skip if already filtered by indexed EQUAL (Scenario 1)
    if (
      AST.where &&
      !(
        AST.where.operator === Operator.EQUAL &&
        indexedColumns.includes(AST.where.criterion)
      )
    ) {
      const { criterion, operator, value } = AST.where;

      // Apply the comparison operator to filter rows

      switch (operator) {
        case Operator.EQUAL:
          // rows where column value equals the search value
          filteredRows = allRows.filter((row) => {
            return row[criterion] == value;
          });
          break;

        case Operator.GREATER_THAN:
          // rows where column value is greater than search value

          filteredRows = allRows.filter((row) => {
            return Number(row[criterion]) > Number(value);
          });
          break;
        case Operator.GREATER_THAN_OR_EQUAL:
          // rows where column value is greater than or equal to search value

          filteredRows = allRows.filter((row) => {
            return Number(row[criterion]) >= Number(value);
          });
          break;
        case Operator.LESS_THAN:
          // rows where column value is less than search value

          filteredRows = allRows.filter((row) => {
            return Number(row[criterion]) < Number(value);
          });
          break;
        case Operator.LESS_THAN_OR_EQUAL:
          // rows where column value is less than or equal to search value

          filteredRows = allRows.filter((row) => {
            return Number(row[criterion]) <= Number(value);
          });
          break;
        case Operator.LIKE:
          // rows where column value contains the search value => string

          filteredRows = allRows.filter((row) => {
            const rowValue = String(row[criterion]).toLowerCase();
            const searchValue = String(value).toLowerCase();
            return rowValue.includes(searchValue);
          });
          break;
      }
    }

    // Select only the requested columns
    let projectedData: Record<string, any>[] = [];

    if (AST.columns.includes('*')) {
      // SELECT * => return all columns
      projectedData = filteredRows;
    } else {
      // Only include requested columns
      projectedData = filteredRows.map((row) => {
        // the projected row with the requested columns
        const projectedRow: Record<string, any> = {};

        // Inject requested columns
        for (const column of AST.columns) {
          projectedRow[column] = row[column];
        }

        return projectedRow;
      });
    }

    return { data: projectedData };
  }

  // insert
  async insert(AST: InsertAST) {
    const currentDB = await this.getCurrentDatabase();
    const schema = await this.readCurrentDBSchema();

    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      AST.table,
      `${AST.table}.ndjson`,
    );

    // Get table schema
    let tableSchema: any;
    for (const table of schema.tables) {
      if (table.name === AST.table) {
        tableSchema = table;
        break;
      }
    }

    // rows array will include many arrays each array will include the values for each column on the order
    // the number of small arrays should match the number of columns
    let rows: any[][] = [];
    let currentRow: any[] = [];
    const columnCount = AST.columns.length; // number of columns

    // iterate on all the values and split them into small arrays to match each column
    for (let i = 0; i < AST.values.length; i++) {
      currentRow.push(AST.values[i]);

      // When we have enough values for one row, save it and start new row
      if (currentRow.length === columnCount) {
        rows.push(currentRow);

        // empty the current row
        currentRow = [];
      }
    }

    // check if there is small array have values more than the columns numbers
    // !
    for (const rowValues of rows) {
      if (AST.columns.length !== rowValues.length) {
        // psql error
        throw new Error('INSERT has more target columns than expressions');
      }

      // row object with provided values + marked as not deleted
      const row: Record<string, any> = { deleted: false };
      for (let i = 0; i < AST.columns.length; i++) {
        row[AST.columns[i]] = rowValues[i];
      }

      // prevent duplicates in indexed columns
      // check before trying to get the next incremented value to prevent incrementing even if an error occured (if the updating value already exist)
      for (const col of tableSchema.columns) {
        if (col.primaryKey || col.unique) {
          // updating value
          const value = row[col.name];

          const indexPath = await this.getIndexFilePath(AST.table, col.name);
          const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
          const lines = readline.createInterface({ input: fileStream });

          for await (const line of lines) {
            const { value: existingValue } = JSON.parse(line);
            if (existingValue == value) {
              throw new Error(`${value} already exists in column ${col.name}`);
            }
          }
        }
      }

      // go through the columns on the schema to handle auto incremented values
      for (const col of tableSchema.columns) {
        // check if the column has attribute autoIncrement === true
        if (col.autoIncrement) {
          const nextId = await this.getNextAutoIncrementValue(
            AST.table,
            col.name,
          );
          row[col.name] = nextId; // assign the a new key and value with the incremented column name and the next id
        }
      }

      // TIMESTAMP columns
      for (const col of tableSchema.columns) {
        if (col.type === 'TIMESTAMP') {
          // check if the user trying to insert into this column
          if (AST.columns.includes(col.name)) {
            // if includes you should get the value from the row object which you created before and get the value and convert it
            // '2026-01-01'
            const providedValue = row[col.name];
            const date = new Date(providedValue);

            // getTime => Returns the stored time value in milliseconds since midnight, January 1, 1970 UTC.
            // 'hello' => NaN
            if (isNaN(date.getTime())) {
              throw new Error(
                `the input should be a timestamp ${providedValue}`,
              );
            }

            // convert to ISO format
            row[col.name] = date.toISOString();
          } else {
            // user didn't provide the date
            // the engine will consider it as => NOW()
            row[col.name] = new Date().toISOString();
          }
        }
      }

      // insert the row into data file
      const insertedLine = JSON.stringify(row) + '\n'; // because you are going to append into a ndjson file
      const { size: offset } = await fs.stat(dataFilePath); // the start point that i will insert from (offset)
      await fs.appendFile(dataFilePath, insertedLine, 'utf-8');

      // update all index files
      const length = Buffer.byteLength(insertedLine);

      for (const col of tableSchema.columns) {
        if (col.primaryKey || col.unique) {
          const indexPath = await this.getIndexFilePath(AST.table, col.name);
          const indexEntry = { value: row[col.name], offset, length };
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
    const { table, updates, where } = AST;
    const { criterion, operator, value } = where!;

    // get current db name
    const currentDB = await this.getCurrentDatabase();
    const schema = await this.readCurrentDBSchema();

    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      table,
      `${table}.ndjson`,
    );

    // Find all indexed columns
    const indexedColumns: string[] = [];

    // Iterate on all the tables to get the specifc table that i am going to update into
    // and then look for the columns which have indexes
    for (const tbl of schema.tables) {
      if (tbl.name === table) {
        for (const col of tbl.columns) {
          if (col.primaryKey || col.unique) {
            indexedColumns.push(col.name);
          }
        }
        break;
      }
    }

    // check if the column that you are going to update values on has index or not if it has an index you should look for the new value if it used before or not
    for (const [columnName, newValue] of Object.entries(updates)) {
      // check if the column that you are going to update indexed or now
      if (indexedColumns.includes(columnName)) {
        // if indexed => check if the new value already exists
        const indexPath = await this.getIndexFilePath(table, columnName);
        const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
        const lines = readline.createInterface({ input: fileStream });

        for await (const line of lines) {
          const { value: existingValue } = JSON.parse(line);
          if (existingValue == newValue) {
            throw new Error(
              `Value: ${newValue} already exists in column ${columnName}, you are not able to use it twice`,
            );
          }
        }
      }
    }

    // Find the row to update
    const isCriterionIndexed = await this.isIndexed(table, criterion);
    let rowToUpdate: {
      row: Record<string, any>;
      offset: number;
      length: number;
    } | null = null;

    if (isCriterionIndexed) {
      // FAST PATH: Use criterion's index
      const indexPath = await this.getIndexFilePath(table, criterion);
      const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
      const lines = readline.createInterface({ input: fileStream });

      for await (const line of lines) {
        const { value: indexValue, offset, length } = JSON.parse(line);

        let isMatch = false;
        switch (operator) {
          case Operator.EQUAL:
            isMatch = indexValue == value;
            break;
          case Operator.GREATER_THAN:
            isMatch = Number(indexValue) > Number(value);
            break;
          case Operator.GREATER_THAN_OR_EQUAL:
            isMatch = Number(indexValue) >= Number(value);
            break;
          case Operator.LESS_THAN:
            isMatch = Number(indexValue) < Number(value);
            break;
          case Operator.LESS_THAN_OR_EQUAL:
            isMatch = Number(indexValue) <= Number(value);
            break;
          case Operator.LIKE:
            const indexStr = String(indexValue).toLowerCase();
            const searchStr = String(value).toLowerCase();
            isMatch = indexStr.includes(searchStr);
            break;
        }

        if (isMatch) {
          // Read the row
          const buffer = Buffer.alloc(length);
          const fileHandle = await fs.open(dataFilePath, 'r');
          await fileHandle.read(buffer, 0, length, offset);
          await fileHandle.close();

          const rowLine = buffer.toString('utf-8').trim();
          const row = JSON.parse(rowLine);

          rowToUpdate = { row, offset, length };
          break;
        }
      }
    } else {
      // SLOW PATH: Scan using primary key index
      let pkName = '';
      for (const tbl of schema.tables) {
        if (tbl.name === table) {
          for (const col of tbl.columns) {
            if (col.primaryKey) {
              pkName = col.name;
              break;
            }
          }
          break;
        }
      }

      if (!pkName) {
        throw new Error(`No primary for table ${table}`);
      }

      const pkIndexPath = await this.getIndexFilePath(table, pkName);
      const fileStream = createReadStream(pkIndexPath, { encoding: 'utf-8' });
      const lines = readline.createInterface({ input: fileStream });

      for await (const line of lines) {
        const { offset, length } = JSON.parse(line);

        // Read the row
        const buffer = Buffer.alloc(length);
        const fileHandle = await fs.open(dataFilePath, 'r');
        await fileHandle.read(buffer, 0, length, offset);
        await fileHandle.close();

        const rowLine = buffer.toString('utf-8').trim();
        const row = JSON.parse(rowLine);

        // Check WHERE condition
        const rowValue = row[criterion];
        let isMatch = false;

        switch (operator) {
          case Operator.EQUAL:
            isMatch = rowValue == value;
            break;
          case Operator.GREATER_THAN:
            isMatch = Number(rowValue) > Number(value);
            break;
          case Operator.GREATER_THAN_OR_EQUAL:
            isMatch = Number(rowValue) >= Number(value);
            break;
          case Operator.LESS_THAN:
            isMatch = Number(rowValue) < Number(value);
            break;
          case Operator.LESS_THAN_OR_EQUAL:
            isMatch = Number(rowValue) <= Number(value);
            break;
          case Operator.LIKE:
            const rowStr = String(rowValue).toLowerCase();
            const searchStr = String(value).toLowerCase();
            isMatch = rowStr.includes(searchStr);
            break;
        }

        if (isMatch) {
          rowToUpdate = { row, offset, length };
          break;
        }
      }
    }

    if (!rowToUpdate) {
      throw new Error('No row found matching the condition');
    }

    const deletedRow = { ...rowToUpdate.row, deleted: true };
    await fs.appendFile(
      dataFilePath,
      JSON.stringify(deletedRow) + '\n',
      'utf-8',
    );

    // Append new updated row
    const updatedRow = { ...rowToUpdate.row, ...updates, deleted: false };
    const updatedLine = JSON.stringify(updatedRow) + '\n';

    await fs.appendFile(dataFilePath, updatedLine, 'utf-8');

    // Rebuild ALL index files after update
    for (const colName of indexedColumns) {
      const indexPath = await this.getIndexFilePath(table, colName);

      // Read entire data file line by line
      const fileStream = createReadStream(dataFilePath, { encoding: 'utf-8' });
      const lines = readline.createInterface({ input: fileStream });

      // store all rows with their positions
      const allRows: any[] = [];
      let position = 0;

      for await (const line of lines) {
        const lineLength = Buffer.byteLength(line + '\n');
        const row = JSON.parse(line);

        allRows.push({
          row: row,
          offset: position,
          length: lineLength,
        });

        position = position + lineLength;
      }

      // Find latest version of each unique value
      const latestVersions: any[] = [];

      for (let i = 0; i < allRows.length; i++) {
        const currentRow = allRows[i];
        const currentValue = currentRow.row[colName];
        let isLatest = true;

        // Check if there's a newer version of this value
        for (let j = i + 1; j < allRows.length; j++) {
          const laterRow = allRows[j];
          const laterValue = laterRow.row[colName];

          if (currentValue === laterValue) {
            // Found a newer version, current is not latest
            isLatest = false;
            break;
          }
        }

        // If this is the latest version, keep it
        if (isLatest) {
          latestVersions.push(currentRow);
        }
      }

      // Build new index (no need to check deleted - latest version is the truth)
      const newIndexLines: any[] = [];

      for (let i = 0; i < latestVersions.length; i++) {
        const item = latestVersions[i];

        // Only add if not deleted
        if (!item.row.deleted) {
          const indexLine = {
            value: item.row[colName],
            offset: item.offset,
            length: item.length,
          };
          newIndexLines.push(JSON.stringify(indexLine));
        }
      }

      // Write new index file
      let indexContent = '';
      for (let i = 0; i < newIndexLines.length; i++) {
        indexContent = indexContent + newIndexLines[i] + '\n';
      }

      await fs.writeFile(indexPath, indexContent, 'utf-8');
    }

    return {
      success: true,
      message: 'Row updated successfully',
    };
  }

  async delete(AST: DeleteAST) {
    const { table, where } = AST;
    const currentDB = await this.getCurrentDatabase();
    const schema = await this.readCurrentDBSchema();

    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      table,
      `${table}.ndjson`,
    );

    let rowsToDelete: Record<string, any>[] = [];

    // Handle DELETE W/O WHERE => (delete all rows)
    if (!where) {
      // Get primary key to read all rows
      let pkName = '';
      for (const tbl of schema.tables) {
        if (tbl.name === table) {
          for (const col of tbl.columns) {
            if (col.primaryKey) {
              pkName = col.name;
              break;
            }
          }
          break;
        }
      }

      const pkIndexPath = await this.getIndexFilePath(table, pkName);
      const fileStream = createReadStream(pkIndexPath, { encoding: 'utf-8' });
      const lines = readline.createInterface({ input: fileStream });

      for await (const line of lines) {
        const { offset, length } = JSON.parse(line);

        const buffer = Buffer.alloc(length);
        const fileHandle = await fs.open(dataFilePath, 'r');
        await fileHandle.read(buffer, 0, length, offset);
        await fileHandle.close();

        const rowLine = buffer.toString('utf-8').trim();
        const row = JSON.parse(rowLine);

        rowsToDelete.push(row);
      }
    } else {
      // DELETE with WHERE clause
      const { criterion, operator, value } = where;
      const isIndexed = await this.isIndexed(table, criterion);

      if (isIndexed) {
        // use index file
        const indexPath = await this.getIndexFilePath(table, criterion);
        const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
        const lines = readline.createInterface({ input: fileStream });

        for await (const line of lines) {
          const { value: indexValue, offset, length } = JSON.parse(line);

          let isMatch = false;

          switch (operator) {
            case Operator.EQUAL:
              isMatch = indexValue == value;
              break;
            case Operator.GREATER_THAN:
              isMatch = Number(indexValue) > Number(value);
              break;
            case Operator.GREATER_THAN_OR_EQUAL:
              isMatch = Number(indexValue) >= Number(value);
              break;
            case Operator.LESS_THAN:
              isMatch = Number(indexValue) < Number(value);
              break;
            case Operator.LESS_THAN_OR_EQUAL:
              isMatch = Number(indexValue) <= Number(value);
              break;
            case Operator.LIKE:
              const indexStr = String(indexValue).toLowerCase();
              const searchStr = String(value).toLowerCase();
              isMatch = indexStr.includes(searchStr);
              break;
          }

          if (isMatch) {
            const buffer = Buffer.alloc(length);
            const fileHandle = await fs.open(dataFilePath, 'r');
            await fileHandle.read(buffer, 0, length, offset);
            await fileHandle.close();

            const rowLine = buffer.toString('utf-8').trim();
            const row = JSON.parse(rowLine);

            rowsToDelete.push(row);
          }
        }
      } else {
        let pkName = '';
        for (const tbl of schema.tables) {
          if (tbl.name === table) {
            for (const col of tbl.columns) {
              if (col.primaryKey) {
                pkName = col.name;
                break;
              }
            }
            break;
          }
        }

        const pkIndexPath = await this.getIndexFilePath(table, pkName);
        const fileStream = createReadStream(pkIndexPath, { encoding: 'utf-8' });
        const lines = readline.createInterface({ input: fileStream });

        for await (const line of lines) {
          const { offset, length } = JSON.parse(line);

          const buffer = Buffer.alloc(length);
          const fileHandle = await fs.open(dataFilePath, 'r');
          await fileHandle.read(buffer, 0, length, offset);
          await fileHandle.close();

          const rowLine = buffer.toString('utf-8').trim();
          const row = JSON.parse(rowLine);

          const rowValue = row[criterion];
          let isMatch = false;

          switch (operator) {
            case Operator.EQUAL:
              isMatch = rowValue == value;
              break;
            case Operator.GREATER_THAN:
              isMatch = Number(rowValue) > Number(value);
              break;
            case Operator.GREATER_THAN_OR_EQUAL:
              isMatch = Number(rowValue) >= Number(value);
              break;
            case Operator.LESS_THAN:
              isMatch = Number(rowValue) < Number(value);
              break;
            case Operator.LESS_THAN_OR_EQUAL:
              isMatch = Number(rowValue) <= Number(value);
              break;
            case Operator.LIKE:
              const rowStr = String(rowValue).toLowerCase();
              const searchStr = String(value).toLowerCase();
              isMatch = rowStr.includes(searchStr);
              break;
          }

          if (isMatch) {
            rowsToDelete.push(row);
          }
        }
      }
    }

    if (rowsToDelete.length === 0) {
      throw new Error(`No rows found matching the condition`);
    }

    // Mark rows as deleted in the data file
    for (const row of rowsToDelete) {
      const deletedRow = { ...row, deleted: true };
      await fs.appendFile(
        dataFilePath,
        JSON.stringify(deletedRow) + '\n',
        'utf-8',
      );
    }

    // Find all indexed columns (PK and UNIQUE)
    const indexedColumns: string[] = [];

    for (const tbl of schema.tables) {
      if (tbl.name === table) {
        for (const col of tbl.columns) {
          if (col.primaryKey || col.unique) {
            indexedColumns.push(col.name);
          }
        }
        break;
      }
    }

    // Rebuild ALL index files
    for (const colName of indexedColumns) {
      const indexPath = await this.getIndexFilePath(table, colName);

      // Read entire data file line by line
      const fileStream = createReadStream(dataFilePath, { encoding: 'utf-8' });
      const lines = readline.createInterface({ input: fileStream });

      // Store all rows with their positions
      const allRows: any[] = [];
      let position = 0;

      for await (const line of lines) {
        const lineLength = Buffer.byteLength(line + '\n');
        const row = JSON.parse(line);

        allRows.push({
          row: row,
          offset: position,
          length: lineLength,
        });

        position = position + lineLength;
      }

      // Find latest version of each unique value
      const latestVersions: any[] = [];

      for (let i = 0; i < allRows.length; i++) {
        const currentRow = allRows[i];
        const currentValue = currentRow.row[colName];
        let isLatest = true;

        // Check if there's a newer version of this value
        for (let j = i + 1; j < allRows.length; j++) {
          const laterRow = allRows[j];
          const laterValue = laterRow.row[colName];

          if (currentValue === laterValue) {
            // Found a newer version, current is not latest
            isLatest = false;
            break;
          }
        }

        // If this is the latest version, keep it
        if (isLatest) {
          latestVersions.push(currentRow);
        }
      }

      // Build new index (no need to check deleted - latest version is the truth)
      const newIndexLines: any[] = [];

      for (let i = 0; i < latestVersions.length; i++) {
        const item = latestVersions[i];

        // Only add if not deleted
        if (!item.row.deleted) {
          const indexLine = {
            value: item.row[colName],
            offset: item.offset,
            length: item.length,
          };
          newIndexLines.push(JSON.stringify(indexLine));
        }
      }

      // Write new index file
      let indexContent = '';
      for (let i = 0; i < newIndexLines.length; i++) {
        indexContent = indexContent + newIndexLines[i] + '\n';
      }

      await fs.writeFile(indexPath, indexContent, 'utf-8');
    }

    return {
      success: true,
      message: `Successfully deleted ${rowsToDelete.length} row(s)`,
      deletedCount: rowsToDelete.length,
    };
  }

  // for altering the table
  // add column => change the structure of the database schema
  // drop column => change the structure of the database schema
  async alterTable(AST: AlterTableAST) {
    // 2 actions
    // add column
    // drop column

    // you checked the existence
    // get the schema file
    // access the schema file
    // add column to the table
    // create index file if it primary key = true or unique = true

    const currentDB = await this.getCurrentDatabase();
    const schema: ISchema = await this.readCurrentDBSchema();

    const newColumn: IColumn = {
      name: AST.columnName as string,
      type: AST.dataType as DataType,
    };

    const table = schema.tables.find((table) => table.name === AST.name);

    table?.columns.push(newColumn);

    await this.updateCurrentDBSchema(schema);
  }

  // drop the database
  async dropDatabase(AST: DropDatabaseAST) {
    // current db name
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

  // drop the table
  async dropTable(AST: DropTableAST) {
    // you just validated the existence of the table in the parser by using the lexical analyzer
    const currentDB = await this.getCurrentDatabase();

    // will be deleted
    const tablePath = path.join(this.databasesPath, currentDB, AST.name);

    // will be modified
    const schemaPath = path.join(this.databasesPath, currentDB, 'schema.json');

    try {
      await fs.rm(tablePath, { recursive: true });
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

  private async getIndexFilePath(tableName: string, columnName: string) {
    // get the current db name
    const currentDB = await this.getCurrentDatabase();

    // get the index file path
    const indexFilePath = path.join(
      this.databasesPath,
      currentDB,
      tableName,
      `${columnName}_idx.ndjson`,
    );

    return indexFilePath;
  }

  // to check if the column has an index or not based on this you will know which methodology you will use to searching
  private async isIndexed(tableName: string, columnName: string) {
    const indexFilePath = await this.getIndexFilePath(tableName, columnName);

    try {
      await fs.access(indexFilePath);
      return true;
    } catch (error) {
      return false;
    }
  }

  private async createDatabaseDir(databaseName: string): Promise<void> {
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

  private async updateCurrentDBSchema(schema: ISchema): Promise<void> {
    const currentDB = await this.getCurrentDatabase();
    const schemaFilePath = path.join(
      this.databasesPath,
      currentDB,
      'schema.json',
    );
    await fs.writeFile(schemaFilePath, JSON.stringify(schema));
  }

  // based on the columns like => (PK, UNIQUE) => directly create index files
  private async createIndexFile(tableName: string, columnName: string) {
    const currentDB = await this.getCurrentDatabase();

    const indexFilePath = path.join(
      this.databasesPath,
      currentDB,
      tableName,
      `${columnName}_idx.ndjson`,
    );

    try {
      // empty file you shouldn't add any thing here
      // then you will append
      await fs.writeFile(indexFilePath, '');
    } catch (error) {
      throw new Error(error);
    }
  }

  // Get next auto increment value and update metadata
  private async getNextAutoIncrementValue(
    table: string,
    column: string,
  ): Promise<number> {
    const currentDB = await this.getCurrentDatabase();
    const metaFilePath = path.join(
      this.databasesPath,
      currentDB,
      table,
      `${table}_meta.json`,
    );

    // read metadata file
    const metaContent = await fs.readFile(metaFilePath, 'utf-8');
    const metadata = JSON.parse(metaContent);

    // get current counter and increment
    // ex: metadata['id'] => 16
    const currentValue = metadata[column];
    const nextValue = currentValue + 1;

    // update metadata with new value
    metadata[column] = nextValue;
    await fs.writeFile(metaFilePath, JSON.stringify(metadata), 'utf-8');

    return nextValue;
  }
}
