import { Injectable } from '@nestjs/common';
import * as fs from 'fs/promises';
import { createReadStream } from 'fs';
import * as path from 'path';
import * as readline from 'readline/promises';
import { SchemaLogic } from '../schema/schema.logic';
import { IndexLogic } from '../index/index-logic';
import { Operator } from 'src/common/enums/operator.enum';
import { matchesCondition } from 'src/common/utils/comparison.helper';
import { ISchema, Index } from 'src/common/types/schema.types';
import {
  SelectAST,
  InsertAST,
  UpdateAST,
  DeleteAST,
} from 'src/common/types/ast.type';

@Injectable()
export class RowLogic {
  constructor(
    private readonly schemaLogic: SchemaLogic,
    private readonly indexLogic: IndexLogic,
  ) {}

  private readonly databasesPath = path.join(process.cwd(), 'databases');

  /**
   * ! row operations
   */
  // select
  async findRows(currentDB: string, AST: SelectAST) {
    // get schema => parsed
    const schema: ISchema =
      await this.schemaLogic.readCurrentDBSchema(currentDB);

    // get data file path
    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      AST.table,
      `${AST.table}.ndjson`,
    );

    // find indexed columns and primary key
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

    // TODO:
    // THERE IS NO TABLE W/O PK
    if (!pkName) {
      throw new Error(`No primary key found for table ${AST.table}`);
    }

    let allRows: Record<string, any>[] = [];

    // ! open the file before the for and try
    const fileHandle = await fs.open(dataFilePath, 'r');

    try {
      // If WHERE uses an indexed column with EQUAL operator => use index directly
      if (
        AST.where &&
        AST.where.operator === Operator.EQUAL &&
        indexedColumns.includes(AST.where.criterion) // check if the searching with the indexed column
      ) {
        // get index file path
        const indexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
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
            // get row (parsed)
            const row = await this.getRowByOffset(
              buffer,
              fileHandle,
              offset,
              length,
            );

            // desruct the data without deleted
            const { deleted, ...data } = row;
            allRows.push(data);

            // break the loop after found the specific row
            break;
          }
        }
      }
      // WHERE uses non-indexed column OR no WHERE at all => use PK index to scan all rows
      else {
        // get the pk index file path
        const pkIndexFilePath = await this.indexLogic.getIndexFilePath(
          currentDB,
          AST.table,
          pkName,
        );
        const fileStream = createReadStream(pkIndexFilePath, {
          encoding: 'utf-8',
        });
        const lines = readline.createInterface({ input: fileStream });

        // get all offsets from PK index
        const indexes: { offset: number; length: number }[] = [];
        for await (const line of lines) {
          const { offset, length } = JSON.parse(line);
          indexes.push({ offset, length });
        }

        // stream data file and check each row
        for (const { offset, length } of indexes) {
          // allocate a buffer with the size of the specific row
          const buffer = Buffer.alloc(length);
          // get row => (parsed)
          const row = await this.getRowByOffset(
            buffer,
            fileHandle,
            offset,
            length,
          );

          // If there's a WHERE clause, filter rows
          if (AST.where?.criterion) {
            const { criterion, operator, value } = AST.where;
            const rowValue = row[criterion]; // get the column value on the row

            // matchesCondition => helper
            if (matchesCondition(operator, rowValue, value)) {
              const { deleted, ...data } = row;
              allRows.push(data);
              // without break to get all the rows which are matching the condition
            }
          }
          // No WHERE clause => return all rows
          else {
            const { deleted, ...data } = row;
            allRows.push(data);
            // without break because i am going to gather all the rows (existing)
          }
        }
      }
    } finally {
      // ! close the descriptor after the loops
      await fileHandle.close();
    }

    // Select only the requested columns
    let projectedData: Record<string, any>[] = [];

    if (AST.columns.includes('*')) {
      // SELECT * => return all columns
      projectedData = allRows;
    } else {
      // Only include requested columns
      projectedData = allRows.map((row) => {
        // the projected row with the requested columns
        const projectedRow: Record<string, any> = {};

        // Inject requested columns
        for (const column of AST.columns) {
          projectedRow[column] = row[column];
        }

        return projectedRow; // add the projectedRow into projectedData
      });
    }

    return { data: projectedData };
  }

  // insert
  async insertRows(currentDB: string, AST: InsertAST) {
    // get schema => parsed
    const schema = await this.schemaLogic.readCurrentDBSchema(currentDB);

    // data file path
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

    for (const rowValues of rows) {
      // row object with provided values + marked as not deleted
      const row: Record<string, any> = { deleted: false };
      for (let i = 0; i < AST.columns.length; i++) {
        row[AST.columns[i]] = rowValues[i];
      }
      // increment the auto increment column first
      for (const col of tableSchema.columns) {
        if (col.autoIncrement) {
          // it will be assigned into the metadata file
          row[col.name] = await this.getNextAutoIncrementValue(
            currentDB,
            AST.table,
            col.name,
          );
        }
      }

      // validate duplicates
      try {
        for (const col of tableSchema.columns) {
          if (col.primaryKey || col.unique) {
            const value = row[col.name];
            const indexPath = await this.indexLogic.getIndexFilePath(
              currentDB,
              AST.table,
              col.name,
            );
            await this.indexLogic.checkDuplicates(indexPath, value, col.name);
          }
        }
      } catch (error) {
        // decrement auto increment column if the value used before
        for (const col of tableSchema.columns) {
          if (col.autoIncrement) {
            const metaFilePath = path.join(
              this.databasesPath,
              currentDB,
              AST.table,
              `${AST.table}_meta.json`,
            );
            const metaContent = await fs.readFile(metaFilePath, 'utf-8');
            const metadata = JSON.parse(metaContent);

            // decrement
            metadata[col.name] = parseInt(metadata[col.name]) - 1;
            await fs.writeFile(metaFilePath, JSON.stringify(metadata), 'utf-8');
          }
        }
        throw error;
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
      const insertedLine = JSON.stringify(row) + '\n'; // because you are going to append into ndjson file
      const { size: offset } = await fs.stat(dataFilePath); // the start point that i will insert from (offset)
      await fs.appendFile(dataFilePath, insertedLine, 'utf-8');

      // update all index files
      const length = Buffer.byteLength(insertedLine);

      for (const col of tableSchema.columns) {
        if (col.primaryKey || col.unique) {
          const indexPath = await this.indexLogic.getIndexFilePath(
            currentDB,
            AST.table,
            col.name,
          );
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

  // update
  async updateRows(currentDB: string, AST: UpdateAST) {
    const { table, updates, where } = AST;
    const { criterion, operator, value } = where!;

    const schema = await this.schemaLogic.readCurrentDBSchema(currentDB);

    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      table,
      `${table}.ndjson`,
    );

    // indexed columns
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

    // check if new value already exists
    for (const [columnName, newValue] of Object.entries(updates)) {
      if (indexedColumns.includes(columnName)) {
        const indexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
          table,
          columnName,
        );
        await this.indexLogic.checkDuplicates(indexPath, newValue, columnName);
      }
    }

    // find row to update
    const isCriterionIndexed = await this.indexLogic.isIndexed(
      currentDB,
      table,
      criterion,
    );

    let rowsToUpdate: {
      row: Record<string, any>;
      offset: number;
      length: number;
    }[] = [];

    // ! open the file before the loops
    const fileHandle = await fs.open(dataFilePath, 'r');

    try {
      if (isCriterionIndexed) {
        // by index
        const indexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
          table,
          criterion,
        );
        const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
        const lines = readline.createInterface({ input: fileStream });

        for await (const line of lines) {
          // get the value from the index file and compare it against the value from the AST later
          const { value: indexValue, offset, length } = JSON.parse(line);

          if (matchesCondition(operator, indexValue, value)) {
            const buffer = Buffer.alloc(length);
            const row = await this.getRowByOffset(
              buffer,
              fileHandle,
              offset,
              length,
            );
            rowsToUpdate.push({ row, offset, length });
          }
        }
      } else {
        // slow way
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

        const pkIndexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
          table,
          pkName,
        );
        const fileStream = createReadStream(pkIndexPath, { encoding: 'utf-8' });
        const lines = readline.createInterface({ input: fileStream });

        for await (const line of lines) {
          const { offset, length } = JSON.parse(line);
          const buffer = Buffer.alloc(length);
          const row = await this.getRowByOffset(
            buffer,
            fileHandle,
            offset,
            length,
          );

          const rowValue = row[criterion];

          if (matchesCondition(operator, rowValue, value)) {
            rowsToUpdate.push({ row, offset, length });
          }
        }
      }
    } finally {
      // ! close descriptor after finding rows
      await fileHandle.close();
    }

    if (rowsToUpdate.length === 0) {
      throw new Error('No row found');
    }

    // to update the offsets and length in the index files
    const newRowsInfo: {
      oldOffset: number;
      newOffset: number;
      newLength: number;
      updatedRow: any;
    }[] = [];

    // go through all rows to mark as deleted and append updated versions
    for (const item of rowsToUpdate) {
      // mark old as deleted
      // spread the old row data + override the deleted with true
      const deletedRow = { ...item.row, deleted: true };
      // append into the data file
      await fs.appendFile(
        dataFilePath,
        JSON.stringify(deletedRow) + '\n',
        'utf-8',
      );

      // get offset BEFORE appending new row (the new version) which should be included into the index file
      const { size: newOffset } = await fs.stat(dataFilePath);

      // add new updated row
      // the old data + updates (override) + by default => deleted: false
      const updatedRow = { ...item.row, ...updates };
      const updatedLine = JSON.stringify(updatedRow) + '\n';
      const newLength = Buffer.byteLength(updatedLine);

      // append the updated version
      await fs.appendFile(dataFilePath, updatedLine, 'utf-8');

      // save info for the index
      newRowsInfo.push({
        oldOffset: item.offset,
        newOffset: newOffset,
        newLength: newLength,
        updatedRow: updatedRow,
      });
    }

    // index rewrite
    for (const colName of indexedColumns) {
      const indexPath = await this.indexLogic.getIndexFilePath(
        currentDB,
        table,
        colName,
      );

      const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
      const lines = readline.createInterface({ input: fileStream });

      const newIndex: Record<string, any>[] = [];
      const oldOffsets = newRowsInfo.map((info) => {
        return info.oldOffset;
      });

      for await (const line of lines) {
        const entry = JSON.parse(line);
        // keep old entries
        // compare old offsets that should be updated against the all entries offsets on the index file
        if (!oldOffsets.includes(entry.offset)) {
          newIndex.push(entry); // rows which are not going to update
        }
      }

      // add the new entries for all updated rows
      for (const info of newRowsInfo) {
        newIndex.push({
          value: info.updatedRow[colName],
          offset: info.newOffset,
          length: info.newLength,
        });
      }

      let indexNewContent = '';
      for (const entry of newIndex) {
        indexNewContent += JSON.stringify(entry) + '\n';
      }

      // rewrite the file again
      await fs.writeFile(indexPath, indexNewContent, 'utf-8');
    }

    return { success: true, message: `${rowsToUpdate.length} rows updated` };
  }

  // delete
  async deleteRows(currentDB: string, AST: DeleteAST) {
    const { table, where } = AST;
    const schema = await this.schemaLogic.readCurrentDBSchema(currentDB);

    const dataFilePath = path.join(
      this.databasesPath,
      currentDB,
      table,
      `${table}.ndjson`,
    );

    // let rowsToDelete: any = [];
    let rowsToDelete: Record<string, any>[] = [];

    // Get PK
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

    // ! open file once for deletion search
    const fileHandle = await fs.open(dataFilePath, 'r');

    try {
      if (!where) {
        // Delete all
        const pkIndexPath = await this.indexLogic.getIndexFilePath(
          currentDB,
          table,
          pkName,
        );
        const fileStream = createReadStream(pkIndexPath, { encoding: 'utf-8' });
        const lines = readline.createInterface({ input: fileStream });

        for await (const line of lines) {
          const { offset, length } = JSON.parse(line);
          const buffer = Buffer.alloc(length);
          const row = await this.getRowByOffset(
            buffer,
            fileHandle,
            offset,
            length,
          );
          rowsToDelete.push(row);
        }
      } else {
        // delete with WHERE
        const { criterion, operator, value } = where;
        const isIndexed = await this.indexLogic.isIndexed(
          currentDB,
          table,
          criterion,
        );

        if (isIndexed) {
          // fast way
          const indexPath = await this.indexLogic.getIndexFilePath(
            currentDB,
            table,
            criterion,
          );
          const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
          const lines = readline.createInterface({ input: fileStream });

          for await (const line of lines) {
            const { value: indexValue, offset, length } = JSON.parse(line);

            if (matchesCondition(operator, indexValue, value)) {
              const buffer = Buffer.alloc(length);
              const row = await this.getRowByOffset(
                buffer,
                fileHandle,
                offset,
                length,
              );
              rowsToDelete.push(row);
            }
          }
        } else {
          // slow path
          const pkIndexPath = await this.indexLogic.getIndexFilePath(
            currentDB,
            table,
            pkName,
          );
          const fileStream = createReadStream(pkIndexPath, {
            encoding: 'utf-8',
          });
          const lines = readline.createInterface({ input: fileStream });

          for await (const line of lines) {
            const { offset, length } = JSON.parse(line);
            const buffer = Buffer.alloc(length);
            const row = await this.getRowByOffset(
              buffer,
              fileHandle,
              offset,
              length,
            );

            const rowValue = row[criterion];

            if (matchesCondition(operator, rowValue, value)) {
              rowsToDelete.push(row);
            }
          }
        }
      }
    } finally {
      // ! close descriptor
      await fileHandle.close();
    }

    if (rowsToDelete.length === 0) {
      throw new Error('No rows found');
    }

    // Mark as deleted
    for (const row of rowsToDelete) {
      await fs.appendFile(
        dataFilePath,
        JSON.stringify({ ...row, deleted: true }) + '\n',
        'utf-8',
      );
    }

    // find indexed columns
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

    // index rewrite
    for (const colName of indexedColumns) {
      const indexPath = await this.indexLogic.getIndexFilePath(
        currentDB,
        table,
        colName,
      );

      const fileStream = createReadStream(indexPath, { encoding: 'utf-8' });
      const lines = readline.createInterface({ input: fileStream });

      const indexEntries: Index[] = [];
      for await (const line of lines) {
        indexEntries.push(JSON.parse(line));
      }

      // remove deleted entries
      const newIndex: Index[] = [];
      for (const entry of indexEntries) {
        let shouldIkeepIt = true;

        for (const deletedRow of rowsToDelete) {
          if (entry.value == deletedRow[colName]) {
            shouldIkeepIt = false;
            break;
          }
        }

        if (shouldIkeepIt) {
          newIndex.push(entry);
        }
      }

      let content = '';
      for (const entry of newIndex) {
        content += JSON.stringify(entry) + '\n';
      }

      await fs.writeFile(indexPath, content, 'utf-8');
    }

    return {
      success: true,
      message: `${rowsToDelete.length} rows Deleted`,
    };
  }

  // reading a specific row by offset
  async getRowByOffset(
    buffer: Buffer<ArrayBuffer>,
    fileHandle: fs.FileHandle,
    offset: number,
    length: number,
  ): Promise<any> {
    // fill the buffer
    // offset the starting point of the data file
    // and length how many bytes to read
    // 0 => where i will start to fill the allocated buffer
    await fileHandle.read(buffer, 0, length, offset);

    // read it as a string
    const rowLine = buffer.toString('utf-8').trim();

    // parse it
    const row = JSON.parse(rowLine);
    return row;
  }

  // auto-increment logic
  private async getNextAutoIncrementValue(
    currentDB: string,
    tableName: string,
    columnName: string,
  ): Promise<number> {
    const metaFilePath = path.join(
      this.databasesPath,
      currentDB,
      tableName,
      `${tableName}_meta.json`,
    );
    const data = await fs.readFile(metaFilePath, 'utf-8');
    const metadata = JSON.parse(data);

    metadata[columnName] = (metadata[columnName] || 0) + 1;
    await fs.writeFile(metaFilePath, JSON.stringify(metadata), 'utf-8');
    return metadata[columnName];
  }
}
