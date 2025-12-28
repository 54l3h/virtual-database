import * as fs from 'fs/promises';

export const appendFile = async (
  filePath: string,
  entry: Record<string, any>,
) => {
  const insertedLine = JSON.stringify(entry) + '\n';
  await fs.appendFile(filePath, insertedLine, 'utf-8');
};
