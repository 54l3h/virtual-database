export const projectData = (
  columns: string[],
  rows: Record<string, any>[],
): Record<string, any>[] => {
  // Select only the requested columns
  let projectedData: Record<string, any>[] = [];

  if (columns.includes('*')) {
    // SELECT * => return all columns
    projectedData = rows;
  } else {
    // Only include requested columns
    projectedData = rows.map((row) => {
      // the projected row with the requested columns
      const projectedRow: Record<string, any> = {};

      // Inject requested columns
      for (const column of columns) {
        projectedRow[column] = row[column];
      }

      return projectedRow; // add the projectedRow into projectedData
    });
  }

  return projectedData;
};
