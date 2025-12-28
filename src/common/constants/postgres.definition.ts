import { DataType } from '../enums/data-type.enum';

export const getPostgresDefinition = () => {
  return {
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
};
