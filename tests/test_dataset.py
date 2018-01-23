import unittest

from vizier.datastore.base import Dataset, DatasetColumn, DatasetRow
from vizier.datastore.base import collabel_2_index


class TestDataset(unittest.TestCase):

    def test_create_dataset(self):
        """Test creating dataset from scratch by adding columns and rows."""
        ds = Dataset()
        # Add columns and ensure that all names and id's are as expected
        col_names = ['A', 'B', 'C']
        for name in col_names:
            ds.add_column(name)
        for i in range(len(col_names)):
            col = ds.columns[i]
            self.assertEquals(col.name, col_names[i])
            self.assertEquals(col.identifier, i)
        # Add rows and ensure that id's and values are as expected
        rows = [['1', '2', '3'], ['4', '5', '6'], ['7', '8', '9']]
        for row in rows:
            ds.add_row(row)
        for i in range(len(rows)):
            row = ds.rows[i]
            self.assertEquals(row.identifier, i)
            for j in range(len(col_names)):
                self.assertEquals(row.get_value(j),  rows[i][j])
                self.assertEquals(row.get_value(col_names[j]), rows[i][j])
        # Update a cell value
        ds.rows[1].set_value('B', 'Hallo')
        row = ds.rows[1]
        self.assertEquals(row.identifier, 1)
        self.assertEquals(row.get_value(0), '4')
        self.assertEquals(row.get_value('A'), '4')
        self.assertEquals(row.get_value('B'), 'Hallo')
        self.assertEquals(row.get_value('C'), '6')
        # Ensure ValueError when adding row with invalid schema
        with self.assertRaises(ValueError):
            ds.add_row(['2', '3'])
        with self.assertRaises(ValueError):
            ds.add_row(['1', '2', '3', '4'])
        # Move things around a bit
        ds = Dataset()
        for name in col_names:
            ds.add_column(name, position=0)
        for i in range(len(col_names)):
            col = ds.columns[i]
            self.assertEquals(col.name, col_names[(len(col_names) - 1) - i])
            self.assertEquals(col.identifier, (len(col_names) - 1) - i)
        rows = [['1', '2', '3'], ['4', '5', '6'], ['7', '8', '9']]
        for row in rows:
            ds.add_row(row, position=0)
        for i in range(len(rows)):
            row = ds.rows[i]
            r_id = (len(rows) - 1) - i
            r = rows[r_id]
            self.assertEquals(row.identifier, r_id)
            for j in range(len(col_names)):
                self.assertEquals(row.get_value(j),  r[j])
                self.assertEquals(row.get_value(col_names[(len(col_names) - 1) - j]), r[j])

    def test_dataset_column_index(self):
        """Test method to convert different column identifier into column index
        in the dataset schema.
        """
        ds = Dataset()
        with self.assertRaises(ValueError):
            ds.column_index(0)
        with self.assertRaises(ValueError):
            ds.column_index('A')
        ds = Dataset(columns=[DatasetColumn(0, 'A'), DatasetColumn(1, 'B'), DatasetColumn(2, 'C')])
        self.assertEquals(ds.column_index(0), 0)
        self.assertEquals(ds.column_index(1), 1)
        self.assertEquals(ds.column_index(2), 2)
        with self.assertRaises(ValueError):
            ds.column_index(4)
        self.assertEquals(ds.column_index('A'), 0)
        self.assertEquals(ds.column_index('B'), 1)
        self.assertEquals(ds.column_index('C'), 2)
        with self.assertRaises(ValueError):
            ds.column_index('D')
        ds = Dataset(columns=[DatasetColumn(0, 'ABC'), DatasetColumn(1, 'ABB'), DatasetColumn(2, 'ABC')])
        self.assertEquals(ds.column_index('A'), 0)
        self.assertEquals(ds.column_index('B'), 1)
        self.assertEquals(ds.column_index('C'), 2)
        self.assertEquals(ds.column_index('ABB'), 1)
        self.assertEquals(ds.column_index('B'), 1)
        with self.assertRaises(ValueError):
            ds.column_index('ABC')

    def test_invalid_initialization(self):
        """Test dataset initialization with invalid column or row sets."""
        with self.assertRaises(ValueError):
            Dataset(
                columns=[DatasetColumn(1, 'A'), DatasetColumn(1, 'B'), DatasetColumn(2, 'C')],
                column_counter=3,
                rows=[DatasetRow(0, ['1', '2', '3']), DatasetRow(1, ['4', '5', '6']), DatasetRow(2, ['7', '8', '9'])],
                row_counter=3
            )
        with self.assertRaises(ValueError):
            Dataset(
                columns=[DatasetColumn(0, 'A'), DatasetColumn(1, 'B'), DatasetColumn(2, 'C')],
                column_counter=3,
                rows=[DatasetRow(0, ['1', '2', '3']), DatasetRow(1, ['4', '5', '6']), DatasetRow(1, ['7', '8', '9'])],
                row_counter=3
            )

    def test_label_2_column_index(self):
        """Test collabel_2_index method."""
        self.assertEquals(collabel_2_index('A'), 1)
        self.assertEquals(collabel_2_index('B'), 2)
        self.assertEquals(collabel_2_index('Z'), 26)
        self.assertEquals(collabel_2_index('AA'), 27)
        self.assertEquals(collabel_2_index('AB'), 28)
        self.assertEquals(collabel_2_index('AZ'), 52)
        self.assertEquals(collabel_2_index('BA'), 53)
        self.assertEquals(collabel_2_index('BB'), 54)
        self.assertEquals(collabel_2_index('a'), -1)
        self.assertEquals(collabel_2_index('xy'), -1)

    def test_modify_dataset(self):
        """Test modification of a dataset."""
        ds = Dataset(
            columns=[DatasetColumn(0, 'A'), DatasetColumn(1, 'B'), DatasetColumn(2, 'C')],
            column_counter=3,
            rows=[DatasetRow(0, ['1', '2', '3']), DatasetRow(1, ['4', '5', '6']), DatasetRow(2, ['7', '8', '9'])],
            row_counter=3
        )
        col = ds.add_column('D', position=1)
        self.assertEquals(col.identifier, 3)
        self.assertEquals(col.name, 'D')
        col = ds.columns[1]
        self.assertEquals(col.identifier, 3)
        self.assertEquals(col.name, 'D')
        # Ensure that all rows have a null value at position 1
        for row in ds.rows:
            self.assertEquals(row.values[1], '')
            self.assertNotEquals(row.values[0], '')
            self.assertNotEquals(row.values[2], '')
            self.assertNotEquals(row.values[3], '')
        col = ds.add_column('E')
        self.assertEquals(col.identifier, 4)
        self.assertEquals(col.name, 'E')
        col = ds.columns[4]
        self.assertEquals(col.identifier, 4)
        self.assertEquals(col.name, 'E')
        for row in ds.rows:
            self.assertNotEquals(row.values[0], '')
            self.assertEquals(row.values[1], '')
            self.assertNotEquals(row.values[2], '')
            self.assertNotEquals(row.values[3], '')
            self.assertEquals(row.values[4], '')
        row = ds.add_row(['A', 'D', 'B', 'C', 'E'], position=1)
        self.assertEquals(row.identifier, 3)
        row = ds.rows[1]
        self.assertEquals(row.identifier, 3)
        for col_name in ['A', 'B', 'C', 'D', 'E']:
            self.assertEquals(row.get_value(col_name), col_name)


if __name__ == '__main__':
    unittest.main()
