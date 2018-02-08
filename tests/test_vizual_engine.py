import os
import shutil
import unittest

from vizier.datastore.mem import InMemDataStore
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.vizual.base import DefaultVizualEngine
from vizier.workflow.vizual.mimir import MimirVizualEngine

DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'
CSV_FILE = './data/dataset.csv'

ENGINEENV_DEFAULT = 'default'
ENGINEENV_MIMIR = 'mimir'


def is_null(val):
    """In Mimir, cells of type INT that have 'no value' will return '0'."""
    return val in ['NULL', '', '0']


class TestVizualEngine(unittest.TestCase):

    def set_up(self, engine):
        """Create an empty file server repository."""
        # Drop project descriptor directory
        if os.path.isdir(FILESERVER_DIR):
            shutil.rmtree(FILESERVER_DIR)
        # Setup project repository
        fs = DefaultFileServer(FILESERVER_DIR)
        if engine == ENGINEENV_DEFAULT:
            self.datastore = InMemDataStore()
            self.vizual = DefaultVizualEngine(
                self.datastore,
                fs
            )
        elif engine == ENGINEENV_MIMIR:
            self.datastore = MimirDataStore(DATASTORE_DIR)
            self.vizual = MimirVizualEngine(
                self.datastore,
                fs
            )
        self.file = fs.upload_file(CSV_FILE)

    def tear_down(self, engine):
        """Clean-up by dropping file server directory.
        """
        # Drop data store directory
        if os.path.isdir(DATASTORE_DIR):
            shutil.rmtree(DATASTORE_DIR)
        # Drop project descriptor directory
        if os.path.isdir(FILESERVER_DIR):
            shutil.rmtree(FILESERVER_DIR)

    def test_default_engine(self):
        """Test functionality if the default VizUAL engine."""
        self.run_engine_tests(ENGINEENV_DEFAULT)

    def test_mimir_engine(self):
        """Test functionality if the Mimir VizUAL engine."""
        import vistrails.packages.mimir.init as mimir
        mimir.initialize()
        self.run_engine_tests(ENGINEENV_MIMIR)
        mimir.finalize()

    def run_engine_tests(self, engine):
        """Run sequence of tests for given engine."""
        self.load_dataset(engine)
        self.insert_column(engine)
        self.insert_row(engine)
        self.delete_column(engine)
        self.delete_row(engine)
        self.move_column(engine)
        self.move_row(engine)
        self.rename_column(engine)
        self.update_cell(engine)
        self.sequence_of_steps(engine)

    def delete_column(self, engine):
        """Test functionality to delete a column."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds.rows]
        # Delete second column
        col_count, id1 = self.vizual.delete_column(ds.identifier, 'Age')
        del col_ids[1]
        # Result should indicate that one column was deleted. The identifier of
        # the resulting dataset should differ from the identifier of the
        # original dataset
        self.assertEquals(col_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        # Retrieve modified dataset and ensure that it cobtains the following
        #
        # Name, Salary
        # ------------
        # Alice, 35K
        # Bob, 30K
        ds = self.datastore.get_dataset(id1)
        # Schema is Name, Salary
        self.assertEquals(len(ds.columns), 2)
        self.assertEquals(ds.columns[0].name, 'Name')
        self.assertEquals(ds.columns[1].name, 'Salary')
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Make sure that all rows only have two columns
        row = ds.rows[0]
        self.assertEquals(len(row.values), 2)
        self.assertEquals(len(row.values), 2)
        self.assertEquals(row.get_value(0), 'Alice')
        self.assertEquals(row.get_value(1), '35K')
        row = ds.rows[1]
        self.assertEquals(len(row.values), 2)
        self.assertEquals(len(row.values), 2)
        self.assertEquals(row.get_value(0), 'Bob')
        self.assertEquals(row.get_value(1), '30K')
        # Ensure that row identifier haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.delete_column('unknown:uri', 'Age')
        self.tear_down(engine)

    def delete_row(self, engine):
        """Test functionality to delete a row."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds.rows]
        # Delete second row
        row_count, id1 = self.vizual.delete_row(ds.identifier, 1)
        del row_ids[1]
        # Result should indicate that one row was deleted. The identifier of the
        #  resulting dataset should differ from the identifier of the original
        # dataset
        self.assertEquals(row_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        # Retrieve modified dataset and ensure that it contains the following
        # data:
        #
        # Name, Age, Salary
        # ------------
        # Alice, 23, 35K
        ds = self.datastore.get_dataset(id1)
        # Schema is Name, Salary
        col_names = ['Name', 'Age', 'Salary']
        self.assertEquals(len(ds.columns), len(col_names))
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].name, col_names[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # There should only be one row
        self.assertEquals(len(ds.rows), 1)
        # Ensure that row identifier haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.delete_row('unknown:uri', 1)
        # Ensure exception is thrown if row index is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.delete_row(ds.identifier, 100)
        self.tear_down(engine)

    def insert_column(self, engine):
        """Test functionality to insert a columns."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds.rows]
        # Insert columns at position 1
        col_ids.insert(1, ds.column_counter)
        col_count, id1 = self.vizual.insert_column(ds.identifier, 1, 'Height')
        # Result should indicate that one column was inserted. The identifier of
        # the resulting dataset should differ from the identifier of the
        # original dataset
        self.assertEquals(col_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        # Retrieve dataset and ensure that it has the following schema:
        # Name, Height, Age, Salary
        ds = self.datastore.get_dataset(id1)
        col_names = ['Name' ,'Height', 'Age', 'Salary']
        # Ensure that there are four rows
        self.assertEquals(len(ds.columns), len(col_names))
        for i in range(len(col_names)):
            col = ds.columns[i]
            self.assertEquals(col.identifier, col_ids[i])
            self.assertEquals(col.name, col_names[i])
        # Insert columns at last position
        col_ids.append(ds.column_counter)
        col_names.append('Weight')
        col_count, id2 = self.vizual.insert_column(id1, 4, 'Weight')
        # Result should indicate that one column was deleted. The identifier of
        # the resulting dataset should differ from the identifier of the
        # previous dataset
        self.assertEquals(col_count, 1)
        self.assertNotEquals(id1, id2)
        # Retrieve dataset and ensure that it has the following schema:
        # Name, Height, Age, Salary, Weight
        ds = self.datastore.get_dataset(id2)
        # Ensure that there are five rows
        self.assertEquals(len(ds.columns), len(col_names))
        for i in range(len(col_names)):
            col = ds.columns[i]
            self.assertEquals(col.identifier, col_ids[i])
            self.assertEquals(col.name, col_names[i])
        # The cell values for new columns are None all other values are not None
        for row in ds.rows:
            for i in range(len(ds.columns)):
                if i == 1 or i == 4:
                    self.assertTrue(is_null(row.get_value(i)))
                else:
                    self.assertFalse(is_null(row.get_value(i)))
        # Ensure that row identifier haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.insert_column('unknown:uri', 1, 'Height')
        # Ensure exception is thrown if column name is invalid
        with self.assertRaises(ValueError):
            self.vizual.insert_column(ds.identifier, 1, 'Height from ground')
        # Ensure exception is thrown if column position is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.insert_column(ds.identifier, 100, 'Height')
        self.tear_down(engine)

    def insert_row(self, engine):
        """Test functionality to insert a row."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds.rows]
        # Insert row at index position 1
        row_ids.insert(1, ds.row_counter)
        # Result should indicate that one row was inserted. The identifier of
        # the resulting dataset should differ from the identifier of the
        # original dataset
        row_count, id1 = self.vizual.insert_row(ds.identifier, 1)
        self.assertEquals(row_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        # Retrieve modified dataset
        ds = self.datastore.get_dataset(id1)
        # Ensure that there are three rows
        self.assertEquals(len(ds.rows), 3)
        # The second row has empty values for each column
        row = ds.rows[1]
        self.assertEquals(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertTrue(is_null(row.values[i]))
        # Append row at end current dataset
        row_ids.append(ds.row_counter)
        row_count, id2 = self.vizual.insert_row(id1, 3)
        self.assertEquals(row_count, 1)
        self.assertNotEquals(id1, id2)
        ds = self.datastore.get_dataset(id2)
        # Ensure that there are three rows
        self.assertEquals(len(ds.rows), 4)
        # The next to last row has non-empty values for each column
        row = ds.rows[2]
        self.assertEquals(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertFalse(is_null(row.values[i]))
        # The last row has empty values for each column
        row = ds.rows[3]
        self.assertEquals(len(row.values), len(ds.columns))
        for i in range(len(ds.columns)):
            self.assertTrue(is_null(row.values[i]))
        # Ensure that row ids haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.insert_row('unknown:uri', 1)
        # Ensure exception is thrown if row index is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.insert_row(ds.identifier, 5)
        # Ensure no exception is raised
        self.vizual.insert_row(ds.identifier, 4)
        self.tear_down(engine)

    def load_dataset(self, engine):
        """Test functionality to load a dataset."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        self.assertEquals(len(ds.columns), 3)
        self.assertEquals(len(ds.rows), 2)
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.load_dataset('unknown:uri')
        self.tear_down(engine)

    def move_column(self, engine):
        """Test functionality to move a column."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds.rows]
        # Swap first two columns
        c = col_ids[0]
        del col_ids[0]
        col_ids.insert(1, c)
        col_count, id1 = self.vizual.move_column(ds.identifier, 'Name', 1)
        self.assertEquals(col_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        ds = self.datastore.get_dataset(id1)
        self.assertEquals(ds.columns[0].name, 'Age')
        self.assertEquals(ds.columns[1].name, 'Name')
        self.assertEquals(ds.columns[2].name, 'Salary')
        row = ds.rows[0]
        self.assertEquals(row.values[0], '23')
        self.assertEquals(row.values[1], 'Alice')
        self.assertEquals(row.values[2], '35K')
        row = ds.rows[1]
        self.assertEquals(row.values[0], '32')
        self.assertEquals(row.values[1], 'Bob')
        self.assertEquals(row.values[2], '30K')
        # Ensure that row ids haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Swap last two columns
        c = col_ids[1]
        del col_ids[1]
        col_ids.append(c)
        col_count, id2 = self.vizual.move_column(id1, 'Salary', 1)
        ds = self.datastore.get_dataset(id2)
        self.assertEquals(ds.columns[0].name, 'Age')
        self.assertEquals(ds.columns[1].name, 'Salary')
        self.assertEquals(ds.columns[2].name, 'Name')
        row = ds.rows[0]
        self.assertEquals(row.values[0], '23')
        self.assertEquals(row.values[1], '35K')
        self.assertEquals(row.values[2], 'Alice')
        row = ds.rows[1]
        self.assertEquals(row.values[0], '32')
        self.assertEquals(row.values[1], '30K')
        self.assertEquals(row.values[2], 'Bob')
        # Ensure that row ids haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Raise error if source column is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.move_column(id2, 4, 1)
        # Raise error if target position is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.move_column(id2, 'Name', -1)
        with self.assertRaises(ValueError):
            self.vizual.move_column(id2, 'Name', 4)
        self.tear_down(engine)

    def move_row(self, engine):
        """Test functionality to move a row."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds.rows]
        # Swap first two rows
        row_ids = [row for row in reversed(row_ids)]
        row_count, id1 = self.vizual.move_row(ds.identifier, 0, 1)
        self.assertEquals(row_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        ds = self.datastore.get_dataset(id1)
        self.assertEquals(ds.columns[0].name, 'Name')
        self.assertEquals(ds.columns[1].name, 'Age')
        self.assertEquals(ds.columns[2].name, 'Salary')
        row = ds.rows[0]
        self.assertEquals(row.values[0], 'Bob')
        self.assertEquals(row.values[1], '32')
        self.assertEquals(row.values[2], '30K')
        row = ds.rows[1]
        self.assertEquals(row.values[0], 'Alice')
        self.assertEquals(row.values[1], '23')
        self.assertEquals(row.values[2], '35K')
        # Ensure that row ids haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Swap last two rows
        row_ids = [row for row in reversed(row_ids)]
        row_count, id2 = self.vizual.move_row(id1, 1, 0)
        ds = self.datastore.get_dataset(id2)
        self.assertEquals(ds.columns[0].name, 'Name')
        self.assertEquals(ds.columns[1].name, 'Age')
        self.assertEquals(ds.columns[2].name, 'Salary')
        row = ds.rows[0]
        self.assertEquals(row.values[0], 'Alice')
        self.assertEquals(row.values[1], '23')
        self.assertEquals(row.values[2], '35K')
        row = ds.rows[1]
        self.assertEquals(row.values[0], 'Bob')
        self.assertEquals(row.values[1], '32')
        self.assertEquals(row.values[2], '30K')
        # Ensure that row ids haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Move first row to the end
        row_count, id3 = self.vizual.move_row(id2, 0, 2)
        row_ids = [row for row in reversed(row_ids)]
        ds = self.datastore.get_dataset(id3)
        row = ds.rows[0]
        self.assertEquals(row.values[0], 'Bob')
        self.assertEquals(row.values[1], '32')
        self.assertEquals(row.values[2], '30K')
        row = ds.rows[1]
        self.assertEquals(row.values[0], 'Alice')
        self.assertEquals(row.values[1], '23')
        self.assertEquals(row.values[2], '35K')
        # Ensure that row ids haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Raise error if source row is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.move_row(id2, 3, 1)
        # Raise error if target position is out of bounds
        with self.assertRaises(ValueError):
            self.vizual.move_row(id2, 0, -1)
        with self.assertRaises(ValueError):
            self.vizual.move_row(id2, 1, 4)
        self.tear_down(engine)

    def rename_column(self, engine):
        """Test functionality to rename a column."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds.rows]
        # Rename first column to Firstname
        col_count, id1 = self.vizual.rename_column(ds.identifier, 'Name', 'Firstname')
        self.assertEquals(col_count, 1)
        self.assertNotEquals(id1, ds.identifier)
        ds = self.datastore.get_dataset(id1)
        self.assertEquals(ds.columns[0].name, 'Firstname')
        self.assertEquals(ds.columns[1].name, 'Age')
        self.assertEquals(ds.columns[2].name, 'Salary')
        col_count, id2 = self.vizual.rename_column(id1, 1, 'BDate')
        ds = self.datastore.get_dataset(id2)
        self.assertEquals(ds.columns[0].name, 'Firstname')
        self.assertEquals(ds.columns[1].name, 'BDate')
        self.assertEquals(ds.columns[2].name, 'Salary')
        # Ensure that row ids haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Ensure exception is thrown if dataset identifier is unknown
        with self.assertRaises(ValueError):
            self.vizual.rename_column('unknown:uri', 'Name', 'Firstname')
        # Ensure exception is thrown for invalid column name
        with self.assertRaises(ValueError):
            self.vizual.rename_column(id2, 'Age', 'BDate')
        self.tear_down(engine)

    def sequence_of_steps(self, engine):
        """Test sequence of calls that modify a dataset."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        count, ds_id = self.vizual.insert_row(ds.identifier, 1)
        count, ds_id = self.vizual.insert_column(ds_id, 3, 'HDate')
        count, ds_id = self.vizual.update_cell(ds_id, 'HDate', 0, '180')
        count, ds_id = self.vizual.update_cell(ds_id, 'HDate', 1, '160')
        count, ds_id = self.vizual.rename_column(ds_id, 'HDate', 'Height')
        count, ds_id = self.vizual.update_cell(ds_id, 'Height', 2, '170')
        count, ds_id = self.vizual.move_row(ds_id, 1, 2)
        count, ds_id = self.vizual.update_cell(ds_id, 'Name', 2, 'Carla')
        count, ds_id = self.vizual.update_cell(ds_id, 'Age', 2, '45')
        count, ds_id = self.vizual.update_cell(ds_id, 'Salary', 2, '56K')
        count, ds_id = self.vizual.move_column(ds_id, 'Salary', 4)
        count, ds_id = self.vizual.delete_column(ds_id, 'Age')
        count, ds_id = self.vizual.delete_row(ds_id, 0)
        count, ds_id = self.vizual.delete_row(ds_id, 0)
        ds = self.datastore.get_dataset(ds_id)
        self.assertEquals([col.name for col in ds.columns], ['Name', 'Height', 'Salary'])
        self.assertEquals([col.identifier for col in ds.columns], [0, 3, 2])
        self.assertEquals(len(ds.rows), 1)
        self.assertEquals(ds.rows[0].values, ['Carla', '160', '56K'])
        self.assertEquals(ds.rows[0].identifier, 2)
        self.tear_down(engine)

    def update_cell(self, engine):
        """Test functionality to update a dataset cell."""
        self.set_up(engine)
        # Create a new dataset
        ds = self.vizual.load_dataset(self.file.identifier)
        # Keep track of column and row identifier
        col_ids = [col.identifier for col in ds.columns]
        row_ids = [row.identifier for row in ds.rows]
        # Update cell [0, 0]. Ensure that one row was updated and a new
        # identifier is generated. Also ensure that the resulting datasets
        # has the new value in cell [0, 0]
        upd_rows, id1 = self.vizual.update_cell(ds.identifier, 0, 0, 'MyValue')
        self.assertEquals(upd_rows, 1)
        self.assertNotEquals(ds.identifier, id1)
        ds = self.datastore.get_dataset(id1)
        self.assertEquals(ds.get_cell(0, 0), 'MyValue')
        upd_rows, id2 = self.vizual.update_cell(id1, 'Name', 0, 'AValue')
        ds = self.datastore.get_dataset(id2)
        self.assertEquals(ds.get_cell(0, 0), 'AValue')
        self.assertEquals(ds.get_cell('Name', 0), 'AValue')
        # Ensure that row ids haven't changed
        for i in range(len(ds.rows)):
            self.assertEquals(ds.rows[i].identifier, row_ids[i])
        # Make sure column identifier haven't changed
        for i in range(len(ds.columns)):
            self.assertEquals(ds.columns[i].identifier, col_ids[i])
        # Ensure exception is thrown if column is unknown
        with self.assertRaises(ValueError):
            self.vizual.update_cell(ds.identifier, 'Income', 0, 'MyValue')
        # Ensure exception is thrown if row index is out ouf bounds
        with self.assertRaises(ValueError):
            self.vizual.update_cell(ds.identifier, 0, 100, 'MyValue')
        self.tear_down(engine)

if __name__ == '__main__':
    unittest.main()
