import csv
import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.datastore.base import create_dataset_from_csv
from vizier.datastore.federated import FederatedDataStore
from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.fs import DATA_FILE, METADATA_FILE
from vizier.datastore.mem import InMemDataStore, VolatileDataStore
from vizier.datastore.mimir import MimirDataStore
from vizier.datastore.metadata import UpdateCellAnnotation as UpdCell
from vizier.datastore.metadata import UpdateColumnAnnotation as UpdCol
from vizier.datastore.metadata import UpdateRowAnnotation as UpdRow


CSV_FILE = './data/dataset.csv'
DATASTORE_DIRECTORY = './env/ds'

FS_DATASTORE = 'FS'
MEM_DATASTORE = 'MEM'
MIMIR_DATASTORE = 'MIMIR'

class TestDataStore(unittest.TestCase):

    def set_up(self, store_type):
        """Create empty data store directory."""
        if store_type == MEM_DATASTORE:
            self.db = InMemDataStore()
        else:
            # Remove directory if it exists
            if os.path.isdir(DATASTORE_DIRECTORY):
                shutil.rmtree(DATASTORE_DIRECTORY)
            os.makedirs(DATASTORE_DIRECTORY)
            if store_type == FS_DATASTORE:
                self.db = FileSystemDataStore(DATASTORE_DIRECTORY)
            elif store_type == MIMIR_DATASTORE:
                self.db = MimirDataStore(DATASTORE_DIRECTORY)

    def tear_down(self, store_type):
        """Delete data store directory.
        """
        if os.path.isdir(DATASTORE_DIRECTORY):
            shutil.rmtree(DATASTORE_DIRECTORY)

    def test_federated_datastore(self):
        """Test functionality of the federated data store."""
        store1 = InMemDataStore()
        store2 = InMemDataStore()
        with open(CSV_FILE, 'r') as f:
            data = create_dataset_from_csv(csv.reader(f))
        ds1 = store1.store_dataset(data)
        ds2 = store2.store_dataset(data)
        fed_store = FederatedDataStore([store1, store2])
        self.assertIsNotNone(fed_store.get_dataset(ds1.identifier))
        self.assertIsNotNone(fed_store.get_dataset(ds2.identifier))
        self.assertIsNone(fed_store.get_dataset('UNDEFINED'))
        with self.assertRaises(NotImplementedError):
            fed_store.store_dataset(data)
        upd = UpdRow(0, 'name', 'My Name')
        self.assertIsNotNone(fed_store.update_annotation(ds1.identifier, upd))
        self.assertIsNotNone(fed_store.update_annotation(ds2.identifier, upd))
        self.assertIsNone(fed_store.update_annotation('UNDEFINED', upd))

    def test_fs_datastore(self):
        """Run test for file system datastore."""
        self.run_tests(FS_DATASTORE)

    def test_mem_datastore(self):
        """Run test for in-memory datastore."""
        self.run_tests(MEM_DATASTORE)

    def test_mimir_datastore(self):
        """Run test for Mimir datastore."""
        mimir.initialize()
        self.run_tests(MIMIR_DATASTORE)
        mimir.finalize()

    def test_volatile_datastore(self):
        """Test volatile data store on top of a file system data store."""
        self.set_up(FS_DATASTORE)
        with open(CSV_FILE, 'r') as f:
            data = create_dataset_from_csv(csv.reader(f))
        ds = self.db.store_dataset(data)
        v_store = VolatileDataStore(self.db)
        # Make sure the existing dataset is accessible via the volatile store
        v_ds = v_store.get_dataset(ds.identifier)
        self.assertIsNotNone(v_ds)
        self.assertEquals(v_ds.get_cell('Salary', 1), '30K')
        # Create an updated dataset. The original should be the same in both
        # stores
        ds.rows[1].set_value('Salary', '40K')
        v_ds = v_store.store_dataset(ds)
        self.assertEquals(self.db.get_dataset(ds.identifier).get_cell('Salary', 1), '30K')
        self.assertEquals(v_store.get_dataset(ds.identifier).get_cell('Salary', 1), '30K')
        self.assertEquals(v_store.get_dataset(v_ds.identifier).get_cell('Salary', 1), '40K')
        self.assertIsNone(self.db.get_dataset(v_ds.identifier))
        # Delete both datasets. The volatile store is empty. The original should
        # be unchanged.
        self.assertTrue(v_store.delete_dataset(ds.identifier))
        self.assertTrue(v_store.delete_dataset(v_ds.identifier))
        self.assertFalse(v_store.delete_dataset(ds.identifier))
        self.assertFalse(v_store.delete_dataset(v_ds.identifier))
        self.assertIsNone(v_store.get_dataset(ds.identifier))
        self.assertIsNone(v_store.get_dataset(v_ds.identifier))
        self.assertEquals(self.db.get_dataset(ds.identifier).get_cell('Salary', 1), '30K')
        self.tear_down(FS_DATASTORE)

    def run_tests(self, store_type):
        """Run sequence of test for given data store type."""
        self.set_up(store_type)
        self.dataset_life_cycle()
        self.tear_down(store_type)
        self.set_up(store_type)
        self.datastore_init(store_type)
        self.tear_down(store_type)
        self.set_up(store_type)
        self.dataset_annotations()
        self.tear_down(store_type)
        self.set_up(store_type)
        self.dataset_read()
        self.tear_down(store_type)
        self.set_up(store_type)
        self.dataset_write()
        self.tear_down(store_type)
        self.set_up(store_type)
        self.dataset_write_invalid_dataset()
        self.tear_down(store_type)

    def datastore_init(self, store_type):
        """Test initalizing a datastore with existing datasets."""
        with open(CSV_FILE, 'r') as f:
            data = create_dataset_from_csv(csv.reader(f))
        ds = self.db.store_dataset(data)
        if store_type == MEM_DATASTORE:
            self.db = InMemDataStore()
        elif store_type == FS_DATASTORE:
            self.db = FileSystemDataStore(DATASTORE_DIRECTORY)
        elif store_type == MIMIR_DATASTORE:
            self.db = MimirDataStore(DATASTORE_DIRECTORY)

    def dataset_life_cycle(self):
        """Test create and delete dataset."""
        with open(CSV_FILE, 'r') as f:
            data = create_dataset_from_csv(csv.reader(f))
        ds = self.db.store_dataset(data)
        # Ensure that the project data has three columns and two rows
        self.assertEquals(len(ds.columns), 3)
        self.assertEquals(len(ds.rows), 2)
        # Delete dataset and ensure that the dataset directory no longer exists
        self.assertTrue(self.db.delete_dataset(ds.identifier))
        self.assertFalse(self.db.delete_dataset(ds.identifier))

    def dataset_annotations(self):
        """Test annotations after writing and reading a dataset."""
        with open(CSV_FILE, 'r') as f:
            data = create_dataset_from_csv(csv.reader(f))
        data.annotations.for_column(1).set_annotation('name', 'Dummy')
        data.annotations.for_row(0).set_annotation('name', 'Dummy')
        data.annotations.for_cell(1, 1).set_annotation('title', 'Dummy')
        dh = self.db.store_dataset(data)
        self.db.update_annotation(dh.identifier, UpdCol(1, 'comment', 'Some Comment'))
        self.db.update_annotation(dh.identifier, UpdCol(1, 'name'))
        self.db.update_annotation(dh.identifier, UpdRow(0, 'name', 'My Name'))
        self.db.update_annotation(dh.identifier, UpdCell(1, 1, 'title', 'A Title'))
        ds = self.db.get_dataset(dh.identifier)
        meta = ds.annotations
        # Column annotation
        col_anno = meta.for_column(0)
        self.assertEquals(col_anno.size, 0)
        col_anno = meta.for_column(1)
        self.assertEquals(col_anno.size, 1)
        self.assertEquals(col_anno.get_annotation('comment'), 'Some Comment')
        # Row annotation
        row_anno = meta.for_row(0)
        self.assertEquals(col_anno.size, 1)
        self.assertEquals(row_anno.get_annotation('name'), 'My Name')
        row_anno = meta.for_row(1)
        self.assertEquals(row_anno.size, 0)
        # Cell Annotations
        cell_anno = meta.for_cell(1, 1)
        self.assertEquals(cell_anno.size, 1)
        self.assertEquals(cell_anno.get_annotation('title'), 'A Title')

    def dataset_read(self):
        """Test reading a dataset."""
        with open(CSV_FILE, 'r') as f:
            data = create_dataset_from_csv(csv.reader(f))
        dh = self.db.store_dataset(data)
        ds = self.db.get_dataset(dh.identifier)
        self.assertEquals(dh.identifier, ds.identifier)
        self.assertEquals(len(dh.columns), len(ds.columns))
        self.assertEquals(len(dh.rows), len(ds.rows))
        # Name,Age,Salary
        # Alice,23,35K
        # Bob,32,30K
        self.assertEquals(ds.column_index('Name'), 0)
        self.assertEquals(ds.column_index('Age'), 1)
        self.assertEquals(ds.column_index('Salary'), 2)
        row = ds.rows[0]
        self.assertEquals(row.get_value('Name'), 'Alice')
        self.assertEquals(row.get_value('Age'), '23')
        self.assertEquals(row.get_value('Salary'), '35K')
        row = ds.rows[1]
        self.assertEquals(row.get_value('Name'), 'Bob')
        self.assertEquals(row.get_value('Age'), '32')
        self.assertEquals(row.get_value('Salary'), '30K')

    def dataset_write(self):
        """Test writing a dataset."""
        with open(CSV_FILE, 'r') as f:
            data = create_dataset_from_csv(csv.reader(f))
        dh = self.db.store_dataset(data)
        ds = self.db.get_dataset(dh.identifier)
        # Update first row
        row = ds.rows[0]
        row.set_value('Name', 'Carla')
        row.set_value('Salary', '45K')
        dh1 = self.db.store_dataset(ds)
        # Read both datasets
        ds = self.db.get_dataset(dh.identifier)
        # First Dataset
        self.assertEquals(ds.column_index('Name'), 0)
        self.assertEquals(ds.column_index('Age'), 1)
        self.assertEquals(ds.column_index('Salary'), 2)
        row = ds.rows[0]
        self.assertEquals(row.get_value('Name'), 'Alice')
        self.assertEquals(row.get_value('Age'), '23')
        self.assertEquals(row.get_value('Salary'), '35K')
        row = ds.rows[1]
        self.assertEquals(row.get_value('Name'), 'Bob')
        self.assertEquals(row.get_value('Age'), '32')
        self.assertEquals(row.get_value('Salary'), '30K')
        # Second dataset
        ds = self.db.get_dataset(dh1.identifier)
        self.assertEquals(ds.column_index('Name'), 0)
        self.assertEquals(ds.column_index('Age'), 1)
        self.assertEquals(ds.column_index('Salary'), 2)
        row = ds.rows[0]
        self.assertEquals(row.get_value('Name'), 'Carla')
        self.assertEquals(row.get_value('Age'), '23')
        self.assertEquals(row.get_value('Salary'), '45K')
        row = ds.rows[1]
        self.assertEquals(row.get_value('Name'), 'Bob')
        self.assertEquals(row.get_value('Age'), '32')
        self.assertEquals(row.get_value('Salary'), '30K')

    def dataset_write_invalid_dataset(self):
        """Test writing a dataset."""
        with open(CSV_FILE, 'r') as f:
            data = create_dataset_from_csv(csv.reader(f))
        data.rows[1].values.append('A')
        with self.assertRaises(ValueError):
            self.db.store_dataset(data)
        with open(CSV_FILE, 'r') as f:
            data = create_dataset_from_csv(csv.reader(f))
        data.columns.append('A')
        with self.assertRaises(ValueError):
            self.db.store_dataset(data)


if __name__ == '__main__':
    unittest.main()
