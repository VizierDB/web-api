import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.datastore.base import Dataset
from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.mem import InMemDataStore
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.vizual.base import DefaultVizualEngine
from vizier.workflow.context import VizierDBClient, WorkflowContext

DATASTORE_DIR = './env/ds'
SERVER_DIR = './data/fs'


class TestVizierClient(unittest.TestCase):

    def setUp(self):
        """Delete metadata file if it exists."""
        # Drop directorie
        self.tearDown()

    def tearDown(self):
        """Clean-up by dropping file server directory.
        """
        if os.path.isdir(DATASTORE_DIR):
            shutil.rmtree(DATASTORE_DIR)
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_fs_client(self):
        """Run tests for default engine and file server data store."""
        fs = DefaultFileServer(SERVER_DIR)
        ds = FileSystemDataStore(DATASTORE_DIR)
        context = WorkflowContext(DefaultVizualEngine(ds, fs), ds)
        self.run_client_tests(VizierDBClient(context, ds))

    def test_mem_client(self):
        """Run tests for default engine and in-memory data store."""
        fs = DefaultFileServer(SERVER_DIR)
        ds = InMemDataStore()
        context = WorkflowContext(DefaultVizualEngine(ds, fs), ds)
        self.run_client_tests(VizierDBClient(context, ds))

    def test_mimir_client(self):
        """Run tests for default engine and Mimir data store."""
        mimir.initialize()
        fs = DefaultFileServer(SERVER_DIR)
        ds = MimirDataStore(DATASTORE_DIR)
        context = WorkflowContext(DefaultVizualEngine(ds, fs), ds)
        self.run_client_tests(VizierDBClient(context, ds))
        mimir.finalize()

    def run_client_tests(self, client):
        """Test creating and updating a dataset via the client."""
        ds = Dataset()
        ds.add_column('Name')
        ds.add_column('Age')
        ds.add_row(['Alice', '23'])
        ds.add_row(['Bob', '25'])
        ds.annotations.for_cell(1, 1).set_annotation('value', '26')
        client.create_dataset('MyDataset', ds)
        # Ensure the returned dataset contains the input data
        ds = client.get_dataset('MyDataset')
        self.assertEquals([c.name for c in ds.columns], ['Name', 'Age'])
        self.assertEquals(ds.rows[0].values, ['Alice', '23'])
        self.assertEquals(ds.rows[1].values, ['Bob', '25'])
        # Update dataset
        ds.rows[1].set_value('Age', '26')
        client.update_dataset('MyDataset', ds)
        ds = client.get_dataset('MyDataset')
        self.assertEquals(ds.rows[1].values, ['Bob', '26'])
        # Value error when creating dataset with existing name
        with self.assertRaises(ValueError):
            client.create_dataset('MyDataset', ds)
        # Value error when retrieving unknown dataset
        with self.assertRaises(ValueError):
            client.get_dataset('SomeDataset')
        # Ensure the returned dataset contains the modified data
        client.rename_dataset('MyDataset', 'SomeDataset')
        ds = client.get_dataset('SomeDataset')
        client.update_dataset('SomeDataset', ds)

if __name__ == '__main__':
    unittest.main()
