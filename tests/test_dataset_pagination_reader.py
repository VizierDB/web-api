import os
import shutil
import tempfile
import unittest

from vizier.datastore.base import DatasetRow
from vizier.datastore.reader import DelimitedFileReader, DefaultJsonDatasetReader

from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.vizual.base import DefaultVizualEngine
from vizier.workflow.vizual.mimir import MimirVizualEngine


CSV_FILE = './data/dataset_7rows.csv'

DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'

ENGINEENV_DEFAULT = 'default'
ENGINEENV_MIMIR = 'mimir'

class TestDatasetPaginationReader(unittest.TestCase):

    def set_up(self, engine):
        """Create an empty file server repository."""
        # Drop project descriptor directory
        if os.path.isdir(FILESERVER_DIR):
            shutil.rmtree(FILESERVER_DIR)
        # Setup project repository
        self.fs = DefaultFileServer(FILESERVER_DIR)
        if engine == ENGINEENV_DEFAULT:
            self.datastore = FileSystemDataStore(DATASTORE_DIR)
            self.vizual = DefaultVizualEngine(
                self.datastore,
                self.fs
            )
        elif engine == ENGINEENV_MIMIR:
            self.datastore = MimirDataStore(DATASTORE_DIR)
            self.vizual = MimirVizualEngine(
                self.datastore,
                self.fs
            )

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
        """Test functionality for the default setup."""
        self.run_tests(ENGINEENV_DEFAULT)

    def test_mimir_engine(self):
        """Test functionality for the Mimir setup."""
        import vistrails.packages.mimir.init as mimir
        mimir.initialize()
        self.run_tests(ENGINEENV_MIMIR)
        mimir.finalize()

    def run_tests(self, engine):
        """Run sequence of tests for given configuration."""
        self.set_up(engine)
        ds = self.vizual.load_dataset(self.fs.upload_file(CSV_FILE).identifier)
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), 7)
        rows = ds.fetch_rows(offset=1)
        self.assertEquals(len(rows), 6)
        self.assertEquals(rows[0].values[0], 'Bob')
        self.assertEquals(rows[5].values[0], 'Gertrud')
        rows = ds.fetch_rows(limit=2)
        self.assertEquals(len(rows), 2)
        self.assertEquals(rows[0].values[0], 'Alice')
        self.assertEquals(rows[1].values[0], 'Bob')
        rows = ds.fetch_rows(offset=4, limit=3)
        self.assertEquals(len(rows), 3)
        self.assertEquals(rows[0].values[0], 'Eileen')
        self.assertEquals(rows[2].values[0], 'Gertrud')
        rows = ds.fetch_rows(offset=5, limit=3)
        self.assertEquals(len(rows), 2)
        self.assertEquals(rows[0].values[0], 'Frank')
        self.assertEquals(rows[1].values[0], 'Gertrud')
        rows = ds.fetch_rows(offset=6, limit=3)
        self.assertEquals(len(rows), 1)
        self.assertEquals(rows[0].values[0], 'Gertrud')
        self.tear_down(engine)


if __name__ == '__main__':
    unittest.main()
