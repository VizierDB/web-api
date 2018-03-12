"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.config import FileServerConfig
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer


DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'

LOAD_DIR = './data/mimir'


class TestLoadMimirDataset(unittest.TestCase):

    def setUp(self):
        """Create an empty work trails repository."""
        # Cleanup first
        self.cleanUp()
        self.datastore = MimirDataStore(DATASTORE_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)

    def tearDown(self):
        """Clean-up by deleting directories.
        """
        self.cleanUp()

    def cleanUp(self):
        """Remove datastore and fileserver directory."""
        # Delete directories
        for d in [DATASTORE_DIR, FILESERVER_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_load(self):
        """Run workflow with default configuration."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        ignore_files = ['cureSource.csv', 'uneven.csv', 'JSONOUTPUTWIDE.csv', 'dataset_load_single.csv']
        # Error: new-line character seen in unquoted field - do you need to open the file in universal-newline mode?
        # UnicodeEncodeError: 'ascii' codec can't encode character u'\u2026' in position 222: ordinal not in range(128)
        mimir.initialize()
        for filename in os.listdir(LOAD_DIR):
            if filename in ignore_files:
                continue
            print 'LOAD ' + filename
            filename = os.path.join(LOAD_DIR, filename)
            f_handle = self.fileserver.upload_file(filename)
            ds = self.datastore.load_dataset(f_handle)
            ds_load = self.datastore.get_dataset(ds.identifier)
            print [col.name_in_rdb + ' AS ' + col.name + '(' + col.data_type + ')' for col in ds_load.columns]
            print str(ds.row_count) + ' row(s)'
            self.assertEquals(len(ds.columns), len(ds_load.columns))
            self.assertEquals(ds.column_counter, ds_load.column_counter)
            self.assertEquals(ds.row_counter, ds_load.row_counter)
            rows = ds.fetch_rows()
            self.assertEquals(ds.row_counter, len(rows))
            self.assertEquals(ds.row_count, len(rows))
            for i in range(len(rows)):
                row = rows[i]
                self.assertEquals(row.identifier, i)
                self.assertEquals(len(row.values), len(ds.columns))
        mimir.finalize()


if __name__ == '__main__':
    unittest.main()
