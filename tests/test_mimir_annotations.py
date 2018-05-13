"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.config import ExecEnv, FileServerConfig
from vizier.config import ENGINEENV_MIMIR
from vizier.datastore.client import DatasetClient
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.base import DEFAULT_BRANCH
from vizier.workflow.engine.viztrails import DefaultViztrailsEngine
from vizier.workflow.module import ModuleSpecification
from vizier.workflow.repository.fs import FileSystemViztrailRepository
from vizier.workflow.vizual.mimir import MimirVizualEngine

import vizier.config as config
import vizier.workflow.command as cmd

DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'
VIZTRAILS_DIR = './env/wt'

CSV_FILE = './data/dataset_with_missing_values.csv'

ENV = ExecEnv(
        FileServerConfig().from_dict({'directory': FILESERVER_DIR}),
        identifier=ENGINEENV_MIMIR
    ).from_dict({'datastore': {'directory': DATASTORE_DIR}})
ENGINE_ID = ENV.identifier

DS_NAME = 'people'


class TestMimirAnnotations(unittest.TestCase):

    def setUp(self):
        """Create an empty work trails repository."""
        # Create fresh set of directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, VIZTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)
            os.mkdir(d)
        self.datastore = MimirDataStore(DATASTORE_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        vizual = MimirVizualEngine(self.datastore, self.fileserver)
        self.db = FileSystemViztrailRepository(
            VIZTRAILS_DIR,
            {ENV.identifier: ENV}
        )

    def tearDown(self):
        """Clean-up by dropping the MongoDB colelction used by the engine.
        """
        # Delete directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, VIZTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_annotations(self):
        """Test DOMAIN lens."""
        # Create new work trail and create dataset from CSV file
        mimir.initialize()
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_value(DS_NAME, ds.get_column_by_name('AGE').identifier)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        print 'MISSING VALUE LENS:'
        print [c.name + ' (' + str(c.identifier) + ')' for c in ds.columns]
        for row in ds.fetch_rows():
            print str(row.identifier) + ' ' + str(row.values)
        for anno in ds.get_annotations(column_id=1, row_id=2):
            print anno.key + ': ' + anno.value
        # Missing key lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_key(DS_NAME, ds.get_column_by_name('AGE').identifier, missing_only=True)
        )
        print 'MISSING KEY LENS'
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.has_error:
            print wf.modules[-1].stderr[0]['data']
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        print [c.name + ' (' + str(c.identifier) + ')' for c in ds.columns]
        for row in ds.fetch_rows():
            print str(row.identifier) + ' ' + str(row.values)
        # Key repair lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_key_repair(DS_NAME, ds.get_column_by_name('SALARY').identifier)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        print 'KEY REPAIR LENS:'
        print [c.name + ' (' + str(c.identifier) + ')' for c in ds.columns]
        for row in ds.fetch_rows():
            print str(row.identifier) + ' ' + str(row.values)
        mimir.finalize()


if __name__ == '__main__':
    unittest.main()
