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

CSV_FILE = './data/dataset.csv'
KEY_REPAIR_FILE = './data/key_repair.csv'
INCOMPLETE_CSV_FILE = './data/dataset_with_missing_values.csv'
PICKER_FILE = './data/dataset_pick.csv'

ENV = ExecEnv(
        FileServerConfig().from_dict({'directory': FILESERVER_DIR}),
        identifier=ENGINEENV_MIMIR
    ).from_dict({'datastore': {'directory': DATASTORE_DIR}})
ENGINE_ID = ENV.identifier

DS_NAME = 'people'


class TestMimirLenses(unittest.TestCase):

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

    def test_domain_lens(self):
        """Test DOMAIN lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(INCOMPLETE_CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        print [c.name for c in ds.columns]
        for row in ds.fetch_rows():
            print row.values
        self.assertFalse(wf.has_error)
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_domain(DS_NAME, 'AGE')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.has_error:
            print wf.modules[-1].stderr[0]
        self.assertEquals(len(wf.modules), 2)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        print [c.name for c in ds.columns]
        rows = ds.fetch_rows()
        for row in rows:
            print row.values
        self.assertNotEquals(rows[2].values[ds.column_index('Age')], '')
        mimir.finalize()

    def test_key_repair_lens(self):
        """Test KEY REPAIR lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(KEY_REPAIR_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        ds1 = self.datastore.get_dataset(wf.modules[0].datasets[DS_NAME])
        for row in ds1.fetch_rows():
            print row.values
        print '\n'
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_key_repair(DS_NAME, 'Empid')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds2 = self.datastore.get_dataset(wf.modules[0].datasets[DS_NAME])
        self.assertEquals(ds1.row_count, ds2.row_count)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds.columns), 4)
        self.assertEquals(ds.row_count, 2)
        names = set()
        empids = set()
        rowids = set()
        for row in DatasetClient(dataset=ds).rows:
            print row.values
            rowids.add(row.identifier)
            empids.add(int(row.get_value('empid')))
            names.add(row.get_value('name'))
        self.assertTrue(1 in empids)
        self.assertTrue(2 in rowids)
        mimir.finalize()

    def test_missing_value_lens(self):
        """Test MISSING_VALUE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(INCOMPLETE_CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        print [c.name for c in ds.columns]
        for row in ds.fetch_rows():
            print row.values
        self.assertFalse(wf.has_error)
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_value(DS_NAME, 'AGE')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.has_error:
            print wf.modules[-1].stderr[0]
        self.assertEquals(len(wf.modules), 2)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        print [c.name for c in ds.columns]
        rows = ds.fetch_rows()
        for row in rows:
            print row.values
        self.assertNotEquals(rows[2].values[ds.column_index('Age')], '')
        mimir.finalize()

    def test_missing_key_lens(self):
        """Test MISSING_KEY lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(INCOMPLETE_CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        print [c.name_in_rdb + ' AS ' + c.name + '(' + c.data_type + ')' for c in ds.columns]
        rows = ds.fetch_rows()
        for i in range(len(rows)):
            row = rows[i]
            print row.values
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_key(DS_NAME, 'Age', missing_only=True)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.has_error:
            print wf.modules[-1].stderr[0]
        self.assertFalse(wf.has_error)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds.columns), 3)
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), 12)
        #self.db.append_workflow_module(
        #    viztrail_id=vt.identifier,
        #    command=cmd.load_dataset(f_handle.identifier, DS_NAME + '2')
        #)
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_key(
                DS_NAME,
                'Salary',
                missing_only=True
            )
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME + '2'])
        self.assertEquals(len(ds.columns), 3)
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), 20)
        print [c.name for c in ds.columns]
        for row in rows:
            print str(row.identifier) + ' -> ' + str(row.values)
        mimir.finalize()

    def test_picker_lens(self):
        """Test PICKER lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(PICKER_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_picker(DS_NAME, [
                {'pickFrom': 'Age'},
                {'pickFrom': 'Salary'}
            ])
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        print wf.modules[-1].stderr
        self.assertFalse(wf.has_error)
        # Get dataset
        self.assertEquals(len(wf.modules[-1].datasets), 1)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        columns = [c.name for c in ds.columns]
        self.assertEquals(len(ds.columns), 5)
        self.assertTrue('PICK_ONE_AGE_SALARY' in columns)
        # Pick another column, this time with custom name
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_picker(DS_NAME, [
                {'pickFrom': 'Age'},
                {'pickFrom': 'Salary'}
            ],
            pick_as='MyColumn')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        self.assertEquals(len(wf.modules[-1].datasets), 1)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        columns = [c.name for c in ds.columns]
        self.assertEquals(len(ds.columns), 6)
        self.assertTrue('PICK_ONE_AGE_SALARY' in columns)
        self.assertTrue('MyColumn' in columns)
        # Pick from a picked column
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_picker(DS_NAME, [
                {'pickFrom': 'Age'},
                {'pickFrom': 'PICK_ONE_AGE_SALARY'}
            ],
            pick_as='MyColumn')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        mimir.finalize()

    def test_schema_matching_lens(self):
        """Test SCHEMA_MATCHING lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_schema_matching(DS_NAME, [
                {'column': 'BDate', 'type': 'int'},
                {'column': 'PName', 'type': 'varchar'}
            ], 'new_' + DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        self.assertEquals(len(wf.modules[-1].datasets), 2)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets['new_' + DS_NAME])
        self.assertEquals(len(ds.columns), 2)
        self.assertEquals(ds.row_count, 2)
        # Error if adding an existing dataset
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_schema_matching(DS_NAME, [{'column': 'BDate', 'type': 'int'}], 'new_' + DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        self.db.replace_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_schema_matching(DS_NAME, [{'column': 'BDate', 'type': 'int'}], 'a_new_' + DS_NAME),
            module_id=wf.modules[-1].identifier,
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Error when adding a dataset with an invalid name
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_schema_matching(DS_NAME, [{'column': 'BDate', 'type': 'int'}], 'SOME NAME')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        mimir.finalize()

    def test_type_inference_lens(self):
        """Test TYPE INFERENCE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.fileserver.upload_file(INCOMPLETE_CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds1 = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertFalse(wf.has_error)
        # Infer type
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_type_inference(DS_NAME, 0.6)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds2 = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds2.columns), 3)
        self.assertEquals(ds2.row_count, 4)
        ds1_rows = ds1.fetch_rows()
        ds2_rows = ds2.fetch_rows()
        for i in range(ds2.row_count):
            self.assertEquals(ds1_rows[i].values, ds2_rows[i].values)
        mimir.finalize()


if __name__ == '__main__':
    unittest.main()
