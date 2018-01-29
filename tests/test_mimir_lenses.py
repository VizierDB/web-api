"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.base import DEFAULT_BRANCH
from vizier.workflow.repository.engine.viztrails import DefaultViztrailsEngine
from vizier.workflow.module import ModuleSpecification
from vizier.workflow.repository.fs import FileSystemViztrailRepository
from vizier.workflow.vizual.mimir import MimirVizualEngine

import vizier.config as config
import vizier.workflow.repository.command as cmd

DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'
WORKTRAILS_DIR = './env/wt'

CSV_FILE = './data/dataset.csv'
KEY_REPAIR_FILE = './data/key_repair.csv'
INCOMPLETE_CSV_FILE = './data/dataset_with_missing_values.csv'
PICKER_FILE = './data/dataset_pick.csv'

ENGINE_ID = 'ENGINE'

DS_NAME = 'people'


class TestMimirLenses(unittest.TestCase):

    def setUp(self):
        """Create an empty work trails repository."""
        # Create fresh set of directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, WORKTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)
            os.mkdir(d)
        self.datastore = MimirDataStore(DATASTORE_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        vizual = MimirVizualEngine(self.datastore, self.fileserver)
        self.db = FileSystemViztrailRepository(
            WORKTRAILS_DIR,
            {
                ENGINE_ID: DefaultViztrailsEngine(
                    config.ENGINE_MIMIR,
                    vizual,
                    self.datastore
                )
            }
        )

    def tearDown(self):
        """Clean-up by dropping the MongoDB colelction used by the engine.
        """
        # Delete directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, WORKTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

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
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_key_repair(DS_NAME, 'Empid')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds2 = self.datastore.get_dataset(wf.modules[0].datasets[DS_NAME])
        self.assertEquals(len(ds1.rows), len(ds2.rows))
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds.columns), 4)
        self.assertEquals(len(ds.rows), 2)
        names = set()
        empids = set()
        rowids = set()
        for row in ds.rows:
            rowids.add(row.identifier)
            empids.add(row.get_value('empid'))
            names.add(row.get_value('name'))
        self.assertTrue('1' in empids)
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
        self.assertFalse(wf.has_error)
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=DEFAULT_BRANCH,
            command=cmd.mimir_missing_value(DS_NAME, 'AGE')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertNotEquals(ds.rows[2].get_value('Age'), '')
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
        # Missing Value Lens
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_key(DS_NAME, 'Age', missing_only=True)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds.columns), 3)
        self.assertEquals(len(ds.rows), 12)
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME + '2')
        )
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_missing_key(
                DS_NAME + '2',
                'Salary',
                missing_only=True
            )
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME + '2'])
        self.assertEquals(len(ds.columns), 3)
        self.assertEquals(len(ds.rows), 20)
        print [c.name for c in ds.columns]
        for row in ds.rows:
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
        self.assertEquals(len(ds.rows), 2)
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
        print 'LENSE'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.mimir_type_inference(DS_NAME, 0.6)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Get dataset
        ds2 = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        self.assertEquals(len(ds2.columns), 3)
        self.assertEquals(len(ds2.rows), 4)
        for i in range(len(ds2.rows)):
            self.assertEquals(ds1.rows[i].values, ds2.rows[i].values)
        mimir.finalize()


if __name__ == '__main__':
    unittest.main()
