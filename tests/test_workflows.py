"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.config import ENGINE_DEFAULT, ENGINE_MIMIR
from vizier.datastore.mem import InMemDataStore
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.repository.engine.viztrails import DefaultViztrailsEngine
from vizier.workflow.repository.fs import FileSystemViztrailRepository
from vizier.workflow.vizual.base import DefaultVizualEngine
from vizier.workflow.vizual.mimir import MimirVizualEngine

import vizier.workflow.repository.command as cmd

DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'
WORKTRAILS_DIR = './env/wt'

CSV_FILE = './data/dataset.csv'
INCOMPLETE_CSV_FILE = './data/dataset_with_missing_values.csv'

DS_NAME = 'people'

CREATE_DATASET_PY = """
from vizier.datastore.base import Dataset
ds = Dataset()
ds.add_column('Name')
ds.add_column('Age')
ds.add_row(['Alice', 23])
ds.add_row(['Bob', 34])
vizierdb.create_dataset('people', ds)
"""

PRINT_DATASET_PY = """
for row in vizierdb.get_dataset('people').rows:
    print row.get_value('Name')
"""

PRINT_UNKNOWN_DATASET_PY = """
for row in vizierdb.get_dataset('employees').rows:
    print row.get_value('Name')
"""

SET_VARIABLES_PY = """
ds = vizierdb.get_dataset('people')
name_new = 'Bobby'
name_2 = ds.rows[2].get_value('Name')
ds.rows[1].set_value('Name', name_new)
vizierdb.update_dataset('people', ds)
ds = vizierdb.get_dataset('people')
"""

SET_VARIABLES_ONLY_PY = """
name_new = 'Bobby'
name_2 = 'Bob'
"""

UPDATE_DATASET_PY = """
ds = vizierdb.get_dataset('people')
for row in ds.rows:
    row.set_value('Name', 'NoName')
vizierdb.update_dataset('people', ds)
"""

UPDATE_DATASET_WITH_FILTER_PY = """
ds = vizierdb.get_dataset('people')
for row in ds.rows:
    if row.get_value('Name') == name_2:
        row.set_value('Name', name_new)
vizierdb.update_dataset('people', ds)
"""

ENGINE_ID = 'ENGINE'


class TestWorkflows(unittest.TestCase):

    def tearDown(self):
        """Clean-up by dropping the MongoDB colelction used by the engine.
        """
        # Delete directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, WORKTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def set_up(self):
        """Create an empty work trails repository."""
        # Create fresh set of directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, WORKTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)
            os.mkdir(d)

    def set_up_default(self):
        """Setup configuration using default Vizual engine."""
        self.set_up()
        self.datastore = InMemDataStore()
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        vizual = DefaultVizualEngine(self.datastore, self.fileserver)
        self.db = FileSystemViztrailRepository(
            WORKTRAILS_DIR,
            {
                ENGINE_ID: DefaultViztrailsEngine(
                    ENGINE_DEFAULT,
                    vizual,
                    self.datastore
                )
            }
        )

    def set_up_mimir(self):
        """Setup configuration using Mimir engine."""
        self.set_up()
        self.datastore = MimirDataStore(DATASTORE_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        vizual = MimirVizualEngine(self.datastore, self.fileserver)
        self.db = FileSystemViztrailRepository(
            WORKTRAILS_DIR,
            {
                ENGINE_ID: DefaultViztrailsEngine(
                    ENGINE_MIMIR,
                    vizual,
                    self.datastore
                )
            }
        )

    def test_vt_default(self):
        """Run workflow with default configuration."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        self.set_up_default()
        self.run_python_workflow()
        self.set_up_default()
        self.run_mixed_workflow()
        self.set_up_default()
        self.run_delete_modules()
        self.set_up_default()
        self.run_erroneous_workflow()

    def test_vt_mimir(self):
        """Run workflows for Mimir configurations."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        self.set_up_mimir()
        self.run_python_workflow()
        self.set_up_mimir()
        self.run_mixed_workflow()
        self.set_up_mimir()
        self.run_delete_modules()
        self.set_up_mimir()
        self.run_erroneous_workflow()
        mimir.finalize()

    def run_delete_modules(self):
        """Test deletion of modules."""
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        #print '(1) CREATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, 'Age', 0, '28')
        )
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, 'Age', 1, '42')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets['people'])
        self.assertEquals(ds.rows[0].get_value('Age'), '28')
        self.assertEquals(ds.rows[1].get_value('Age'), '42')
        # DELETE UPDATE CELL
        self.db.delete_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[1].identifier
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets['people'])
        self.assertEquals(ds.rows[0].get_value('Age'), '23')
        # DELETE LOAD (will introduce error)
        self.db.delete_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[0].identifier
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        # DELETE last remaining module
        self.db.delete_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[0].identifier
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)

    def run_erroneous_workflow(self):
        """Test workflow that has errors."""
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        #print '(1) CREATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, 'Age', 0, '28')
        )
        # This should create an error because of the invalid column name
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.rename_column(DS_NAME, 1, '')
        )
        # This should not have any effect
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, 'Age', 0, '29')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        # Make sure that all workflow modules have a non-negative identifier
        # and that they are all unique
        identifier = set()
        for m in wf.modules:
            self.assertTrue(m.identifier >= 0)
            self.assertTrue(not m.identifier in identifier)
            identifier.add(m.identifier)

    def run_mixed_workflow(self):
        """Test functionality to execute a workflow module."""
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        #print '(1) CREATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        #print '(2) INSERT ROW'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.insert_row(DS_NAME, 1)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        #print '(3) Set name to Bobby and set variables'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(SET_VARIABLES_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        #print '(4) Set age to 28'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, 'Age', 1, '28')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        #print '(5) Change Alice to Bob'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, 'Name', 0, 'Bob')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        #print '(6) UPDATE DATASET WITH FILTER'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(UPDATE_DATASET_WITH_FILTER_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # Ensure that all names are Bobby
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        age = ['23', '28', '32']
        for i in range(len(ds.rows)):
            row = ds.rows[i]
            self.assertEquals(row.get_value('Name'), 'Bobby')
            self.assertEquals(row.get_value('Age'), age[i])

    def run_python_workflow(self):
        """Test functionality to execute a workflow module."""
        vt = self.db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
        #print '(1) CREATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(CREATE_DATASET_PY)
        )
        # from vizier.database.client import VizierDBClient\nv = VizierDBClient(__vizierdb__)
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        self.assertEquals(wf.version, 0)
        self.assertEquals(len(wf.modules), 1)
        self.assertTrue(len(wf.modules[0].stdout) == 0)
        self.assertTrue(len(wf.modules[0].stderr) == 0)
        self.assertEquals(len(wf.modules[0].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[0].datasets)
        #print '(2) PRINT DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(PRINT_DATASET_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        self.assertEquals(wf.version, 1)
        self.assertEquals(len(wf.modules), 2)
        self.assertTrue(len(wf.modules[0].stdout) == 0)
        self.assertTrue(len(wf.modules[0].stderr) == 0)
        self.assertEquals(len(wf.modules[0].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[0].datasets)
        self.assertTrue(len(wf.modules[1].stdout) == 1)
        self.assertTrue(len(wf.modules[1].stderr) == 0)
        self.assertEquals(wf.modules[1].stdout[0], 'Alice\nBob')
        self.assertEquals(len(wf.modules[1].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[1].datasets)
        ds_id = wf.modules[1].datasets[DS_NAME]
        #print '(3) UPDATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(UPDATE_DATASET_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.version, 2)
        self.assertEquals(len(wf.modules), 3)
        self.assertTrue(len(wf.modules[0].stdout) == 0)
        self.assertTrue(len(wf.modules[0].stderr) == 0)
        self.assertEquals(len(wf.modules[0].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[0].datasets)
        self.assertEquals(wf.modules[0].datasets[DS_NAME], ds_id)
        self.assertTrue(len(wf.modules[1].stdout) == 1)
        self.assertTrue(len(wf.modules[1].stderr) == 0)
        self.assertEquals(wf.modules[1].stdout[0], 'Alice\nBob')
        self.assertEquals(len(wf.modules[1].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[1].datasets)
        self.assertEquals(wf.modules[1].datasets[DS_NAME], ds_id)
        self.assertTrue(len(wf.modules[2].stdout) == 0)
        self.assertTrue(len(wf.modules[2].stderr) == 0)
        self.assertEquals(len(wf.modules[2].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[2].datasets)
        self.assertNotEquals(wf.modules[2].datasets[DS_NAME], ds_id)
        #print '(4) PRINT DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(PRINT_DATASET_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        self.assertEquals(wf.version, 3)
        self.assertEquals(len(wf.modules), 4)
        self.assertEquals(wf.modules[1].stdout[0], 'Alice\nBob')
        self.assertTrue(len(wf.modules[3].stdout) == 1)
        self.assertTrue(len(wf.modules[3].stderr) == 0)
        self.assertEquals(wf.modules[3].stdout[0], 'NoName\nNoName')
        #print '(5) UPDATE DATASET WITH FILTER'
        self.db.replace_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[2].identifier,
            command=cmd.python_cell(UPDATE_DATASET_WITH_FILTER_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        self.assertTrue(wf.has_error)
        self.assertEquals(wf.version, 4)
        self.assertEquals(len(wf.modules), 4)
        # print '(6) INSERT SET VARIABLES BEFORE UPDATE'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(SET_VARIABLES_ONLY_PY),
            before_id= wf.modules[2].identifier

        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[4].stdout[0], 'Alice\nBobby')
        #print '(7) INTRODUCE ERROR'
        self.db.replace_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[1].identifier,
            command=cmd.python_cell(PRINT_UNKNOWN_DATASET_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        self.assertTrue(wf.has_error)
        # Ensure that the second module has output to stderr
        self.assertNotEquals(len( wf.modules[1].stderr), 0)
        # Ensure that the last two modules hav no output (either to STDOUT or
        # STDERR)
        for m in wf.modules[2:]:
            self.assertEquals(len(m.stdout), 0)
            self.assertEquals(len(m.stderr), 0)
        #print '(8) FIX ERROR'
        self.db.replace_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[1].identifier,
            command=cmd.python_cell(PRINT_DATASET_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        #print (9) DELETE MODULE UPDATE_DATASET_WITH_FILTER_PY
        self.db.delete_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[3].identifier
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[3].stdout[0], 'Alice\nBob')

if __name__ == '__main__':
    unittest.main()