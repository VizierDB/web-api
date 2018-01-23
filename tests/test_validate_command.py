"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

from vizier.config import ENGINE_MIMIR
from vizier.datastore.mem import InMemDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.base import DEFAULT_BRANCH
from vizier.workflow.repository.engine.viztrails import DefaultViztrailsEngine
from vizier.workflow.module import ModuleSpecification
from vizier.workflow.vizual.base import DefaultVizualEngine

import vizier.workflow.repository.command as cmd


FILESERVER_DIR = './env/fs'


class TestValidateCommand(unittest.TestCase):

    def setUp(self):
        """Create an empty work trails repository."""
        # Drop project descriptor directory
        if os.path.isdir(FILESERVER_DIR):
            shutil.rmtree(FILESERVER_DIR)
        # Setup project repository
        self.datastore = InMemDataStore()
        self.fs = DefaultFileServer(FILESERVER_DIR)
        vizual = DefaultVizualEngine(self.datastore, self.fs)
        self.engine = DefaultViztrailsEngine(ENGINE_MIMIR, vizual, self.datastore)

    def tearDown(self):
        """Clean-up by dropping the MongoDB colelction used by the engine.
        """
        shutil.rmtree(FILESERVER_DIR)

    def test_validate_mimir(self):
        """Test validation of Mimir cell command specifications."""
        obj = cmd.mimir_missing_value('ds', 'colA', True)
        self.engine.validate_command(obj)
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            self.engine.validate_command(obj)
        obj = cmd.mimir_missing_value('ds', 'colA', True)
        del obj.arguments['dataset']
        with self.assertRaises(ValueError):
            self.engine.validate_command(obj)
        obj = cmd.mimir_missing_value('ds', 'colA', True)
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            self.engine.validate_command(obj)
        # KEY REPAIR
        obj = cmd.mimir_key_repair('ds', 'colA', True)
        self.engine.validate_command(obj)
        # MISSING KEY
        obj = cmd.mimir_missing_key('ds', 'colA', make_input_certain=True)
        self.engine.validate_command(obj)
        # PICKER
        obj = cmd.mimir_picker('ds', [{'pickFrom': 'A', 'pickAs': 'a'}, {'pickFrom': 'B'}], make_input_certain=True)
        self.engine.validate_command(obj)
        # SCHEMA Matching
        obj = cmd.mimir_schema_matching('ds', [{'column': 'colA', 'type':'int'}, {'column':'colB', 'type':'int'}], 'myds')
        self.engine.validate_command(obj)
        # TYPE INFERENCE
        obj = cmd.mimir_type_inference('ds', 0.6, make_input_certain=True)
        self.engine.validate_command(obj)

    def test_validate_nested(self):
        """Validate nested parameter specification."""
        spec = dict()
        spec['dbname'] = cmd.parameter_specification('dbname', '', '', 0, label='name')
        spec['tables'] = cmd.parameter_specification('tables', '', '', 1)
        spec['tabname'] = cmd.parameter_specification('tabname', '', '', 2, label='name', parent='tables')
        spec['schema'] = cmd.parameter_specification('schema', '', '', 3, parent='tables')
        spec['column'] = cmd.parameter_specification('column', '', '', 4, parent='schema')
        spec['type'] = cmd.parameter_specification('type', '', '', 5, parent='schema')
        args = {
            'name': 'My Name',
            'tables': [{
                'name': 'T1',
                'schema': [{
                    'column': 'A',
                    'type': 'int'
                },
                {
                    'column': 'B',
                    'type': 'varchar'
                }]
            },
            {
                'name': 'T1',
                'schema': [{
                    'column': 'A',
                    'type': 'int'
                },
                {
                    'column': 'B',
                    'type': 'varchar'
                }]
            }]
        }
        cmd.validate_arguments(spec, args)
        del spec['type']
        with self.assertRaises(ValueError):
            cmd.validate_arguments(spec, args)

    def test_validate_python(self):
        """Test validation of python cell command specifications."""
        self.engine.validate_command(cmd.python_cell('print 2'))
        with self.assertRaises(ValueError):
            self.engine.validate_command(ModuleSpecification(cmd.MODTYPE_PYTHON, cmd.PYTHON_CODE, {'content' : 'abc'}))
        obj = cmd.python_cell('print 2')
        obj.arguments['content'] = 'abc'
        with self.assertRaises(ValueError):
            self.engine.validate_command(obj)

    def test_validate_vizual(self):
        """Test validation ofVizUAL cell command specifications."""
        # DELETE COLUMN
        obj = cmd.delete_column('dataset', 'column')
        self.engine.validate_command(obj)
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            self.engine.validate_command(obj)
        obj = cmd.delete_column('dataset', 'column')
        del obj.arguments['dataset']
        with self.assertRaises(ValueError):
            self.engine.validate_command(obj)
        obj = cmd.delete_column('dataset', 'column')
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            self.engine.validate_command(obj)
        # DELETE ROW
        obj = cmd.delete_row('dataset', 'row')
        self.engine.validate_command(obj)
        # INSERT COLUMN
        obj = cmd.insert_column('dataset', 1, 'A')
        self.engine.validate_command(obj)
        # INSERT ROW
        obj = cmd.insert_row('dataset', 1)
        self.engine.validate_command(obj)
        # LOAD DATASET
        obj = cmd.load_dataset('file', 'dataset')
        self.engine.validate_command(obj)
        # MOVE COLUMN
        obj = cmd.move_column('dataset', 'A', 2)
        self.engine.validate_command(obj)
        # MOVE ROW
        obj = cmd.move_row('dataset', 1, 2)
        self.engine.validate_command(obj)
        # RENAME COLUMN
        obj = cmd.rename_column('dataset', 'A', 'B')
        self.engine.validate_command(obj)
        # UPDATE CELL
        obj = cmd.update_cell('dataset', 'A', 1, 'X')
        self.engine.validate_command(obj)
        # Unknown VizUAL Command
        obj = {'name' : 'unknown', 'arguments': {'dataset': '1', 'name': '2', 'position': '3'}}
        with self.assertRaises(ValueError):
            self.engine.validate_command(ModuleSpecification(cmd.MODTYPE_VIZUAL, 'unknown', obj))


if __name__ == '__main__':
    unittest.main()
