"""Test worktrail repository implementation that uses the file system for
storage.
"""

import unittest

from vizier.config import ENGINEENV_MIMIR, env_commands
from vizier.workflow.module import ModuleSpecification

import vizier.workflow.command as cmd


class TestValidateCommand(unittest.TestCase):

    def setUp(self):
        """Create an empty work trails repository."""
        self.command_repository = env_commands(ENGINEENV_MIMIR)

    def test_validate_mimir(self):
        """Test validation of Mimir cell command specifications."""
        obj = cmd.mimir_missing_value('ds', 'colA', True)
        cmd.validate_command(self.command_repository, obj)
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            cmd.validate_command(self.command_repository, obj)
        obj = cmd.mimir_missing_value('ds', 'colA', True)
        del obj.arguments['dataset']
        with self.assertRaises(ValueError):
            cmd.validate_command(self.command_repository, obj)
        obj = cmd.mimir_missing_value('ds', 'colA', True)
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            cmd.validate_command(self.command_repository, obj)
        # KEY REPAIR
        obj = cmd.mimir_key_repair('ds', 'colA', True)
        cmd.validate_command(self.command_repository, obj)
        # MISSING KEY
        obj = cmd.mimir_missing_key('ds', 'colA', make_input_certain=True)
        cmd.validate_command(self.command_repository, obj)
        # PICKER
        obj = cmd.mimir_picker(
            'ds',
            [{'pickFrom': 'A'}, {'pickFrom': 'B'}],
            pick_as='A',
            make_input_certain=True
        )
        cmd.validate_command(self.command_repository, obj)
        # SCHEMA Matching
        obj = cmd.mimir_schema_matching(
            'ds',
            [{'column': 'colA', 'type':'int'}, {'column':'colB', 'type':'int'}],
            'myds'
        )
        cmd.validate_command(self.command_repository, obj)
        # TYPE INFERENCE
        obj = cmd.mimir_type_inference('ds', 0.6, make_input_certain=True)
        cmd.validate_command(self.command_repository, obj)

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

    def test_validate_plot(self):
        """Test validation of plot cell command specifications."""
        plot = ModuleSpecification(
            'plot',
            'CHART',
            {
                u'series': [
                    {u'series_column': u'A', u'series_label': 'Fatal', u'series_range':'0:20'},
                    {u'series_column': u'B'}
                ],
                u'chart': {
                    u'chartType': u'Bar Chart',
                    u'chartGrouped': True
                },
                u'name': u'My Chart',
                u'xaxis': {u'xaxis_column': u'Year'},
                u'dataset': u'accidents'
            }
        )
        cmd.validate_command(self.command_repository, plot)


    def test_validate_python(self):
        """Test validation of python cell command specifications."""
        cmd.validate_command(self.command_repository, cmd.python_cell('print 2'))
        with self.assertRaises(ValueError):
            cmd.validate_command(
                self.command_repository,
                ModuleSpecification(
                    cmd.PACKAGE_PYTHON,
                    cmd.PYTHON_CODE,
                    {'content' : 'abc'}
                )
            )
        obj = cmd.python_cell('print 2')
        obj.arguments['content'] = 'abc'
        with self.assertRaises(ValueError):
            cmd.validate_command(self.command_repository, obj)

    def test_validate_vizual(self):
        """Test validation ofVizUAL cell command specifications."""
        # DELETE COLUMN
        obj = cmd.delete_column('dataset', 'column')
        cmd.validate_command(self.command_repository, obj)
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            cmd.validate_command(self.command_repository, obj)
        obj = cmd.delete_column('dataset', 'column')
        del obj.arguments['dataset']
        with self.assertRaises(ValueError):
            cmd.validate_command(self.command_repository, obj)
        obj = cmd.delete_column('dataset', 'column')
        obj.arguments['row'] = 'row'
        with self.assertRaises(ValueError):
            cmd.validate_command(self.command_repository, obj)
        # DELETE ROW
        obj = cmd.delete_row('dataset', 'row')
        cmd.validate_command(self.command_repository, obj)
        # INSERT COLUMN
        obj = cmd.insert_column('dataset', 1, 'A')
        cmd.validate_command(self.command_repository, obj)
        # INSERT ROW
        obj = cmd.insert_row('dataset', 1)
        cmd.validate_command(self.command_repository, obj)
        # LOAD DATASET
        obj = cmd.load_dataset('file', 'dataset')
        cmd.validate_command(self.command_repository, obj)
        # MOVE COLUMN
        obj = cmd.move_column('dataset', 'A', 2)
        cmd.validate_command(self.command_repository, obj)
        # MOVE ROW
        obj = cmd.move_row('dataset', 1, 2)
        cmd.validate_command(self.command_repository, obj)
        # RENAME COLUMN
        obj = cmd.rename_column('dataset', 'A', 'B')
        cmd.validate_command(self.command_repository, obj)
        # UPDATE CELL
        obj = cmd.update_cell('dataset', 'A', 1, 'X')
        cmd.validate_command(self.command_repository, obj)
        # Unknown VizUAL Command
        obj = {
            'name' : 'unknown',
            'arguments': {
                'dataset': '1',
                'name': '2',
                'position': '3'
            }
        }
        with self.assertRaises(ValueError):
            cmd.validate_command(
                self.command_repository,
                ModuleSpecification(cmd.PACKAGE_VIZUAL, 'unknown', obj)
            )


if __name__ == '__main__':
    unittest.main()
