import copy
import csv
import sys
import urllib

from StringIO import StringIO

from vistrails.core.modules.vistrails_module import Module, NotCacheable

import vistrails.packages.mimir.init as mimir

from vizier.core.util import is_valid_name, get_unique_identifier
from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.mem import VolatileDataStore
from vizier.datastore.metadata import DatasetMetadata
from vizier.datastore.mimir import COL_PREFIX, ROW_ID
from vizier.datastore.mimir import MimirDatasetColumn
from vizier.datastore.mimir import MimirDataStore, create_missing_key_view
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.base import TXT_NORMAL, TXT_ERROR
from vizier.workflow.context import VizierDBClient
from vizier.workflow.vizual.base import DefaultVizualEngine
from vizier.workflow.vizual.mimir import MimirVizualEngine

import vizier.config as config
import vizier.workflow.command as cmd
import vizier.workflow.context as ctx


name = 'Vizier'
identifier = 'org.vistrails.vistrails.vizier'
version = '0.1'


class FakeStream(object):
    def __init__(self, tag, stream):
        self.closed = False
        self._tag = tag
        self._stream = stream

    def close(self):
        self.closed = True

    def flush(self):
        pass

    def writelines(self, iterable):
        for text in iterable:
            self.write(text)

    def write(self, text):
        if self._stream and self._stream[-1][0] == self._tag:
            self._stream[-1][1].append(text)
        else:
            self._stream.append((self._tag, [text]))


class MimirLens(Module):
    """Creates a Lens in mimir specific type."""
    _input_ports = [
        ('name', 'basic:String'),
        ('arguments', 'basic:Dictionary'),
        ('context', 'basic:Dictionary')
    ]
    _output_ports = [
        ('context', 'basic:Dictionary')
    ]

    def compute(self):
        # Get input arguments
        lens = self.get_input('name')
        args = self.get_input('arguments')
        context = self.get_input('context')
        # Get module identifier and VizierDB client for current workflow state
        module_id = self.moduleInfo['moduleId']
        vizierdb = get_env(module_id, context)
        # Module outputs
        outputs = dict({TXT_NORMAL: list(), TXT_ERROR: list()})
        store_as_dataset = None
        update_rows = False
        # Get dataset. Raise exception if dataset is unknown
        ds_name = get_argument(cmd.PARA_DATASET, args).lower()
        dataset_id = vizierdb.get_dataset_identifier(ds_name)
        dataset = vizierdb.datastore.get_dataset_descriptor(dataset_id)
        if dataset is None:
            raise ValueError('unknown dataset \'' + ds_name + '\'')
        mimir_table_name = dataset.table_name
        if lens == cmd.MIMIR_KEY_REPAIR:
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            column = dataset.columns[dataset.column_index(c_col)]
            params = [column.name_in_rdb]
            update_rows = True
        elif lens == cmd.MIMIR_MISSING_KEY:
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            column = dataset.columns[dataset.column_index(c_col)]
            params = [column.name_in_rdb]
            # Set MISSING ONLY to FALSE to ensure that all rows are returned
            params += ['MISSING_ONLY(FALSE)']
            # Need to run this lens twice in order to generate row ids for
            # any potential new tuple
            mimir_table_name = mimir._mimir.createLens(
                dataset.table_name,
                mimir._jvmhelper.to_scala_seq(params),
                lens,
                get_argument(cmd.PARA_MAKE_CERTAIN, args),
                False
            )
        elif lens == cmd.MIMIR_MISSING_VALUE or lens == cmd.MIMIR_DOMAIN:
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            column = dataset.columns[dataset.column_index(c_col)]
            params = [column.name_in_rdb]
        elif lens == cmd.MIMIR_PICKER:
            pick_from = list()
            column_names = list()
            for col in get_argument(cmd.PARA_SCHEMA, args):
                c_idx = dataset.column_index(get_argument(cmd.PARA_PICKFROM, col))
                column = dataset.columns[c_idx]
                pick_from.append(column.name_in_rdb)
                column_names.append(column.name.upper())
            # Add result column to dataset schema
            pick_as = ''
            if cmd.PARA_PICKAS in args:
                pick_as = args[cmd.PARA_PICKAS].strip()
            if pick_as == '':
                pick_as = 'PICK_ONE_' + '_'.join(column_names)
            target_column = COL_PREFIX + str(dataset.column_counter)
            dataset.columns.append(
                MimirDatasetColumn(
                    dataset.column_counter,
                    pick_as,
                    target_column
                )
            )
            dataset.column_counter += 1
            params = ['PICK_FROM(' + ','.join(pick_from) + ')']
            params.append('PICK_AS(' + target_column + ')')
        elif lens == cmd.MIMIR_SCHEMA_MATCHING:
            store_as_dataset = get_argument(cmd.PARA_RESULT_DATASET, args)
            if vizierdb.has_dataset_identifier(store_as_dataset):
                raise ValueError('dataset \'' + store_as_dataset + '\' exists')
            if not is_valid_name(store_as_dataset):
                raise ValueError('invalid dataset name \'' + store_as_dataset + '\'')
            column_names = list()
            params = ['\'' + ROW_ID + ' int\'']
            for col in get_argument(cmd.PARA_SCHEMA, args):
                c_name = get_argument(cmd.PARA_COLUMN, col)
                c_type = get_argument(cmd.PARA_TYPE, col)
                params.append('\'' + COL_PREFIX + str(len(column_names)) + ' ' + c_type + '\'')
                column_names.append(c_name)
        elif lens == cmd.MIMIR_TYPE_INFERENCE:
            params = [str(get_argument(cmd.PARA_PERCENT_CONFORM, args))]
        else:
            raise ValueError('unknown Mimir lens \'' + str(lens) + '\'')
        # Create Mimir lens
        if lens in [cmd.MIMIR_SCHEMA_MATCHING, cmd.MIMIR_TYPE_INFERENCE]:
            lens_name = mimir._mimir.createAdaptiveSchema(
                mimir_table_name,
                mimir._jvmhelper.to_scala_seq(params),
                lens
            )
        else:
            lens_name = mimir._mimir.createLens(
                mimir_table_name,
                mimir._jvmhelper.to_scala_seq(params),
                lens,
                get_argument(cmd.PARA_MAKE_CERTAIN, args),
                False
            )
        # Create a view including missing row ids for the result of a
        # MISSING KEY lens
        if lens == cmd.MIMIR_MISSING_KEY:
            lens_name, row_counter = create_missing_key_view(
                dataset,
                lens_name,
                column.name_in_rdb
            )
            dataset.row_counter = row_counter
        # Create datastore entry for lens.
        if not store_as_dataset is None:
            columns = list()
            for c_name in column_names:
                col_id = len(columns)
                columns.append(MimirDatasetColumn(
                    col_id,
                    c_name,
                    COL_PREFIX + str(col_id)
                ))
            #ds = vizierdb.datastore.create_dataset(table_name, columns)
            ds, _ = vizierdb.datastore.register_dataset(
                table_name=lens_name,
                columns=columns,
                row_ids=dataset.row_ids,
                annotations=dataset.annotations,
                update_rows=True
            )
            ds_name = store_as_dataset
        else:
            ds, _ = vizierdb.datastore.register_dataset(
                table_name=lens_name,
                columns=dataset.columns,
                row_ids=dataset.row_ids,
                column_counter=dataset.column_counter,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations,
                update_rows=update_rows
            )
        print_dataset_schema(outputs, store_as_dataset, ds.columns)
        vizierdb.set_dataset_identifier(ds_name, ds.identifier)
        # Propagate potential changes to the dataset mappings
        propagate_changes(module_id, vizierdb.datasets, context)
        # Set the module outputs
        self.set_output('context', context)
        self.set_output('output', outputs)


class PythonCell(NotCacheable, Module):
    _input_ports = [
        ('source', 'basic:String'),
        ('context', 'basic:Dictionary')
    ]
    _output_ports = [
        ('context', 'basic:Dictionary'),
        ('output', 'basic:Dictionary')
    ]

    def compute(self):
        # Get Python source code that is execyted in this cell and the global
        # variables
        source = urllib.unquote(self.get_input('source'))
        context = self.get_input('context')
        # Get module identifier and VizierDB client for current workflow state
        module_id = self.moduleInfo['moduleId']
        vizierdb = get_env(module_id, context)
        # Get Python variables from context and set the current vizier client
        variables = context[ctx.VZRENV_VARS]
        variables[ctx.VZRENV_VARS_DBCLIENT] = vizierdb
        # Redirect standard output and standard error
        out = sys.stdout
        err = sys.stderr
        stream = []
        sys.stdout = FakeStream('out', stream)
        sys.stderr = FakeStream('err', stream)
        # Run the Pyhton code
        try:
            exec source in variables, variables
        except Exception as ex:
            template = "{0}:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            sys.stderr.write(str(message) + '\n')
        finally:
            sys.stdout = out
            sys.stderr = err
        # Propagate potential changes to the dataset mappings
        propagate_changes(module_id, vizierdb.datasets, context)
        # Set module outputs
        outputs = dict({TXT_NORMAL: list(), TXT_ERROR: list()})
        for tag, text in stream:
            key = TXT_NORMAL if tag == 'out' else TXT_ERROR
            outputs[key].append(''.join(text).strip())
        self.set_output('context', context)
        self.set_output('output', outputs)


class VizualCell(NotCacheable, Module):
    """Vistrails module to execute VizUAL commands. Expects a command type
    (name)and a dictionary of arguments that specify the actual VizUAL command
    and its arguments. The context contains the dataset mapping and reference to
    the VizUAL engine.
    """
    _input_ports = [
        ('name', 'basic:String'),
        ('arguments', 'basic:Dictionary'),
        ('context', 'basic:Dictionary')
    ]
    _output_ports = [
        ('context', 'basic:Dictionary'),
        ('output', 'basic:Dictionary')
    ]

    def compute(self):
        """Excute the specified VizUAL command on the current database state.
        Will raise ValueError if the referenced datasets does not exist.
        """
        name = self.get_input('name')
        args = self.get_input('arguments')
        context = self.get_input('context')
        # Get module identifier and VizierDB client for current workflow state
        module_id = self.moduleInfo['moduleId']
        vizierdb = get_env(module_id, context)
        # Set VizUAL engine (shortcut)
        v_eng = vizierdb.vizual
        outputs = dict({TXT_NORMAL: list(), TXT_ERROR: list()})
        if name == cmd.VIZUAL_DEL_COL:
            # Get dataset name, and column specification. Raise exception if
            # the specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute delete column command and set number of affected
            # columns in output
            col_count, ds_id = v_eng.delete_column(ds, c_col)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' column deleted')
        elif name == cmd.VIZUAL_DEL_ROW:
            # Get dataset name, and row index. Raise exception if the
            # specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_row = get_argument(cmd.PARA_ROW, args, as_int=True)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute delete row command and set number of affected rows in
            # output
            col_count, ds_id = v_eng.delete_row(ds, c_row)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' row deleted')
        elif name == cmd.VIZUAL_DROP_DS:
            # Get dataset name and remove the associated entry from the
            # dictionary of datasets in the context. Will raise exception if the
            # specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            vizierdb.remove_dataset_identifier(ds_name)
            outputs[TXT_NORMAL].append('1 dataset dropped')
        elif name == cmd.VIZUAL_INS_COL:
            # Get dataset name, column index, and new column name. Raise
            # exception if the specified dataset does not exist or the
            # column position is not an integer.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_pos = int(get_argument(cmd.PARA_POSITION, args))
            c_name = get_argument(cmd.PARA_NAME, args)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute insert column command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # columns in output
            col_count, ds_id = v_eng.insert_column(ds, c_pos, c_name)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' column inserted')
        elif name == cmd.VIZUAL_INS_ROW:
            # Get dataset name, and row index. Raise exception if the
            # specified dataset does not exist or row index is not an int.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_row = int(get_argument(cmd.PARA_POSITION, args))
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute insert row command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # rows in output
            col_count, ds_id = v_eng.insert_row(ds, c_row)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' row inserted')
        elif name == cmd.VIZUAL_LOAD:
            # Get the name of the file and dataset name from command
            # arguments. Raise exception if a dataset with the specified
            # name already exsists in the project or if the given name is
            # not a valid dataset name
            ds_file = get_argument(cmd.PARA_FILE, args)
            ds_name = get_argument(cmd.PARA_NAME, args).lower()
            if vizierdb.has_dataset_identifier(ds_name):
                raise ValueError('dataset \'' + ds_name + '\' exists')
            if not is_valid_name(ds_name):
                raise ValueError('invalid dataset name \'' + ds_name + '\'')
            # Execute VizUAL creat dataset command. Add new dataset to
            # dictionary and add dataset schema and row count to output
            ds, rows = v_eng.load_dataset(ds_file)
            vizierdb.set_dataset_identifier(ds_name, ds.identifier)
            print_dataset_schema(outputs, ds_name, ds.columns)
            outputs[TXT_NORMAL].append(str(rows) + ' row(s)')
        elif name == cmd.VIZUAL_MOV_COL:
            # Get dataset name, column name, and target position. Raise
            # exception if the specified dataset does not exist or the
            # target position is not an integer.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_pos = int(get_argument(cmd.PARA_POSITION, args))
            c_name = get_argument(cmd.PARA_COLUMN, args)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute move column command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # columns in output
            col_count, ds_id = v_eng.move_column(ds, c_name, c_pos)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' column moved')
        elif name == cmd.VIZUAL_MOV_ROW:
            # Get dataset name, row index, and target index. Raise exception
            # if the specified dataset does not exist or if either of the
            # row indexes are not an integer.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_row = int(get_argument(cmd.PARA_ROW, args))
            c_pos = int(get_argument(cmd.PARA_POSITION, args))
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute insert row command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # rows in output
            col_count, ds_id = v_eng.move_row(ds, c_row, c_pos)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' row moved')
        elif name == cmd.VIZUAL_REN_COL:
            # Get dataset name, column specification, and new column nmae.
            # Raise exception if the specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            c_name = get_argument(cmd.PARA_NAME, args)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute rename colum command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # columns in output.
            col_count, ds_id = v_eng.rename_column(ds, c_col, c_name)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' column renamed')
        elif name == cmd.VIZUAL_REN_DS:
            # Get name of existing dataset and the new dataset name. Raise
            # exception if the specified dataset does not exist, a dataset with
            # the new name already exists, or if the new dataset name is not a
            # valid name.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            new_name = get_argument(cmd.PARA_NAME, args)
            if vizierdb.has_dataset_identifier(new_name):
                raise ValueError('dataset \'' + new_name + '\' exists')
            if not is_valid_name(new_name):
                raise ValueError('invalid dataset name \'' + new_name + '\'')
            ds = vizierdb.get_dataset_identifier(ds_name)
            vizierdb.remove_dataset_identifier(ds_name)
            vizierdb.set_dataset_identifier(new_name, ds)
            outputs[TXT_NORMAL].append('1 dataset renamed')
        elif name == cmd.VIZUAL_UPD_CELL:
            # Get dataset name, cell coordinates, and update value. Raise
            # exception if the specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            c_row = get_argument(cmd.PARA_ROW, args, as_int=True)
            c_val = get_argument(cmd.PARA_VALUE, args)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute update cell command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # rows in output
            upd_count, ds_id = v_eng.update_cell(ds, c_col, c_row, c_val)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(upd_count) + ' row updated')
        else:
            raise ValueError('unknown vizual command \'' + str(name) + '\'')
        # Propagate potential changes to the dataset mappings
        propagate_changes(module_id, vizierdb.datasets, context)
        # Set the module outputs
        self.set_output('context', context)
        self.set_output('output', outputs)


# ------------------------------------------------------------------------------
#
# VizUAL Helper Methods
#
# ------------------------------------------------------------------------------

def get_argument(key, args, as_int=False):
    """Retrieve command argument with given key. Will raise ValueError if no
    argument with given key is present.

    Parameters
    ----------
    key : string
        Argument name
    args : dict(dict())
        Dictionary of command arguments
    as_int : bool
        Flag indicating whther the argument should be cobverted to int. If the
        given value cannot be converted the original argument value is returned.
    Returns
    -------
    dict
    """
    if not key in args:
        raise ValueError('missing argument: ' + key)
    val = args[key]
    if as_int:
        try:
            val = int(val)
        except ValueError as ex:
            pass
    return val


def get_env(module_id, context):
    """Get the VizierDB client for the workflow state of the given module.

    Patameters
    ----------
    module_id: int
        Unique module identifier
    context: dict
        Workflow execution context

    Returns
    -------
    vizier.workflow.context.VizierDBClient
    """
    # Get context type, environment type, and dataset mappings from the context
    # dictionary
    context_type = context[ctx.VZRENV_TYPE]
    env_type = context[ctx.VZRENV_ENV][ctx.VZRENV_ENV_IDENTIFIER]
    # Get dataset mapping for the given module. Note that the input datasets for
    # a module is the set of datasets that are in the state of the previous
    # module.
    datasets = None
    prev_map = None
    for module_map in context[ctx.VZRENV_DATASETS]:
        if module_map[ctx.VZRENV_DATASETS_MODULEID] == module_id:
            # Copy dataset mapping from previous module
            if not prev_map is None:
                datasets = dict(prev_map[ctx.VZRENV_DATASETS_MAPPING])
            else:
                datasets = dict()
            break
        prev_map = module_map
    # Get file server and datastore directories
    datastore_dir = context[ctx.VZRENV_ENV][ctx.VZRENV_ENV_DATASTORE]
    fileserver_dir = context[ctx.VZRENV_ENV][ctx.VZRENV_ENV_FILESERVER]
    # Use the default file server for vizual engine
    fileserver = DefaultFileServer(fileserver_dir)
    # Create the datastore. Use a volatile store if the context is volatile
    datastore = None
    if env_type == config.ENGINEENV_DEFAULT:
        datastore = FileSystemDataStore(datastore_dir)
    elif env_type == config.ENGINEENV_MIMIR:
        datastore = MimirDataStore(datastore_dir)
    if context_type == ctx.CONTEXT_VOLATILE:
        datastore = VolatileDataStore(datastore)
    # Create Viual engine depending on environment type
    vizual = None
    if env_type == config.ENGINEENV_DEFAULT:
        vizual = DefaultVizualEngine(datastore, fileserver)
    elif env_type == config.ENGINEENV_MIMIR:
        vizual = MimirVizualEngine(datastore, fileserver)
    # Return vizier client
    return VizierDBClient(datastore, datasets, vizual)


def print_dataset_schema(outputs, name, columns):
    """Add schema infromation for given dataset to cell output.

    Parameters
    ----------
    outputs: dict
        Cell outputs dictionary
    name: string
        Dataset name
    columns: list(vizier.datasetore.base.DatasetColumn)
        Columns in the dataset schema
    """
    outputs[TXT_NORMAL].append(name + ' (')
    for i in range(len(columns)):
        text = '  ' + str(columns[i])
        if i != len(columns) - 1:
            text += ','
        outputs[TXT_NORMAL].append(text)
    outputs[TXT_NORMAL].append(')')


def propagate_changes(module_id, datasets, context):
    """After executing the module, identify potential changes and propagate them
    to the dataset mappings in the global workflow context.

    Parameters
    ----------
    module_id: int
        Unique module identifier
    datasets: dict
        Dataset name to identifier mapping for module after execution finished
    context: dict
        Global workflow context
    """
    # Only propagate if not volatile
    if context[ctx.VZRENV_TYPE] != ctx.CONTEXT_VOLATILE:
        mappings = context[ctx.VZRENV_DATASETS]
        for i in range(len(mappings)):
            m_map = mappings[i]
            if m_map[ctx.VZRENV_DATASETS_MODULEID] == module_id:
                m_map[ctx.VZRENV_DATASETS_MAPPING] = datasets
                if i < len(mappings) - 1:
                    mappings[i+1][ctx.VZRENV_DATASETS_MAPPING] = dict(datasets)
                # Clear the datasets for the remaining modules
                for j in range(i+2,len(mappings)):
                    mappings[j][ctx.VZRENV_DATASETS_MAPPING] = dict()
                break


# Package modules
_modules = [MimirLens, PythonCell, VizualCell]
