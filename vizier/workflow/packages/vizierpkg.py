import csv
import sys
import urllib

from StringIO import StringIO

from vistrails.core.modules.vistrails_module import Module, NotCacheable

import vistrails.packages.mimir.init as mimir

from vizier.core.util import is_valid_name, get_unique_identifier
from vizier.datastore.metadata import DatasetMetadata
from vizier.datastore.mimir import COL_PREFIX, ROW_ID
from vizier.datastore.mimir import MimirDatasetColumn, MimirDatasetDescriptor
from vizier.workflow.base import TXT_NORMAL, TXT_ERROR

import vizier.workflow.repository.command as cmd

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

    def compute(self):
        # Get input arguments
        lens = self.get_input('name')
        args = self.get_input('arguments')
        context = self.get_input('context')
        # Module outputs
        outputs = dict({TXT_NORMAL: list(), TXT_ERROR: list()})
        store_as_dataset = None
        # Get dataset. Raise exception if dataset is unknown
        ds_name = get_argument(cmd.PARA_DATASET, args).lower()
        dataset_id = context.get_dataset_identifier(ds_name)
        if not dataset_id in context.datastore.datasets:
            raise ValueError('unknown dataset \'' + dataset_id + '\'')
        dataset = context.datastore.datasets[dataset_id]
        mimir_table_name = dataset.table_name
        if lens == cmd.MIMIR_KEY_REPAIR:
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            params = [COL_PREFIX + str(dataset.column_index(c_col))]
        elif lens == cmd.MIMIR_MISSING_KEY:
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            params = [COL_PREFIX + str(dataset.column_index(c_col))]
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
            # Now prepare to run the same lens on ROW_ID
            params = [ROW_ID] + ['MISSING_ONLY(FALSE)']
        elif lens == cmd.MIMIR_MISSING_VALUE:
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            params = [COL_PREFIX + str(dataset.column_index(c_col))]
        elif lens == cmd.MIMIR_PICKER:
            pick_from = list()
            pick_as = list()
            for col in get_argument(cmd.PARA_SCHEMA, args):
                c_idx = dataset.column_index(get_argument(cmd.PARA_PICKFROM, col))
                column = dataset.columns[c_idx]
                if cmd.PARA_PICKAS in col:
                    c_as = col[cmd.PARA_PICKAS]
                else:
                    c_as = 'Pick_' + column.name
                pick_from.append(COL_PREFIX + str(c_idx))
                pick_as.append(COL_PREFIX + str(dataset.column_counter))
                dataset.columns.append(
                    MimirDatasetColumn(
                        dataset.column_counter,
                        c_as,
                        COL_PREFIX + str(dataset.column_counter)
                    )
                )
                dataset.column_counter += 1
            params = ['PICK_FROM(' + ','.join(pick_from) + ')']
            params.append('PICK_AS(' + ','.join(pick_as) + ')')
            print params
        elif lens == cmd.MIMIR_SCHEMA_MATCHING:
            store_as_dataset = get_argument(cmd.PARA_RESULT_DATASET, args)
            if context.has_dataset_identifier(store_as_dataset):
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
        if lens == cmd.MIMIR_SCHEMA_MATCHING:
            outputs[TXT_NORMAL].append('Created ' + lens_name + ' as ' + store_as_dataset)
        else:
            outputs[TXT_NORMAL].append('Created ' + lens_name)
        # Create datastore entry for lens.
        if not store_as_dataset is None:
            ds = context.datastore.create_dataset(column_names, lens_name)
            context.set_dataset_identifier(store_as_dataset, ds.identifier)
        else:
            ds_id = get_unique_identifier()
            ds = MimirDatasetDescriptor(
                ds_id,
                dataset.columns,
                lens_name,
                dataset.row_ids,
                dataset.column_counter,
                dataset.row_counter,
                dataset.annotations
            )
            context.datastore.update_dataset(ds_id, ds)
            context.set_dataset_identifier(ds_name, ds.identifier)
        # Set the module outputs
        self.set_output('output', outputs)


class PythonCell(NotCacheable, Module):
    _input_ports = [
        ('source', 'basic:String'),
        ('context', 'basic:Dictionary')
    ]
    _output_ports = [
        ('output', 'basic:Dictionary')
    ]

    def compute(self):
        # Get Python source code that is execyted in this cell and the global
        # variables
        source = urllib.unquote(self.get_input('source'))
        local = self.get_input('context').variables
        # Redirect standard output and standard error
        out = sys.stdout
        err = sys.stderr
        stream = []
        sys.stdout = FakeStream('out', stream)
        sys.stderr = FakeStream('err', stream)
        # Run the Pyhton code
        try:
            exec source in local, local
        except Exception as ex:
            template = "{0}:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            sys.stderr.write(str(message) + '\n')
        finally:
            sys.stdout = out
            sys.stderr = err
        # Set module outputs
        self.set_output('context', local)
        outputs = dict({TXT_NORMAL: list(), TXT_ERROR: list()})
        for tag, text in stream:
            key = TXT_NORMAL if tag == 'out' else TXT_ERROR
            outputs[key].append(''.join(text).strip())
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
        ('output', 'basic:Dictionary')
    ]

    def compute(self):
        """Excute the specified VizUAL command on the current database state.
        Will raise ValueError if the referenced datasets does not exist.
        """
        name = self.get_input('name')
        args = self.get_input('arguments')
        context = self.get_input('context')
        # Set VizUAL engine (shortcut)
        v_eng = context.vizual
        outputs = dict({TXT_NORMAL: list(), TXT_ERROR: list()})
        if name == cmd.VIZUAL_DEL_COL:
            # Get dataset name, and column specification. Raise exception if
            # the specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            ds = context.get_dataset_identifier(ds_name)
            # Execute delete column command and set number of affected
            # columns in output
            col_count, ds_id = v_eng.delete_column(ds, c_col)
            context.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' column deleted')
        elif name == cmd.VIZUAL_DEL_ROW:
            # Get dataset name, and row index. Raise exception if the
            # specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_row = get_argument(cmd.PARA_ROW, args, as_int=True)
            ds = context.get_dataset_identifier(ds_name)
            # Execute delete row command and set number of affected rows in
            # output
            col_count, ds_id = v_eng.delete_row(ds, c_row)
            context.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' row deleted')
        elif name == cmd.VIZUAL_INS_COL:
            # Get dataset name, column index, and new column name. Raise
            # exception if the specified dataset does not exist or the
            # column position is not an integer.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_pos = int(get_argument(cmd.PARA_POSITION, args))
            c_name = get_argument(cmd.PARA_NAME, args)
            ds = context.get_dataset_identifier(ds_name)
            # Execute insert column command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # columns in output
            col_count, ds_id = v_eng.insert_column(ds, c_pos, c_name)
            context.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' column inserted')
        elif name == cmd.VIZUAL_INS_ROW:
            # Get dataset name, and row index. Raise exception if the
            # specified dataset does not exist or row index is not an int.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_row = int(get_argument(cmd.PARA_POSITION, args))
            ds = context.get_dataset_identifier(ds_name)
            # Execute insert row command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # rows in output
            col_count, ds_id = v_eng.insert_row(ds, c_row)
            context.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' row inserted')
        elif name == cmd.VIZUAL_LOAD:
            # Get the name of the file and dataset name from command
            # arguments. Raise exception if a dataset with the specified
            # name already exsists in the project or if the given name is
            # not a valid dataset name
            ds_file = get_argument(cmd.PARA_FILE, args)
            ds_name = get_argument(cmd.PARA_NAME, args).lower()
            if context.has_dataset_identifier(ds_name):
                raise ValueError('dataset \'' + ds_name + '\' exists')
            if not is_valid_name(ds_name):
                raise ValueError('invalid dataset name \'' + ds_name + '\'')
            # Execute VizUAL creat dataset command. Add new dataset to
            # dictionary and add dataset schema and row count to output
            ds = v_eng.load_dataset(ds_file)
            context.set_dataset_identifier(ds_name, ds.identifier)
            outputs[TXT_NORMAL].append(ds_name + ' (')
            for i in range(len(ds.columns)):
                text = '  ' + ds.columns[i].name
                if i != len(ds.columns) - 1:
                    text += ','
                outputs[TXT_NORMAL].append(text)
            outputs[TXT_NORMAL].append(')')
            outputs[TXT_NORMAL].append(str(len(ds.rows)) + ' row(s)')
        elif name == cmd.VIZUAL_MOV_COL:
            # Get dataset name, column name, and target position. Raise
            # exception if the specified dataset does not exist or the
            # target position is not an integer.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_pos = int(get_argument(cmd.PARA_POSITION, args))
            c_name = get_argument(cmd.PARA_COLUMN, args)
            ds = context.get_dataset_identifier(ds_name)
            # Execute move column command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # columns in output
            col_count, ds_id = v_eng.move_column(ds, c_name, c_pos)
            context.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' column moved')
        elif name == cmd.VIZUAL_MOV_ROW:
            # Get dataset name, row index, and target index. Raise exception
            # if the specified dataset does not exist or if either of the
            # row indexes are not an integer.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_row = int(get_argument(cmd.PARA_ROW, args))
            c_pos = int(get_argument(cmd.PARA_POSITION, args))
            ds = context.get_dataset_identifier(ds_name)
            # Execute insert row command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # rows in output
            col_count, ds_id = v_eng.move_row(ds, c_row, c_pos)
            context.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' row moved')
        elif name == cmd.VIZUAL_REN_COL:
            # Get dataset name, column specification, and new column nmae.
            # Raise exception if the specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            c_name = get_argument(cmd.PARA_NAME, args)
            ds = context.get_dataset_identifier(ds_name)
            # Execute rename colum command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # columns in output.
            col_count, ds_id = v_eng.rename_column(ds, c_col, c_name)
            context.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(col_count) + ' column renamed')
        elif name == cmd.VIZUAL_UPD_CELL:
            # Get dataset name, cell coordinates, and update value. Raise
            # exception if the specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            c_row = get_argument(cmd.PARA_ROW, args, as_int=True)
            c_val = get_argument(cmd.PARA_VALUE, args)
            ds = context.get_dataset_identifier(ds_name)
            # Execute update cell command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # rows in output
            upd_count, ds_id = v_eng.update_cell(ds, c_col, c_row, c_val)
            context.set_dataset_identifier(ds_name, ds_id)
            outputs[TXT_NORMAL].append(str(upd_count) + ' row updated')
        else:
            raise ValueError('unknown vizual command \'' + str(name) + '\'')
        # Set the module outputs
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


# Package modules
_modules = [MimirLens, PythonCell, VizualCell]
