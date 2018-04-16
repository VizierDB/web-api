"""Specifications for supported workflow module types."""

from vizier.workflow.module import ModuleSpecification


# ------------------------------------------------------------------------------
# Module Specifications
# ------------------------------------------------------------------------------

"""Definition of parameter data types."""
DT_AS_ROW = 'asRow'
DT_BOOL = 'bool'
DT_COLUMN_ID = 'colid'
DT_DATASET_ID = 'datasetid'
DT_DECIMAL = 'decimal'
DT_FILE_ID = 'fileid'
DT_GROUP = 'group'
DT_INT = 'int'
DT_PYTHON_CODE = 'pyCode'
DT_ROW_ID = 'rowid'
DT_STRING = 'string'

DATA_TYPES = [
    DT_AS_ROW,
    DT_BOOL,
    DT_COLUMN_ID,
    DT_DATASET_ID,
    DT_DECIMAL,
    DT_FILE_ID,
    DT_GROUP,
    DT_INT,
    DT_PYTHON_CODE,
    DT_ROW_ID,
    DT_STRING
]

"""Definition of common module parameter names."""
PARA_CHART = 'chart'
PARA_CHART_TYPE = 'chartType'
PARA_CHART_GROUPED = 'chartGrouped'
PARA_COLUMN = 'column'
PARA_CONSTRAINT = 'constraint'
PARA_DATASET = 'dataset'
PARA_FILE = 'file'
PARA_LABEL = 'label'
PARA_MAKE_CERTAIN = 'makeInputCertain'
PARA_NAME = 'name'
PARA_PERCENT_CONFORM = 'percentConform'
PARA_PICKAS = 'pickAs'
PARA_PICKFROM = 'pickFrom'
PARA_POSITION = 'position'
PARA_RANGE = 'range'
PARA_RESULT_DATASET = 'resultName'
PARA_ROW = 'row'
PARA_SCHEMA = 'schema'
PARA_SERIES = 'series'
PARA_TYPE = 'type'
PARA_VALUE = 'value'
PARA_XAXIS = 'xaxis'


def para_column(index, parent=None):
    """Return dictionary specifying the default column parameter used by most
    modules.

    Returns
    -------
    dict
    """
    return parameter_specification(PARA_COLUMN, 'Column', DT_COLUMN_ID, index, parent=parent)


def para_dataset(index):
    """Return dictionary specifying the default dataset parameter used by most
    modules.

    Returns
    -------
    dict
    """
    return parameter_specification(PARA_DATASET, 'Dataset', DT_DATASET_ID, index)


def para_make_input_certain(index):
    """Return dictionary for 'makeInputCertain' parameter of Mimir lenses.

    Returns
    -------
    dict
    """
    return parameter_specification(
        PARA_MAKE_CERTAIN,
        'Make Input Certain',
        DT_BOOL,
        index,
        required=False
    )


def para_position(index):
    """Return dictionary for position parameter used by some Vizual moduels.

    Returns
    -------
    dict
    """
    return parameter_specification(PARA_POSITION, 'Position', DT_INT, index)


def para_row(index):
    """Return dictionary specifying the default row parameter used by most
    modules.

    Returns
    -------
    dict
    """
    return parameter_specification(PARA_ROW, 'Row', DT_ROW_ID, index)


def parameter_specification(
        identifier, name, data_type, index, label=None, required=True,
        values=None, parent=None
    ):
    """Create a dictionary that contains a module parameter specification.

    Parameters
    ----------
    identifier: string
        Unique parameter identifier
    name: string
        Printable parameter name
    data_type: string
        Parameter type
    index: int
        Index position of argument in input form
    required: bool, optional
        Required flag
    values: list, optional
        List of valid parameter values
    parent: int, optional
        Identifier of a grouping element

    Returns
    -------
    dict
    """
    if not data_type in DATA_TYPES:
        raise ValueError('invalid parameter data type \'' + data_type + '\'')
    para = {
        'id': identifier,
        'name': name,
        'datatype': data_type,
        'index': index,
        'required': required
    }
    if not label is None:
        para['label'] = label
    else:
        para['label'] = identifier
    if not values is None:
        para['values'] = values
    if not parent is None:
        para['parent'] = parent
    return para




"""Identifier for currently supported module types."""
PACKAGE_MIMIR = 'mimir'
PACKAGE_PLOT = 'plot'
PACKAGE_PYTHON = 'python'
PACKAGE_SYS = '_sys'
PACKAGE_VIZUAL = 'vizual'

MODULE_NAME = 'name'
MODULE_ARGUMENTS = 'arguments'

"""Identifier for plot commands."""
PLOT_SIMPLE_CHART = 'CHART'

"""Identifier for Mimir lenses."""
MIMIR_DOMAIN = 'DOMAIN'
MIMIR_KEY_REPAIR ='KEY_REPAIR'
MIMIR_MISSING_KEY ='MISSING_KEY'
MIMIR_MISSING_VALUE = 'MISSING_VALUE'
MIMIR_PICKER ='PICKER'
MIMIR_SCHEMA_MATCHING ='SCHEMA_MATCHING'
MIMIR_TYPE_INFERENCE ='TYPE_INFERENCE'

"""Components for Python requests."""
PYTHON_SOURCE = 'source'

"""Identifier for Python commands."""
PYTHON_CODE = 'CODE'

"""Identifier for sysyem commands."""
SYS_CREATE_BRANCH = 'CREATE_BRANCH'
SYS_DELETE_MODULE = 'DELETE_MODULE'

"""Identifier for VizUAL commands."""
VIZUAL_DEL_COL = 'DELETE_COLUMN'
VIZUAL_DEL_ROW = 'DELETE_ROW'
VIZUAL_DROP_DS = 'DROP_DATASET'
VIZUAL_INS_COL = 'INSERT_COLUMN'
VIZUAL_INS_ROW = 'INSERT_ROW'
VIZUAL_LOAD = 'LOAD'
VIZUAL_MOV_COL = 'MOVE_COLUMN'
VIZUAL_MOV_ROW = 'MOVE_ROW'
VIZUAL_REN_COL = 'RENAME_COLUMN'
VIZUAL_REN_DS = 'RENAME_DATASET'
VIZUAL_UPD_CELL = 'UPDATE_CELL'

"""Mimir lens specification schema."""
MIMIR_LENSES = {
    MIMIR_DOMAIN: {
        MODULE_NAME: 'Domain Lens',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMN: para_column(1),
            PARA_MAKE_CERTAIN: para_make_input_certain(2)
        }
    },
    MIMIR_KEY_REPAIR: {
        MODULE_NAME: 'Key Repair Lens',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMN: para_column(1),
            PARA_MAKE_CERTAIN: para_make_input_certain(2)
        }
    },
    MIMIR_MISSING_VALUE: {
        MODULE_NAME: 'Missing Value Lens',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMN: para_column(1),
            PARA_CONSTRAINT: parameter_specification(
                PARA_CONSTRAINT,
                'Constraint',
                DT_STRING,
                2,
                required=False
            ),
            PARA_MAKE_CERTAIN: para_make_input_certain(3)
        }
    },
    MIMIR_MISSING_KEY: {
        MODULE_NAME: 'Missing Key Lens',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMN: para_column(1),
            PARA_MAKE_CERTAIN: para_make_input_certain(2)
        }
    },
    MIMIR_PICKER: {
        MODULE_NAME: 'Picker Lens',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_SCHEMA: parameter_specification(
                PARA_SCHEMA,
                'Columns',
                DT_GROUP,
                1
            ),
            PARA_PICKFROM: parameter_specification(
                PARA_PICKFROM,
                'Pick From',
                DT_COLUMN_ID,
                2,
                parent=PARA_SCHEMA
            ),
            PARA_PICKAS: parameter_specification(
                PARA_PICKAS,
                'Pick As',
                DT_STRING,
                3,
                required=False
            ),
            PARA_MAKE_CERTAIN: para_make_input_certain(4)
        }
    },
    MIMIR_SCHEMA_MATCHING: {
        MODULE_NAME: 'Schema Matching Lens',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_SCHEMA: parameter_specification(
                PARA_SCHEMA,
                'Schema',
                DT_GROUP,
                1
            ),
            PARA_COLUMN: parameter_specification(
                PARA_COLUMN,
                'Column Name',
                DT_STRING,
                2,
                parent=PARA_SCHEMA
            ),
            PARA_TYPE: parameter_specification(
                PARA_TYPE,
                'Data Type',
                DT_STRING,
                3,
                values=['int', 'varchar'],
                parent=PARA_SCHEMA
            ),
            PARA_RESULT_DATASET: parameter_specification(
                PARA_RESULT_DATASET,
                'Store Result As ...',
                DT_STRING,
                4
            ),
            PARA_MAKE_CERTAIN: para_make_input_certain(5)
        }
    },
    MIMIR_TYPE_INFERENCE: {
        MODULE_NAME: 'Type Inference Lens',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_PERCENT_CONFORM: parameter_specification(
                PARA_PERCENT_CONFORM,
                'Percent Conform',
                DT_DECIMAL,
                1
            ),
            PARA_MAKE_CERTAIN: para_make_input_certain(2)
        }
    }
}

"""Plot commands."""
PLOT_COMMANDS = {
    PLOT_SIMPLE_CHART: {
        MODULE_NAME: 'Simple Chart',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_NAME: parameter_specification(PARA_NAME, 'Chart Name', DT_STRING, 1),
            PARA_SERIES: parameter_specification(
                PARA_SERIES,
                'Data Series',
                DT_GROUP,
                2
            ),
            PARA_SERIES + '_' + PARA_COLUMN: parameter_specification(
                PARA_SERIES + '_' + PARA_COLUMN,
                'Column',
                DT_DATASET_ID,
                3,
                parent=PARA_SERIES
            ),
            PARA_SERIES + '_' + PARA_RANGE: parameter_specification(
                PARA_SERIES + '_' + PARA_RANGE,
                'Range',
                DT_STRING,
                4,
                parent=PARA_SERIES,
                required=False
            ),
            PARA_SERIES + '_' + PARA_LABEL: parameter_specification(
                PARA_SERIES + '_' + PARA_LABEL,
                'Label',
                DT_STRING,
                5,
                parent=PARA_SERIES,
                required=False
            ),
            PARA_XAXIS: parameter_specification(
                PARA_XAXIS,
                'X-Axis',
                DT_AS_ROW,
                6
            ),
            PARA_XAXIS + '_' + PARA_COLUMN: parameter_specification(
                PARA_XAXIS + '_' + PARA_COLUMN,
                'Column',
                DT_DATASET_ID,
                7,
                parent=PARA_XAXIS
            ),
            PARA_XAXIS + '_' + PARA_RANGE: parameter_specification(
                PARA_XAXIS + '_' + PARA_RANGE,
                'Range',
                DT_STRING,
                8,
                parent=PARA_XAXIS,
                required=False
            ),
            PARA_CHART: parameter_specification(
                PARA_CHART,
                'Chart',
                DT_AS_ROW,
                9
            ),
            PARA_CHART_TYPE: parameter_specification(
                PARA_CHART_TYPE,
                'Type',
                DT_STRING,
                10,
                values=[
                    'Area Chart',
                    {'value': 'Bar Chart', 'isDefault': True},
                    'Line Chart',
                    'Scatter Plot'
                ],
                parent=PARA_CHART
            ),
            PARA_CHART_GROUPED: parameter_specification(
                PARA_CHART_GROUPED,
                'Grouped',
                DT_BOOL,
                11,
                parent=PARA_CHART
            )
        }
    }
}

"""Python commands."""
PYTHON_COMMANDS = {
    PYTHON_CODE: {
        MODULE_NAME: 'Python Script',
        MODULE_ARGUMENTS: {
            PYTHON_SOURCE: parameter_specification(
                PYTHON_SOURCE,
                'Python Code',
                DT_PYTHON_CODE,
                0
            )
        }
    }
}

"""VizUAL command specification schema."""
VIZUAL_COMMANDS = {
    VIZUAL_DEL_COL: {
        MODULE_NAME: 'Delete Column',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMN: para_column(1)
        }
    },
    VIZUAL_DEL_ROW: {
        MODULE_NAME: 'Delete Row',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_ROW: para_row(1)
        }
    },
    VIZUAL_DROP_DS: {
        MODULE_NAME: 'Drop Dataset',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0)
        }
    },
    VIZUAL_INS_COL: {
        MODULE_NAME: 'Insert Column',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_POSITION: para_position(2),
            PARA_NAME: parameter_specification(PARA_NAME, 'Column Name', DT_STRING, 1)
        }
    },
    VIZUAL_INS_ROW: {
        MODULE_NAME: 'Insert Row',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_POSITION: para_position(1)
        }
    },
    VIZUAL_LOAD: {
        MODULE_NAME: 'Load Dataset',
        MODULE_ARGUMENTS: {
            PARA_FILE: parameter_specification(PARA_FILE, 'File', DT_FILE_ID, 0),
            PARA_NAME: parameter_specification(PARA_NAME, 'Dataset Name', DT_STRING, 1)
        }
    },
    VIZUAL_MOV_COL: {
        MODULE_NAME: 'Move Column',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMN: para_column(1),
            PARA_POSITION: para_position(2)
        }
    },
    VIZUAL_MOV_ROW: {
        MODULE_NAME: 'Move Row',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_ROW: para_row(1),
            PARA_POSITION: para_position(2)
        }
    },
    VIZUAL_REN_COL: {
        MODULE_NAME: 'Rename Column',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMN: para_column(1),
            PARA_NAME: parameter_specification(
                PARA_NAME,
                'New Column Name',
                DT_STRING,
                2
            )
        }
    },
    VIZUAL_REN_DS: {
        MODULE_NAME: 'Rename Dataset',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_NAME: parameter_specification(
                PARA_NAME,
                'New Dataset Name',
                DT_STRING,
                1
            )
        }
    },
    VIZUAL_UPD_CELL: {
        MODULE_NAME: 'Update Cell',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMN: para_column(1),
            PARA_ROW: para_row(2),
            PARA_VALUE: parameter_specification(
                PARA_VALUE,
                'Value',
                DT_STRING,
                3,
                required=False
            )
        }
    }
}


"""Dictionary of available packages."""
AVAILABLE_PACKAGES = {
    PACKAGE_MIMIR: MIMIR_LENSES,
    PACKAGE_PLOT: PLOT_COMMANDS,
    PACKAGE_PYTHON: PYTHON_COMMANDS,
    PACKAGE_VIZUAL: VIZUAL_COMMANDS
}


# ------------------------------------------------------------------------------
# Mimir
# ------------------------------------------------------------------------------

def mimir_domain(dataset_name, column, make_input_certain=False):
    """Create a Mimir Missing Value Lens.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_DOMAIN,
        {
            PARA_DATASET : dataset_name,
            PARA_COLUMN: column,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )


def mimir_key_repair(dataset_name, column, make_input_certain=False):
    """Create a Mimir Key Repair Lens.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_KEY_REPAIR,
        {
            PARA_DATASET : dataset_name,
            PARA_COLUMN: column,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )


def mimir_missing_key(dataset_name, column, missing_only=None, make_input_certain=False):
    """Create a Mimir Missing Key Lens.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    missing_only: boolean, optional
        Optional MISSING_ONLY parameter
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_MISSING_KEY,
        {
            PARA_DATASET : dataset_name,
            PARA_COLUMN: column,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )


def mimir_missing_value(dataset_name, column, make_input_certain=False):
    """Create a Mimir Missing Value Lens.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_MISSING_VALUE,
        {
            PARA_DATASET : dataset_name,
            PARA_COLUMN: column,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )


def mimir_picker(dataset_name,  schema, pick_as=None, make_input_certain=False):
    """Create a Mimir Picker Lens.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    schema: list(dict)
        List of objects containing 'pickFrom' and 'pickAs' elements
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    args = {
        PARA_DATASET : dataset_name,
        PARA_SCHEMA: schema,
        PARA_MAKE_CERTAIN: make_input_certain
    }
    if not pick_as is None:
        args[PARA_PICKAS] = pick_as
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_PICKER,
        args
    )


def mimir_schema_matching(dataset_name, schema, result_name, make_input_certain=False):
    """Create a Mimir Schema Matching Lens.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    schema: list(dict)
        List of objects containing 'column' and 'type' elements
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_SCHEMA_MATCHING,
        {
            PARA_DATASET : dataset_name,
            PARA_SCHEMA: schema,
            PARA_RESULT_DATASET: result_name,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )


def mimir_type_inference(dataset_name, percent_conform, make_input_certain=False):
    """Create a Mimir Type Inference Lens.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    percent_conform: float
        Percent that conforms
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_TYPE_INFERENCE,
        {
            PARA_DATASET : dataset_name,
            PARA_PERCENT_CONFORM: percent_conform,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )


# ------------------------------------------------------------------------------
# Python
# ------------------------------------------------------------------------------

def python_cell(source):
    """Module specification for a Python cell.

    Parameters
    ----------
    source: string
        Python code for cell body

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_PYTHON,
        PYTHON_CODE,
        {PYTHON_SOURCE: source}
    )


# ------------------------------------------------------------------------------
# VizUAL Command specifications
# ------------------------------------------------------------------------------

def delete_column(dataset_name, column):
    """Delete dataset column.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column that is being deleted

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_DEL_COL,
        {
            PARA_DATASET : dataset_name,
            PARA_COLUMN: column
        }
    )


def delete_row(dataset_name, row):
    """Delete dataset row.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    row: int
        Index for row that is being deleted

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_DEL_ROW,
        {
            PARA_DATASET : dataset_name,
            PARA_ROW: row
        }
    )


def drop_dataset(dataset_name):
    """Drop a dataset.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_DROP_DS,
        {
            PARA_DATASET : dataset_name
        }
    )


def insert_column(dataset_name, position, name):
    """Insert a column into a dataset.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    position: int
        Index position where column is inserted
    name: string
        New column name

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_INS_COL,
        {
            PARA_DATASET : dataset_name,
            PARA_POSITION: position,
            PARA_NAME: name
        }
    )


def insert_row(dataset_name, position):
    """Insert a row into a dataset.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    position: int
        Index position where row is inserted

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_INS_ROW,
        {
            PARA_DATASET : dataset_name,
            PARA_POSITION: position
        }
    )


def load_dataset(file_id, dataset_name):
    """Load dataset from file. Expects file identifier and new dataset name.

    Parameters
    ----------
    file_id: string
        Unique file identifier
    dataset_name: string
        Name for the new dataset

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_LOAD,
        {
            PARA_FILE : file_id,
            PARA_NAME: dataset_name
        }
    )


def move_column(dataset_name, column, position):
    """Move a column in a dataset.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column that is being moves
    position: int
        Index position where column is moved to

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_MOV_COL,
        {
            PARA_DATASET : dataset_name,
            PARA_COLUMN: column,
            PARA_POSITION: position
        }
    )


def move_row(dataset_name, row, position):
    """Move a row in a dataset.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    row: int
        Index of row that is being moved
    position: int
        Index position where row is moved

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_MOV_ROW,
        {
            PARA_DATASET : dataset_name,
            PARA_ROW: row,
            PARA_POSITION: position
        }
    )


def rename_column(dataset_name, column, name):
    """Rename a dataset column.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column that is being renamed
    name: string
        New column name

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_REN_COL,
        {
            PARA_DATASET : dataset_name,
            PARA_COLUMN: column,
            PARA_NAME: name
        }
    )


def rename_dataset(dataset_name, new_name):
    """Rename a dataset.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    new_name: string
        New dataset name

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_REN_DS,
        {
            PARA_DATASET : dataset_name,
            PARA_NAME: new_name
        }
    )


def update_cell(dataset_name, column, row, value):
    """Update a dataset cell value.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Cell Columne name or index
    row: int
        Cell row index
    value: string
        New cell value

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_UPD_CELL,
        {
            PARA_DATASET : dataset_name,
            PARA_COLUMN: column,
            PARA_ROW: row,
            'value': value
        }
    )


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------


def validate_arguments(spec, args):
    """Validate a dictionary of command arguments against a command
    specification. Raises ValueError if invalid argument list is given.

    Parameters
    ----------
    spec: list(string)
        List of required elements for argument dictionary
    args: dict()
        Argument dictionary.
    """
    # Build nested structure
    nodes = dict()
    for key in spec:
        node = dict(spec[key])
        nodes[key] = dict(spec[key])
    roots = dict()
    for key in nodes:
        node = nodes[key]
        if 'parent' in node:
            parent = nodes[node['parent']]
            if not 'children' in parent:
                parent['children'] = [node]
            else:
                parent['children'].append(node)
        else:
            roots[node['label']] = node
    # Make sure that all given arguments are valid
    for key in args:
        if not key in roots:
            raise ValueError('invalid argument \'' + key + '\'')
        if 'children' in roots[key]:
            validate_nested_arguments(roots[key]['children'], args[key])
    # Make sure that all required arguments are given
    for key in roots:
        if roots[key]['required'] and not key in args:
            raise ValueError('missing argument \'' + key + '\'')


def validate_command(command_repository, command):
    """Validate the given command specification. Raises ValueError if an
    invalid specification is given.

    Parameters
    ----------
    command_repository: dict
        Dictionary containing specifications for all commands that are
        supported by an execution environment.
    command : vizier.workflow.module.ModuleSpecification
        Specification of the command that is to be evaluated
    """
    cmd_type = command.module_type
    if not cmd_type in command_repository:
        raise ValueError('unknown command type \'' + cmd_type + '\'')
    type_commands = command_repository[cmd_type]
    command_id = command.command_identifier
    if not command_id in type_commands:
        raise ValueError('unknown command \'' + command_id + '\'')
    validate_arguments(
        type_commands[command_id][MODULE_ARGUMENTS],
        command.arguments
    )


def validate_nested_arguments(spec, args):
    """Validate recursively a dictionary of command arguments against a command
    specification. Raises ValueError if invalid argument list is given.

    Parameters
    ----------
    spec: list(dict)
        List of required elements for argument dictionary
    args: list(dict) or dict()
        Argument dictionary.
    """
    roots = dict()
    for obj in spec:
        roots[obj['label']] = obj
    # Make sure that all given arguments are valid
    if isinstance(args, list):
        nested_args = args
    else:
        nested_args = [args]
    for arg in nested_args:
        for key in arg:
            if not key in roots:
                raise ValueError('invalid argument \'' + key + '\'')
            if 'children' in roots[key]:
                validate_nested_arguments(roots[key]['children'], arg[key])
        # Make sure that all required arguments are given
        for key in roots:
            if roots[key]['required'] and not key in arg:
                raise ValueError('missing argument \'' + key + '\'')
