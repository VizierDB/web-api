# Copyright (C) 2018 New York University
#                    University at Buffalo,
#                    Illinois Institute of Technology.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Specifications for supported workflow module types."""

from vizier.workflow.module import ModuleSpecification


# ------------------------------------------------------------------------------
# Module Specifications
# ------------------------------------------------------------------------------

"""Definition of parameter data types."""
DT_AS_ROW = 'asRow'
DT_BOOL = 'bool'
DT_COLUMN_ID = 'colid'
DT_DATASET_ID = 'dataset'
DT_DECIMAL = 'decimal'
DT_FILE_ID = 'fileid'
DT_GROUP = 'group'
DT_INT = 'int'
DT_PYTHON_CODE = 'pyCode'
DT_SQL_CODE = 'sqlCode'
DT_SCALA_CODE = 'scalaCode'
DT_MARKDOWN_CODE = 'markdownCode'
DT_ROW_INDEX = 'rowidx'
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
    DT_SQL_CODE,
    DT_SCALA_CODE,
    DT_MARKDOWN_CODE,
    DT_ROW_INDEX,
    DT_STRING
]

"""Definition of common module parameter names."""
PARA_CHART = 'chart'
PARA_CHART_TYPE = 'chartType'
PARA_CHART_GROUPED = 'chartGrouped'
PARA_CITY = 'city'
PARA_COLUMN = 'column'
PARA_COLUMNS = 'columns'
PARA_CONSTRAINT = 'constraint'
PARA_DATASET = 'dataset'
PARA_OUTPUT_DATASET = 'output_dataset'
PARA_FILE = 'file'
PARA_FILEID = 'fileid'
PARA_GEOCODER = 'geocoder'
PARA_HOUSE_NUMBER = 'strnumber'
PARA_LABEL = 'label'
PARA_MAKE_CERTAIN = 'makeInputCertain'
PARA_NAME = 'name'
PARA_ORDER = 'order'
PARA_PERCENT_CONFORM = 'percentConform'
PARA_PICKAS = 'pickAs'
PARA_PICKFROM = 'pickFrom'
PARA_POSITION = 'position'
PARA_RANGE = 'range'
PARA_RESULT_DATASET = 'resultName'
PARA_ROW = 'row'
PARA_SCHEMA = 'schema'
PARA_SERIES = 'series'
PARA_STATE = 'state'
PARA_STREET = 'strname'
PARA_TYPE = 'type'
PARA_VALUE = 'value'
PARA_XAXIS = 'xaxis'
PARA_LOAD_OPTION_KEY = 'loadOptionKey'
PARA_LOAD_OPTION_VALUE = 'loadOptionValue'
PARA_LOAD_OPTIONS = 'loadOptions'
PARA_LOAD_FORMAT = 'loadFormat'
PARA_LOAD_TI = 'loadInferTypes'
PARA_LOAD_DH = 'loadDetectHeaders'
PARA_LOAD_DSE = 'loadDataSourceErrors'
PARA_DSE_MODEL_NAME = 'dseModel'
# Concatenation of parameter kets
PARA_COLUMNS_COLUMN = PARA_COLUMNS + '_' + PARA_COLUMN
PARA_COLUMNS_ORDER = PARA_COLUMNS + '_' + PARA_ORDER
PARA_COLUMNS_RENAME = PARA_COLUMNS + '_' + PARA_NAME
PARA_COLUMNS_CONSTRAINT = PARA_COLUMNS + '_' + PARA_CONSTRAINT


"""Values for sort order."""
SORT_ASC = 'A-Z'
SORT_DESC = 'Z-A'


def para_column(index, parent=None):
    """Return dictionary specifying the default column parameter used by most
    modules.

    Returns
    -------
    dict
    """
    return parameter_specification(
        PARA_COLUMN,
        name='Column',
        data_type=DT_COLUMN_ID,
        index=index,
        parent=parent
    )


def para_dataset(index):
    """Return dictionary specifying the default dataset parameter used by most
    modules.

    Returns
    -------
    dict
    """
    return parameter_specification(
        PARA_DATASET,
        name='Dataset',
        data_type=DT_DATASET_ID,
        index=index
    )


def para_make_input_certain(index):
    """Return dictionary for 'makeInputCertain' parameter of Mimir lenses.

    Returns
    -------
    dict
    """
    return parameter_specification(
        PARA_MAKE_CERTAIN,
        name='Make Input Certain',
        data_type=DT_BOOL,
        index=index,
        required=False
    )


def para_position(index):
    """Return dictionary for position parameter used by some Vizual moduels.

    Returns
    -------
    dict
    """
    return parameter_specification(
        PARA_POSITION,
        name='Position',
        data_type=DT_INT,
        index=index
    )


def para_row(index):
    """Return dictionary specifying the default row parameter used by most
    modules.

    Returns
    -------
    dict
    """
    return parameter_specification(
        PARA_ROW,
        name='Row',
        data_type=DT_ROW_INDEX,
        index=index
    )


def parameter_specification(
        identifier, name=None, data_type=None, index=0, label=None, required=True,
        values=None, parent=None, hidden=False, value=None
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
        'required': required,
        'hidden': hidden
    }
    if not label is None:
        para['label'] = label
    else:
        para['label'] = identifier
    if not values is None:
        para['values'] = values
    if not parent is None:
        para['parent'] = parent
    if not value is None:
        para['value'] = value
    return para




"""Identifier for currently supported module types."""
PACKAGE_MIMIR = 'mimir'
PACKAGE_SQL = 'sql'
PACKAGE_SCALA = 'scala'
PACKAGE_MARKDOWN = 'markdown'
PACKAGE_PLOT = 'plot'
PACKAGE_PYTHON = 'python'
PACKAGE_SYS = '_sys'
PACKAGE_VIZUAL = 'vizual'

MODULE_NAME = 'name'
MODULE_GROUP = 'group'
MODULE_ARGUMENTS = 'arguments'

"""Identifier for plot commands."""
PLOT_SIMPLE_CHART = 'CHART'

"""Identifier for Mimir lenses."""
MIMIR_DOMAIN = 'DOMAIN'
MIMIR_GEOCODE = 'GEOCODE'
MIMIR_KEY_REPAIR ='KEY_REPAIR'
MIMIR_MISSING_KEY ='MISSING_KEY'
MIMIR_MISSING_VALUE = 'MISSING_VALUE'
MIMIR_PICKER ='PICKER'
MIMIR_SCHEMA_MATCHING ='SCHEMA_MATCHING'
MIMIR_TYPE_INFERENCE ='TYPE_INFERENCE'
MIMIR_SHAPE_DETECTOR ='SHAPE_WATCHER'

"""Components for Python requests."""
PYTHON_SOURCE = 'source'

"""Identifier for Python commands."""
PYTHON_CODE = 'CODE'

"""Components for Sql requests."""
SQL_SOURCE = 'source'

"""Identifier for Sql commands."""
SQL_CODE = 'CODE'

"""Components for Sql requests."""
SCALA_SOURCE = 'source'

"""Identifier for Sql commands."""
SCALA_CODE = 'CODE'

"""Components for markdown requests."""
MARKDOWN_SOURCE = 'source'

"""Identifier for markdown commands."""
MARKDOWN_CODE = 'CODE'

"""Identifier for sysyem commands."""
SYS_CREATE_BRANCH = 'CREATE_BRANCH'

"""Identifier for VizUAL commands."""
VIZUAL_DEL_COL = 'DELETE_COLUMN'
VIZUAL_DEL_ROW = 'DELETE_ROW'
VIZUAL_DROP_DS = 'DROP_DATASET'
VIZUAL_INS_COL = 'INSERT_COLUMN'
VIZUAL_INS_ROW = 'INSERT_ROW'
VIZUAL_LOAD = 'LOAD'
VIZUAL_UNLOAD = 'UNLOAD'
VIZUAL_MOV_COL = 'MOVE_COLUMN'
VIZUAL_MOV_ROW = 'MOVE_ROW'
VIZUAL_PROJECTION = 'PROJECTION'
VIZUAL_REN_COL = 'RENAME_COLUMN'
VIZUAL_REN_DS = 'RENAME_DATASET'
VIZUAL_SORT = 'SORT_DATASET'
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
    MIMIR_GEOCODE: {
        MODULE_NAME: 'Geocode Lens',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_HOUSE_NUMBER: parameter_specification(
                PARA_HOUSE_NUMBER,
                name='House Nr.',
                data_type=DT_COLUMN_ID,
                index=1,
                required=False
            ),
            PARA_STREET: parameter_specification(
                PARA_STREET,
                name='Street',
                data_type=DT_COLUMN_ID,
                index=2,
                required=False
            ),
            PARA_CITY: parameter_specification(
                PARA_CITY,
                name='City',
                data_type=DT_COLUMN_ID,
                index=3,
                required=False
            ),
            PARA_STATE: parameter_specification(
                PARA_STATE,
                name='State',
                data_type=DT_COLUMN_ID,
                index=4,
                required=False
            ),
            PARA_GEOCODER: parameter_specification(
                PARA_GEOCODER,
                name='Geocoder',
                data_type=DT_STRING,
                index=5,
                values=['GOOGLE', 'OSM']
            ),
            PARA_MAKE_CERTAIN: para_make_input_certain(6)
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
            PARA_COLUMNS: parameter_specification(
                PARA_COLUMNS,
                name='Columns',
                data_type=DT_GROUP,
                index=1
            ),
            PARA_COLUMNS_COLUMN: parameter_specification(
                PARA_COLUMNS_COLUMN,
                name='Column',
                data_type=DT_COLUMN_ID,
                index=2,
                parent=PARA_COLUMNS
            ),
            PARA_COLUMNS_CONSTRAINT: parameter_specification(
                PARA_COLUMNS_CONSTRAINT,
                name='Constraint',
                data_type=DT_STRING,
                index=3,
                parent=PARA_COLUMNS,
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
                name='Columns',
                data_type=DT_GROUP,
                index=1
            ),
            PARA_PICKFROM: parameter_specification(
                PARA_PICKFROM,
                name='Pick From',
                data_type=DT_COLUMN_ID,
                index=2,
                parent=PARA_SCHEMA
            ),
            PARA_PICKAS: parameter_specification(
                PARA_PICKAS,
                name='Pick As',
                data_type=DT_STRING,
                index=3,
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
                name='Schema',
                data_type=DT_GROUP,
                index=1
            ),
            PARA_COLUMN: parameter_specification(
                PARA_COLUMN,
                name='Column Name',
                data_type=DT_STRING,
                index=2,
                parent=PARA_SCHEMA
            ),
            PARA_TYPE: parameter_specification(
                PARA_TYPE,
                name='Data Type',
                data_type=DT_STRING,
                index=3,
                values=['int', 'varchar'],
                parent=PARA_SCHEMA
            ),
            PARA_RESULT_DATASET: parameter_specification(
                PARA_RESULT_DATASET,
                name='Store Result As ...',
                data_type=DT_STRING,
                index=4
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
                name='Percent Conform',
                data_type=DT_DECIMAL,
                index=1
            ),
            PARA_MAKE_CERTAIN: para_make_input_certain(2)
        }
    },
    MIMIR_SHAPE_DETECTOR: {
        MODULE_NAME: 'Shape Detection Adaptive Schema',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_DSE_MODEL_NAME: parameter_specification(
                PARA_DSE_MODEL_NAME,
                name='Shape Detect Warning Model',
                data_type=DT_STRING,
                index=1,
                required=False
            )
        }
    }
}

"""SQL commands."""
SQL_COMMANDS = {
    SQL_CODE: {
        MODULE_NAME: 'SQL Statement',
        MODULE_ARGUMENTS: {
            PARA_OUTPUT_DATASET: parameter_specification(
                PARA_OUTPUT_DATASET,
                name='Output Dataset',
                data_type=DT_STRING,
                index=2,
                required=False
            ),
            SQL_SOURCE: parameter_specification(
                SQL_SOURCE,
                name='SQL Code',
                data_type=DT_SQL_CODE,
                index=1
            )
        }
    }
}

"""Scala commands."""
SCALA_COMMANDS = {
    SCALA_CODE: {
        MODULE_NAME: 'Scala Script',
        MODULE_ARGUMENTS: {
            SCALA_SOURCE: parameter_specification(
                SCALA_SOURCE,
                name='Scala Code',
                data_type=DT_SCALA_CODE,
                index=0
            )
        }
    }
}

"""Markdown commands."""
MARKDOWN_COMMANDS = {
    MARKDOWN_CODE: {
        MODULE_NAME: 'Markdown Twig',
        MODULE_ARGUMENTS: {
            MARKDOWN_SOURCE: parameter_specification(
                MARKDOWN_SOURCE,
                name='Markdown Code',
                data_type=DT_MARKDOWN_CODE,
                index=0
            )
        }
    }
}

"""Plot commands."""
PLOT_COMMANDS = {
    PLOT_SIMPLE_CHART: {
        MODULE_NAME: 'Simple Chart',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_NAME: parameter_specification(
                PARA_NAME,
                name='Chart Name',
                data_type=DT_STRING,
                index=1,
                required=False
            ),
            PARA_SERIES: parameter_specification(
                PARA_SERIES,
                name='Data Series',
                data_type=DT_GROUP,
                index=2
            ),
            PARA_SERIES + '_' + PARA_COLUMN: parameter_specification(
                PARA_SERIES + '_' + PARA_COLUMN,
                name='Column',
                data_type=DT_COLUMN_ID,
                index=3,
                parent=PARA_SERIES
            ),
            PARA_SERIES + '_' + PARA_RANGE: parameter_specification(
                PARA_SERIES + '_' + PARA_RANGE,
                name='Range',
                data_type=DT_STRING,
                index=4,
                parent=PARA_SERIES,
                required=False
            ),
            PARA_SERIES + '_' + PARA_LABEL: parameter_specification(
                PARA_SERIES + '_' + PARA_LABEL,
                name='Label',
                data_type=DT_STRING,
                index=5,
                parent=PARA_SERIES,
                required=False
            ),
            PARA_XAXIS: parameter_specification(
                PARA_XAXIS,
                name='X-Axis',
                data_type=DT_AS_ROW,
                index=6,
                required=False
            ),
            PARA_XAXIS + '_' + PARA_COLUMN: parameter_specification(
                PARA_XAXIS + '_' + PARA_COLUMN,
                name='Column',
                data_type=DT_COLUMN_ID,
                index=7,
                parent=PARA_XAXIS,
                required=False
            ),
            PARA_XAXIS + '_' + PARA_RANGE: parameter_specification(
                PARA_XAXIS + '_' + PARA_RANGE,
                name='Range',
                data_type=DT_STRING,
                index=8,
                parent=PARA_XAXIS,
                required=False
            ),
            PARA_CHART: parameter_specification(
                PARA_CHART,
                name='Chart',
                data_type=DT_AS_ROW,
                index=9
            ),
            PARA_CHART_TYPE: parameter_specification(
                PARA_CHART_TYPE,
                name='Type',
                data_type=DT_STRING,
                index=10,
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
                name='Grouped',
                data_type=DT_BOOL,
                index=11,
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
                name='Python Code',
                data_type=DT_PYTHON_CODE,
                index=0
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
        MODULE_GROUP: 'dataset',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0)
        }
    },
    VIZUAL_INS_COL: {
        MODULE_NAME: 'Insert Column',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_POSITION: para_position(2),
            PARA_NAME: parameter_specification(
                PARA_NAME,
                name='Column Name',
                data_type=DT_STRING,
                index=1
            )
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
        MODULE_GROUP: 'dataset',
        MODULE_ARGUMENTS: {
            PARA_NAME: parameter_specification(
                PARA_NAME,
                name='Dataset Name',
                data_type=DT_STRING,
                index=0,
                required=False
            ),
            PARA_FILE: parameter_specification(
                PARA_FILE,
                name='Source File',
                data_type=DT_FILE_ID,
                index=1
            ),
            PARA_LOAD_FORMAT: parameter_specification(
                PARA_LOAD_FORMAT,
                name='Load Format',
                data_type=DT_STRING,
                values=[{'value':'csv', 'isDefault':True}, 'json', {'key':'com.databricks.spark.xml', 'value':'xml'}, {'key':'com.crealytics.spark.excel', 'value':'excel'}, 'jdbc', 'text', 'parquet', 'orc'],
                index=2,
                required=True
            ),
            PARA_LOAD_TI: parameter_specification(
                PARA_LOAD_TI,
                name='Infer Types',
                data_type=DT_BOOL,
                index=3,
                value=True,
                required=False
            ),
            PARA_LOAD_DH: parameter_specification(
                PARA_LOAD_DH,
                name='Detect Headers',
                data_type=DT_BOOL,
                index=4,
                value=True,
                required=False
            ),
            PARA_LOAD_DSE: parameter_specification(
                PARA_LOAD_DSE,
                name='Data Source Error Annotations',
                data_type=DT_BOOL,
                index=5,
                required=False
            ),
            PARA_LOAD_OPTIONS: parameter_specification(
                PARA_LOAD_OPTIONS,
                name='Load Options',
                data_type=DT_GROUP,
                index=6,
                required=False
            ),
            PARA_LOAD_OPTION_KEY: parameter_specification(
                PARA_LOAD_OPTION_KEY,
                name='Option Key',
                data_type=DT_STRING,
                index=7,
                #values=['delimeter', 'varchar'],
                parent=PARA_LOAD_OPTIONS,
                required=False
            ),
            PARA_LOAD_OPTION_VALUE: parameter_specification(
                PARA_LOAD_OPTION_VALUE,
                name='Option Value',
                data_type=DT_STRING,
                index=8,
                parent=PARA_LOAD_OPTIONS,
                required=False
            ),               
        }
    },
    VIZUAL_UNLOAD: {
        MODULE_NAME: 'Unload Dataset',
        MODULE_GROUP: 'dataset',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_FILE: parameter_specification(
                PARA_FILE,
                name='Output File',
                data_type=DT_STRING,
                index=1
            ),
            PARA_LOAD_FORMAT: parameter_specification(
                PARA_LOAD_FORMAT,
                name='Save Format',
                data_type=DT_STRING,
                values=[{'value':'csv', 'isDefault':True}, 'json', {'key':'com.databricks.spark.xml', 'value':'xml'}, {'key':'com.crealytics.spark.excel', 'value':'excel'}, 'jdbc', 'text', 'parquet', 'orc'],
                index=2,
                required=True
            ),
            PARA_LOAD_OPTIONS: parameter_specification(
                PARA_LOAD_OPTIONS,
                name='Unload Options',
                data_type=DT_GROUP,
                index=5,
                required=False
            ),
            PARA_LOAD_OPTION_KEY: parameter_specification(
                PARA_LOAD_OPTION_KEY,
                name='Option Key',
                data_type=DT_STRING,
                index=6,
                #values=['delimeter', 'varchar'],
                parent=PARA_LOAD_OPTIONS,
                required=False
            ),
            PARA_LOAD_OPTION_VALUE: parameter_specification(
                PARA_LOAD_OPTION_VALUE,
                name='Option Value',
                data_type=DT_STRING,
                index=7,
                parent=PARA_LOAD_OPTIONS,
                required=False
            ),               
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
    VIZUAL_PROJECTION: {
        MODULE_NAME: 'Filter Columns',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMNS: parameter_specification(
                PARA_COLUMNS,
                name='Columns',
                data_type=DT_GROUP,
                index=1
            ),
            PARA_COLUMNS_COLUMN: parameter_specification(
                PARA_COLUMNS_COLUMN,
                name='Column',
                data_type=DT_COLUMN_ID,
                index=2,
                parent=PARA_COLUMNS
            ),
            PARA_COLUMNS_RENAME: parameter_specification(
                PARA_COLUMNS_RENAME,
                name='Rename as ...',
                data_type=DT_STRING,
                index=3,
                parent=PARA_COLUMNS,
                required=False
            )
        }
    },
    VIZUAL_REN_COL: {
        MODULE_NAME: 'Rename Column',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMN: para_column(1),
            PARA_NAME: parameter_specification(
                PARA_NAME,
                name='New Column Name',
                data_type=DT_STRING,
                index=2
            )
        }
    },
    VIZUAL_REN_DS: {
        MODULE_NAME: 'Rename Dataset',
        MODULE_GROUP: 'dataset',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_NAME: parameter_specification(
                PARA_NAME,
                name='New Dataset Name',
                data_type=DT_STRING,
                index=1
            )
        }
    },
    VIZUAL_SORT: {
        MODULE_NAME: 'Sort Dataset',
        MODULE_GROUP: 'dataset',
        MODULE_ARGUMENTS: {
            PARA_DATASET: para_dataset(0),
            PARA_COLUMNS: parameter_specification(
                PARA_COLUMNS,
                name='Columns',
                data_type=DT_GROUP,
                index=1
            ),
            PARA_COLUMNS_COLUMN: parameter_specification(
                PARA_COLUMNS_COLUMN,
                name='Column',
                data_type=DT_COLUMN_ID,
                index=2,
                parent=PARA_COLUMNS
            ),
            PARA_COLUMNS_ORDER: parameter_specification(
                PARA_COLUMNS_ORDER,
                name='Order',
                data_type=DT_STRING,
                index=3,
                values=[
                    {'value': SORT_ASC, 'isDefault': True},
                    SORT_DESC
                ],
                parent=PARA_COLUMNS,
                required=True
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
                name='Value',
                data_type=DT_STRING,
                index=3,
                required=False
            )
        }
    }
}

"""System commands."""
SYSTEM_COMMANDS = {
    SYS_CREATE_BRANCH: {
        MODULE_NAME: 'Create Branch'
    }
}

"""Dictionary of available packages. These are packages that contain modules
which can be used in vizier workflows.
"""
AVAILABLE_PACKAGES = {
    PACKAGE_MIMIR: MIMIR_LENSES,
    PACKAGE_SQL: SQL_COMMANDS,
    PACKAGE_SCALA: SCALA_COMMANDS,
    PACKAGE_MARKDOWN: MARKDOWN_COMMANDS,
    PACKAGE_PLOT: PLOT_COMMANDS,
    PACKAGE_PYTHON: PYTHON_COMMANDS,
    PACKAGE_VIZUAL: VIZUAL_COMMANDS
}

"""The packages dictionary extends the available packages with the system
commands. Note that system commands are primarily used for the branch history
and a re not intended for the user. Thus, the distinction between available
packages and packages.
"""
PACKAGES = dict(AVAILABLE_PACKAGES)
PACKAGES[PACKAGE_SYS] = SYSTEM_COMMANDS


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


def mimir_geocode(dataset_name, geocoder, house_nr=None, street=None, city=None, state=None, make_input_certain=False):
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
    args = {
        PARA_DATASET : dataset_name,
        PARA_GEOCODER: geocoder,
        PARA_MAKE_CERTAIN: make_input_certain
    }
    if not house_nr is None:
        args[PARA_HOUSE_NUMBER] = house_nr
    if not street is None:
        args[PARA_STREET] = street
    if not city is None:
        args[PARA_CITY] = city
    if not state is None:
        args[PARA_STATE] = state
    return ModuleSpecification(PACKAGE_MIMIR, MIMIR_GEOCODE, args)


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


def mimir_missing_value(dataset_name, columns, make_input_certain=False):
    """Create a Mimir Missing Value Lens.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    constraint: string, optional
        Optional value constraint
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    args = {
        PARA_DATASET : dataset_name,
        PARA_COLUMNS: columns,
        PARA_MAKE_CERTAIN: make_input_certain
    }
    
    return ModuleSpecification(PACKAGE_MIMIR, MIMIR_MISSING_VALUE, args)


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
# SQL
# ------------------------------------------------------------------------------

def sql_cell(output_ds_name, source):
    """Module specification for a SQL cell.
    Parameters
    ----------
    ds_name: string
        Input dataset name
    source: string
        SQL code for cell body
    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_SQL,
        SQL_CODE,
        {
            PARA_OUTPUT_DATASET: output_ds_name,
            SQL_SOURCE: source
        }
    )


# ------------------------------------------------------------------------------
# Plot
# ------------------------------------------------------------------------------
def create_plot(ds_name, chart_name, series):
    """Module specification to create a simple plot.

    Parameters
    ----------
    ds_name: string
        Dataset name
    chart_name: string
        Name of the chart
    series: list()
        Specification of data series

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_PLOT,
        PLOT_SIMPLE_CHART,
        {
            PARA_DATASET: ds_name,
            PARA_NAME: chart_name,
            PARA_SERIES: series,
            PARA_CHART: {PARA_CHART_TYPE: 'bar', PARA_CHART_GROUPED: False}
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


def load_dataset(file_id, dataset_name, filename=None, url=None, infer_types=False, detect_headers=False, load_format='csv', load_options=None):
    """Load dataset from file. Expects file identifier and new dataset name.

    Parameters
    ----------
    file_id: string
        Unique file identifier
    dataset_name: string
        Name for the new dataset
    filename: string, optional
        Optional name of the source file
    url: string, optional
        Optional Url of the source file

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    file = {'fileid': file_id}
    if not filename is None:
        file['filename'] = filename
    if not url is None:
        file['url'] = url
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_LOAD,
        {
            PARA_FILE : file,
            PARA_NAME: dataset_name,
            PARA_LOAD_TI : infer_types,
            PARA_LOAD_DH : detect_headers,
            PARA_LOAD_FORMAT : load_format,
            PARA_LOAD_OPTIONS : load_options
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
