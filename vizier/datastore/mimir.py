"""Implementation of Vizier data store using Mimir."""

import csv
import os
import shutil
import tempfile
import yaml

from StringIO import StringIO

import vistrails.packages.mimir.init as mimir

from vizier.core.system import build_info
from vizier.core.util import get_unique_identifier
from vizier.datastore.base import Dataset, DatasetColumn, DatasetRow, DataStore
from vizier.datastore.base import dataset_from_file
from vizier.datastore.metadata import DatasetMetadata


"""Column name prefix."""
COL_PREFIX = 'COL'

"""Column data types."""
DT_NUMERIC = 'N'
DT_STRING = 'S'

"""Name of file storing dataset (schema) information."""
DATASETS_FILE = 'dataset-index.yaml'

"""Name of the database column that contains the row id for tuples."""
ROW_ID = 'RID'


class MimirDatasetColumn(DatasetColumn):
    """Column in a dataset that is stored as a Mimir table or view. Given that
    column names are not necessarily unique in a dataset, there is a need to
    maintain a mapping of dataset names to attribute names for tables/views in
    the relational database.

    Attributes
    ----------
    identifier: int
        Unique column identifier
    name: string
        Name of column in the dataset
    name_in_rdb: string
        Name of the corresponding attribute in a relational table or views
    """
    def __init__(self, identifier, name_in_dataset, name_in_rdb, data_type='varchar'):
        """Initialize the dataset column.

        Parameters
        ----------
        identifier: int
            Unique column identifier
        name_in_dataset: string
            Name of column in the dataset
        name_in_rdb: string
            Name of the corresponding attribute in a relational table or views
        data_type: string, optional
            Identifier for data type of column values. Default is String
        """
        # Ensure that a valid data type is given
        super(MimirDatasetColumn, self).__init__(identifier, name_in_dataset)
        self.name_in_rdb = name_in_rdb.upper()
        self.data_type = data_type

    @property
    def is_numeric(self):
        """Flag indicating whether the values in this column are of numeric
        type.

        Returns
        -------
        bool
        """
        return self.data_type == DT_NUMERIC

    @property
    def is_string(self):
        """Flag indicating whether the values in this column are of string
        type.

        Returns
        -------
        bool
        """
        return self.data_type == DT_STRING

    @staticmethod
    def from_dict(doc):
        """Create dataset column object from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for dataset column object

        Returns
        -------
        vizier.datastore.mimir.DatasetColumn
        """
        return MimirDatasetColumn(
            doc['id'],
            doc['name'],
            doc['rdbName'],
            doc['dataType']
        )

    def to_dict(self):
        """Get dictionary serialization for dataset column object.

        Returns
        -------
        dict
        """
        return {
            'id': self.identifier,
            'name': self.name,
            'rdbName': self.name_in_rdb,
            'dataType': self.data_type
        }


class MimirDatasetDescriptor(Dataset):
    """Internal descriptor for datasets managed by the Mimir data store.
    Contains mapping for column names from a dataset to the corresponding object
    in a relational and a reference to the table or view that contains the
    dataset.
    """
    def __init__(self, identifier, columns, table_name, row_ids, column_counter, row_counter, annotations):
        """Initialize the descriptor.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        columns: list(vizier.datastore.mimir.MimirDatasetColumn)
            List of column names in the dataset schema and their corresponding
            names in the relational database table or view.
        table_name: string
            Reference to relational database table containing the dataset.
        row_ids: list(int)
            List of row ids. Determines the order of rows in the dataset
        column_counter: int
            Counter for unique column ids
        row_counter: int
            Counter for unique row ids
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components
        """
        super(MimirDatasetDescriptor, self).__init__(
            identifier=identifier,
            columns=columns,
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=annotations
        )
        self.table_name = table_name
        self.row_ids = row_ids
        # The type of row id's is determined by update_dataset.
        self.rowid_type = None

    @staticmethod
    def from_file(filename, annotations=None):
        """Read dataset from file. Expects the file to be in Yaml format which
        is the default serialization format used by to_file().

        Parameters
        ----------
        filename: string
            Name of the file to read.
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components
        Returns
        -------
        vizier.datastore.base.Dataset
        """
        with open(filename, 'r') as f:
            doc = yaml.load(f.read())
        return MimirDatasetDescriptor(
            doc['id'],
            [MimirDatasetColumn.from_dict(obj) for obj in doc['columns']],
            doc['tableName'],
            doc['rows'],
            doc['columnCounter'],
            doc['rowCounter'],
            annotations
        )

    @property
    def row_count(self):
        """Number of rows in the dataset.

        Returns
        -------
        int
        """
        return len(self.row_ids)

    def rowid_to_sql_string(self, row_id):
        """Converts a given row identifier into the appropriate SQL string based
        on the type of the datasets ROW_ID column.

        Parameters
        ----------
        row_id: int
            Unique row identifier

        Returns
        -------
        string
        """
        if self.rowid_type == DT_STRING:
            return '\'' + str(row_id) + '\''
        else:
            return str(row_id)

    def to_file(self, filename):
        """Write dataset to file. The default serialization format is Yaml.

        Parameters
        ----------
        filename: string
            Name of the file to write
        """
        doc = {
            'id': self.identifier,
            'columns': [col.to_dict() for col in self.columns],
            'rows': self.row_ids,
            'tableName': str(self.table_name),
            'columnCounter': self.column_counter,
            'rowCounter': self.row_counter
        }
        with open(filename, 'w') as f:
            yaml.dump(doc, f, default_flow_style=False)


class MimirDataStore(DataStore):
    """Vizier data store implementation using Mimir.

    Maintains information about the dataset schema separately. This is necessary
    because in a dataset column names are not necessarily unique. For each
    dataset a new subfolder is created in the store base directory. In that
    folder a dataset file and an annotation file are maintained. All files are
    in Yaml format.

    Note that every write_dataset call creates a new table in the underlying
    Mimir database. Other datasets are views on these tables.
    """
    def __init__(self, base_dir):
        """Initialize the base directory that contains the dataset index and
        metadata files.

        Parameters
        ----------
        base_dir: string
            Name of the directory where metadata is stored
        """
        super(MimirDataStore, self).__init__(build_info('MimirDataStore'))
        self.base_dir = os.path.abspath(base_dir)
        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)

    def create_dataset(self, dataset, adjust_metadata=True):
        """Create a new record for a dataset that results from executing a lens,
        a VizUAL command, or a Vizier Client create_dataset or update_dataset
        call.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        dataset: vizier.datastore.mimir.MimirDatasetDescriptor
            Dataset descriptor that is being updated
        adjust_metadata: bool, optional
            Flag indicating whether the metadata in the given dataset is correct
            (False) or needs to be adjusted (True). The former is the case for
            VizUAL MOVE and RENAME_COLUMN operations.

        Returns
        -------
        vizier.datastore.mimir.MimirDatasetDescriptor
        """
        if adjust_metadata:
            # In the default case dataset metadata needs to be adjusted
            dataset = set_dataset_metadata(dataset)
        # Create a new directory for the dataset if it doesn't exist.
        dataset_dir = self.get_dataset_dir(dataset.identifier)
        if not os.path.isdir(dataset_dir):
            os.makedirs(dataset_dir)
        # Write dataset and annotation file to disk
        dataset.to_file(self.get_dataset_file(dataset.identifier))
        dataset.annotations.to_file(
            self.get_metadata_filename(dataset.identifier)
        )
        return dataset

    def delete_dataset(self, identifier):
        """Delete dataset with given identifier. Returns True if dataset existed
        and False otherwise.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier.

        Returns
        -------
        bool
        """
        dataset_dir = self.get_dataset_dir(identifier)
        if os.path.isdir(dataset_dir):
            shutil.rmtree(dataset_dir)
            return True
        return False

    def get_dataset_dir(self, identifier):
        """Get the base directory for a dataset with given identifier. Having a
        separate method makes it easier to change the folder structure used to
        store datasets.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return os.path.join(self.base_dir, identifier)

    def get_dataset_file(self, identifier):
        """Get the absolute path of the file that maintains the dataset metadata
        such as the order of row id's and column information.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return os.path.join(self.get_dataset_dir(identifier), 'dataset.yaml')

    def get_dataset(self, identifier):
        """Read a full dataset from the data store. Returns None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.base.Dataset
        """
        dataset_file = self.get_dataset_file(identifier)
        if os.path.isfile(dataset_file):
            ds = MimirDatasetDescriptor.from_file(
                dataset_file,
                annotations=DatasetMetadata.from_file(
                    self.get_metadata_filename(identifier)
                )
            )
            col_list = ','.join([col.name_in_rdb for col in ds.columns])
            sql = 'SELECT ' + ROW_ID + ',' + col_list + ' FROM ' + ds.table_name
            rs = mimir._mimir.vistrailsQueryMimir(sql, False, False)
            rows = list([None] * len(ds.row_ids))
            # Create mapping of row ids to order of rows in the dataset
            row_index = dict()
            for i in range(len(ds.row_ids)):
                row_index[ds.row_ids[i]] = i
            reader = csv.reader(StringIO(rs.csvStr()), delimiter=',')
            # Skip headline row
            reader.next()
            # Remaining rows are dataset rows
            for row in reader:
                row_id = row[0].strip()
                if row_id.startswith('\'') and row_id.endswith('\''):
                    row_id = int(row_id[1:-1])
                else:
                    row_id = int(row_id)

                # Note that the returned string does not parse well beacause
                # of the space character that is trailing the commas in the
                # returned csvStr.
                values = []
                for val in row[1:]:
                    val = val.strip()
                    if val.startswith('\'') and val.endswith('\''):
                        val = val[1:-1]
                    values.append(str(val))
                rows[row_index[row_id]] = DatasetRow(row_id, values)
            return Dataset(
                identifier=identifier,
                columns=ds.columns,
                column_counter=ds.column_counter,
                rows=rows,
                row_counter=ds.row_counter,
                annotations=ds.annotations
            )
        return None

    def get_dataset_descriptor(self, identifier, include_annotations=True):
        """Get the Mimir dataset descriptor for the dataset with tthe given
        identifier. The result is None if no dataset with the given identifier
        exists.

        Dataset annotations are only returned if the include_annotations flag is
        set to True

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        include_annotations: bool, optional
            Optional flag indicating whether dataset annotations should be
            included in the result or not

        Returns
        -------
        vizier.datastore.mimir.MimirDatasetDescriptor
        """
        # Return None if the dataset file does not exist
        dataset_file = self.get_dataset_file(identifier)
        if not os.path.isfile(dataset_file):
            return None
        annotations = None
        if include_annotations:
            annotations = DatasetMetadata.from_file(
                self.get_metadata_filename(identifier)
            )
        return MimirDatasetDescriptor.from_file(
            dataset_file,
            annotations=annotations
        )

    def get_metadata_filename(self, identifier):
        """Get filename of meatdata file for the dataset with the given
        identifier.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return os.path.join(self.get_dataset_dir(identifier), 'annotation.yaml')

    def load_dataset(self, f_handle):
        """Create a new dataset from a given file.

        Raises ValueError if the given file could not be loaded as a dataset.

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle
            handle for an uploaded file on the associated file server.

        Returns
        -------
        vizier.datastore.base.Dataset
        """
        # Create a copy of the original file under a unique name.
        tmp_file = get_tempfile()
        shutil.copyfile(f_handle.filepath, tmp_file)
        # Load dataset and retrieve the result to get the dataset schema
        init_load_name = mimir._mimir.loadCSV(tmp_file)
        sql = 'SELECT * FROM ' + init_load_name
        rs = mimir._mimir.vistrailsQueryMimir(sql, True, True)
        mimir_schema = rs.schema()
        reader = csv.reader(StringIO(rs.csvStr()), delimiter=',', quoting=csv.QUOTE_MINIMAL)
        # Write retieved result to temp file. Add unique column names and row
        # identifier
        os.remove(tmp_file)
        tmp_file = get_tempfile()
        # List of Mimir dataset column descriptors for the dataset schema
        columns = list()
        with open(tmp_file, 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_NONE)
            # Get dataset schema information from retrieved result
            out_schema = [ROW_ID.upper()]
            for name_in_dataset in reader.next():
                name_in_dataset = name_in_dataset.strip()
                col_id = len(columns)
                name_in_rdb = COL_PREFIX + str(col_id)
                out_schema.append(name_in_rdb)
                columns.append(
                    MimirDatasetColumn(
                        col_id,
                        name_in_dataset,
                        name_in_rdb,
                        data_type=str(mimir_schema.get(name_in_dataset))[5:-1]
                    )
                )
            writer.writerow(out_schema)
            # Remaining rows are dataset rows
            row_ids = list()
            for row in reader:
                row_id = len(row_ids)
                row_ids.append(row_id)
                out_row = [str(row_id)]
                for val in row:
                    val = val.strip()
                    if val.startswith('\'') and val.endswith('\''):
                        val = val[1:-1]
                    elif val == 'NULL':
                        val = ''
                    out_row.append(val)
                writer.writerow(out_row)
        table_name = mimir._mimir.loadCSV(tmp_file)
        os.remove(tmp_file)
        sql = 'SELECT * FROM ' + table_name
        rs = mimir._mimir.vistrailsQueryMimir(sql, True, True)
        reasons = rs.celReasons()
        uncertainty = rs.colsDet()
        return self.create_dataset(
            MimirDatasetDescriptor(
                get_unique_identifier(),
                columns,
                table_name,
                row_ids,
                len(columns),
                len(row_ids),
                annotations=get_annotations(columns, row_ids, reasons, uncertainty)
            ),
            adjust_metadata=False
        )

    def store_dataset(self, dataset):
        """Create a new dataset in the data store for the given data.

        Raises ValueError if the number of values in each row of the dataset
        doesn't match the number of columns in the dataset schema.

        Parameters
        ----------
        dataset : vizier.datastore.base.Dataset
            Dataset object

        Returns
        -------
        vizier.datastore.base.Dataset
        """
        # Validate the given dataset schema. Will raise ValueError in case of
        # schema violations
        dataset.validate_schema()
        # Get unique identifier for new dataset
        identifier = 'DS_' + get_unique_identifier()
        dataset.identifier = identifier
        # Write rows to temporary file in CSV format
        tmp_dir = tempfile.mkdtemp()
        tmp_file = os.path.join(tmp_dir, identifier + '.csv')
        with open(tmp_file, 'w') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_MINIMAL)
            col_names = ['col' + str(i) for i in range(len(dataset.columns))]
            writer.writerow([ROW_ID] + col_names)
            for row in dataset.rows:
                writer.writerow([str(row.identifier)] + row.values)
        # Load CSV file using Mimirs loadCSV method.
        table_name = mimir._mimir.loadCSV(tmp_file)
        # Update the dataset index
        ds = MimirDatasetDescriptor(
            identifier,
            [
                MimirDatasetColumn(
                    dataset.columns[i].identifier,
                    dataset.columns[i].name,
                    'col' + str(i)
                ) for i in range(len(dataset.columns))
            ],
            table_name,
            [row.identifier for row in dataset.rows],
            dataset.column_counter,
            dataset.row_counter,
            dataset.annotations
        )
        # Make sure to determine the types of all columns in the created view
        self.create_dataset(ds)
        # Delete temporary file
        shutil.rmtree(tmp_dir)
        # Return dataset handle
        return dataset

    def update_annotation(self, identifier, upd_stmt):
        """Update the annotations for a component of the datasets with the given
        identifier. Returns the updated annotations or None if the dataset
        does not exist.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        upd_stmt: vizier.datastore.metadata.AnnotationUpdateStatement
            Update statement that handles update of an existing DatasetMetadata
            object.

        Returns
        -------
        vizier.datastore.metadata.AnnotationUpdateStatement
        """
        metadata_file = self.get_metadata_filename(identifier)
        if not os.path.isfile(metadata_file):
            return False
        # Read annotations from file, evaluate update statement and write result
        # back to file.
        annotations = upd_stmt.eval(
            DatasetMetadata.from_file(
                os.path.join(metadata_file)
            )
        )
        annotations.to_file(os.path.join(metadata_file))
        return annotations



# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def create_missing_key_view(dataset, lens_name, key_column_name):
    """ Create a view for missing ROW_ID's on a MISSING_KEY lens.

    Parameters
    ----------
    dataset: vizier.datastore.mimir.MimirDatasetDescriptor
        Descriptor for the dataset on which the lens was created
    lens_name: string
        Identifier of the created MISSING_KEY lens
    key_column_name: string
        Name of the column for which the missing values where generated

    Returns
    -------
    string, int
        Returns the name of the created view and the adjusted counter  for row
        ids.
    """
    # Select the rows that have missing row ids
    sql = 'SELECT ' + key_column_name + ' FROM ' + lens_name
    sql += ' WHERE ' + ROW_ID + ' IS NULL'
    csv_str = mimir._mimir.vistrailsQueryMimir(sql, False, False).csvStr()
    reader = csv.reader(StringIO(csv_str), delimiter=',')
    # Skip headline row
    reader.next()
    case_conditions = []
    for row in reader:
        row_id = dataset.row_counter + len(case_conditions)
        case_conditions.append(
            'WHEN ' + key_column_name + ' = ' + row[0] + ' THEN ' + str(row_id)
        )
    # If no new rows where inserted we are good to go with the existing lens
    if len(case_conditions) == 0:
        return lens_name, dataset.row_counter
    # Create the view SQL statement
    stmt = 'CASE ' + (' '.join(case_conditions)).strip()
    stmt += ' ELSE ' + ROW_ID + ' END AS ' + ROW_ID
    col_list = [stmt]
    for column in dataset.columns:
        col_list.append(column.name_in_rdb)
    sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + lens_name
    view_name = mimir._mimir.createView(dataset.table_name, sql)
    return view_name, dataset.row_counter + len(case_conditions)


def mimir_type_2_rdb_type(mimir_type):
    """Convert a column datatype as returned by Mimir's database schema method
    into one of the two distinguised RDB types (numerig or string).

    Parameters
    ----------
    mimir_type: string
        Column datatype as returned by Mimir's schema method. The value is
        expected to be of format 'Some(<type>)'.

    Returns
    -------
    string
    """
    if str(mimir_type)[5:-1] in ['int', 'tweight']:
        return DT_NUMERIC
    else:
        return DT_STRING


def set_dataset_metadata(dataset):
    """Set metadata information for a given dataset object.

    Determines the type of each column in the given dataset and updates cell
    annotations. The method furthermore adjusts the list or row identifier if
    necessary. This method will modify the provided dataset descriptor. Thus,
    the returned object is the same (but possibly modified) object as the
    input dataset.

    Parameters
    ----------
    dataset: vizier.datastore.mimir.MimirDatasetDescriptor
        Dataset descriptor that is being updated

    Returns
    -------
    vizier.datastore.mimir.MimirDatasetDescriptor
    """
    # Retrieve lens metadata
    col_list = ','.join([col.name_in_rdb for col in dataset.columns])
    col_list = ROW_ID + ', ' + col_list
    sql = 'SELECT ' + col_list + ' FROM ' + dataset.table_name
    rs = mimir._mimir.vistrailsQueryMimir(sql, True, True)
    # Determine column data types.
    rdb_schema = rs.schema()
    dataset.rowid_type = mimir_type_2_rdb_type(
        rdb_schema.get(ROW_ID.upper())
    )
    for column in dataset.columns:
        mimir_type = rdb_schema.get(column.name_in_rdb.upper())
        column.data_type = mimir_type_2_rdb_type(mimir_type)
    # Create cell annotations
    reasons = rs.celReasons()
    uncertainty = rs.colsDet()
    csv_str = rs.csvStr()
    reader = csv.reader(StringIO(csv_str), delimiter=',')
    # Skip headline row
    reader.next()
    # Remaining rows are dataset rows
    row_index = 0
    row_ids = list()
    for row in reader:
        if row[0].startswith('\'') and row[0].endswith('\''):
            row_id = int(row[0][1:-1])
        else:
            row_id = int(row[0])
        row_ids.append(row_id)
        # Add cell annotations
        if len(reasons) > row_index:
            comments = reasons[row_index]
            for i in range(len(row) - 1):
                anno = comments[i + 1]
                if anno != '':
                    dataset.annotations.for_cell(
                        dataset.columns[i].identifier,
                        row_id
                    ).set_annotation('mimir:reason', anno)
        if len(uncertainty) > row_index:
            for col_index in range(len(row) - 1):
                if uncertainty[row_index][col_index + 1] == False:
                    dataset.annotations.for_cell(
                        dataset.columns[col_index].identifier,
                        row_id
                    ).set_annotation('mimir:uncertain', 'true')
        row_index += 1
    # Adjust row ids if necessary.
    if dataset.row_ids is None:
        # We were given a dataset that is being created from a lens
        dataset.row_ids = row_ids
        dataset.row_counter = max(dataset.row_ids) + 1
    else:
        # Keep row ids in their original order. All new rows will appear at the
        # end of the dataset.
        modified_ids = list()
        # Determine the rows that are still present in the dataset
        for row_id in dataset.row_ids:
            if row_id in row_ids:
                modified_ids.append(row_id)
        # Add all rows that are new
        for row_id in row_ids:
            if not row_id in dataset.row_ids:
                modified_ids.append(row_id)
        dataset.row_ids = modified_ids
    # Return the modified dataset object
    return dataset


def get_annotations(columns, row_ids, reasons, uncertainty, annotations=None):
    """Set metadata information for a given dataset object.

    Determines the type of each column in the given dataset and updates cell
    annotations. The method furthermore adjusts the list or row identifier if
    necessary. This method will modify the provided dataset descriptor. Thus,
    the returned object is the same (but possibly modified) object as the
    input dataset.

    Parameters
    ----------
    dataset: vizier.datastore.mimir.MimirDatasetDescriptor
        Dataset descriptor that is being updated

    Returns
    -------
    vizier.datastore.mimir.MimirDatasetDescriptor
    """
    if annotations is None:
        annotations = DatasetMetadata()
    for row_index in range(len(row_ids)):
        if len(reasons) > row_index:
            comments = reasons[row_index]
            for col_index in range(len(columns)):
                anno = comments[col_index + 1]
                if anno != '':
                    annotations.for_cell(
                        columns[col_index].identifier,
                        row_ids[row_index]
                    ).set_annotation('mimir:reason', anno)
        if len(uncertainty) > row_index:
            for col_index in range(len(columns)):
                if uncertainty[row_index][col_index + 1] == False:
                    annotations.for_cell(
                        columns[col_index].identifier,
                        row_ids[row_index]
                    ).set_annotation('mimir:uncertain', 'true')
    return annotations


def get_tempfile():
    """Return the path to a temporary CSV file. Try to get a unique name to
    avoid problems with existing datasets.

    Returns
    -------
    string
    """
    tmp_prefix = 'DS_' + get_unique_identifier()
    return tempfile.mkstemp(suffix='.csv', prefix=tmp_prefix)[1]
