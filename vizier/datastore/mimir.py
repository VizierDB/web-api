"""Implementation of Vizier data store using Mimir."""

import csv
import gzip
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


"""Column name prefix (Important: Use upper case!)."""
COL_PREFIX = 'COL'

"""Name of file storing dataset (schema) information."""
DATASETS_FILE = 'dataset-index.yaml'

"""Name of the database column that contains the row id for tuples
(Important: Use upper case!).
"""
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
    def __init__(self, identifier, name_in_dataset, name_in_rdb=None, data_type=None):
        """Initialize the dataset column.

        Parameters
        ----------
        identifier: int
            Unique column identifier
        name_in_dataset: string
            Name of column in the dataset
        name_in_rdb: string, optional
            Name of the corresponding attribute in a relational table or views
        data_type: string, optional
            Identifier for data type of column values. Default is String
        """
        # Ensure that a valid data type is given
        super(MimirDatasetColumn, self).__init__(identifier, name_in_dataset)
        if not name_in_rdb is None:
            self.name_in_rdb = name_in_rdb.upper()
        else:
            self.name_in_rdb = name_in_dataset.upper()
        self.data_type = data_type

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

    def to_sql_value(self, value):
        """Return an SQL conform representation of the given value based on the
        column's data type.

        Parameters
        ----------
        value: string
            Dataset cell value

        Returns
        -------
        string
        """
        if self.data_type == 'int':
            try:
                return str(int(value))
            except ValueError:
                pass
        elif self.data_type in ['real', 'tweight']:
            try:
                return str(float(value))
            except ValueError:
                pass
        # By default and in case the given value could not be transformed into
        # the target format return a representation for a string value
        return '\'' + value + '\''


class MimirDatasetDescriptor(Dataset):
    """Internal descriptor for datasets managed by the Mimir data store.
    Contains mapping for column names from a dataset to the corresponding object
    in a relational and a reference to the table or view that contains the
    dataset.
    """
    def __init__(
        self, identifier, columns, rowid_column, table_name, row_ids,
        column_counter, row_counter, rows=None, annotations=None
    ):
        """Initialize the descriptor.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        columns: list(vizier.datastore.mimir.MimirDatasetColumn)
            List of column names in the dataset schema and their corresponding
            names in the relational database table or view.
        rowid_column: vizier.datastore.mimir.MimirDatasetColumn
            Descriptor for unique row id column
        table_name: string
            Reference to relational database table containing the dataset.
        row_ids: list(int)
            List of row ids. Determines the order of rows in the dataset
        column_counter: int
            Counter for unique column ids
        row_counter: int
            Counter for unique row ids
        rows: list(vizier.datastore.base.DatasetRow), optional
            List of dataset rows. It is expected that each row has a unique
            identifier.
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components
        """
        super(MimirDatasetDescriptor, self).__init__(
            identifier=identifier,
            columns=columns,
            column_counter=column_counter,
            row_counter=row_counter,
            rows=rows,
            annotations=annotations
        )
        self.rowid_column = rowid_column
        self.table_name = table_name
        self.row_ids = row_ids

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
            identifier=doc['id'],
            columns=[MimirDatasetColumn.from_dict(obj) for obj in doc['columns']],
            rowid_column=MimirDatasetColumn.from_dict(doc['rowIdColumn']),
            table_name=doc['tableName'],
            row_ids=doc['rows'],
            column_counter=doc['columnCounter'],
            row_counter=doc['rowCounter'],
            annotations=annotations
        )

    @property
    def row_count(self):
        """Number of rows in the dataset.

        Returns
        -------
        int
        """
        return len(self.row_ids)

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
            'rowIdColumn': self.rowid_column.to_dict(),
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

    def create_dataset(self, dataset):
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
        # Get unique identifier for new dataset
        identifier = 'DS_' + get_unique_identifier()
        # Write rows to temporary file in CSV format
        tmp_dir = tempfile.mkdtemp()
        tmp_file = os.path.join(tmp_dir, identifier + '.csv')
        # Create a list of columns that contain the user-vizible column name and
        # the name in the database
        columns = list()
        with open(tmp_file, 'w') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_MINIMAL)
            col_names = list()
            for i in range(len(dataset.columns)):
                col = dataset.columns[i]
                name_in_rdb = COL_PREFIX + str(i)
                col_names.append(name_in_rdb)
                columns.append(
                    MimirDatasetColumn(
                        col.identifier,
                        col.name,
                        name_in_rdb
                    )
                )
            writer.writerow([ROW_ID] + col_names)
            for row in dataset.rows:
                writer.writerow([str(row.identifier)] + row.values)
        # Load CSV file using Mimirs loadCSV method.
        table_name = mimir._mimir.loadCSV(tmp_file)
        # Delete temporary files
        shutil.rmtree(tmp_dir)
        # Create and return metadata descriptor for the loaded dataset
        return self.insert_dataset(
            table_name,
            columns,
            [row.identifier for row in dataset.rows],
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            rows=dataset.rows
        )

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
        ds = self.get_dataset_descriptor(identifier)
        if not ds is None:
            sql = get_select_query(ds.table_name, ds.columns)
            rs = mimir._mimir.vistrailsQueryMimir(sql, False, False)
            reader = csv.reader(
                StringIO(rs.csvStr()),
                delimiter=',',
                quotechar='\'',
                quoting=csv.QUOTE_MINIMAL,
                skipinitialspace=True
            )
            # Skip headline row
            reader.next()
            # Remaining rows are dataset rows
            rows = [parse_row(row, dataset=ds) for row in reader]
            ds.rows = sort_rows(rows, ds.row_ids)
        return ds

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

    def insert_dataset(
        self, table_name, columns, row_ids=None, column_counter=None,
        row_counter=None, rowid_column=None, rows=None, annotations=None,
        adjust_metadata=True
    ):
        """Create a new record for a database table or view. Note that this
        method does not actually create the table or view in the database but
        adds the datasets metadata to the data store. The table or view will
        have been created by a load command or be the result from executing
        a lens or a VizUAL commandhas.

        Parameters
        ----------
        table_name: string
            Reference to relational database table or view containing the dataset.
        columns: list(vizier.datastore.mimir.MimirDatasetColumn)
            List of column names in the dataset schema and their corresponding
            names in the relational database table or view.
        row_ids: list(int)
            List of row ids. Determines the order of rows in the dataset
        column_counter: int
            Counter for unique column ids
        row_counter: int
            Counter for unique row ids
        rowid_column: vizier.datastore.mimir.MimirDatasetColumn, optional
            Descriptor for unique row id column
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components
        adjust_metadata: bool, optional
            Flag indicating whether the metadata in the given dataset is correct
            (False) or needs to be adjusted (True). The former is the case for
            VizUAL MOVE and RENAME_COLUMN operations.

        Returns
        -------
        vizier.datastore.mimir.MimirDatasetDescriptor
        """
        # If the adjust metadata flag is set retrieve the full dataset from
        # the underlying database.
        if adjust_metadata:
            # Query the database
            sql = get_select_query(table_name, columns)
            rs = mimir._mimir.vistrailsQueryMimir(sql, True, True)
            # Get dataset schema information
            mimir_schema = rs.schema()
            reader = csv.reader(
                StringIO(rs.csvStr()),
                delimiter=',',
                quotechar='\'',
                quoting=csv.QUOTE_MINIMAL,
                skipinitialspace=True
            )
            # Create a dictionary of columns including the hidden ROW ID column.
            rowid_column = MimirDatasetColumn(-1, ROW_ID)
            ds_columns = dict({rowid_column.name_in_rdb: rowid_column})
            for col in columns:
                ds_columns[col.name_in_rdb] = col
            # Use the dictionary to adjust the columns data types.
            for name_in_dataset in reader.next():
                name_in_dataset = name_in_dataset.strip()
                data_type=str(mimir_schema.get(name_in_dataset))[5:-1]
                ds_columns[name_in_dataset.upper()].data_type = data_type
            # Read all rows to get list of row ids. If no rows have been passed
            # as argument we need to create objects for each row as well.
            read_rows = rows is None
            if read_rows:
                rows = list()
            ds_rowids = list()
            for row in reader:
                if read_rows:
                    r = parse_row(row)
                    rows.append(r)
                    row_id = r.identifier
                else:
                    row_id = int(row[0].strip())
                ds_rowids.append(row_id)
            # Remove row ids from the array if they do not exist in the dataset.
            # The row_ids argument may be None. In this case create a list of
            # all the row_ids in the result. Make sure to adjust the row counter
            # as well.
            if not row_ids is None:
                if len(ds_rowids) != len(row_ids):
                    adjusted_row_ids = list()
                    for row_id in row_ids:
                        if row_id in ds_rowids:
                            adjusted_row_ids.append(row_id)
                    row_ids = adjusted_row_ids
            else:
                row_ids = ds_rowids
            # Sort rows if they were read
            if read_rows:
                rows = sort_rows(rows, row_ids)
            # Create dataset descriptor with annotations
            annotations = get_annotations(
                columns,
                row_ids,
                rs.celReasons(),
                rs.colsDet(),
                annotations=annotations
            )
        # Set column counter to max. column id + 1 if None
        if column_counter is None:
            column_counter = -1
            for col in columns:
                if col.identifier > column_counter:
                    column_counter = col.identifier
            column_counter += 1
        # Set row counter to max. row id + 1 if None
        if row_counter is None:
            row_counter = -1
            # The set of row ids may be None at this point
            if not row_ids is None:
                for row_id in row_ids:
                    if row_id > row_counter:
                        row_counter = row_id
            row_counter += 1
        dataset = MimirDatasetDescriptor(
            identifier=get_unique_identifier(),
            columns=columns,
            rowid_column=rowid_column,
            table_name=table_name,
            row_ids=row_ids,
            column_counter=column_counter,
            row_counter=row_counter,
            rows=rows,
            annotations=annotations
        )
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
        # Create a copy of the original file under a unique name. If the input
        # file is tab-delimited (and therefore has been successfully parsed on
        # upload) we create a comma-separated version. Otherwise, we simply copy
        # the given file.
        tmp_file = get_tempfile()
        if f_handle.is_verified_csv and f_handle.delimiter == '\t':
            if f_handle.compressed:
                csvfile = gzip.open(f_handle.filepath, 'rb')
            else:
                csvfile = open(f_handle.filepath, 'rb')
            reader = csv.reader(csvfile, delimiter='\t')
            with open(tmp_file, 'w') as f:
                writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                for row in reader:
                    writer.writerow(row)
                csvfile.close()
        else:
            shutil.copyfile(f_handle.filepath, tmp_file)
        # Load dataset and retrieve the result to get the dataset schema
        init_load_name = mimir._mimir.loadCSV(tmp_file)
        sql = 'SELECT * FROM ' + init_load_name
        rs = mimir._mimir.vistrailsQueryMimir(sql, True, True)
        mimir_schema = rs.schema()
        reader = csv.reader(
            StringIO(rs.csvStr()),
            delimiter=',',
            quotechar='\'',
            quoting=csv.QUOTE_MINIMAL,
            skipinitialspace=True
        )
        # Write retieved result to temp file. Add unique column names and row
        # identifier
        os.remove(tmp_file)
        tmp_file = get_tempfile()
        # List of Mimir dataset column descriptors for the dataset schema.
        # Create load file for dataset that includes unique row identifier
        columns = list()
        with open(tmp_file, 'w') as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter=',',
                quoting=csv.QUOTE_MINIMAL
            )
            # Get dataset schema information from retrieved result
            out_schema = [ROW_ID.upper()]
            # Add a column descriptor for the row identifier so we can keep
            # track of the data type that Mimir assigs to the column
            col_line = reader.next()
            for name_in_dataset in col_line:
                name_in_dataset = name_in_dataset.strip()
                col_id = len(columns)
                name_in_rdb = COL_PREFIX + str(col_id)
                col = MimirDatasetColumn(
                    col_id,
                    name_in_dataset,
                    name_in_rdb,
                    data_type=str(mimir_schema.get(name_in_dataset))[5:-1]
                )
                columns.append(col)
                out_schema.append(name_in_rdb)
            writer.writerow(out_schema)
            # Remaining rows are dataset rows
            row_ids = list()
            for row in reader:
                row_id = len(row_ids)
                row_ids.append(row_id)
                out_row = [str(row_id)]
                for val in row:
                    val = val.strip()
                    if val == 'NULL':
                        val = ''
                    out_row.append(val)
                writer.writerow(out_row)
        table_name = mimir._mimir.loadCSV(tmp_file)
        os.remove(tmp_file)
        # Create and return metadata descriptor for the loaded dataset
        return self.insert_dataset(table_name, columns, row_ids)

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


def get_select_query(table_name, columns):
    """Get SQL query to select a full dataset with columns in order of their
    appearance as defined in the given column list. The first column will be
    the ROW ID.

    Parameters
    ----------
    table_name: string
        Name of the database table or view
    columns: list(vizier.datastore.mimir.MimirDatasetColumn)
        List of columns in the dataset

    Returns
    -------
    str
    """
    col_list = ','.join([col.name_in_rdb for col in columns])
    return 'SELECT ' + ROW_ID + ',' + col_list + ' FROM ' + table_name


def get_tempfile():
    """Return the path to a temporary CSV file. Try to get a unique name to
    avoid problems with existing datasets.

    Returns
    -------
    string
    """
    tmp_prefix = 'DS_' + get_unique_identifier()
    return tempfile.mkstemp(suffix='.csv', prefix=tmp_prefix)[1]


def parse_row(row, dataset=None):
    """Converts a row from a CSV fiel into a data set row object.

    Parameters
    ----------
    row: list()
        Row in a CSV file as returned by the CSV parser.
    dataset: vizier.datastore.mimir.MimirDatasetDescriptor, optional
        Descriptor for dataset the row belongs to

    Returns
    -------
    vizier.datastore.base.DatasetRow
    """
    row_id = int(row[0].strip())
    values = []
    for val in row[1:]:
        val = val.strip()
        if val == 'NULL':
            val = None
        values.append(val)
    return DatasetRow(row_id, values, dataset=dataset)


def sort_rows(rows, row_ids):
    """Sort the given list of rows according to the given order of row ids.

    Parameters
    ----------
    rows: list(vizier.datastore.base.DatasetRow)
        List of rows
    row_ids: list(int)
        List of rows defining the order of rows

    Returns
    -------
    list(vizier.datastore.base.DatasetRow)
    """
    # Create mapping of row ids to positions
    row_index = dict()
    for i in range(len(row_ids)):
        row_index[row_ids[i]] = i
    # Create ordered list of rows
    result = list([None] * len(rows))
    for row in rows:
        result[row_index[row.identifier]] = row
    return result
