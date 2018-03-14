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
from vizier.datastore.base import DatasetHandle, DatasetColumn, DatasetRow
from vizier.datastore.base import DataStore, max_column_id
from vizier.datastore.metadata import DatasetMetadata
from vizier.datastore.reader import DatasetReader, InMemDatasetReader


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
    def __init__(self, identifier=None, name_in_dataset=None, name_in_rdb=None, data_type=None):
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
        super(MimirDatasetColumn, self).__init__(
            identifier=identifier,
            name=name_in_dataset
        )
        if not name_in_rdb is None:
            self.name_in_rdb = name_in_rdb.upper()
        else:
            self.name_in_rdb = name_in_dataset.upper()
        self.data_type = data_type

    def __str__(self):
        """Human-readable string representation for the column.

        Returns
        -------
        string
        """
        name = self.name
        if not self.data_type is None:
             name += '(' + str(self.data_type) + ')'
        return name

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


class MimirDatasetHandle(DatasetHandle):
    """Internal descriptor for datasets managed by the Mimir data store.
    Contains mapping for column names from a dataset to the corresponding object
    in a relational and a reference to the table or view that contains the
    dataset.
    """
    def __init__(
        self, identifier, columns, rowid_column, table_name, row_ids,
        column_counter, row_counter, annotations=None
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
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components
        """
        super(MimirDatasetHandle, self).__init__(
            identifier=identifier,
            columns=columns,
            row_count=len(row_ids),
            column_counter=column_counter,
            row_counter=row_counter,
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
        vizier.datastore.base.DatasetHandle
        """
        with open(filename, 'r') as f:
            doc = yaml.load(f.read())
        return MimirDatasetHandle(
            identifier=doc['id'],
            columns=[MimirDatasetColumn.from_dict(obj) for obj in doc['columns']],
            rowid_column=MimirDatasetColumn.from_dict(doc['rowIdColumn']),
            table_name=doc['tableName'],
            row_ids=doc['rows'],
            column_counter=doc['columnCounter'],
            row_counter=doc['rowCounter'],
            annotations=annotations
        )

    def reader(self):
        """Get reader for the dataset to access the dataset rows.

        Returns
        -------
        vizier.datastore.reader.DatasetReader
        """
        return MimirDatasetReader(self.table_name, self.columns, self.row_ids)

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


class MimirDatasetReader(DatasetReader):
    """Dataset reader for delimited files (CSV or TSV)."""
    def __init__(self, table_name, columns, row_ids):
        """Initialize information about the delimited file and the file format.

        Parameters
        ----------
        filename: string
            Path to the file on disk
        compressed: bool, optional
            Flag indicating if the file is compressed (gzip)
        delimiter: string, optional
            The column delimiter used by the file
        quotechar: string, optional
            The quote char used in the file
        has_row_ids: bool, optional
            Flag indicating whether the first columnn in the file contains
            unique row identifier
        """
        self.table_name = table_name
        self.columns = columns
        self.row_ids = row_ids
        # Uses a InMemReader when open
        self.reader = None

    def close(self):
        """Close any open files and set the is_open flag to False."""
        if not self.reader is None:
            self.reader.close()
        self.reader = None

    def next(self):
        """Return the next row in the dataset iterator. Raises StopIteration if
        end of file is reached or file has been closed.

        Automatically closes any open file when end of iteration is reached for
        the first time.

        Returns
        -------
        vizier.datastore.base.DatasetRow
        """
        if not self.reader is None:
            return self.reader.next()
        raise StopIteration()

    def open(self):
        """Setup the reader by opening the associacted file and instantiating
        the csv reader.

        Returns
        -------
        vizier.datastore.base.DelimitedFileReader
        """
        # If there is an associated reader that is still open return
        if not self.reader is None:
            if self.reader.is_open:
                return self
        # Query the database to get the list of rows. Sort rows according to
        # order in row_ids and return a InMemReader
        sql = get_select_query(self.table_name, self.columns)
        rs = mimir._mimir.vistrailsQueryMimir(sql, False, False)
        reader = csv.reader(
            StringIO(rs.csvStr()),
            delimiter=',',
            quotechar='\'',
            quoting=csv.QUOTE_MINIMAL,
            skipinitialspace=True
        )
        # Skip header information
        reader.next()
        # Read remaining rows containing dataset rows
        rows = list()
        for row in reader:
            row_id = int(row[0].strip())
            values = []
            for val in row[1:]:
                val = val.strip()
                if val == 'NULL':
                    val = None
                values.append(val)
            rows.append(DatasetRow(row_id, values))
        # Sort rows according to order in row_ids
        row_index = dict()
        for i in range(len(self.row_ids)):
            row_index[self.row_ids[i]] = i
        # Create ordered list of rows
        sorted_rows = list([None] * len(rows))
        for row in rows:
            sorted_rows[row_index[row.identifier]] = row
        self.reader = InMemDatasetReader(sorted_rows).open()
        return self


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

    def create_dataset(
        self, identifier=None, columns=list(), rows=list(), column_counter=0,
        row_counter=0, annotations=None
    ):
        """Create a new dataset in the data store for the given data.

        Raises ValueError if the number of values in each row of the dataset
        doesn't match the number of columns in the dataset schema.

        Parameters
        ----------
        dataset : vizier.datastore.base.DatasetHandle
            Dataset object

        Returns
        -------
        vizier.datastore.mimit.MimirDatasetHandle
        """
        # Get unique identifier for new dataset
        identifier = 'DS_' + get_unique_identifier()
        # Write rows to temporary file in CSV format
        tmp_file = get_tempfile()
        # Create a list of columns that contain the user-vizible column name and
        # the name in the database
        db_columns = list()
        for col in columns:
            db_columns.append(MimirDatasetColumn(
                identifier=col.identifier,
                name_in_dataset=col.name,
                name_in_rdb=COL_PREFIX + str(len(db_columns))
            ))
        # List of row ids in the new dataset
        db_row_ids = list()
        # Create CSV file for load
        with open(tmp_file, 'w') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_MINIMAL)
            writer.writerow([ROW_ID] + [col.name_in_rdb for col in db_columns])
            for row in rows:
                writer.writerow([str(row.identifier)] + row.values)
                db_row_ids.append(row.identifier)
        # Load CSV file using Mimirs loadCSV method.
        table_name = mimir._mimir.loadCSV(tmp_file)
        # Delete temporary files
        os.remove(tmp_file)
        # Insert the new dataset metadata information into the datastore
        return self.register_dataset(
            table_name=table_name,
            columns=db_columns,
            row_ids=db_row_ids
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

    def get_dataset(self, identifier, include_annotations=True):
        """Read a full dataset from the data store. Returns None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        include_annotations: bool, optional
            Flag indicating whether to include annotations
            
        Returns
        -------
        vizier.datastore.mimir.MimirDatasetHandle
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
        return MimirDatasetHandle.from_file(
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
        vizier.datastore.mimir.MimirDatasetHandle
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
        row_ids = list()
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
                    identifier=col_id,
                    name_in_dataset=name_in_dataset,
                    name_in_rdb=name_in_rdb
                )
                columns.append(col)
                out_schema.append(name_in_rdb)
            writer.writerow(out_schema)
            # Remaining rows are dataset rows
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
        # Insert the new dataset metadata information into the datastore
        return self.register_dataset(
            table_name=table_name,
            columns=columns,
            row_ids=row_ids
        )

    def register_dataset(
        self, table_name, columns, row_ids, column_counter=None,
        row_counter=None, annotations=None, update_rows=False,
        update_annotations=True
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
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components
        update_rows: bool, optional
            Flag indicating that the number of rows may have changed and the
            list of row identifier therefore needs to be checked.
        update_annotations: bool, optional
            Flag indicating whether the metadata in the given dataset is correct
            (False) or needs to be adjusted (True). The former is the case for
            VizUAL MOVE and RENAME_COLUMN operations.

        Returns
        -------
        vizier.datastore.mimir.MimirDatasetHandle
        """
        # Query the database to get schema information and row ids if necessary
        sql = get_select_query(table_name, columns)
        rs = mimir._mimir.vistrailsQueryMimir(sql, True, True)
        # Get dataset schema information
        mimir_schema = rs.schema()
        # Update schema information for dataset columns. Will modify the data
        # type of columns in the list and return row id column object
        rowid_column = update_schema_information(columns, mimir_schema)
        # Update row identifier if flag is set to true
        if update_rows:
            # Create dataset reader. Make sure to skip the first row
            reader = csv.reader(
                StringIO(rs.csvStr()),
                delimiter=',',
                quotechar='\'',
                quoting=csv.QUOTE_MINIMAL,
                skipinitialspace=True
            )
            reader.next()
            row_ids = update_rowids(row_ids, reader)
        # Update annotations is flag is set
        if update_annotations:
            # Get updated annotations annotations
            annotations = get_annotations(
                columns,
                row_ids,
                rs.celReasons(),
                rs.colsDet(),
                annotations=annotations
            )
        # Set column counter to max column id + 1if None
        if column_counter is None:
            column_counter = max_column_id(columns) + 1
        # Set row counter to max. row id + 1 if None
        if row_counter is None:
            row_counter = -1
            for row_id in row_ids:
                if row_id > row_counter:
                    row_counter = row_id
            row_counter += 1
        dataset = MimirDatasetHandle(
            identifier=get_unique_identifier(),
            columns=columns,
            rowid_column=rowid_column,
            table_name=table_name,
            row_ids=row_ids,
            column_counter=column_counter,
            row_counter=row_counter,
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
    dataset: vizier.datastore.mimir.MimirDatasetHandle
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
    dataset: vizier.datastore.mimir.MimirDatasetHandle
        Dataset descriptor that is being updated

    Returns
    -------
    vizier.datastore.mimir.MimirDatasetHandle
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


def update_rowids(row_ids, reader):
    """Update the list of row identifier by iterating through the dataset reader

    Parameters
    ----------
    row_ids: list(int)
        Current list of row identifier
    reader: csv.reader
        Row reader

    Returns
    -------
    list(int)
    """
    # Get list of row identifier in current dataset
    dataset_row_ids = set()
    for row in reader:
        dataset_row_ids.add(int(row[0].strip()))
    modified_row_ids = list()
    for row_id in row_ids:
        if row_id in dataset_row_ids:
            modified_row_ids.append(row_id)
    return modified_row_ids


def update_schema_information(columns, schema):
    """Update data type information for a given list of columns.

    Parameters
    ----------
    columns: list(vizier.databstore.mimir.MimirDatasetColumn)
        List of columns in the dataset schema
    schema: obj
        Schema information as returned by Mimir

    Returns
    -------
    vizier.datastore.base.DatasetColumn
    """
    # Create column for row Identifier
    rowid_column = MimirDatasetColumn(
        name_in_dataset=ROW_ID,
        data_type=str(schema.get(ROW_ID))[5:-1]
    )
    # Get data type information for each column in the schema
    for col in columns:
        data_type=str(schema.get(col.name_in_rdb))[5:-1]
        col.data_type = data_type
    return rowid_column
