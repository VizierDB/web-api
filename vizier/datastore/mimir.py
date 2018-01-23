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
from vizier.datastore.metadata import DatasetMetadata


"""Column name prefix."""
COL_PREFIX = 'COL'

"""Column data types."""
DT_NUMERIC = 'N'
DT_STRING = 'S'

"""Name of file storing dataset (schema) information."""
DATASETS_FILE = 'dataset-index.yaml'

"""Name of the database column that contains the row id for tuples."""
ROW_ID = 'rid'


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
    def __init__(self, identifier, name_in_dataset, name_in_rdb, data_type=None):
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
        if not data_type is None:
            if not data_type in [DT_NUMERIC, DT_STRING]:
                raise ValueError('invalid data type \'' + data_tye + '\'')
        super(MimirDatasetColumn, self).__init__(identifier, name_in_dataset)
        self.name_in_rdb = name_in_rdb.upper()
        self.data_type = data_type if not data_type is None else DT_STRING

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
        self.row_ids = list(row_ids)
        # The type of row id's is determined by update_dataset.
        self.rowid_type = None

    @staticmethod
    def from_dict(doc, annotations):
        """Create a dataset descriptor form a dictionary serialization.

        Parameters
        ----------
        doc: dict()
            Dictionary serialization of a dataset descriptor
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.mimir.MimirDatasetDescriptor
        """
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

    def to_dict(self):
        """Create dictionary serialization for a dataset descriptor.

        Returns
        -------
        dict
        """
        return {
            'id': self.identifier,
            'columns': [col.to_dict() for col in self.columns],
            'rows': self.row_ids,
            'tableName': str(self.table_name),
            'columnCounter': self.column_counter,
            'rowCounter': self.row_counter
        }


class MimirDataStore(DataStore):
    """Vizier data store implementation using Mimir.

    Maintains information about the dataset schema separately. This is necessary
    because in a dataset column names are not necessarily unique. Uses a file
    in Yaml format to store dataset information.

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
        self.directory = os.path.abspath(base_dir)
        if not os.path.isdir(self.directory):
            os.makedirs(self.directory)
        self.dataset_index_file = os.path.join(self.directory, DATASETS_FILE)
        # Read dataset index files
        self.datasets = dict()
        if os.path.isfile(self.dataset_index_file):
            with open(self.dataset_index_file, 'r') as f:
                doc = yaml.load(f.read())
            for obj in doc['datasets']:
                ds = MimirDatasetDescriptor.from_dict(
                    obj,
                    DatasetMetadata.from_file(
                        self.get_metadata_filename(obj['id'])
                    )
                )
                self.datasets[ds.identifier] = ds

    def create_dataset(self, column_names, lens_id):
        """Create a new dataset that result from executing a lens on an existing
        dataset (e.g., SCHEMA_MATCHING lens).

        Parameters
        ----------
        column_names: list(string)
            List of column names in the dataset schema
        lens_id: string
            Identifier of the lens which this dataset is created

        Returns
        -------
        vizier.datastore.mimir.MimirDatasetDescriptor
        """
        identifier = get_unique_identifier()
        columns = list()
        for c_name in column_names:
            col_id = len(columns)
            columns.append(MimirDatasetColumn(
                col_id,
                c_name,
                COL_PREFIX + str(col_id)
            ))
        dataset = MimirDatasetDescriptor(
            identifier,
            columns,
            lens_id,
            [],
            len(columns),
            0,
            DatasetMetadata()
        )
        self.update_dataset(identifier, dataset, new_dataset=True)
        return dataset

    def delete_dataset(self, identifier):
        """Delete dataset with given identifier. Returns True if dataset existed
        and False otherwise.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier.

        Returns
        -------_count
        bool
        """
        if identifier in self.datasets:
            # Remove dataset from index. Note that the dataset (view) will
            # remain in the database
            del self.datasets[identifier]
            self.write_dataset_index()
            # Delete the metadata file
            metadata_file = self.get_metadata_filename(identifier)
            if os.path.isfile(metadata_file):
                os.remove(metadata_file)
            return True
        else:
            return False

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
        if identifier in self.datasets:
            ds = self.datasets[identifier]
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
                annotations=DatasetMetadata.from_file(
                    self.get_metadata_filename(identifier)
                )
            )
        return None

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
        return os.path.join(self.directory, identifier + '.yaml')

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
        self.update_dataset(identifier, ds)
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
        if not identifier in self.datasets:
            return False
        metadata_file = self.get_metadata_filename(identifier)
        # Read annotations from file, evaluate update statement and write result
        # back to file.
        annotations = upd_stmt.eval(
            DatasetMetadata.from_file(
                os.path.join(metadata_file)
            )
        )
        annotations.to_file(os.path.join(metadata_file))
        return annotations

    def update_dataset(self, identifier, dataset, new_dataset=False):
        """Determine the type of each column in the given dataset and update
        dataset annotations.

        The optional parameters include_uncertainty and include_reasons are
        used as input to the Mimir query.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        dataset: vizier.datastore.mimir.MimirDatasetDescriptor
            Dataset descriptor that is being updated
        new_dataset: bool, optional
            Flag indicating whether the dataset is being created (by
            create_dataset()). If Ture, the row ids of the given dataset have
            not been set yet.
        """
        dataset.annotations = dataset.annotations.copy_metadata()
        # Retrieve lens metadata
        col_list = ','.join([col.name_in_rdb for col in dataset.columns])
        col_list = ROW_ID + ', ' + col_list
        sql = 'SELECT ' + col_list + ' FROM ' + dataset.table_name
        rs = mimir._mimir.vistrailsQueryMimir(sql, True, True)
        # Determine column data types.
        rdb_schema = rs.schema()
        dataset.rowid_type = mimir_type_2_rdb_type(rdb_schema.get(ROW_ID.upper()))
        #for i in range(len(dataset.columns)):
        #    mimir_type = rdb_schema.get(COL_PREFIX + str(i))
        #    dataset.columns[i].data_type = mimir_type_2_rdb_type(mimir_type)
        for column in dataset.columns:
            mimir_type = rdb_schema.get(column.name_in_rdb.upper())
            column.data_type = mimir_type_2_rdb_type(mimir_type)
        # Create cell annotations
        reasons = rs.celReasons()
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
            if len(reasons) > row_index:
                comments = reasons[row_index]
                for i in range(len(row) - 1):
                    anno = comments[i + 1]
                    if anno != '':
                        dataset.annotations.for_cell(
                            dataset.columns[i].identifier,
                            row_id
                        ).set_annotation('mimir:reason', anno)
            row_index += 1
        if new_dataset:
            # We were given a dataset that is being created from a lens
            dataset.row_ids = row_ids
        elif len(row_ids) < dataset.row_ids:
            # Adjust row ids if the query returned fewer results that expected
            row_index = 0
            while row_index < len(dataset.row_ids):
                if not dataset.row_ids[row_index] in row_ids:
                    del dataset.row_ids[row_index]
                else:
                    row_index += 1
        self.datasets[identifier] = dataset
        self.write_dataset_index()

    def write_dataset_index(self):
        """Write internal dataset index to file."""
        datasets = list()
        for ds in self.datasets.values():
            # Store dataset metadata
            ds.annotations.to_file(self.get_metadata_filename(ds.identifier))
            datasets.append(ds.to_dict())
        doc = {'datasets': datasets}
        with open(self.dataset_index_file, 'w') as f:
            yaml.dump(doc, f, default_flow_style=False)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

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
