"""Vizier DB - Database - Collection of objects and methods to maintain and
manipulate different versions of datasets that are manipulated by data curation
workflows.
"""

from abc import abstractmethod
import csv
import yaml

from vizier.core.system import component_descriptor, VizierSystemComponent
from vizier.datastore.metadata import DatasetMetadata


# ------------------------------------------------------------------------------
# Datsets
# ------------------------------------------------------------------------------

class Dataset(object):
    """Object to manipulate a Vizier DB dataset. Contains lists of column names
    and rows objects for the datasets.

    Attributes
    ----------
    annotations: vizier.datastore.metadata.DatasetMetadata
        Annotations for dataset components
    columns: list(DatasetColumns)
        List of dataset columns
    identifier : string
        Unique dataset identifier
    rows : list(DatasetRow)
        List of rows in the dataset
    uri: string
        Unique resource identifier for the dataset
    """
    def __init__(self, identifier=None, columns=None, column_counter=0, rows=None, row_counter=0, annotations=None):
        """Initialize the dataset.

        Raises ValueError if dataset columns or rows do not have unique
        identifiers.

        Parameters
        ----------
        identifier: string, optional
            Unique dataset identifier.
        columns: list(DatasetColumn), optional
            List of columns. It is expected that each column has a unique
            identifier.
        column_counter: int
            Counter for unique column identifier
        rows: list(vizier.datastore.base.DatasetRow), optional
            List of dataset rows. It is expected that each row has a unique
            identifier.
        row_counter: int
            Counter for unique row identifier
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components
        """
        self.identifier = identifier
        # Initialize the list of columns
        if columns is None:
            self.columns = list()
        else:
            # Ensure that all columns have a unique identifier
            ids = set()
            for col in columns:
                if col.identifier in ids:
                    raise ValueError('duplicate column identifier \'' + str(col.identifier) + '\'')
                ids.add(col.identifier)
            self.columns = columns
        self.column_counter = column_counter
        # Initialize the list or rows.
        if rows is None:
            self.rows = list()
        else:
            # Ensure that all rows have a unique identifiers
            ids = set()
            for row in rows:
                if row.identifier in ids:
                    raise ValueError('duplicate row identifier \'' + str(row.identifier) + '\'')
                ids.add(row.identifier)
            self.rows = rows
            # Set dataset handle for each row
            for row in self.rows:
                row.dataset = self
        self.row_counter = row_counter
        if annotations is None:
            self.annotations = DatasetMetadata()
        else:
            self.annotations = annotations

    def add_column(self, name, position=None):
        """Add a new column to the dataset schema.

        Parameters
        ----------
        name: string
            Name of the new column
        position: int, optional
            Position in the dataset schema where new column is inserted. If
            None, the column is appended to the list of dataset columns.

        Returns
        DatasetColumn
        """
        column = DatasetColumn(self.column_counter, name)
        self.column_counter += 1
        if not position is None:
            self.columns.insert(position, column)
            # Add a null value to each row for the new column
            for row in self.rows:
                row.values.insert(position, '')
        else:
            self.columns.append(column)
            # Add a null value to each row for the new column
            for row in self.rows:
                row.values.append('')
        return column

    def add_row(self, values=None, position=None):
        """Add a new row to the dataset. Expects a list of string values, one
        for each of the columns.

        Raises ValueError if the length of the values list does not match the
        number of columns in the dataset.

        Parameters
        ----------
        values: list(string), optional
            List of column values. Use empty string if no values are given
        position: int, optional
            Position where row is inserted. If None, the new row is appended to
            the list of dataset rows.

        Returns
        -------
        DatasetRow
        """
        # Ensure that there is exactly one value for each column in the dataset
        if not values is None:
            if len(values) != len(self.columns):
                raise ValueError('invalid number of values for dataset schema')
            row = DatasetRow(
                self.row_counter,
                [str(v) for v in values],
                dataset=self
            )
        else:
            # All values in the new row are set to the empty string by default.
            row = DatasetRow(
                self.row_counter,
                values = [''] * len(self.columns),
                dataset=self
            )

        self.row_counter += 1
        if not position is None:
            self.rows.insert(position, row)
        else:
            self.rows.append(row)
        return row

    def column_index(self, column_id):
        """Get position of a given column in the dataset schema. The given
        column identifier could either be of type int (i.e., the index position
        of the column), or a string (either the column name or column label). If
        column_id is of type string it is first assumed to be a column name.
        Only if no column matches the column name or if multiple columns with
        the given name exist will the value of column_id be interpreted as a
        label.

        Raises ValueError if column_id does not reference an existing column in
        the dataset schema.

        Parameters
        ----------
        column_id : int or string
            Column index, name, or label

        Returns
        -------
        int
        """
        if isinstance(column_id, int):
            # Return column if it is a column index and withing the range of
            # dataset columns
            if column_id >= 0 and column_id < len(self.columns):
                return column_id
            raise ValueError('invalid column index \'' + str(column_id) + '\'')
        elif isinstance(column_id, basestring):
            # Get index for column that has a name that matches column_id. If
            # multiple matches are detected column_id will be interpreted as a
            # column label
            name_index = -1
            for i in range(len(self.columns)):
                col_name = self.columns[i].name
                if col_name.lower() == column_id.lower():
                    if name_index == -1:
                        name_index = i
                    else:
                        # Multiple columns with the same name exist. SIgnal that
                        # no unique column was found.
                        name_index = -1
                        break
            if name_index == -1:
                # Check whether column_id is a column label that is within the
                # range of the dataset schema
                label_index = collabel_2_index(column_id)
                if label_index > 0:
                    if label_index <= len(self.columns):
                        name_index = label_index - 1
            # Return index of column with matching name or label if there exists
            # a unique solution. Otherwise raise exception.
            if name_index != -1:
                return name_index
            raise ValueError('unknown column \'' + str(column_id) + '\'')

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
        return Dataset(
            identifier=doc['id'],
            columns=[
                DatasetColumn(col['id'], col['name']) for col in doc['columns']
            ],
            column_counter=doc['column_counter'],
            rows=[DatasetRow(row['id'], row['values']) for row in doc['rows']],
            row_counter=doc['row_counter'],
            annotations=annotations
        )

    def get_cell(self, column, row):
        """Get dataset value for specified cell.

        Raises ValueError if [column, row] does not reference an existing cell.

        Parameters
        ----------
        column : int or string
            Column identifier
        row : int
            Row index

        Returns
        -------
        string
        """
        if row < 0 or row > len(self.rows):
            raise ValueError('unknown row \'' + str(row) + '\'')
        return self.rows[row].get_value(column)

    def to_file(self, filename):
        """Write dataset to file. The default serialization format is Yaml.

        Parameters
        ----------
        filename: string
            Name of the file to write
        """
        doc = {
            'id': self.identifier,
            'columns': [
                {'id': col.identifier, 'name': col.name} for col in self.columns
            ],
            'column_counter': self.column_counter,
            'rows': [
                {'id': row.identifier, 'values': row.values}
                    for row in self.rows
            ],
            'row_counter': self.row_counter
        }
        with open(filename, 'w') as f:
            yaml.dump(doc, f, default_flow_style=False)

    @property
    def uri(self):
        """Unique resource identifier for the dataset.

        Returns
        -------
        string
        """
        return 'dataset://' + self.identifier

    def validate_schema(self):
        """Validate the given dataset to ensure that all rows have exactly one
        value for each column in the dataset schema.

        Raises ValueError in case of a schema violation.
        """
        for i in range(len(self.rows)):
            row = self.rows[i]
            if len(row.values) != len(self.columns):
                raise ValueError('schema violation for row \'' + str(i) + '\'')


class DatasetColumn(object):
    """Column in a dataset. Each column has a unique identifier and a name.

    Attributes
    ----------
    identifier: int
        Unique column identifier
    name: string
        Column name
    """
    def __init__(self, identifier, name):
        """Initialize the column object.

        Parameters
        ----------
        identifier: int
            Unique column identifier
        name: string
            Column name
        """
        self.identifier = identifier
        self.name = name


class DatasetRow(object):
    """Row in a Vizier DB dataset.

    Attributes
    ----------
    identifier: int
        Unique row identifier
    values : list(string)
        List of column values in the row
    """
    def __init__(self, identifier, values, dataset=None):
        """Initialize the row object.

        Parameters
        ----------
        identifier: int
            Unique row identifier
        values : list(string)
            List of column values in the row
        dataset : Dataset, optional
            Reference to dataset that contains the row
        """
        self.identifier = identifier
        self.values = values
        self.dataset = dataset

    def get_value(self, column):
        """Get the row value for the given column.

        Parameters
        ----------
        column : int or string
            Column index, name, or label

        Returns
        -------
        string
        """
        col_index = self.dataset.column_index(column)
        return self.values[col_index]

    def set_value(self, column, value, clear_annotations=True):
        """Set the row value for the given column.

        Parameters
        ----------
        column : int or string
            Column index, name, or label
        value : string
            New cell value
        keep_annotations: bool, optional
            Flag indicating whether to keep or clear the annotations that are
            associated with this cell
        """
        col_index = self.dataset.column_index(column)
        self.values[col_index] = value
        if clear_annotations:
            self.dataset.annotations.clear_cell(
                self.dataset.columns[col_index].identifier,
                self.identifier
            )


# ------------------------------------------------------------------------------
# Datastore
# ------------------------------------------------------------------------------

class DataStore(VizierSystemComponent):
    """Abstract API to store and retireve Vizier datasets."""
    def __init__(self, build):
        """Initialize the build information. Expects a dictionary containing two
        elements: name and version.

        Raises ValueError if build dictionary is invalid.

        Parameters
        ---------
        build : dict()
            Build information
        """
        super(DataStore, self).__init__(build)

    def components(self):
        """List containing component descriptor.

        Returns
        -------
        list
        """
        return [component_descriptor('datastore', self.system_build())]

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def update_annotation(self, identifier, upd_stmt):
        """Update the annotations for a component of the datasets with the given
        identifier. Returns the updated annotations or None if the dataset
        does not exist.

        Update is handled by the update statement to avoid too much code
        repetition for different data store implementations. The annotation
        update statement captures the logic to identify the component that is
        being updated.

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
        raise NotImplementedError


# ------------------------------------------------------------------------------
#
# Helper Methods
#
# ------------------------------------------------------------------------------

def collabel_2_index(label):
    """Convert a column label into a column index (based at 0), e.g., 'A'-> 1,
    'B' -> 2, ..., 'AA' -> 27, etc.

    Returns -1 if the given labe is not composed only of upper case letters A-Z.
    Parameters
    ----------
    label : string
        Column label (expected to be composed of upper case letters A-Z)

    Returns
    -------
    int
    """
    # The following code is adopted from
    # https://stackoverflow.com/questions/7261936/convert-an-excel-or-spreadsheet-column-letter-to-its-number-in-pythonic-fashion
    num = 0
    for c in label:
        if ord('A') <= ord(c) <= ord('Z'):
            num = num * 26 + (ord(c) - ord('A')) + 1
        else:
            return -1
    return num


def dataset_from_file(f_handle):
    """Create dataset instance from a handle to a CSV/TSV file on the file
    server.

    Raises ValueError if the file cannot be parsed correctly.

    Parameters
    ----------
    f_handle: vizier.filestore.base.FileHandle
        Handle for file on file server

    Returns
    -------
    vizier.datastore.base.Dataset
    """
    # Make sure that the given file has been parsed successfully on upload
    if not f_handle.is_verified_csv:
        raise ValueError('failed to create dataset from file \'' + f_handle.name + '\'')
    with f_handle.open() as csvfile:
        columns = []
        column_counter = 0
        reader = csv.reader(
            csvfile,
            delimiter=f_handle.delimiter,
            skipinitialspace=True
        )
        for col_name in reader.next():
            columns.append(DatasetColumn(column_counter, col_name))
            column_counter += 1
        dataset = Dataset(columns=columns, column_counter=column_counter)
        for row in reader:
            dataset.add_row(row)
    return dataset
