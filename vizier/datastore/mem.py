"""Main memory based data store. Datasets are not stored permanently. This
implementation is primarily for test purposes.
"""

import csv

from vizier.core.system import build_info
from vizier.core.util import get_unique_identifier
from vizier.datastore.base import DatasetHandle, DatasetColumn, DatasetRow
from vizier.datastore.base import DataStore, max_column_id, max_row_id
from vizier.datastore.base import validate_schema
from vizier.datastore.metadata import DatasetMetadata
from vizier.datastore.reader import InMemDatasetReader


class InMemDatasetHandle(DatasetHandle):
    """Handle for a dataset that are kept in memory."""
    def __init__(
        self, identifier, columns, rows, column_counter=0, row_counter=0,
        annotations=None
    ):
        """Initialize the dataset handle.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.
        columns: list(vizier.datastore.base.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        rows: list(vizier.datastore.base.DatasetRow)
            List of rows in the dataset
        column_counter: int, optional
            Counter to generate unique column identifier
        row_counter: int, optional
            Counter to generate unique row identifier
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components
        """
        super(InMemDatasetHandle, self).__init__(
            identifier=identifier,
            columns=columns,
            row_count=len(rows),
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=annotations
        )
        self.datarows = rows

    @staticmethod
    def from_file(f_handle):
        """Read dataset from file. Expects the file to be in Json format which
        is the default serialization format used by to_file().

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle
            Handle for an uploaded file on a file server

        Returns
        -------
        vizier.datastore.base.Dataset
        """
        # Expects a CSV/TSV file. The first row contains the column names.
        # Read all information and return a InMemDatasetHandle
        if not f_handle.is_verified_csv:
            raise ValueError('failed to create dataset from file \'' + f_handle.name + '\'')
        # Read all information and return a InMemDatasetHandle
        columns = []
        rows = []
        with f_handle.open() as csvfile:
            reader = csv.reader(csvfile, delimiter=f_handle.delimiter)
            for col_name in reader.next():
                columns.append(DatasetColumn(len(columns), col_name))
            for row in reader:
                rows.append(DatasetRow(len(rows), row))
        # Return InMemDatasetHandle
        return InMemDatasetHandle(
            identifier=get_unique_identifier(),
            columns=columns,
            rows=rows,
            column_counter=len(columns),
            row_counter=len(rows)
        )

    def reader(self):
        """Get reader for the dataset to access the dataset rows.

        Returns
        -------
        vizier.datastore.reader.DatasetReader
        """
        return InMemDatasetReader(self.datarows)


class InMemDataStore(DataStore):
    """Non-persistent implementation of data store. Maintains a dictionary of
    datasets in main memory. Data is not stored persistently.
    """
    def __init__(self):
        """Initialize the build information and data store dictionary."""
        super(InMemDataStore, self).__init__(build_info('InMemDataStore'))
        self.datasets = dict()

    def create_dataset(
        self, identifier=None, columns=None, rows=None, column_counter=None,
        row_counter=None, annotations=None
    ):
        """Create a new dataset in the data store for the given data.

        Raises ValueError if (1) any of the column or row identifier have a
        negative value, or (2) if the given column or row counter have value
        lower or equal to any of the column or row identifier.

        Parameters
        ----------
        identifier: string, optional
            Unique dataset identifier
        columns: list(vizier.datastore.base.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        rows: list(vizier.datastore.base.DatasetRow)
            List of dataset rows.
        column_counter: int, optional
            Counter to generate unique column identifier
        row_counter: int, optional
            Counter to generate unique row identifier
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.mem.InMemDatasetHandle
        """
        # Set columns and rows if not given
        if columns is None:
            columns = list()
        if rows is None:
            rows = list()
        else:
            # Validate the number of values in the given rows
            validate_schema(columns, rows)
        # Validate the given dataset schema. Will raise ValueError in case of
        # schema violations
        if identifier is None:
            identifier = get_unique_identifier()
        if column_counter is None:
            column_counter = max_column_id(columns) + 1
        if row_counter is None:
            row_counter = max_row_id(rows)
        # Make sure annotation sis not None
        if annotations is None:
            annotations = DatasetMetadata()
        self.datasets[identifier] = InMemDatasetHandle(
            identifier=identifier,
            columns=list(columns),
            rows=list(rows),
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=annotations.copy_metadata()
        )
        return self.datasets[identifier]

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
        if identifier in self.datasets:
            del self.datasets[identifier]
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
        vizier.datastore.base.DatasetHandle
        """
        if identifier in self.datasets:
            dataset = self.datasets[identifier]
            return InMemDatasetHandle(
                identifier=identifier,
                columns=[
                    DatasetColumn(col.identifier, col.name)
                        for col in dataset.columns
                ],
                rows=[
                    DatasetRow(row.identifier, list(row.values))
                        for row in dataset.fetch_rows()
                ],
                column_counter=dataset.column_counter,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations.copy_metadata()
            )

    def load_dataset(self, f_handle):
        """Create a new dataset from a given file.

        Raises ValueError if the given file could not be loaded as a dataset.

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle
            Handle for an uploaded file on a file server.

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        dataset = InMemDatasetHandle.from_file(f_handle)
        return self.create_dataset(
            identifier=dataset.identifier,
            columns=dataset.columns,
            rows=dataset.fetch_rows(),
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )

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
        if identifier in self.datasets:
            dataset = self.datasets[identifier]
            # Update dataset annotations
            dataset.annotations = upd_stmt.eval(dataset.annotations)
            return dataset.annotations
        return None


class VolatileDataStore(DataStore):
    """Non-persistent implementation of data store that reads datasets from
    an existing data store.

    This data store is primarily used to re-execute non-cached Python modules
    without manipulating the underlying persistent data store. Updates to
    datasets are kept in main-memory and discarded when the object is destroyed.
    """
    def __init__(self, datastore):
        """Initialize the build information and data store dictionary.

        Parameters
        ----------
        datastore: vizier.datastore.base.DataStore
            Existing data store containing the database state.
        """
        super(VolatileDataStore, self).__init__(build_info('VolatileDataStore'))
        self.datastore = datastore
        self.mem_store = InMemDataStore()
        self.deleted_datasets = set()

    def create_dataset(
        self, identifier=None, columns=None, rows=None, column_counter=None,
        row_counter=None, annotations=None
    ):
        """Create a new dataset in the data store for the given data.

        Raises ValueError if (1) any of the column or row identifier have a
        negative value, or (2) if the given column or row counter have value
        lower or equal to any of the column or row identifier.

        Parameters
        ----------
        identifier: string, optional
            Unique dataset identifier
        columns: list(vizier.datastore.base.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        rows: list(vizier.datastore.base.DatasetRow)
            List of dataset rows.
        column_counter: int, optional
            Counter to generate unique column identifier
        row_counter: int, optional
            Counter to generate unique row identifier
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.mem.InMemDatasetHandle
        """
        return self.mem_store.create_dataset(
            identifier=identifier,
            columns=columns,
            rows=rows,
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=annotations
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
        if identifier in self.mem_store.datasets:
            return self.mem_store.delete_dataset(identifier)
        elif not identifier in self.deleted_datasets:
            if not self.datastore.get_dataset(identifier) is None:
                self.deleted_datasets.add(identifier)
                return True
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
        vizier.datastore.base.DatasetHandle
        """
        if identifier in self.deleted_datasets:
            return None
        else:
            ds = self.mem_store.get_dataset(identifier)
            if not ds is None:
                return ds
            else:
                return self.datastore.get_dataset(identifier)
