"""VizUAL API - Defines and interface to a (persistent) data store engine for
the VizUAL data curation language. The engine allows manipulation of datasets
via VizUAL commands.
"""

from abc import abstractmethod
import csv
import gzip

from vizier.core.util import is_valid_name
from vizier.core.system import build_info, component_descriptor
from vizier.core.system import VizierSystemComponent
from vizier.datastore.base import Dataset, DatasetColumn


class VizualEngine(VizierSystemComponent):
    """Abstract interface to Vizual engine that allows manipulation of datasets
    via VizUAL commands. There may be various implementations of this interface
    for different storage backends.
    """
    def __init__(self, build):
        """Initialize the build information. Expects a dictionary containing two
        elements: name and version.

        Raises ValueError if build dictionary is invalid.

        Parameters
        ---------
        build : dict()
            Build information
        """
        super(VizualEngine, self).__init__(build)

    def components(self):
        """List containing component descriptor.

        Returns
        -------
        list
        """
        return [component_descriptor('vizual', self.system_build())]

    @abstractmethod
    def delete_column(self, identifier, column):
        """Delete a column in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column: int or string
            Column index, name or label for column that is being deleted

        Returns
        -------
        int, string
            Number of deleted columns (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
    def delete_row(self, identifier, row):
        """Delete a row in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        row: int
            Row index for deleted row

        Returns
        -------
        int, string
            Number of deleted rows (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
    def insert_column(self, identifier, position, name=None):
        """Insert column with given name at given position in dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column position is outside of the current schema bounds, or if
        the column name is invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        position: int
            Index position at which the column will be inserted
        name: string, optional
            New column name

        Returns
        -------
        int, string
            Number of inserted columns (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
    def insert_row(self, identifier, position):
        """Insert row at given position in a dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row psotion isoutside the dataset bounds.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        position: int
            Index position at which the row will be inserted

        Returns
        -------
        int, string
            Number of inserted rows (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
    def load_dataset(self, dataset_name, file_id):
        """Create (or load) a new dataset from a given Uri. The format of the
        Uri and the method to resolve the Uri and retireve the data are all
        implementation dependent.

        Reaise ValueError if (1) the Uri is invalid, or (2) the Uri references
        a non-existing resource.

        Parameters
        ----------
        file_id: string
            Identifier of the file on the file server from which the dataset is
            being created

        Returns
        -------
        vizier.datastore.base.Dataset
        """
        raise NotImplementedError

    @abstractmethod
    def move_column(self, identifier, column, position):
        """Move a column within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown or the target position invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column: int or string
            Column index, name or label for column that is being deleted
        position: int
            Target position for the column

        Returns
        -------
        int, string
            Number of moved columns (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
    def move_row(self, identifier, row, position):
        """Move a row within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row or position is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        row: int
            Row index for deleted row
        position: int
            Target position for the row

        Returns
        -------
        int, string
            Number of moved rows (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
    def rename_column(self, identifier, column, name):
        """Rename column in a given dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column is unknown, or if the given column name is invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column: int or string
            Column index, name or label for renamed column
        name: string
            New column name

        Returns
        -------
        int, string
            Number of renamed columns (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
    def update_cell(self, identifier, column, row, value):
        """Update a cell in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified cell is outside of the current dataset ranges.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column: int
            Column index for updated cell (starting at 0)
        row: int
            Row index for updated cell (starting at 0)
        value: string
            New cell value

        Returns
        -------
        int, string
            Number of updated rows (i.e., 0 or 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError


class DefaultVizualEngine(VizualEngine):
    """Default implementation for VizUAL DB Engine. Manipulates datasets in
    memory and uses a datastore object to store changes.
    """
    def __init__(self, datastore, fileserver, build=None):
        """Initialize the datastore that is used to retrieve and update
        datasets and the file server managing CSV files.

        Parameters
        ----------
        datastore : vizier.datastore.base.DataStore
            Datastore to retireve and update datasets.
        fileserver:  vizier.filestore.base.FileSever
            File server to access uploaded  CSV files
        """
        if build is None:
            build = build_info('DefaultVizualEngine')
        super(DefaultVizualEngine, self).__init__(build)
        self.datastore = datastore
        self.fileserver = fileserver

    def delete_column(self, identifier, column):
        """Delete a column in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column : int or string
            Column index, name or label for column that is being deleted

        Returns
        -------
        int, string
            Number of deleted columns (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Get the index of the specified column that is to be deleted.
        col_index = dataset.column_index(column)
        # Delete column from schema
        del dataset.columns[col_index]
        # Delete all value for the deleted column
        for row in dataset.rows:
            del row.values[col_index]
        # Store updated dataset to get new identifier
        ds = self.datastore.store_dataset(dataset)
        return 1, ds.identifier

    def delete_row(self, identifier, row):
        """Delete a row in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row is not within the range of the dataset.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        row : int
            Row index for deleted row

        Returns
        -------
        int, string
            Number of deleted rows (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that row refers a valid row in the dataset
        if row < 0 or row >= len(dataset.rows):
            raise ValueError('invalid row index \'' + str(row) + '\'')
        # Delete the row at the given position
        del dataset.rows[row]
        # Store updated dataset to get new identifier
        ds = self.datastore.store_dataset(dataset)
        return 1, ds.identifier

    def insert_column(self, identifier, position, name):
        """Insert column with given name at given position in dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column position is outside of the current schema bounds, or if
        the column name is invalid.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        position : int
            Index position at which the column will be inserted
        name : string
            New column name

        Returns
        -------
        int, string
            Number of inserted columns (i.e., 1) and identifier of resulting
            dataset
        """
        # Raise ValueError if given colum name is invalid
        if not is_valid_name(name):
            raise ValueError('invalid column name \'' + name + '\'')
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that position is a valid column index in the new dataset
        if position < 0 or position > len(dataset.columns):
            raise ValueError('invalid column index \'' + str(position) + '\'')
        # Insert new column into dataset
        dataset.add_column(name, position)
        # Store updated dataset to get new identifier
        ds = self.datastore.store_dataset(dataset)
        return 1, ds.identifier

    def insert_row(self, identifier, position):
        """Insert row at given position in a dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row psotion isoutside the dataset bounds.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        position : int
            Index position at which the row will be inserted

        Returns
        -------
        int, string
            Number of inserted rows (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > len(dataset.rows):
            raise ValueError('invalid row index \'' + str(position) + '\'')
        # Create empty set of values
        dataset.add_row(position=position)
        # Store updated dataset to get new identifier
        ds = self.datastore.store_dataset(dataset)
        return 1, ds.identifier

    def load_dataset(self, file_id):
        """Create (or load) a new dataset from a given Uri. The format of the
        Uri and the method to resolve the Uri and retireve the data are all
        implementation dependent.

        Reaise ValueError if (1) the Uri is invalid, or (2) the Uri references
        a non-existing resource.

        Parameters
        ----------
        file_id: string
            Identifier of the file on the file server from which the dataset is
            being created

        Returns
        -------
        vizier.datastore.base.Dataset
        """
        # Ensure that file name references a previously uploaded file.
        f_handle = self.fileserver.get_file(file_id)
        if f_handle is None:
            raise ValueError('unknown file \'' + file_id + '\'')
        # Open CSV file reader. By default, all files on the file server are
        # stored as gzipped TSV files
        csvfile = self.fileserver.open_file(file_id)
        columns = []
        column_counter = 0
        for col_name in csvfile.reader.next():
            columns.append(DatasetColumn(column_counter, col_name))
            column_counter += 1
        dataset = Dataset(columns=columns, column_counter=column_counter)
        for row in csvfile.reader:
            dataset.add_row(row)
        csvfile.close()
        # Create dataset and return handle
        return self.datastore.store_dataset(dataset)

    def move_column(self, identifier, column, position):
        """Move a column within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown or the target position invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column: int or string
            Column index, name or label for column that is being deleted
        position: int
            Target position for the column

        Returns
        -------
        int, string
            Number of moved columns (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that position is a valid column index in the new dataset
        if position < 0 or position > len(dataset.columns):
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # Get index position of column that is being moved
        source_idx = dataset.column_index(column)
        # No need to do anything if source position equals target position
        if source_idx != position:
            dataset.columns.insert(position, dataset.columns.pop(source_idx))
            for row in dataset.rows:
                row.values.insert(position, row.values.pop(source_idx))
            # Store updated dataset to get new identifier
            ds = self.datastore.store_dataset(dataset)
            return 1, ds.identifier
        else:
            return 0, identifier

    def move_row(self, identifier, row, position):
        """Move a row within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row or position is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        row: int
            Row index for deleted row
        position: int
            Target position for the row

        Returns
        -------
        int, string
            Number of moved rows (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that row is within dataset bounds
        if row < 0 or row >= len(dataset.rows):
            raise ValueError('invalid source row \'' + str(row) + '\'')
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > len(dataset.rows):
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # No need to do anything if source position equals target position
        if row != position:
            dataset.rows.insert(position, dataset.rows.pop(row))
            # Store updated dataset to get new identifier
            ds = self.datastore.store_dataset(dataset)
            return 1, ds.identifier
        else:
            return 0, identifier

    def rename_column(self, identifier, column, name):
        """Rename column in a given dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column is unknown, or if the given column name is invalid.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column : int or string
            Column index, name or label for renamed column
        name : string
            New column name

        Returns
        -------
        int, string
            Number of renamed columns (i.e., 1) and identifier of resulting
            dataset
        """
        # Raise ValueError if given colum name is invalid
        if not is_valid_name(name):
            raise ValueError('invalid column name \'' + name + '\'')
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Get the specified column that is to be renamed and set the column name
        # to the new name
        col_idx = dataset.column_index(column)
        # Nothing needs to be changed if name does not differ from column name
        if dataset.columns[col_idx].name.lower() != name.lower():
            dataset.columns[col_idx].name = name
            # Store updated dataset to get new identifier
            ds = self.datastore.store_dataset(dataset)
            return 1, ds.identifier
        else:
            return 0, identifier

    def update_cell(self, identifier, column, row, value):
        """Update a cell in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified cell is outside of the current dataset ranges.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column : int or string
            Column index, name or label for updated cell (starting at 0)
        row : int
            Row index for updated cell (starting at 0)
        value : string
            New cell value

        Returns
        -------
        int, string
            Number of updated rows (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that row refers a valid row in the dataset
        if row < 0 or row >= len(dataset.rows):
            raise ValueError('invalid cell [' + str(column) + ', ' + str(row) + ']')
        # Update the specified cell in the given data array
        dataset.rows[row].set_value(column, value)
        # Store updated dataset to get new identifier
        ds = self.datastore.store_dataset(dataset)
        return 1, ds.identifier