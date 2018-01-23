"""Main memory based data store. Datasets are not stored permanently. This
implementation is primarily for test purposes.
"""

from vizier.core.system import build_info
from vizier.core.util import get_unique_identifier
from vizier.datastore.base import DataStore, Dataset, DatasetRow


class InMemDataStore(DataStore):
    """Non-persistent implementation of data store. Maintains a dictionary of
    datasets in main memory. Data is not stored persistently.
    """
    def __init__(self):
        """Initialize the build information and data store dictionary."""
        super(InMemDataStore, self).__init__(build_info('InMemDataStore'))
        self.datasets = dict()

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
        vizier.datastore.base.Dataset
        """
        if identifier in self.datasets:
            dataset = self.datasets[identifier]
            return Dataset(
                identifier=identifier,
                columns=list(dataset.columns),
                rows=[
                    DatasetRow(row.identifier, list(row.values))
                        for row in dataset.rows
                ],
                column_counter=dataset.column_counter,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations.copy_metadata()
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
        identifier = get_unique_identifier()
        self.datasets[identifier] = Dataset(
            identifier=identifier,
            columns=list(dataset.columns),
            rows=[
                DatasetRow(row.identifier, list(row.values))
                    for row in dataset.rows
            ],
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations.copy_metadata()
        )
        return self.datasets[identifier]

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
        vizier.datastore.base.Dataset
        """
        if identifier in self.deleted_datasets:
            return None
        else:
            ds = self.mem_store.get_dataset(identifier)
            if not ds is None:
                return ds
            else:
                return self.datastore.get_dataset(identifier)

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
        return self.mem_store.store_dataset(dataset)
