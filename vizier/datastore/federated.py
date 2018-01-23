"""Federated Data Store - Provide read-only access to a set of data stores."""

from vizier.core.system import build_info, component_descriptor
from vizier.datastore.base import DataStore


class FederatedDataStore(DataStore):
    """Provide read-only access to a collection of data stores. This class is
    primarily used by the Web API to provide read access to the datastores that
    are associated with different workflow engines.

    This implementation assumes that dataset identifier that are used by
    different data stores are globally unique.
    """
    def __init__(self, datastores):
        """Initialize the collection of federated data stores.

        Parameters
        ---------
        datastores : list(vizier.datastore.base.DataStore)
            List of federated data store instances
        """
        super(FederatedDataStore, self).__init__(
            build_info('FederatedDataStore')
        )
        self.datastores = datastores

    def components(self):
        """List containing component descriptor.

        Returns
        -------
        list
        """
        comp = list()
        comp.append(component_descriptor('datastore', self.system_build()))
        for ds in self.datastores:
            comp.extend(ds.components())
        return comp

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
        # Assumes that at most one data store will contain a dataset with the
        # given identifier
        for store in self.datastores:
            ds = store.get_dataset(identifier)
            if not ds is None:
                return ds
        return None

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
        # Try all data stores. At most one would return a non-None value if the
        # dataset exists
        for store in self.datastores:
            result = store.update_annotation(identifier, upd_stmt)
            if not result is None:
                return result
        return None
