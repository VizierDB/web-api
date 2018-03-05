"""File System Data Store - File System-based implementation of the Vizier DB
dataset store.

Maintains individual datasets in individual sub-folders of a base directory.
For each dataset two files are maintained: info.yaml contains the dataset
handle and data.tsv.gz contains the tab-delimited data file.
"""

import os
import shutil

from vizier.core.system import build_info
from vizier.core.util import get_unique_identifier
from vizier.datastore.base import Dataset, DatasetRow, DataStore
from vizier.datastore.base import dataset_from_file
from vizier.datastore.metadata import DatasetMetadata


"""Constants for data file names."""
DATA_FILE = 'dataset.yaml'
METADATA_FILE = 'annotation.yaml'


class FileSystemDataStore(DataStore):
    """Implementation of Vizier data store. Uses the file system to maintain
    datasets.
    """
    def __init__(self, base_dir):
        """Initialize the base directory that contains datasets. Each dataset is
        maintained in a separate sub-folder.

        Parameters
        ---------
        base_dir : string
            base directory for the data store
        """
        super(FileSystemDataStore, self).__init__(
            build_info('FileSystemDataStore')
        )
        self.base_dir = os.path.abspath(base_dir)
        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)

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
        separate method will make future changes to the folder structure that is
        used to store datasets easire.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return os.path.join(self.base_dir, identifier)

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
        dataset_dir = self.get_dataset_dir(identifier)
        if os.path.isdir(dataset_dir):
            return Dataset.from_file(
                os.path.join(dataset_dir, DATA_FILE),
                DatasetMetadata.from_file(
                    os.path.join(dataset_dir, METADATA_FILE)
                )
            )
        return None

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
        return self.store_dataset(dataset_from_file(f_handle))

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
        # Get new identifier and create directory for new dataset
        dataset.identifier = get_unique_identifier()
        dataset_dir = self.get_dataset_dir(dataset.identifier)
        os.makedirs(dataset_dir)
        # Write dataset file
        dataset.to_file(os.path.join(dataset_dir, DATA_FILE))
        # Write metadata file
        dataset.annotations.to_file(os.path.join(dataset_dir, METADATA_FILE))
        # Return handle for new dataset
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
        dataset_dir = self.get_dataset_dir(identifier)
        if not os.path.isdir(dataset_dir):
            return None
        # Read annotations from file, evaluate update statement and write result
        # back to file.
        annotations = upd_stmt.eval(
            DatasetMetadata.from_file(
                os.path.join(dataset_dir, METADATA_FILE)
            )
        )
        annotations.to_file(os.path.join(dataset_dir, METADATA_FILE))
        return annotations
