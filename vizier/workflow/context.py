# Copyright (C) 2018 New York University
#                    University at Buffalo,
#                    Illinois Institute of Technology.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Objects in the execution environment for data curation workflows. Primarily
intended for use within VisTrails modules compute() metho.

The workflow context maintains environment configuration parameters, mappings
of dataset names to dataset identifier, and a dictionary of variables accessed
by the Python cells. The context type (DEFAULT or VOLATILE) determines how
dataset updates are handled. In a volatile context dataset updates are not
persisted or propagated to following modules.

The Vizier datastore client enables access to and manipulation of datasets in a
Vizier datastore from within a python script.
"""

from vizier.core.util import is_valid_name
from vizier.datastore.base import max_column_id, max_row_id
from vizier.datastore.client import DatasetClient
from vizier.datastore.metadata import DatasetMetadata


"""Context types"""
CONTEXT_DEFAULT = 'DEFAULT'
CONTEXT_VOLATILE = 'VOLATILE'

"""Components of the workflow context dictionary."""
VZRENV_ENV = 'env'
VZRENV_DATASETS = 'datasets'
VZRENV_VARS = 'variables'
VZRENV_TYPE = 'type'

"""Environment configuration parameter."""
VZRENV_ENV_DATASTORE = 'datastore'
VZRENV_ENV_FILESERVER = 'fileserver'
VZRENV_ENV_IDENTIFIER = 'identifier'

"""Components of a dataset mapping."""
VZRENV_DATASETS_MODULEID = 'moduleId'
VZRENV_DATASETS_MAPPING = 'mapping'

"""Context variable name for Vizier DB Client."""
VZRENV_VARS_DBCLIENT = 'vizierdb'


class VizierDBClient(object):
    """The Vizier DB Client provides access to datasets that are identified by
    a unique name.
    """
    def __init__(self, datastore, datasets, vizual):
        """Initialize the reference to the workflow context and the datastore.

        Parameters
        ----------
        datastore: vizier.datastore.base.DataStore
            Data store to access and manipulate datasets
        datasets: dict
            Mapping of dataset names to unique persistent dataset identifier
            generated by the data store.
        vizual: vizier.worktrail.vizual.engine.VizualEngine
            Vizual commands execution engine
        """
        self.datastore = datastore
        self.datasets = datasets
        self.vizual = vizual

    def create_dataset(self, name, dataset):
        """Create a new dataset with given name.

        Raises ValueError if a dataset with given name already exist.

        Parameters
        ----------
        name : string
            Unique dataset name
        dataset : vizier.datastore.base.Dataset
            Dataset object

        Returns
        -------
        vizier.datastore.client.DatasetClient
        """
        # Raise an exception if a dataset with the given name already exists or
        # if the name is not valid
        if self.has_dataset_identifier(name):
            raise ValueError('dataset \'' + name + '\' already exists')
        if not is_valid_name(name):
            raise ValueError('invalid dataset name \'' + name + '\'')
        columns = dataset.columns
        rows = dataset.rows
        column_counter = max(max_column_id(columns) + 1, 0)
        row_counter = max(max_row_id(rows) + 1, 0)
        # Ensure that all columns has positive identifier
        for col in columns:
            if col.identifier < 0:
                col.identifier = column_counter
                column_counter += 1
        # Ensure that all rows have positive identifier
        for row in rows:
            if row.identifier < 0:
                row.identifier = row_counter
                row_counter += 1
        # Write dataset to datastore and add new dataset to context
        ds = self.datastore.create_dataset(
            columns=columns,
            rows=rows,
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=dataset.annotations
        )
        self.set_dataset_identifier(name, ds.identifier)
        return DatasetClient(dataset=ds)

    def drop_dataset(self, name):
        """Remove the dataset with the given name.

        Raises ValueError if no dataset with given name exist.

        Parameters
        ----------
        name : string
            Unique dataset name
        """
        # Remove the context dataset identifier for the given name. Will raise
        # a ValueError if dataset does not exist
        self.remove_dataset_identifier(name)

    def get_dataset(self, name):
        """Get dataset with given name.

        Raises ValueError if the specified dataset does not exist.

        Parameters
        ----------
        name : string
            Unique dataset name

        Returns
        -------
        vizier.datastore.client.DatasetClient
        """
        # Get identifier for the dataset with the given name. Will raise an
        # exception if the name is unknown
        identifier = self.get_dataset_identifier(name)
        # Read dataset from datastore and return it.
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        return DatasetClient(dataset=dataset)

    def get_dataset_identifier(self, name):
        """Returns the unique identifier for the dataset with the given name.

        Raises ValueError if no dataset with the given name exists.

        Parameters
        ----------
        name: string
            Dataset name

        Returns
        -------
        string
        """
        # Datset names should be case insensitive
        key = name.lower()
        if not key in self.datasets:
            raise ValueError('unknown dataset \'' + name + '\'')
        return self.datasets[key]

    def has_dataset_identifier(self, name):
        """Test whether a mapping for the dataset with the given name exists.

        Parameters
        ----------
        name: string
            Dataset name

        Returns
        -------
        bool
        """
        # Dataset names are case insensitive
        return name.lower() in self.datasets

    def new_dataset(self):
        """Get a dataset client instance for a new dataset.

        Returns
        -------
        vizier.datastore.client.DatasetClient
        """
        return DatasetClient()

    def remove_dataset_identifier(self, name):
        """Remove the entry in the dataset distionary that is associated with
        the given name. Raises ValueError if not dataset with name exists.

        Parameters
        ----------
        name: string
            Dataset name
        identifier: string
            Unique identifier for persistent dataset
        """
        # Convert name to lower case to ensure that names are case insensitive
        key = name.lower()
        if not key in self.datasets:
            raise ValueError('unknown dataset \'' + name + '\'')
        del self.datasets[key]

    def rename_dataset(self, name, new_name):
        """Rename an existing dataset.

        Raises ValueError if a dataset with given name already exist.

        Raises ValueError if dataset with name does not exist or if dataset with
        new_name already exists.

        Parameters
        ----------
        name : string
            Unique dataset name
        new_name : string
            New dataset name
        """
        # Raise exception if new_name exists or is not valid.
        if self.has_dataset_identifier(new_name):
            raise ValueError('dataset \'' + new_name + '\' exists')
        if not is_valid_name(new_name):
            raise ValueError('invalid dataset name \'' + new_name + '\'')
        # Raise an exception if no dataset with the given name exists
        identifier = self.get_dataset_identifier(name)
        self.remove_dataset_identifier(name)
        self.set_dataset_identifier(new_name, identifier)

    def set_dataset_identifier(self, name, identifier):
        """Sets the identifier to which the given dataset name points.

        Parameters
        ----------
        name: string
            Dataset name
        identifier: string
            Unique identifier for persistent dataset
        """
        # Convert name to lower case to ensure that names are case insensitive
        self.datasets[name.lower()] = identifier

    def update_dataset(self, name, dataset):
        """Update a given dataset.

        Raises ValueError if the specified dataset does not exist.

        Parameters
        ----------
        name : string
            Unique dataset name
        dataset : vizier.datastore.base.Dataset
            Dataset object

        Returns
        -------
        vizier.datastore.client.DatasetClient
        """
        # Get identifier for the dataset with the given name. Will raise an
        # exception if the name is unknown
        identifier = self.get_dataset_identifier(name)
        # Read dataset from datastore to get the column and row counter.
        source_dataset = self.datastore.get_dataset(identifier)
        if source_dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        column_counter = source_dataset.column_counter
        row_counter = source_dataset.row_counter
        # Update column and row identifier
        columns = dataset.columns
        rows = dataset.rows
        # Ensure that all columns has positive identifier
        for col in columns:
            if col.identifier < 0:
                col.identifier = column_counter
                column_counter += 1
        # Ensure that all rows have positive identifier
        for row in rows:
            if row.identifier < 0:
                row.identifier = row_counter
                row_counter += 1
        # Write dataset to datastore and add new dataset to context
        ds = self.datastore.create_dataset(
            columns=columns,
            rows=rows,
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=dataset.annotations
        )
        self.set_dataset_identifier(name, ds.identifier)
        return DatasetClient(dataset=ds)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def WorkflowContext(exec_env, context_type=CONTEXT_DEFAULT, datasets=None, variables=None):
    """Helper method to create the Viztrail context dictionary. The context
    contains a dictionary for the relevant execution environment configuration
    parameters, a dictionary for mappings between dataset names and dataset
    identifier for each module, a dictionary mapping dataset views from their
    workflow name to their internal identifier, and a dictionary for Python
    variables.

    Raises ValueError if an invalid context type identifier is given.

    Parameters
    ----------
    exec_env: vizier.config.ExecEnv
        Environment for execution of viztrail workflows
    context_type: string
        Identifier for context type (DEFAULT or VOLATILE)
    datasets: list, optional
        Initial mapping of datasets for each module
    variables: dict, optional
        Dictionary of global variables for Python cells.
    datasets: list, optional
        Initial mapping of datasets for each module

    Returns
    -------
    dict()
    """
    # Raise exception if invalid context type identifier is given
    if not context_type in [CONTEXT_DEFAULT, CONTEXT_VOLATILE]:
        raise ValueError('invalid context type \'' + context_type + '\'')
    # Initialize the context dictionary
    context = {
        VZRENV_TYPE: context_type
    }
    # Add environment configuration
    context[VZRENV_ENV] = {
        VZRENV_ENV_IDENTIFIER: exec_env.identifier,
        VZRENV_ENV_DATASTORE: exec_env.datastore.directory,
        VZRENV_ENV_FILESERVER: exec_env.fileserver.directory
    }
    # Initialize the dataset mapping
    if not datasets is None:
        context[VZRENV_DATASETS] = list(datasets)
    else:
        context[VZRENV_DATASETS] = list()
    # Create dictionary for Python variables.
    if variables is None:
        context[VZRENV_VARS] = dict()
    else:
        context[VZRENV_VARS] = variables
    # Return context
    return context


def get_datasets(datasets, module_id):
    """Get the dataset mapping for the module with the given identifier.

    Parameters
    ----------
    datasets: list
        List of (module-id, dataset-mapping)-pairs
    module_id: int
        Unique module identifier

    Returns
    -------
    int
    """
    for i in range(len(datasets)):
        if datasets[i][VZRENV_DATASETS_MODULEID] == module_id:
            return datasets[i][VZRENV_DATASETS_MAPPING]
