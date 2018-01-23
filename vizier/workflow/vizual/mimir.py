"""VizUAL API - Defines and interface to a (persistent) data store engine for
the VizUAL data curation language. The engine allows manipulation of datasets
via VizUAL commands.
"""

from abc import abstractmethod
import csv
import gzip

import vistrails.packages.mimir.init as mimir

from vizier.core.system import build_info
from vizier.core.util import is_valid_name, get_unique_identifier
from vizier.datastore.base import Dataset
from vizier.datastore.mimir import MimirDatasetColumn, MimirDatasetDescriptor
from vizier.datastore.mimir import COL_PREFIX, ROW_ID
from vizier.workflow.vizual.base import DefaultVizualEngine


class MimirVizualEngine(DefaultVizualEngine):
    """Implementation for VizUAL DB Engine unsing Mimir. Translates most VizUAL
    commands into SQL views.
    """
    def __init__(self, datastore, fileserver):
        """Initialize the Mimir datastore that is used to retrieve and update
        datasets and the file server managing CSV files.

        Parameters
        ----------
        datastore : vizier.datastore.mimir.MimirDataStore
            Datastore to retireve and update datasets.
        fileserver:  vizier.filestore.base.FileSever
            File server to access uploaded  CSV files
        """
        super(MimirVizualEngine, self).__init__(
            datastore,
            fileserver,
            build=build_info('MimirVizualEngine')
        )

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
        if not identifier in self.datastore.datasets:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        dataset = self.datastore.datasets[identifier]
        # Get the index of the specified column that is to be deleted.
        col_index = dataset.column_index(column)
        # Delete column from schema
        schema = list(dataset.columns)
        del schema[col_index]
        # Create a view for the modified schema
        col_list = [ROW_ID]
        for col in schema:
            col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds_id = get_unique_identifier()
        ds = MimirDatasetDescriptor(
            ds_id,
            schema,
            view_name,
            dataset.row_ids,
            dataset.column_counter,
            dataset.row_counter,
            dataset.annotations
        )
        self.datastore.update_dataset(ds_id, ds)
        return 1, ds_id

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
        if not identifier in self.datastore.datasets:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        dataset = self.datastore.datasets[identifier]
        # Make sure that row refers a valid row in the dataset
        if row < 0 or row >= len(dataset.row_ids):
            raise ValueError('invalid row index \'' + str(row) + '\'')
        # Get the id of the row that is being deleted and modify the row id list
        # for the resulting dataset
        rows = list(dataset.row_ids)
        row_id = rows[row]
        del rows[row]
        # Create a view for the modified dataset
        col_list = [ROW_ID]
        for col in dataset.columns:
            col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        sql += ' WHERE ' + ROW_ID + ' <> ' + dataset.rowid_to_sql_string(row_id)
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds_id = get_unique_identifier()
        ds = MimirDatasetDescriptor(
            ds_id,
            dataset.columns,
            view_name,
            rows,
            dataset.column_counter,
            dataset.row_counter,
            dataset.annotations
        )
        self.datastore.update_dataset(ds_id, ds)
        return 1, ds_id

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
        if not identifier in self.datastore.datasets:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        dataset = self.datastore.datasets[identifier]
        # Make sure that position is a valid column index in the new dataset
        if position < 0 or position > len(dataset.columns):
            raise ValueError('invalid column index \'' + str(position) + '\'')
        # Get name for new column
        col_id = dataset.column_counter
        dataset.column_counter += 1
        # Insert new column into schema
        schema = list(dataset.columns)
        new_column = MimirDatasetColumn(col_id, name, COL_PREFIX + str(col_id))
        schema.insert(position, new_column)
        # Create a view for the modified schema
        col_list = [ROW_ID]
        for col in schema:
            if col.identifier == new_column.identifier:
                col_list.append('\'\' AS ' + col.name_in_rdb)
            else:
                col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds_id = get_unique_identifier()
        ds = MimirDatasetDescriptor(
            ds_id,
            schema,
            view_name,
            dataset.row_ids,
            dataset.column_counter,
            dataset.row_counter,
            dataset.annotations
        )
        self.datastore.update_dataset(ds_id, ds)
        return 1, ds_id

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
        if not identifier in self.datastore.datasets:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        dataset = self.datastore.datasets[identifier]
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > len(dataset.row_ids):
            raise ValueError('invalid row index \'' + str(position) + '\'')
        # Get unique id for new row
        row_id = dataset.row_counter
        dataset.row_counter += 1
        row_ids = list(dataset.row_ids)
        row_ids.insert(position, row_id)
        # Create a view for the modified schema
        col_list = [ROW_ID]
        for col in dataset.columns:
            col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        union_list = [dataset.rowid_to_sql_string(row_id) + ' AS ' + ROW_ID]
        for col in dataset.columns:
            union_list.append('NULL AS ' + col.name_in_rdb)
        sql = '(' + sql + ') UNION ALL (SELECT ' + ','.join(union_list) + ')'
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds_id = get_unique_identifier()
        ds = MimirDatasetDescriptor(
            ds_id,
            dataset.columns,
            view_name,
            row_ids,
            dataset.column_counter,
            dataset.row_counter,
            dataset.annotations
        )
        self.datastore.update_dataset(ds_id, ds)
        return 1, ds_id

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
        if not identifier in self.datastore.datasets:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        dataset = self.datastore.datasets[identifier]
        # Make sure that position is a valid column index in the new dataset
        if position < 0 or position > len(dataset.columns):
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # Get index position of column that is being moved
        source_idx = dataset.column_index(column)
        # No need to do anything if source position equals target position
        if source_idx != position:
            # Get a new identifier for the dataset.
            ds_id = get_unique_identifier()
            # There are no changes to the underlying database. We only need to
            # change the column information in the dataset schema.

            schema = list(dataset.columns)
            schema.insert(position, schema.pop(source_idx))
            # Store updated dataset to get new identifier
            self.datastore.datasets[ds_id] = MimirDatasetDescriptor(
                ds_id,
                schema,
                dataset.table_name,
                dataset.row_ids,
                dataset.column_counter,
                dataset.row_counter,
                dataset.annotations
            )
            self.datastore.write_dataset_index()
            return 1, ds_id
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
        if not identifier in self.datastore.datasets:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        dataset = self.datastore.datasets[identifier]
        # Make sure that row is within dataset bounds
        if row < 0 or row >= dataset.row_count:
            raise ValueError('invalid source row \'' + str(row) + '\'')
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > dataset.row_count:
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # No need to do anything if source position equals target position
        if row != position:
            dataset.row_ids.insert(position, dataset.row_ids.pop(row))
            # Get a new identifier for the dataset.
            ds_id = get_unique_identifier()
            # Store updated dataset to get new identifier
            self.datastore.datasets[ds_id] = MimirDatasetDescriptor(
                ds_id,
                dataset.columns,
                dataset.table_name,
                dataset.row_ids,
                dataset.column_counter,
                dataset.row_counter,
                dataset.annotations
            )
            self.datastore.write_dataset_index()
            return 1, ds_id
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
        if not identifier in self.datastore.datasets:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        dataset = self.datastore.datasets[identifier]
        # Get the specified column that is to be renamed and set the column name
        # to the new name
        schema = list(dataset.columns)
        col = schema[dataset.column_index(column)]
        # No need to do anything if the name hasn't changed
        if col.name.lower() != name.lower():
            # Get a new identifier for the dataset.
            ds_id = get_unique_identifier()
            # There are no changes to the underlying database. We only need to
            # change the column information in the dataset schema.
            col.name = name
            # Store updated dataset to get new identifier
            self.datastore.datasets[ds_id] = MimirDatasetDescriptor(
                ds_id,
                schema,
                dataset.table_name,
                dataset.row_ids,
                dataset.column_counter,
                dataset.row_counter,
                dataset.annotations
            )
            self.datastore.write_dataset_index()
            return 1, ds_id
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
        if not identifier in self.datastore.datasets:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        dataset = self.datastore.datasets[identifier]
        # Make sure that row refers a valid row in the dataset
        if row < 0 or row >= dataset.row_count:
            raise ValueError('invalid cell [' + str(column) + ', ' + str(row) + ']')
        # Get the index of the specified cell column
        col_index = dataset.column_index(column)
        # Get id of the cell row
        row_id = dataset.row_ids[row]
        # Create a view for the modified dataset
        col_list = [ROW_ID]
        for i in range(len(dataset.columns)):
            col = dataset.columns[i]
            if i == col_index:
                if col.is_numeric:
                    try:
                        val_stmt = str(int(value))
                    except ValueError:
                        try:
                            val_stmt = str(float(value))
                        except ValueError:
                            val_stmt = '\'' + value + '\''
                else:
                    val_stmt = '\'' + value + '\''
                stmt = 'CASE WHEN ' + ROW_ID + ' = ' + dataset.rowid_to_sql_string(row_id) + ' THEN '
                stmt += val_stmt + ' ELSE ' + col.name_in_rdb + ' END '
                stmt += 'AS ' + col.name_in_rdb
                col_list.append(stmt)
            else:
                col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds_id = get_unique_identifier()
        ds = MimirDatasetDescriptor(
            ds_id,
            dataset.columns,
            view_name,
            dataset.row_ids,
            dataset.column_counter,
            dataset.row_counter,
            dataset.annotations
        )
        self.datastore.update_dataset(ds_id, ds)
        return 1, ds_id