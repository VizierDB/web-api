"""Dataset reader - Contains implementations of dataset reader for different
data storage formats.

Dataset reader implement the context manager interface and the iterator
interface.
"""
from abc import abstractmethod
import csv
import gzip
import json

from vizier.datastore.base import DatasetHandle, DatasetColumn, DatasetRow


class DatasetReader(object):
    """Reader for datasets. Allows to iterate over the the rows in a dataset.
    Rows are lists of values, one for each column.
    """
    def __enter__(self):
        """Context manager enter method.

        Returns
        -------
        vizier.datastore.base.DelimitedFileReader
        """
        return self.__iter__()

    def __exit__(self, type, value, traceback):
        """Context manager exit method.

        Returns
        -------
        bool
        """
        return self.close()

    def __iter__(self):
        """Iterator start method.

        Returns
        -------
        vizier.datastore.reader.DatasetReader
        """
        return self.open()

    @abstractmethod
    def close(self):
        """Signal the reader that no more rows will be read."""
        raise NotImplementedError

    @abstractmethod
    def next(self):
        """Return the next row in the dataset iterator. Raises StopIteration if
        end of dataset is reached.

        Returns
        -------
        vizier.datastore.base.DatasetRow
        """
        raise NotImplementedError

    @abstractmethod
    def open(self):
        """Setup the internal reader state to start reading at the first row in
        the dataset.

        Returns
        -------
        vizier.datastore.reader.DatasetReader
        """
        raise NotImplementedError


class DelimitedFileReader(DatasetReader):
    """Dataset reader for delimited files (CSV or TSV)."""
    def __init__(self, filename, compressed=False, delimiter=',', quotechar='"', has_row_ids=False):
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
        self.filename = filename
        # File format information
        self.compressed = compressed
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.has_row_ids = has_row_ids
        # Variables that maintain the internal state of the reader, i.e., the
        # opened file and the csv reader. If the is_open flag is True the file
        # handle (fd) and csv reader should not be None.
        self.is_open = False
        self.fh = None
        self.line_count = 0
        self.reader = None

    def close(self):
        """Close any open files and set the is_open flag to False."""
        if self.is_open:
            self.fh.close()
        self.fh = None
        self.reader = None
        self.line_count = -1
        self.is_open = False

    def next(self):
        """Return the next row in the dataset iterator. Raises StopIteration if
        end of file is reached or file has been closed.

        Automatically closes any open file when end of iteration is reached for
        the first time.

        Returns
        -------
        vizier.datastore.base.DatasetRow
        """
        if self.is_open:
            # Catch exception to close any open file
            try:
                row = self.reader.next()
                if self.has_row_ids:
                    row = DatasetRow(int(row[0]), row[1:])
                else:
                    row = DatasetRow(self.line_count, row)
                self.line_count += 1
                return row
            except StopIteration as ex:
                self.close()
                raise ex
        raise StopIteration

    def open(self):
        """Setup the reader by opening the associacted file and instantiating
        the csv reader.

        Returns
        -------
        vizier.datastore.base.DelimitedFileReader
        """
        # Only open if flag is false. Otherwise, return immediately
        if not self.is_open:
            if self.compressed:
                self.fh = gzip.open(self.filename, 'rb')
            else:
                self.fh = open(self.filename, 'r')
            self.reader = csv.reader(
                self.fh,
                delimiter=self.delimiter,
                quotechar=self.quotechar
            )
            self.line_count = 0
            self.is_open = True
        return self


class DefaultJsonDatasetReader(DatasetReader):
    """Dataset reader for datasets stored in defautl Json format. The default
    Json serialization has the following structure:
        {
            'rows': [
                {'id': int, 'values': [...]}
            ]
        }
    """
    def __init__(self, filename, compressed=False):
        """Initialize information about the Json file.

        Parameters
        ----------
        filename: string
            Path to the file on disk
        compressed: bool, optional
            Flag indicating if the file is compressed (gzip)
        """
        self.filename = filename
        self.compressed = compressed
        # Variables that maintain the internal state of the reader, i.e., the
        # opened file and the list of rows (in original Json format). If the
        # is_open flag is True the file handle (fd) and row list and read index
        # should not be None.
        self.is_open = False
        self.fh = None
        self.read_index = None
        self.rows = None

    def close(self):
        """Close any open files and set the is_open flag to False."""
        if self.is_open:
            self.fh.close()
        self.fh = None
        self.rows = None
        self.read_index = None
        self.is_open = False

    def next(self):
        """Return the next row in the dataset iterator. Raises StopIteration if
        end of file is reached or file has been closed.

        Automatically closes any open file when end of iteration is reached for
        the first time.

        Returns
        -------
        vizier.datastore.base.DatasetRow
        """
        if self.is_open:
            if self.read_index < len(self.rows):
                row = DatasetRow.from_dict(self.rows[self.read_index])
                self.read_index += 1
                return row
        raise StopIteration

    def open(self):
        """Setup the reader by opening the associacted file and instantiating
        the csv reader.

        Returns
        -------
        vizier.datastore.base.DelimitedFileReader
        """
        # Only open if flag is false. Otherwise, return immediately
        if not self.is_open:
            if self.compressed:
                self.fh = gzip.open(self.filename, 'rb')
            else:
                self.fh = open(self.filename, 'r')
            # Read the Json file and get the array of rows
            self.rows = json.loads(self.fh.read())['rows']
            self.read_index = 0
            self.is_open = True
        return self

    def write(self, rows):
        """Write the given list of dataset rows to file in default Json format.

        Parameters
        ----------
        rows: list(vizier.datastore.base.DatasetRow)
            List of dataset rows
        """
        # Open file handle
        if self.compressed:
            fh = gzip.open(self.filename, 'wb')
        else:
            fh = open(self.filename, 'w')
        # Write dataset rows
        json.dump({'rows': [row.to_dict() for row in rows]}, fh)
        fh.close()


class InMemDatasetReader(DatasetReader):
    """Dataset reader for datasets stored in memory."""
    def __init__(self, rows):
        """Initialize the list of rows in the dataset.

        Parameters
        ----------
        rows: list(vizier.datastore.base.DatasetRow)
            List of rows in the dataset
        """
        # Variables that maintain the internal state of the reader, i.e., the
        # list of rows and the row read index. The is_open flag indicates if the
        # read_index has a valid value
        self.is_open = False
        self.read_index = None
        self.rows = rows

    def close(self):
        """Close any open files and set the is_open flag to False."""
        self.read_index = None
        self.is_open = False

    def next(self):
        """Return the next row in the dataset iterator. Raises StopIteration if
        end of file is reached or file has been closed.

        Automatically closes any open file when end of iteration is reached for
        the first time.

        Returns
        -------
        vizier.datastore.base.DatasetRow
        """
        if self.is_open:
            if self.read_index < len(self.rows):
                row = self.rows[self.read_index]
                self.read_index += 1
                return row
            self.close()
        raise StopIteration

    def open(self):
        """Setup the reader by opening the associacted file and instantiating
        the csv reader.

        Returns
        -------
        vizier.datastore.base.DelimitedFileReader
        """
        # Only open if flag is false. Otherwise, return immediately
        if not self.is_open:
            self.read_index = 0
            self.is_open = True
        return self

    def write(self, rows):
        """Write the given list of dataset rows to file in default Json format.

        Parameters
        ----------
        rows: list(vizier.datastore.base.DatasetRow)
            List of dataset rows
        """
        # Open file handle
        if self.compressed:
            fh = gzip.open(self.filename, 'wb')
        else:
            fh = open(self.filename, 'w')
        # Write dataset rows
        json.dump({'rows': [row.to_dict() for row in rows]}, fh)
        fh.close()