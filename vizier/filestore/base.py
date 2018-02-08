"""Simple interface to upload and retieve files."""

from abc import abstractmethod
import csv
import gzip
import os
import yaml

from vizier.core.util import get_unique_identifier
from vizier.core.system import build_info, component_descriptor
from vizier.core.system import VizierSystemComponent
from vizier.core.timestamp import get_current_time, to_datetime


class FileHandle(object):
    """File handle containing statistics for an uploaded CSV file."""
    def __init__(self, identifier, name, columns, rows, created_at):
        """Initialize the file identifier, the (full) file path, and information
        about number of columns and rows in the CSV file.

        Parameters
        ----------
        identifier: string
            Unique file identifier
        name: string
            File base name (with suffix)
        columns: int
            Number of columns in the CSV file
        rows: int
            Number of rows in the CSV file (excluding the header).
        created_at : datetime.datetime
            Timestamp of worktrail creation (UTC)
        """
        self.identifier = identifier
        self.name = name
        self.columns = columns
        self.rows = rows
        self.created_at = created_at

    @property
    def base_name(self):
        """Get the file base name without suffix identifying the file format.

        Returns
        -------
        string
        """
        if self.name.endswith('.csv') or self.name.endswith('.tsv'):
            return self.name[:-4]
        elif self.name.endswith('.csv.gz') or  self.name.endswith('.tsv.gz'):
            return self.name[:7]
        else:
            return self.name


class FileReader(object):
    """Reader for CSV file stored by the file server. This is a wrapper around a
    csv reader and the opened file. The goal is to hide internal storage format
    of csv files from code that reads these files.

    The problem is that the csv.reader class does not have a close method. Thus,
    code that requests a csv file from the fileserver cannot simply receive a
    csv.reader or otherwise the opened file would not be closed properly.
    """
    def __init__(self, f, reader):
        """Initialize the file and the csv reader objects.

        Parameters
        ----------
        f: FileObject
            Handle for file that is being read
        reader: csv.reader
            CSV reader
        """
        self.f = f
        self.reader = reader

    def close(self):
        """Close the file handle after finished reading."""
        self.f.close()


class FileServer(VizierSystemComponent):
    """Abstract API to upload and retrieve files."""
    def __init__(self, build):
        """Initialize the build information. Expects a dictionary containing two
        elements: name and version.

        Raises ValueError if build dictionary is invalid.

        Parameters
        ---------
        build : dict()
            Build information
        """
        super(FileServer, self).__init__(build)

    def components(self):
        """List containing component descriptor.

        Returns
        -------
        list
        """
        return [component_descriptor('fileserver', self.system_build())]

    @abstractmethod
    def delete_file(self, identifier):
        """Delete file with given identifier. Returns True if file was deleted
        or False if no such file existed.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def get_file(self, identifier):
        """Get handle for file with given identifier. Returns None if no file
        with given identifier exists.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        FileHandle
        """
        raise NotImplementedError

    @abstractmethod
    def list_files(self):
        """Get list of file handles for all uploaded files.

        Returns
        -------
        list(FileHandle)
        """
        raise NotImplementedError

    @abstractmethod
    def open_file(self, identifier):
        """Open file with given identifier for input.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        FileReader
        """
        raise NotImplementedError

    @abstractmethod
    def rename_file(self, identifier, name):
        """Rename file with given identifier. Returns the file handle for the
        renamed file or None if no such file existed.

        Raises ValueError if a another file with the given name already exists.

        Parameters
        ----------
        identifier: string
            Unique file identifier
        name: string
            New file name

        Returns
        -------
        FileHandle
        """
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, filename):
        """Upload a new file. Will raise a ValueError if a file with the given
        name already exists.

        Parameters
        ----------
        filename: string
            Path to file on disk

        Returns
        -------
        FileHandle
        """
        raise NotImplementedError


class DefaultFileServer(FileServer):
    """Default file server implementation. Keeps all files in a folder on disk.
    File metadata is kept in a separate Yaml file."""
    def __init__(self, base_directory):
        """Initialize the base directory that is used for file storage. The
        actual files are kept in a sub-folder (named 'files').

        The base directory and sub-folder will be created if they do not exist.

        Parameters
        ---------
        base_directory : string
            Path to the base directory.
        """
        super(DefaultFileServer, self).__init__(build_info('DefaultFileServer'))
        if not os.path.isdir(base_directory):
            os.makedirs(base_directory)
        self.base_directory = base_directory
        self.index_file = os.path.join(self.base_directory, 'index.yaml')
        self.file_directory = os.path.join(base_directory, 'files')
        if not os.path.isdir(self.file_directory):
            os.makedirs(self.file_directory)

    def delete_file(self, identifier):
        """Delete file with given identifier. Returns True if file was deleted
        or False if no such file existed. In the current implementation files
        will not be deleted from disk to maintain provenance information.
        Deleted files have their active flag set to False, instead.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        bool
        """
        files = []
        found = False
        for f_desc in self.read_index()['files']:
            if f_desc['identifier'] == identifier and f_desc['active']:
                f_desc['active'] = False
                found = True
            files.append(f_desc)
        if found:
            self.write_index({'files': files})
        return found

    def get_file(self, identifier):
        """Get handle for file with given identifier. Returns None if no file
        with given identifier exists.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        FileHandle
        """
        for f_desc in self.read_index()['files']:
            if f_desc['identifier'] == identifier and f_desc['active']:
                return FileHandle(
                    f_desc['identifier'],
                    f_desc['name'],
                    f_desc['columns'],
                    f_desc['rows'],
                    to_datetime(f_desc['createdAt'])
                )
        return None

    def list_files(self):
        """Get list of file handles for all uploaded files.

        Returns
        -------
        list(FileHandle)
        """
        files = list()
        for f_desc in self.read_index()['files']:
            if f_desc['active']:
                files.append(
                    FileHandle(
                        f_desc['identifier'],
                        f_desc['name'],
                        f_desc['columns'],
                        f_desc['rows'],
                        to_datetime(f_desc['createdAt'])
                    )
                )
        return files

    def open_file(self, identifier):
        """Open file with given identifier for input.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        FileReader
        """
        filename = os.path.join(self.file_directory, identifier + '.tsv.gz')
        f = gzip.open(filename, 'rb')
        return FileReader(f, csv.reader(f, delimiter='\t'))

    def read_index(self):
        """Return content of the file index.

        Returns
        -------
        dict
        """
        if os.path.isfile(self.index_file):
            with open(self.index_file, 'r') as f:
                return yaml.load(f.read())
        else:
            return {'files':[]}

    def rename_file(self, identifier, name):
        """Rename file with given identifier. Returns the file handle for the
        renamed file or None if no such file existed.

        Raises ValueError if a another file with the given name already exists.

        Parameters
        ----------
        identifier: string
            Unique file identifier
        name: string
            New file name

        Returns
        -------
        FileHandle
        """
        # Raise ValueError if a file with the given name already exists
        files = []
        f_handle = None
        for f_desc in self.read_index()['files']:
            if f_desc['identifier'] == identifier and f_desc['active']:
                f_desc['name'] = name
                f_handle = FileHandle(
                        f_desc['identifier'],
                        f_desc['name'],
                        f_desc['columns'],
                        f_desc['rows'],
                        to_datetime(f_desc['createdAt'])
                )
            elif f_desc['identifier'] != identifier and f_desc['name'] == name and f_desc['active']:
                raise ValueError('file \'' + name + '\' already exists')
            files.append(f_desc)
        if not f_handle is None:
            self.write_index({'files': files})
        return f_handle

    def upload_file(self, filename):
        """Upload a new file. Will raise a ValueError if a file with the given
        name already exists.

        Parameters
        ----------
        filename: string
            Path to file on disk

        Returns
        -------
        FileHandle
        """
        name = os.path.basename(filename).lower()
        # Raise ValueError if a file with the given name already exists
        files = self.read_index()['files']
        for f_desc in files:
            if f_desc['name'] == name and f_desc['active']:
                raise ValueError('file \'' + name + '\' already exists')
        # Determine the file type based on the file name suffix
        if name.endswith('.csv'):
            csvfile = open(filename, 'r')
            reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        elif name.endswith('.csv.gz'):
            csvfile = gzip.open(filename, 'rb')
            reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        elif name.endswith('.tsv'):
            csvfile = open(filename, 'r')
            reader = csv.reader(csvfile, delimiter='\t')
        elif name.endswith('.tsv.gz'):
            csvfile = gzip.open(filename, 'rb')
            reader = csv.reader(csvfile, delimiter='\t')
        else:
            raise ValueError('unknown file suffix for file \'' + name + '\'')
        print 'GOT READER'
        # Create a new unique identifier for the file
        identifier = get_unique_identifier()
        columns = None
        rows = 0
        # Parse csv file to get column and row statistics (and to ensure that
        # the file parses). Write file content to target directory. Independent
        # of the file suffix the target file will be a gzipped TSV file.
        output_file = os.path.join(self.file_directory, identifier + '.tsv.gz')
        with gzip.open(output_file, 'wb') as f_out:
            writer = csv.writer(f_out, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            try:
                for row in reader:
                    print row
                    if columns == None:
                        columns = row
                        writer.writerow(row)
                    else:
                        rows += 1
                        writer.writerow(row)
            except Exception as ex:
                raise ValueError(ex)
        csvfile.close()
        created_at = get_current_time()
        files.append({
            'identifier': identifier,
            'name': name,
            'columns': len(columns),
            'rows': rows,
            'createdAt' : created_at.isoformat(),
            'active': True
        })
        self.write_index({'files': files})
        return FileHandle(identifier, name, len(columns), rows, created_at)

    def write_index(self, content):
        """Write content of the file index.

        Parameters
        -------
        content: dict
            New context for file index
        """
        with open(self.index_file, 'w') as f:
            yaml.dump(content, f, default_flow_style=False)
