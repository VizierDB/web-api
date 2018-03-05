"""Simple interface to upload and retieve files."""

from abc import abstractmethod
import csv
import gzip
import os
import shutil
import yaml

from vizier.core.util import get_unique_identifier
from vizier.core.system import build_info, component_descriptor
from vizier.core.system import VizierSystemComponent
from vizier.core.timestamp import get_current_time, to_datetime


class FileHandle(object):
    """File handle containing statistics for an uploaded CSV file."""
    def __init__(self, identifier, name, columns, rows, size, created_at, filepath, upload_name=None, active=True):
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
        size: int
            File size in bytes
        created_at : datetime.datetime
            Timestamp of worktrail creation (UTC)
        filepath: string
            Absolute path to file on disk
        upload_name: string
            Name of the original uploaded file
        active: bool
            Flag indicating whether the file is active
        """
        self.identifier = identifier
        self.name = name
        self.columns = columns
        self.rows = rows
        self.size = size
        self.created_at = created_at
        self.filepath = filepath
        self.upload_name = upload_name if not upload_name is None else name
        self.active = active

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
        elif '.' in self.name:
            return self.name[:self.name.rfind('.')]
        else:
            return self.name

    @property
    def delimiter(self):
        """Determine the column delimiter for a CSV/TSV file based on the suffix
        of the name of the uploaded file.

        Returns
        -------
        string
        """
        if self.upload_name.endswith('.csv'):
            return ','
        elif self.upload_name.endswith('.csv.gz'):
            return ','
        elif self.upload_name.endswith('.tsv'):
            return '\t'
        elif self.upload_name.endswith('.tsv.gz'):
            return '\t'
        else:
            return None

    @staticmethod
    def from_dict(doc, func_filepath):
        """
        """
        return FileHandle(
            doc['identifier'],
            doc['name'],
            get_optional_property(doc, 'columns', -1),
            get_optional_property(doc, 'rows', -1),
            get_optional_property(doc, 'size', -1),
            to_datetime(doc['createdAt']),
            func_filepath(doc['identifier']),
            upload_name=get_optional_property(doc, 'uploadName'),
            active=doc['active']
        )

    @property
    def is_verified_csv(self):
        """Flag indicating whether the original file was parsed correctly by
        the CSV parser (i.e., column and row information is not a negative
        number).

        Returns
        -------
        bool
        """
        return self.columns >= 0 and self.rows >= 0

    def open(self):
        """Get open file object for associated file.

        Returns
        -------
        FileObject
        """
        if self.upload_name.endswith('.gz'):
            return gzip.open(self.filepath, 'rb')
        else:
            return open(self.filepath, 'r')

    def to_dict(self):
        """Get dictionary serialization for the file object.

        Returns
        -------
        dict
        """
        return {
            'identifier': self.identifier,
            'name': self.name,
            'columns': self.columns,
            'rows': self.rows,
            'size': self.size,
            'createdAt' : self.created_at.isoformat(),
            'uploadName': self.upload_name,
            'active': self.active
        }


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
                return FileHandle.from_dict(f_desc, self.get_filepath)
        return None

    def get_filepath(self, identifier):
        """Get absolute path for file with given identifier. Does not check if
        the file exists.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        string
        """
        return os.path.join(self.file_directory, identifier)

    def list_files(self):
        """Get list of file handles for all uploaded files.

        Returns
        -------
        list(FileHandle)
        """
        files = list()
        for f_desc in self.read_index()['files']:
            if f_desc['active']:
                files.append(FileHandle.from_dict(f_desc, self.get_filepath))
        return files

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
                f_handle = FileHandle.from_dict(f_desc, self.get_filepath)
                f_handle.name = name
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
        # Determine the file type based on the file name suffix. If the file
        # type is unknoen reader will be None
        reader = None
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
        # Create a new unique identifier for the file.
        identifier = get_unique_identifier()
        # File properties
        columns = -1
        rows = 0
        size =  os.stat(filename).st_size
        created_at = get_current_time()
        # Parse csv file to get column and row statistics (and to ensure that
        # the file parses).
        output_file = self.get_filepath(identifier)
        if not reader is None:
            try:
                for row in reader:
                    if columns == -1:
                        columns = len(row)
                    else:
                        rows += 1
            except Exception as ex:
                columns = -1
                rows = -1
            csvfile.close()
        else:
            # Make sure to set number of rows to undefined (-1)
            rows = -1
        # Copy the uploaded file
        shutil.copyfile(filename, output_file)
        # Add file to file index
        f_handle = FileHandle(
            identifier,
            name,
            columns,
            rows,
            size,
            created_at,
            output_file
        )
        files.append(f_handle.to_dict())
        self.write_index({'files': files})
        return f_handle

    def write_index(self, content):
        """Write content of the file index.

        Parameters
        -------
        content: dict
            New context for file index
        """
        with open(self.index_file, 'w') as f:
            yaml.dump(content, f, default_flow_style=False)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_optional_property(properties, key, default_value=None):
    """Return the property value for the given key or the default value if the
    key does not exist.

    Parameters
    ----------
    properties: dict
        Dictionary of file properties
    key: string
        Property key
    default_value: any
        Default value for property key

    Returns
    -------
    any
    """
    return properties[key] if key in properties else default_value
