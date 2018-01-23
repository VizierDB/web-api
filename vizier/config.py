"""Helper class for managing applicatin configuration.

The app configuration has three main parts: API and Web service configuration,
file server, and the workflow engine. The structure of the configuration object
is as follows:

api:
    server_url: Url of the server (e.g., http://localhost)
    server_port: Server port (e.g., 5000)
    app_path: Application path for Web API (e.g., /vizier-db/api/v1)
    app_base_url: Concatenation of server_url, server_port and app_path
    doc_url: Url to API documentation
fileserver:
    directory: Path to base directory for file server
    max_file_size: Maximum size for file uploads
engines:
    - identifier: Engine type (i.e., DEFAULT or MIMIR)
      name: Engine printable name
      description: Descriptive text for engine
      datastore:
          directory: Base directory for datastore
viztrails:
  directory: Base directory for storing worktrail information and metadata
name: Web Service name
debug: Flag indicating whether server is started in debug mode
logs: Path to log directory

App configurations are read from a file in Yaml format. The structure of the
file is expected to be the same as the structre shown above. the configuration
file can either be specified using the environment variable VIZIERSERVER_CONFIG
or be located (as file config.yaml) in the current working directory.
"""

import os
import yaml


"""Environment Variable containing path to config file."""
ENV_CONFIG = 'VIZIERSERVER_CONFIG'

"""Default directory for API data."""
ENV_DIRECTORY = '../.env'

"""API Engine types."""
ENGINE_DEFAULT = 'DEFAULT'
ENGINE_MIMIR = 'MIMIR'


class AppConfig(object):
    """Application configuration object. This object contains all configuration
    parameters for the Vizier DB Web API. The structture is as follows:

        api:
            server_url
            server_port
            app_path
            app_base_url
            doc_url
        fileserver:
            directory
            max_file_size
        engines:
            - identifier
              name
              description
              default
              datastore:
                  directory
        viztrails:
            directory
        name
        debug
        logs

    Configuration is read from a configuration file (in Yaml format) that is
    expected to have the same structure as shown above.
    """
    def __init__(self, configuration_file=None):
        """Read configuration parameters from a configuration file. The file is
        expected to be in Yaml format, having the same structure as shown above.

        If no file is specified attempts will be made to read the following
        configuration files:

        (1) The file referenced by the environment variable VIZIERSERVER_CONFIG
        (2) The file config.yaml in the current working directory

        If the specified files are not found the default configuration is used.

        Parameters
        ----------
        configuration_file: string, optional
            Optional path to configuration file
        """
        # Set the default configuration before reading (on overwriting)
        # configuration parameters from file
        self.api = ServerConfig()
        self.fileserver = FileServerConfig()
        # Leave engines dictionary empty initially. Add default engine later if
        # necessary
        self.engines = dict()
        self.viztrails = FSObjectConfig(os.path.join(ENV_DIRECTORY, 'wt'))
        self.name = 'Vizier Web API'
        self.debug = True
        self.logs = os.path.join(ENV_DIRECTORY, 'logs')
        # Read configuration from either of the following:
        doc = None
        files = [
            configuration_file,
            os.getenv(ENV_CONFIG),
            './config.yaml'
        ]
        for config_file in files:
            if not config_file is None and os.path.isfile(config_file):
                with open(config_file, 'r') as f:
                    doc = yaml.load(f.read())
                    break
        if not doc is None:
            if 'api' in doc:
                self.api.from_dict(doc['api'])
            if 'fileserver' in doc:
                self.fileserver.from_dict(doc['fileserver'])
            if 'engines' in doc:
                for obj in doc['engines']:
                    engine = EngineConfig().from_dict(obj)
                    self.engines[engine.identifier] = engine
            if 'viztrails' in doc:
                self.viztrails.from_dict(doc['viztrails'])
            if 'name' in doc:
                self.name = doc['name']
            if 'debug' in doc:
                self.debug = doc['debug']
            if 'logs' in doc:
                self.logs = doc['logs']
        # Make sure to add the default workflow engine if no engine was
        # specified in the configuration file
        if len(self.engines) == 0:
            engine = EngineConfig()
            self.engines[engine.identifier] = engine


class ServerConfig(object):
    """Configuration parameter for the Web service."""
    def __init__(self):
        """Initialize the configuration parameters for the Web service with
        default values.
        """
        self.server_url = 'http://localhost'
        # Ensure that the port number is an integer. Will raise ValueError if
        # a non-integer value is provided
        self.server_port = 5000
        self.app_path = '/vizier-db/api/v1'
        self.doc_url = 'http://cds-swg1.cims.nyu.edu/vizier-db/doc/api/v1'

    @property
    def app_base_url(self):
        """Full Url (prefix) for all resources that are available on the Web
        Service. this is a concatenation of the service Url, port number and
        application path.

        Returns
        -------
        string
        """
        base_url = self.server_url
        if self.server_port != 80:
            base_url += ':' + str(self.server_port)
        base_url += self.app_path
        return base_url

    def from_dict(self, doc):
        """Read configuration parameters from the given dictionary.

        Parameters
        ----------
        doc: dict
            Dictionary containing configuration information.
        """
        if 'server_url' in doc:
            self.server_url = doc['server_url']
        elif 'server.url' in doc:
            self.server_url = doc['server.url']
        if 'server_port' in doc:
            self.server_port = int(doc['server_port'])
        elif 'server.port' in doc:
            self.server_port = int(doc['server.port'])
        if 'app_path' in doc:
            self.app_path = doc['app_path']
        elif 'app.path' in doc:
            self.app_path = doc['app.path']
        if 'doc_url' in doc:
            self.doc_url = doc['doc_url']
        elif 'doc.url' in doc:
            self.doc_url = doc['doc.url']


class EngineConfig(object):
    """Configuration for an API workflow engine. Contains user-readable
    description for the underlying workflow engine and configuration for the
    engine-specific data store.
    """
    def __init__(self):
        """Initialize the configuration parameters for the API engine with
        default values.
        """
        self.identifier = ENGINE_DEFAULT
        self.name = 'Vizier (Lite)'
        self.description = 'Workflow engine with basic functionality'
        self.default = False
        self.datastore = FSObjectConfig(os.path.join(ENV_DIRECTORY, 'ds'))

    def from_dict(self, doc):
        """Read configuration parameters from the given dictionary.

        Parameters
        ----------
        doc: dict
            Dictionary containing configuration information.

        Returns
        -------
        vizier.config.EngineConfig
        """
        if 'id' in doc:
            self.identifier = doc['id']
        elif 'identifier' in doc:
            self.identifier = doc['identifier']
        if not self.identifier in [ENGINE_DEFAULT, ENGINE_MIMIR]:
            raise ValueError('unknown workflow engine \'' + self.name + '\'')
        if 'name' in doc:
            self.name = doc['name']
        if 'description' in doc:
            self.description = doc['description']
        if 'default' in doc:
            self.default = doc['default']
        if 'datastore' in doc:
            self.datastore.from_dict(doc['datastore'])
        return self

    @property
    def is_default_engine(self):
        """Flag indicating whether the configuration is for a default API
        engine.

        Returns
        -------
        bool
        """
        return self.identifier == ENGINE_DEFAULT

    @property
    def is_mimir_engine(self):
        """Flag indicating whether the configuration is for a API engine that
        uses Mimir as storage backend.

        Returns
        -------
        bool
        """
        return self.identifier == ENGINE_MIMIR


class FSObjectConfig(object):
    """Simple configuration object for a system component that uses a directory
    on the file system to maintain information.
    """
    def __init__(self, directory):
        """Initialize the directory path.

        Parameters
        ----------
        directory: string
            Path to the directory where files and subfolders will be created.
        """
        self.directory = directory

    def from_dict(self, doc):
        """Read directory path from the given dictionary.

        Parameters
        ----------
        doc: dict
            Dictionary containing configuration information.
        """
        if 'directory' in doc:
            self.directory = doc['directory']


class FileServerConfig(FSObjectConfig):
    """Configuration for the file server."""
    def __init__(self):
        """initialize the file server base directory and maximum file size with
        default values.

        Parameters
        ----------
        directory: string
            Path to directory where files are stored
        max_file_size: int
            Maximum file size for uploads.
        """
        super(FileServerConfig, self).__init__(
            os.path.join(ENV_DIRECTORY, 'fs')
        )
        # Ensure that the value is an integer
        self.max_file_size = 16 * 1024 * 1024

    def from_dict(self, doc):
        """Read configuration parameters from the given dictionary.

        Parameters
        ----------
        doc: dict
            Dictionary containing configuration information.
        """
        super(FileServerConfig, self).from_dict(doc)
        if 'max_file_size' in doc:
            self.max_file_size = int(doc['max_file_size'])
        if 'maxFileSize' in doc:
            self.max_file_size = int(doc['maxFileSize'])
