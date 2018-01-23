import unittest

from vizier.config import AppConfig, ENGINE_DEFAULT, ENGINE_MIMIR


class TestConfig(unittest.TestCase):

    def test_default_config(self):
        """Test the default configuration settings.
        """
        config = AppConfig()
        # API
        self.assertEquals(config.api.server_url, 'http://localhost')
        self.assertEquals(config.api.server_port, 5000)
        self.assertEquals(config.api.app_path, '/vizier-db/api/v1')
        self.assertEquals(config.api.doc_url, 'http://cds-swg1.cims.nyu.edu/vizier-db/doc/api/v1')
        self.assertEquals(config.api.app_base_url, 'http://localhost:5000/vizier-db/api/v1')
        # File server
        self.assertEquals(config.fileserver.directory, '../.env/fs')
        self.assertEquals(config.fileserver.max_file_size, 16 * 1024 * 1024)
        # Engine
        self.assertEquals(len(config.engines), 1)
        engine = config.engines[ENGINE_DEFAULT]
        self.assertEquals(engine.identifier, ENGINE_DEFAULT)
        self.assertEquals(engine.name, 'Vizier (Lite)')
        self.assertEquals(engine.description, 'Workflow engine with basic functionality')
        self.assertEquals(engine.datastore.directory, '../.env/ds')
        # Misc
        self.assertEquals(config.viztrails.directory, '../.env/wt')
        self.assertEquals(config.name, 'Vizier Web API')
        self.assertEquals(config.debug, True)
        self.assertEquals(config.logs, '../.env/logs')

    def test_local_file(self):
        """Test reading configuration from local config file.
        """
        for config_file in ['./data/config.yaml', './data/alternate-config.yaml']:
            config = AppConfig(configuration_file=config_file)
            # API
            self.assertEquals(config.api.server_url, 'http://vizier-db.info')
            self.assertEquals(config.api.server_port, 80)
            self.assertEquals(config.api.app_path, '/vizier-db/api/v2')
            self.assertEquals(config.api.doc_url, 'http://vizier-db.info/doc/api/v1')
            self.assertEquals(config.api.app_base_url, 'http://vizier-db.info/vizier-db/api/v2')
            # File server
            self.assertEquals(config.fileserver.directory, 'fs-directory')
            self.assertEquals(config.fileserver.max_file_size, 1024)
            # Engine
            self.assertEquals(len(config.engines), 1)
            engine = config.engines[ENGINE_MIMIR]
            self.assertEquals(engine.identifier, ENGINE_MIMIR)
            self.assertEquals(engine.datastore.directory, 'ds-directory')
            # Misc
            self.assertEquals(config.viztrails.directory, 'wf-directory')
            self.assertEquals(config.name, 'Alternate Vizier Web API')
            self.assertEquals(config.debug, False)
            self.assertEquals(config.logs, 'logs')
        # Partial Configuration file
        config = AppConfig(configuration_file='./data/partial-config.yaml')
        # API
        self.assertEquals(config.api.server_url, 'http://vizier-db.info')
        self.assertEquals(config.api.server_port, 80)
        self.assertEquals(config.api.app_path, '/vizier-db/api/v1')
        self.assertEquals(config.api.doc_url, 'http://cds-swg1.cims.nyu.edu/vizier-db/doc/api/v1')
        self.assertEquals(config.api.app_base_url, 'http://vizier-db.info/vizier-db/api/v1')
        # File server
        self.assertEquals(config.fileserver.directory, 'fs-directory')
        self.assertEquals(config.fileserver.max_file_size, 16 * 1024 * 1024)
        # Engines
        self.assertEquals(len(config.engines), 2)
        for key in [ENGINE_DEFAULT, ENGINE_MIMIR]:
            engine = config.engines[key]
            self.assertEquals(engine.identifier, key)
            self.assertEquals(engine.name, 'NAME-' + key)
            self.assertEquals(engine.datastore.directory, 'DIR-' + key)
        # Misc
        self.assertEquals(config.viztrails.directory, '../.env/wt')
        self.assertEquals(config.name, 'Vizier Web API')
        self.assertEquals(config.debug, False)
        self.assertEquals(config.logs, '../.env/logs')

    def test_missing_file(self):
        """Test reading configuration from missing config file.
        """
        config = AppConfig(configuration_file='./data/missing_config.yaml')
        # API
        self.assertEquals(config.api.server_url, 'http://localhost')
        self.assertEquals(config.api.server_port, 5000)
        self.assertEquals(config.api.app_path, '/vizier-db/api/v1')
        self.assertEquals(config.api.doc_url, 'http://cds-swg1.cims.nyu.edu/vizier-db/doc/api/v1')
        self.assertEquals(config.api.app_base_url, 'http://localhost:5000/vizier-db/api/v1')
        # File server
        self.assertEquals(config.fileserver.directory, '../.env/fs')
        self.assertEquals(config.fileserver.max_file_size, 16 * 1024 * 1024)
        # Engine
        self.assertEquals(len(config.engines), 1)
        engine = config.engines[ENGINE_DEFAULT]
        self.assertEquals(engine.identifier, ENGINE_DEFAULT)
        self.assertEquals(engine.name, 'Vizier (Lite)')
        self.assertEquals(engine.description, 'Workflow engine with basic functionality')
        self.assertEquals(engine.datastore.directory, '../.env/ds')
        # Misc
        self.assertEquals(config.viztrails.directory, '../.env/wt')
        self.assertEquals(config.name, 'Vizier Web API')
        self.assertEquals(config.debug, True)
        self.assertEquals(config.logs, '../.env/logs')


if __name__ == '__main__':
    unittest.main()
