import csv
import gzip
import os
import shutil
import sys
import time
import unittest

from vizier.filestore.base import DefaultFileServer


SERVER_DIR = './data/fs'

CSV_FILE = './data/dataset.csv'
GZIP_CSV_FILE = './data/dataset.csv.gz'
TSV_FILE = './data/dataset.tsv'
GZIP_TSV_FILE = './data/dataset.tsv.gz'


class TestFileServer(unittest.TestCase):

    def setUp(self):
        """Create an empty file server repository."""
        # Drop project descriptor directory
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        # Setup project repository
        self.db = DefaultFileServer(SERVER_DIR)

    def tearDown(self):
        """Clean-up by dropping file server directory.
        """
        shutil.rmtree(SERVER_DIR)

    def test_delete_file(self):
        """Test delete file method."""
        f = self.db.upload_file(CSV_FILE)
        f = self.db.get_file(f.identifier)
        self.assertIsNotNone(f)
        self.assertTrue(self.db.delete_file(f.identifier))
        f = self.db.get_file(f.identifier)
        self.assertIsNone(f)

    def test_get_file(self):
        """Test file get method."""
        f = self.db.upload_file(CSV_FILE)
        f = self.db.get_file(f.identifier)
        self.assertEquals(f.columns, 3)
        self.assertEquals(f.rows, 2)
        self.assertEquals(f.name, 'dataset.csv')
        # Ensure that the file parses as a zipped TSV file
        csvfile = self.db.open_file(f.identifier)
        rows = 0
        for row in csvfile.reader:
            self.assertEquals(len(row), f.columns)
            rows += 1
        csvfile.close()
        self.assertEquals(rows  -1, f.rows)

    def test_list_file(self):
        """Test upload of different file types and the list files method."""
        self.db.upload_file(CSV_FILE)
        self.db.upload_file(GZIP_CSV_FILE)
        self.db.upload_file(TSV_FILE)
        self.db.upload_file(GZIP_TSV_FILE)
        files = self.db.list_files()
        self.assertEquals(len(files), 4)
        # Ensure that each of the files parses as a zipped TSV file
        for f in files:
            csvfile = self.db.open_file(f.identifier)
            rows = 0
            for row in csvfile.reader:
                self.assertEquals(len(row), f.columns)
                rows += 1
            csvfile.close()
            self.assertEquals(rows  -1, f.rows)

    def test_rename_file(self):
        """Test rename file method."""
        f = self.db.upload_file(CSV_FILE)
        f = self.db.get_file(f.identifier)
        f = self.db.rename_file(f.identifier, 'somename')
        self.assertEquals(f.name, 'somename')
        f = self.db.get_file(f.identifier)
        f = self.db.rename_file(f.identifier, 'somename')
        self.assertEquals(f.name, 'somename')
        f = self.db.rename_file(f.identifier, 'somename')
        self.assertEquals(f.name, 'somename')
        f2 = self.db.upload_file(CSV_FILE)
        with self.assertRaises(ValueError):
            self.db.rename_file(f2.identifier, 'somename')


    def test_upload_file(self):
        """Test file upload."""
        f = self.db.upload_file(CSV_FILE)
        self.assertEquals(f.columns, 3)
        self.assertEquals(f.rows, 2)
        self.assertEquals(f.name, 'dataset.csv')
        with self.assertRaises(ValueError):
            self.db.upload_file(CSV_FILE)

if __name__ == '__main__':
    unittest.main()