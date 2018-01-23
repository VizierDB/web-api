import os
import unittest

from vizier.datastore.metadata import DatasetMetadata
from vizier.datastore.metadata import UpdateCellAnnotation as UpdCell
from vizier.datastore.metadata import UpdateColumnAnnotation as UpdCol
from vizier.datastore.metadata import UpdateRowAnnotation as UpdRow


METADATA_FILE = './data/metadata.yaml'


class TestDatasetMetadata(unittest.TestCase):

    def setUp(self):
        """Delete metadata file if it exists."""
        self.tearDown()


    def tearDown(self):
        """Delete metadata file if it exists."""
        if os.path.isdir(METADATA_FILE):
            os.remove(METADATA_FILE)

    def test_copy_metadata(self):
        """Test adding and retrieving metadata for different object types."""
        ds_meta = DatasetMetadata()
        ds_meta.for_column(1).set_annotation('comment', 'Some Comment')
        ds_meta.for_row(0).set_annotation('name', 'Nonsense')
        ds_meta.for_cell(1, 1).set_annotation('title', 'Nonsense')
        meta = ds_meta.copy_metadata()
        # Column annotation
        col_anno = meta.for_column(0)
        self.assertEquals(col_anno.size, 0)
        col_anno = meta.for_column(1)
        self.assertEquals(col_anno.size, 1)
        self.assertEquals(col_anno.get_annotation('comment'), 'Some Comment')
        # Row annotation
        row_anno = meta.for_row(0)
        self.assertEquals(col_anno.size, 1)
        self.assertEquals(row_anno.get_annotation('name'), 'Nonsense')
        row_anno = meta.for_row(1)
        self.assertEquals(row_anno.size, 0)
        # Cell Annotations
        cell_anno = meta.for_cell(1, 1)
        self.assertEquals(cell_anno.size, 1)
        self.assertEquals(cell_anno.get_annotation('title'), 'Nonsense')
        # Ensure that changes to the copy don't affect the original
        meta.for_column(1).set_annotation('comment', 'New Comment')
        meta.for_column(2).set_annotation('comment', 'Some Comment')
        self.assertEquals(ds_meta.for_column(2).size, 0)
        self.assertEquals(meta.for_column(2).size, 1)
        self.assertEquals(ds_meta.for_column(1).get_annotation('comment'), 'Some Comment')
        self.assertEquals(meta.for_column(1).get_annotation('comment'), 'New Comment')
        self.assertEquals(meta.for_column(2).get_annotation('comment'), 'Some Comment')

    def test_dataset_metadata(self):
        """Test adding and retrieving metadata for different object types."""
        meta = DatasetMetadata()
        meta.for_column(1).set_annotation('comment', 'Some Comment')
        meta.for_row(0).set_annotation('name', 'Nonsense')
        meta.for_cell(1, 1).set_annotation('title', 'Nonsense')
        # Column annotation
        col_anno = meta.for_column(0)
        self.assertEquals(col_anno.size, 0)
        col_anno = meta.for_column(1)
        self.assertEquals(col_anno.size, 1)
        self.assertEquals(col_anno.get_annotation('comment'), 'Some Comment')
        # Row annotation
        row_anno = meta.for_row(0)
        self.assertEquals(col_anno.size, 1)
        self.assertEquals(row_anno.get_annotation('name'), 'Nonsense')
        row_anno = meta.for_row(1)
        self.assertEquals(row_anno.size, 0)
        # Cell Annotations
        cell_anno = meta.for_cell(1, 1)
        self.assertEquals(cell_anno.size, 1)
        self.assertEquals(cell_anno.get_annotation('title'), 'Nonsense')

    def test_io_metadata(self):
        """Test reading and writing metadata from/to file."""
        ds_meta = DatasetMetadata()
        ds_meta.for_column(1).set_annotation('comment', 'Some Comment')
        ds_meta.for_column(1).set_annotation('name', 'foo')
        ds_meta.for_row(0).set_annotation('name', 'Nonsense')
        ds_meta.for_cell(1, 1).set_annotation('title', 'Nonsense')
        ds_meta.to_file(METADATA_FILE)
        meta = DatasetMetadata.from_file(METADATA_FILE)
        # Column annotation
        col_anno = meta.for_column(0)
        self.assertEquals(col_anno.size, 0)
        col_anno = meta.for_column(1)
        self.assertEquals(col_anno.size, 2)
        self.assertEquals(col_anno.get_annotation('comment'), 'Some Comment')
        self.assertEquals(col_anno.get_annotation('name'), 'foo')
        # Row annotation
        row_anno = meta.for_row(0)
        self.assertEquals(row_anno.size, 1)
        self.assertEquals(row_anno.get_annotation('name'), 'Nonsense')
        row_anno = meta.for_row(1)
        self.assertEquals(row_anno.size, 0)
        # Cell Annotations
        cell_anno = meta.for_cell(1, 1)
        self.assertEquals(cell_anno.size, 1)
        self.assertEquals(cell_anno.get_annotation('title'), 'Nonsense')

    def test_update_statements(self):
        """Test update annotation statements."""
        meta = DatasetMetadata()
        col_upd = UpdCol(1, 'comment', 'Some Comment')
        meta = col_upd(meta)
        self.assertEquals(meta.for_column(1).get_annotation('comment'), 'Some Comment')
        meta = UpdRow(0, 'name', 'Some Name')(meta)
        self.assertEquals(meta.for_row(0).get_annotation('name'), 'Some Name')
        meta = UpdCell(0, 1, 'quality', 'Low')(meta)
        self.assertTrue('quality' in meta.for_cell(0, 1).keys())
        self.assertEquals(meta.for_cell(0, 1).get_annotation('quality'), 'Low')
        self.assertIsNone(meta.for_cell(0, 1).get_annotation('name'))


if __name__ == '__main__':
    unittest.main()
