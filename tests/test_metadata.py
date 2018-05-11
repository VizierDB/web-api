import os
import unittest

from vizier.datastore.metadata import DatasetMetadata
from vizier.datastore.metadata import add_annotation, get_first, update_annotations

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
        add_annotation(ds_meta.for_column(1), 'comment', 'Some Comment')
        add_annotation(ds_meta.for_row(0), 'name', 'Nonsense')
        add_annotation(ds_meta.for_cell(1, 1), 'title', 'Nonsense')
        meta = ds_meta.copy_metadata()
        # Column annotation
        col_anno = meta.for_column(0)
        self.assertEquals(len(col_anno), 0)
        col_anno = meta.for_column(1)
        self.assertEquals(len(col_anno), 1)
        self.assertEquals(get_first(col_anno, 'comment').value, 'Some Comment')
        # Row annotation
        row_anno = meta.for_row(0)
        self.assertEquals(len(col_anno), 1)
        self.assertEquals(get_first(row_anno, 'name').value, 'Nonsense')
        row_anno = meta.for_row(1)
        self.assertEquals(len(row_anno), 0)
        # Cell Annotations
        cell_anno = meta.for_cell(1, 1)
        self.assertEquals(len(cell_anno), 1)
        self.assertEquals(get_first(cell_anno, 'title').value, 'Nonsense')
        # Ensure that changes to the copy don't affect the original
        add_annotation(meta.for_column(1), 'comment', 'New Comment')
        add_annotation(meta.for_column(2), 'comment', 'Some Comment')
        self.assertEquals(len(meta.for_column(1)), 2)
        self.assertEquals(len(ds_meta.for_column(1)), 1)
        self.assertEquals(get_first(ds_meta.for_column(1), 'comment').value, 'Some Comment')
        values = [a.value for a in meta.for_column(1).values()]
        self.assertTrue('Some Comment' in values)
        self.assertTrue('New Comment' in values)
        self.assertEquals(len(ds_meta.for_column(2)), 0)
        self.assertEquals(len(meta.for_column(2)), 1)
        self.assertEquals(get_first(meta.for_column(2), 'comment').value, 'Some Comment')

    def test_dataset_metadata(self):
        """Test adding and retrieving metadata for different object types."""
        meta = DatasetMetadata()
        add_annotation(meta.for_column(1), 'comment', 'Some Comment')
        add_annotation(meta.for_row(0), 'name', 'Nonsense')
        add_annotation(meta.for_cell(1, 1), 'title', 'Nonsense')
        # Column annotation
        col_anno = meta.for_column(0)
        self.assertEquals(len(col_anno), 0)
        col_anno = meta.for_column(1)
        self.assertEquals(len(col_anno), 1)
        self.assertEquals(get_first(col_anno, 'comment').value, 'Some Comment')
        # Row annotation
        row_anno = meta.for_row(0)
        self.assertEquals(len(col_anno), 1)
        self.assertEquals(get_first(row_anno, 'name').value, 'Nonsense')
        row_anno = meta.for_row(1)
        self.assertEquals(len(row_anno), 0)
        # Cell Annotations
        cell_anno = meta.for_cell(1, 1)
        self.assertEquals(len(cell_anno), 1)
        self.assertEquals(get_first(cell_anno, 'title').value, 'Nonsense')

    def test_io_metadata(self):
        """Test reading and writing metadata from/to file."""
        ds_meta = DatasetMetadata()
        add_annotation(ds_meta.for_column(1), 'comment', 'Some Comment')
        add_annotation(ds_meta.for_column(1), 'name', 'foo')
        add_annotation(ds_meta.for_row(0), 'name', 'Nonsense')
        add_annotation(ds_meta.for_cell(1, 1), 'title', 'Nonsense')
        ds_meta.to_file(METADATA_FILE)
        meta = DatasetMetadata.from_file(METADATA_FILE)
        # Column annotation
        col_anno = meta.for_column(0)
        self.assertEquals(len(col_anno), 0)
        col_anno = meta.for_column(1)
        self.assertEquals(len(col_anno), 2)
        self.assertEquals(get_first(col_anno, 'comment').value, 'Some Comment')
        self.assertEquals(get_first(col_anno, 'name').value, 'foo')
        # Row annotation
        row_anno = meta.for_row(0)
        self.assertEquals(len(row_anno), 1)
        self.assertEquals(get_first(row_anno, 'name').value, 'Nonsense')
        row_anno = meta.for_row(1)
        self.assertEquals(len(row_anno), 0)
        # Cell Annotations
        cell_anno = meta.for_cell(1, 1)
        self.assertEquals(len(cell_anno), 1)
        self.assertEquals(get_first(cell_anno, 'title').value, 'Nonsense')
        annotated_cells = ds_meta.cells_with_annotations()
        self.assertEquals(len(annotated_cells), 1)
        self.assertEquals(annotated_cells[0]['column'], 1)
        self.assertEquals(annotated_cells[0]['row'], 1)

    def test_update_statements(self):
        """Test update annotation statements."""
        meta = DatasetMetadata()
        # Create a new annotation
        anno1 = update_annotations(meta.for_column(1), key='comment', value='Some Comment')
        self.assertEquals(get_first(meta.for_column(1), 'comment').identifier, anno1.identifier)
        self.assertEquals(get_first(meta.for_column(1), 'comment').key, 'comment')
        self.assertEquals(get_first(meta.for_column(1), 'comment').value, 'Some Comment')
        # Create another annotation
        anno2 = update_annotations(meta.for_column(1), key='comment', value='Some Comment')
        self.assertEquals(len(meta.for_column(1)), 2)
        # Update first annotation value
        anno1_2 = update_annotations(meta.for_column(1), identifier=anno1.identifier, value='Other Comment')
        self.assertEquals(anno1_2.value, 'Other Comment')
        self.assertEquals(meta.for_column(1)[anno1.identifier].value, 'Other Comment')
        self.assertEquals(meta.for_column(1)[anno2.identifier].value, 'Some Comment')
        # Delete the second annotation
        update_annotations(meta.for_column(1), identifier=anno2.identifier)
        self.assertEquals(len(meta.for_column(1)), 1)
        self.assertEquals(get_first(meta.for_column(1), 'comment').value, 'Other Comment')
        # Update the key of the remaining annotation
        update_annotations(meta.for_column(1), identifier=anno1.identifier, key='remark', value='Other Comment')
        self.assertEquals(len(meta.for_column(1)), 1)
        self.assertIsNone(get_first(meta.for_column(1), 'commanr'))
        self.assertEquals(get_first(meta.for_column(1), 'remark').value, 'Other Comment')


if __name__ == '__main__':
    unittest.main()
