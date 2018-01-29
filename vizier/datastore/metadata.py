"""Metadata annotation object for datasets.

Maintains a list of annotation objects, i.e., (key,value)-pairs, for different
identifiable components of a dataset. the identifiable components of a dataset
are COLUMN, ROW, and CELL.
"""

import yaml


# ------------------------------------------------------------------------------
# Metadata Objects
# ------------------------------------------------------------------------------

class ObjectMetadataSet(object):
    """Manage annotations for a single objects."""
    def __init__(self, annotations=None,):
        """initialize the metadata set.

        Parameters
        ----------
        annotations: dict
            Dictionary of object annotations
        """
        if annotations is None:
            self.annotations = dict()
        else:
            self.annotations=annotations

    def get_annotation(self, key):
        """Get an existing annotation with given key. Returns None if no such
        annotation exists.
        identifier.

        Parameters
        ----------
        key: string
            Annotation key

        Returns
        -------
        string
        """
        if not key in self.annotations:
            return None
        else:
            return self.annotations[key]

    def has_annotation(self, key):
        """Test if an annotation with given key exists for the object.

        Parameters
        ----------
        key: string
            Annotation key

        Returns
        -------
        bool
        """
        return key in self.annotations

    def keys(self):
        """List of existing annotation keys for the object.

        Returns
        -------
        list(string)
        """
        return self.annotations.keys()

    def set_annotation(self, key, value):
        """Add an annotation for the object. Will overwrite an existing
        annotation with the same key.

        If the annotation value is None an exisitng annotation with the given
        key will be deleted.

        Parameters
        ----------
        key: string
            Annotation key
        value: string
            Annotation value
        """
        if not value is None:
            self.annotations[key] = value
        elif key in self.annotations:
            del self.annotations[key]

    @property
    def size(self):
        """Number of annotations for this object.

        Returns
        -------
        int
        """
        return len(self.annotations)


class DatasetMetadata(object):
    """Collection of annotations for a dataset object. For each object a
    dictionary of annotations (key,value pairs) is maintained.

    Attributes
    ----------
    column_annotations: dict(), optional
        Annotations for dataset columns
    row_annotations: dict(), optional
        Annotations for dataset rows
    cell_annotations: dict(), optional
        Annotations for dataset cells
    """
    def __init__(self, column_annotations=None, row_annotations=None, cell_annotations=None):
        """Initialize the metadata sets for the three different types of dataset
        components that can be annotated (columns, rows, and cells).

        Parameters
        ----------
        column_annotations: dict(), optional
            Annotations for dataset columns
        row_annotations: dict(), optional
            Annotations for dataset rows
        cell_annotations: dict(), optional
            Annotations for dataset cells
        """
        if not column_annotations is None:
            self.column_annotations = column_annotations
        else:
            self.column_annotations = dict()
        if not row_annotations is None:
            self.row_annotations = row_annotations
        else:
            self.row_annotations = dict()
        if not cell_annotations is None:
            self.cell_annotations = cell_annotations
        else:
            self.cell_annotations = dict()

    @staticmethod
    def cell_key_serializer(key):
        """Serializer for cell keys. Basically reverses get_cell_key by
        returning a dictionary containing column and row elements for the
        column and row identifier of the respective cell.

        Parameters
        ----------
        key: string
            Cell key as generated by get_cell_key

        Returns
        -------
        dict
        """
        pos = key.index('#')
        return {
            'column': int(key[:pos]),
            'row': int(key[pos+1:])
        }

    def clear_cell(self, column_id, row_id):
        """Remove all annotations for a given cell."""
        cell_id = DatasetMetadata.get_cell_key(column_id, row_id)
        if cell_id in self.cell_annotations:
            del self.cell_annotations[cell_id]

    def copy_metadata(self):
        """Make a copy of the metadata set.

        Returns
        -------
        vizier.database.metadata.DatasetMetadata
        """
        return DatasetMetadata(
            column_annotations={
                key: dict(self.column_annotations[key])
                    for key in self.column_annotations
            },
            row_annotations={
                key: dict(self.row_annotations[key])
                    for key in self.row_annotations
            },
            cell_annotations={
                key: dict(self.cell_annotations[key])
                    for key in self.cell_annotations
            }
        )

    def for_cell(self, column_id, row_id):
        """Get object metadata set for a dataset cell.

        Parameters
        ----------
        column_id: int
            Unique column identifier
        row_id: int
            Unique row identifier

        Returns
        -------
        vizier.database.metadata.ObjectMetadataSet
        """
        cell_id = DatasetMetadata.get_cell_key(column_id, row_id)
        if not cell_id in self.cell_annotations:
            self.cell_annotations[cell_id] =  dict()
        return ObjectMetadataSet(self.cell_annotations[cell_id])

    def for_column(self, column_id):
        """Get object metadata set for a dataset column.

        Parameters
        ----------
        column_id: int
            Unique column identifier

        Returns
        -------
        vizier.database.metadata.ObjectMetadataSet
        """
        key = str(column_id)
        if not key in self.column_annotations:
            self.column_annotations[key] =  dict()
        return ObjectMetadataSet(self.column_annotations[key])

    def for_row(self, row_id):
        """Get object metadata set for a dataset row.

        Parameters
        ----------
        row_id: int
            Unique row identifier

        Returns
        -------
        vizier.database.metadata.ObjectMetadataSet
        """
        key = str(row_id)
        if not key in self.row_annotations:
            self.row_annotations[key] =  dict()
        return ObjectMetadataSet(self.row_annotations[key])

    @staticmethod
    def from_file(filename):
        """Read dataset annotations from file. Assumes that the file has been
        created using the default serialization (to_file), i.e., is in Yaml
        format.

        Parameters
        ----------
        filename: string
            Name of the file to read from

        Returns
        -------
        vizier.database.metadata.DatsetMetadata
        """
        with open(filename, 'r') as f:
            doc = yaml.load(f.read())
        return DatasetMetadata(
            column_annotations=annotations_from_list(doc['columns']),
            row_annotations=annotations_from_list(doc['rows']),
            cell_annotations=annotations_from_list(doc['cells'])
        )

    @staticmethod
    def get_cell_key(column_id, row_id):
        """Get string identifier for a dataset cell.

        Parameters
        ----------
        column_id: int
            Unique column identifier
        row_id: int
            Unique row identifier

        Returns
        -------
        string
        """
        return str(column_id) + '#' + str(row_id)

    def to_dict(self):
        """Get a dictionary serialization of all object annotations.

        Returns
        ----------
        dict
        """
        return {
            'columns': annotations_to_list(
                self.column_annotations,
                serializer=lambda k: {'id': int(k)}
            ),
            'rows': annotations_to_list(
                self.row_annotations,
                serializer=lambda k: {'id': int(k)}
            ),
            'cells': annotations_to_list(
                self.cell_annotations,
                serializer=self.cell_key_serializer
            )
        }

    def to_file(self, filename):
        """Write current annotations to file in default file format. The default
        serializartion format is Yaml.

        Parameters
        ----------
        filename: string
            Name of the file to write
        """
        doc = dict({
            'columns': annotations_to_list(self.column_annotations),
            'rows': annotations_to_list(self.row_annotations),
            'cells': annotations_to_list(self.cell_annotations)
        })
        with open(filename, 'w') as f:
            yaml.dump(doc, f, default_flow_style=False)


# ------------------------------------------------------------------------------
# Update Statement
# ------------------------------------------------------------------------------

"""Update statement object types."""
UPD_CELL = 'CELL'
UPD_COLUMN = 'COL'
UPD_ROW = 'ROW'


class AnnotationUpdateStatement(object):
    """The annotation update statement handles the logic of updating an existing
    annotation set.
    """
    def __init__(self, obj_type, obj_id, key, value):
        """Initialize the update statement. Will raise a ValueError if an
        invalid object type is given.

        Parameters
        ----------
        obj_type: string
            Object type identifier (one of UPD_CELL, UPD_COLUMN, UPD_ROW)
        obj_id: int or tuple(int, int)
            Object identifier. For column and row updates this is expected to be
            an integer. For cell updates it is expected to be a pair of column
            identifier and row identifier, i.e., (column_id, row_id)
        key: String
            Annotations key
        value: string
            New annotation value. If the value is None the annotation will be
            deleted.
        """
        # Raise ValueError if object type is invalid
        if not obj_type in [UPD_CELL, UPD_COLUMN, UPD_ROW]:
            raise ValueError('unknown object type \'' + str(obj_type) + '\'')
        self.obj_type = obj_type
        self.obj_id = obj_id
        self.key = key
        self.value = value

    def __call__(self, metadata):
        """Make the object callable.

        Parameters
        ----------
        metadata: vizier.datastore.metadata.DatasetMetadata
            Annotation set that is being updated

        Returns
        -------
        vizier.datastore.metadata.DatasetMetadata
        """
        return self.eval(metadata)

    def eval(self, metadata):
        """Evaluate the update statement on the given annotation set. Returns
        the modified annotation set.

        Parameters
        ----------
        metadata: vizier.datastore.metadata.DatasetMetadata
            Annotation set that is being updated

        Returns
        -------
        vizier.datastore.metadata.DatasetMetadata
        """
        if self.obj_type == UPD_COLUMN:
            metadata.for_column(self.obj_id).set_annotation(self.key, self.value)
        elif self.obj_type == UPD_ROW:
            metadata.for_row(self.obj_id).set_annotation(self.key, self.value)
        elif self.obj_type == UPD_CELL:
            col_id = self.obj_id[0]
            row_id = self.obj_id[1]
            metadata.for_cell(col_id, row_id).set_annotation(self.key, self.value)
        else:
            raise RuntimeError('unknown object type \'' + str(self.obj_type) + '\'')
        return metadata


class UpdateCellAnnotation(AnnotationUpdateStatement):
    """Update statement for a dataset cell."""
    def __init__(self, column_id, row_id, key, value=None):
        """Initialize the update statement.

        Parameters
        ----------
        column_id: int
            Unique column identifier
        column_id: int
            Unique row identifier
        key: String
            Annotations key
        value: string, optional
            New annotation value. If the value is None the annotation will be
            deleted.
        """
        super(UpdateCellAnnotation, self).__init__(
            UPD_CELL,
            (column_id, row_id),
            key,
            value
        )


class UpdateColumnAnnotation(AnnotationUpdateStatement):
    """Update statement for a dataset column."""
    def __init__(self, column_id, key, value=None):
        """Initialize the update statement.

        Parameters
        ----------
        column_id: int
            Unique column identifier
        key: String
            Annotations key
        value: string, optional
            New annotation value. If the value is None the annotation will be
            deleted.
        """
        super(UpdateColumnAnnotation, self).__init__(
            UPD_COLUMN,
            column_id,
            key,
            value
        )


class UpdateRowAnnotation(AnnotationUpdateStatement):
    """Update statement for a dataset row."""
    def __init__(self, row_id, key, value=None):
        """Initialize the update statement.

        Parameters
        ----------
        row_id: int
            Unique row identifier
        key: String
            Annotations key
        value: string, optional
            New annotation value. If the value is None the annotation will be
            deleted.
        """
        super(UpdateRowAnnotation, self).__init__(
            UPD_ROW,
            row_id,
            key,
            value
        )


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def annotations_from_list(annotations):
    """Convert a list of (key, dict) pairs into a object metadata set.

    Patameters
    ----------
    annotations: list
        List of (key, dict) pairs

    Returns
    -------
    dict
    """
    result = dict()
    for obj in annotations:
        result[obj['key']] = {
            anno['key'] : anno['value'] for anno in obj['annotations']
        }
    return result


def annotations_to_list(annotations, serializer=None):
    """Convert an object metadata setinto a list of (key, annotations) pairs.

    Patameters
    ----------
    annotations: dict(vizier.database.metadata.ObjectMetadataSet)
        Object annotations
    serializer: func
        Object key Serializer

    Returns
    -------
    list
    """
    if serializer is None:
        serializer = lambda k: {'key': k}
    result = list()
    for key in annotations:
        obj_anno = annotations[key]
        if len(obj_anno) > 0:
            doc = serializer(key)
            doc['annotations'] = [{
                    'key': anno,
                    'value': obj_anno[anno]
                } for anno in obj_anno
            ]
            result.append(doc)
    return result
