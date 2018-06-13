"""Metadata annotation object for datasets.

Maintains a list of annotation objects, i.e., (key,value)-pairs, for different
identifiable components of a dataset. the identifiable components of a dataset
are COLUMN, ROW, and CELL.
"""

import json


# ------------------------------------------------------------------------------
# Metadata Objects
# ------------------------------------------------------------------------------

class Annotation(object):
    """Annotations are identifiable key value pairs. The object identifier are
    unique only among annotations for the same dataset component, They are not
    global unique identifier.

    Attributes
    ----------
    identifier: int
        Unique annotation identifier
    key: string
        Annotation key
    value: string
        Annotation value
    """
    def __init__(self, identifier, key, value):
        self.identifier = identifier
        self.key = key
        self.value = value

    @staticmethod
    def from_dict(doc):
        """Create an annotation instance from a dictionary representation.

        Parameters
        ----------
        doc: dict()
            Dictionary representation as generated by to_dict()

        Returns
        -------
        vizier.datastore.metadata.Annotation
        """
        return Annotation(doc['id'], key=doc['key'], value=doc['value'])

    def to_dict(self):
        """Dictionary representation for the annotation.

        Returns
        -------
        dict
        """
        return {'id': self.identifier, 'key': self.key, 'value': self.value}


class ObjectMetadataSet(object):
    """Manage annotations for a single identifiable dataset component. This is
    a wrapper around the dictionary that contains the annotations. In the
    dictionary, annotations are indexed by their unique identifier.
    """
    def __init__(self, annotations=None):
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

    def add(self, key, value):
        """Add a new annotation to the given metadata set. The unique annotation
        identifier is the current size of the dictionary.

        Parameters
        ----------
        key: string
            Key value for new annotation
        value: string
            Value for new annotation

        Returns
        -------
        vizier.datastore.metadata.Annotation
        """
        anno = Annotation(len(self.annotations), key, value)
        self.annotations[anno.identifier] = anno
        return anno

    def contains(self, key):
        """Test if an annotation with given key exists for the object.

        Parameters
        ----------
        key: string
            Annotation key

        Returns
        -------
        bool
        """
        return not self.find_one(key) is None

    def find_all(self, key):
        """Get a list with all annotations that have a given key. Returns an
        empty list if no annotation with the given key exists.

        Parameters
        ----------
        key: string
            Key value for new annotation

        Returns
        -------
        list(vizier.datastore.metadata.Annotation)
        """
        result = list()
        for anno in self.annotations.values():
            if anno.key == key:
                result.append(anno)
        return result

    def find_one(self, key):
        """Find the first annotation with given key. Returns None if no
        annotation with the given key exists.

        Parameters
        ----------
        key: string
            Key value for new annotation

        Returns
        -------
        vizier.datastore.metadata.Annotation
        """
        for anno in self.annotations.values():
            if anno.key == key:
                return anno

    def get(self, identifier):
        """Get the annotation with the given identifier. Returns None if no
        annotation with the given identifier exists.

        Parameters
        ----------
        identifier: int
            Unique annotation identifier

        Returns
        -------
        vizier.datastore.metadata.Annotation
        """
        if identifier in self.annotations:
            return self.annotations[identifier]

    def keys(self):
        """List of existing annotation keys for the object.

        Returns
        -------
        list(string)
        """
        result = set()
        for anno in self.annotations.values():
            result.add(anno.key)
        return list(result)

    def remove_all(self, keys):
        """Remove all annotations for a given list of keys.

        Parameters
        ----------
        key: list(string)
            List of keys for annotations to be removed
        """
        for anno_id in self.annotations.keys():
            anno = self.annotations[anno_id]
            if anno.key in keys:
                del self.annotations[anno_id]

    def size(self):
        """Number of annotations for this object.

        Returns
        -------
        int
        """
        return len(self.annotations)

    def update(self, identifier=-1, key=None, value=None):
        """Update the annotation dictionary. If the annotation identifier is not
        negative an attempt to update an existing annotation is made. Otherwise
        a new annotation is being inserted.

        If the annotation value is None an existing annotation will be deleted.

        Parameters
        ----------
        identifier: int
            Unique annotation identifier
        key: string
            Annotation key
        value: string
            Annotation value

        Returns
        -------
        vizier.datastore.metadata.Annotation
        """
        # Check if an existing annotation is being updated or a new annotation
        # added
        if identifier >= 0:
            # Delete the existing annotation if the given value is None
            if value is None:
                anno = self.annotations[identifier]
                del self.annotations[identifier]
                return anno
            else:
                if key is None:
                    # Get the key for the current annotation
                    key = self.annotations[identifier].key
                anno = Annotation(identifier, key=key, value=value)
                self.annotations[identifier] = anno
                return anno
        else:
            # New annotation. Only add an annotation if value is not None
            if not key is None and not value is None:
                return self.add(key=key, value=value)

    def values(self):
        """List of all annotations in the dictionary.

        Returns
        -------
        list(vizier.datastore.metadata.Annotation)
        """
        return self.annotations.values()


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
        vizier.datastore.metadata.ObjectMetadataSet
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
        dict()
        """
        key = str(column_id)
        if not key in self.column_annotations:
            self.column_annotations[key] =  dict()
        return ObjectMetadataSet(self.column_annotations[key])

    def for_object(self, column_id=-1, row_id=-1):
        """Get annotation dictionary for the dataset component identified by the
        gieven column and row identifier.

        Parameters
        ----------
        column_id: int, optional
            Unique columnidentifier
        row_id: int, optional
            Unique row identifier

        Returns
        -------
        vizier.datastore.metadata.ObjectMetadataSet
        """
        if column_id >= 0 and row_id < 0:
            return self.for_column(column_id)
        elif column_id < 0 and row_id >= 0:
            return self.for_row(row_id)
        elif column_id >= 0 and row_id >= 0:
            return self.for_cell(column_id, row_id)

    def for_row(self, row_id):
        """Get object metadata set for a dataset row.

        Parameters
        ----------
        row_id: int
            Unique row identifier

        Returns
        -------
        vizier.datastore.metadata.ObjectMetadataSet
        """
        key = str(row_id)
        if not key in self.row_annotations:
            self.row_annotations[key] =  dict()
        return ObjectMetadataSet(self.row_annotations[key])

    @staticmethod
    def from_file(filename):
        """Read dataset annotations from file. Assumes that the file has been
        created using the default serialization (to_file), i.e., is in Json
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
            doc = json.loads(f.read())
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

    def has_cell_annotation(self, column_id, row_id):
        """Check whether there exist any annotations for a given dataset cell.

        Parameters
        ----------
        column_id: int
            Unique column identifier
        row_id: int
            Unique row identifier

        Returns
        -------
        bool
        """
        cell_id = DatasetMetadata.get_cell_key(column_id, row_id)
        if cell_id in self.cell_annotations:
            return len(self.cell_annotations[cell_id]) > 0
        return False


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
        serializartion format is Json.

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
            json.dump(doc, f)


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
        items = dict()
        for doc in obj['annotations']:
            anno = Annotation.from_dict(doc)
            items[anno.identifier] = anno
        result[obj['key']] = items
    return result


def annotations_to_list(annotations, serializer=None):
    """Convert an object metadata setinto a list of (key, annotations) pairs.

    Patameters
    ----------
    annotations: dict
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
            doc['annotations'] = [anno.to_dict() for anno in obj_anno.values()]
            result.append(doc)
    return result
