"""Classes and methods to support simple queries over datasets."""

class DataStreamConsumer(object):
    """Row consumers are used to filter values for a data series."""
    def __init__(self, column_index, range_start=None, range_end=None):
        """
        """
        self.column_index = column_index
        self.range_start = range_start if not range_start is None else 0
        self.range_end = range_end
        self.values = list()

    def consume(self, row, row_index):
        """
        """
        if row_index >= self.range_start:
            if self.range_end is None or row_index <= self.range_end:
                self.values.append(row.values[self.column_index])
