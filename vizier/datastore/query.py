"""Classes and methods to support simple queries over datasets."""

class DataStreamConsumer(object):
    """Row consumers are used to filter values for a data series."""
    def __init__(self, column_index, range_start=None, range_end=None, cast_to_number=True):
        """
        """
        self.column_index = column_index
        self.range_start = range_start if not range_start is None else 0
        self.range_end = range_end
        self.cast_to_number = cast_to_number
        self.values = list()

    def consume(self, row, row_index, cast_to_numeric=True):
        """
        """
        if row_index >= self.range_start:
            if self.range_end is None or row_index <= self.range_end:
                val = row.values[self.column_index]
                if self.cast_to_number:
                    # Only convert if not already a numeric value. Assumes a
                    # string if not numeric
                    if not isinstance(val, int) and not isinstance(val, float):
                        # Try to cast to integer first. Remove commas.
                        try:
                            val = int(val.replace(',', ''))
                        except ValueError:
                            # Try to convert to float if int failed
                            try:
                                val = float(val)
                            except ValueError:
                                pass
                self.values.append(val)
