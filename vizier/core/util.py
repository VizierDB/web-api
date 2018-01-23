"""Collection of helper methods."""

import uuid


# ------------------------------------------------------------------------------
# Classes
# ------------------------------------------------------------------------------

class Sequence(object):
    """Sequence of integer values. Maintains a counter that is incremented to
    generate new values in the sequence.
    """
    def __init__(self, value=0):
        """initialize the internal counter.

        Parameters
        ----------
        value: int, optional
            Counter for sequence values
        """
        self.value = value

    def inc(self):
        """Increment the internal counter and return the previous value.

        Returns
        -------
        int
        """
        result = self.value
        self.value += 1
        return result


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_unique_identifier():
    """Create a new unique identifier.

    Returns
    -------
    string
    """
    return str(uuid.uuid4()).replace('-', '')


def is_valid_name(name):
    """Returns Ture if a given string represents a valid name (e.g., for a
    dataset). Valid names contain only letters, digits or underline. A
    valid name has to contain at least one digit or letter.

    Parameters
    ----------
    name : string
        Name for dataset in VizUAL dataset or column

    Returns
    -------
    bool
    """
    allnums = 0
    for c in name:
        if c.isalnum():
            allnums += 1
        elif c != '_':
            return False
    return (allnums > 0)
