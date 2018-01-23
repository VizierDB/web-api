"""Helper methods for timestamps - Contains functions to create and Convert
timestamps.
"""

import datetime


def get_current_time():
    """Return timestamp for current system time in UTC time zone.

    Returns
    -------
    datetime
        Current system time
    """
    return datetime.datetime.utcnow()


def to_datetime(timestamp):
    """Converts a timestamp string in ISO format into a datatime object.

    Parameters
    ----------
    timstamp : string
        Timestamp in ISO format

    Returns
    -------
    datatime.datetime
        Datetime object
    """
    return datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
