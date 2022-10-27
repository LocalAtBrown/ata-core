from datetime import datetime
from operator import attrgetter


def convert_to_s3_folder(timestamp: datetime) -> str:
    """
    Given a date-hour datetime-like timestamp, returns the S3 bucket folder name
    corresponding to it.
    """
    timestamp_attrs = attrgetter("year", "month", "day", "hour")(timestamp)
    # Need to left-pad month, day & hour to 2 digits
    # zfill never truncates string, so the 4-digit year stays the same
    return "/".join([str(attr).zfill(2) for attr in timestamp_attrs])
