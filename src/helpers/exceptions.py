# Snowplow fetching
class EventObjectFetchError(Exception):
    """
    Custom exception upon failure while fetching an S3 object.
    """

    pass


class EventObjectDecompressError(Exception):
    """
    Custon exception upon failure while decompressing/unzipping an S3 object.
    """

    pass


class EventObjectParseError(Exception):
    """
    Custom exception upon failure while parsing the contents of an S3 object.
    """


# Snowplow transformation

# Snowplow upload to Redshift
