import gzip
import itertools
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Tuple

import pandas as pd
from mypy_boto3_s3.service_resource import ObjectSummary, S3ServiceResource
from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef

from src.helpers.exceptions import (
    EventObjectDecompressError,
    EventObjectFetchError,
    EventObjectParseError,
)


def fetch_events(
    s3_resource: S3ServiceResource, site_bucket_name: str, timestamps: List[datetime], num_concurrent_downloads: int
) -> pd.DataFrame:
    """
    Given config inputs, including a site's bucket name and a list of date-hour
    timestamps, fetches corresponding S3 Snowplow event files and returns them
    as a single DataFrame.

    Requires an S3ServiceResource as a parameter; here's how to create it:
    >>> import boto3
    >>> s3_resource = boto3.resource("s3")
    """
    # Grab S3 bucket
    bucket = s3_resource.Bucket(site_bucket_name)

    # Get a list of summaries of S3 objects to fetch
    object_summaries_by_timestamp = [
        bucket.objects.filter(Prefix=f"enriched/good/{ts.strftime('%Y/%m/%d/%H')}") for ts in timestamps
    ]
    object_summaries = itertools.chain(*object_summaries_by_timestamp)

    # Spread fetching tasks over a number of CPU threads. Until aioboto3 is thoroughly documented and isn't a pain
    # to work with (or until boto3 is asyncio-friendly), multithreading is a decent alternative
    # Fetching all data (even gzipped) from the get-go might incur significant memory footprint, but this is a simple start
    with ThreadPoolExecutor(max_workers=num_concurrent_downloads) as executor:
        dfs = executor.map(_fetch_decompress_parse, object_summaries)

    return pd.concat(dfs)


def _fetch_decompress_parse(object_summary: ObjectSummary) -> pd.DataFrame:
    key, response = _fetch_object(object_summary)
    data = _decompress_object(key, response)
    df = _parse_object(key, data)
    return df


def _fetch_object(object_summary: ObjectSummary) -> Tuple[str, GetObjectOutputTypeDef]:
    key = object_summary.key
    try:
        response = object_summary.get()
    except Exception as e:
        raise EventObjectFetchError(f"Failed to fetch object {object_summary.key}") from e
    return key, response


def _decompress_object(object_key: str, object_response: GetObjectOutputTypeDef) -> bytes:
    try:
        data = object_response["Body"].read()
        data = gzip.decompress(data)
    except Exception as e:
        raise EventObjectDecompressError(f"Failed to decompress/unzip object {object_key}") from e
    return data


def _parse_object(object_key: str, object_data: bytes) -> pd.DataFrame:
    try:
        data = object_data.decode("utf-8").strip().split("\n")
        data = [json.loads(row) for row in data]
    except Exception as e:
        raise EventObjectParseError(f"Failed to parse object {object_key} into a DataFrame") from e
    return pd.DataFrame.from_records(data)
