import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Generator

import pandas as pd

from src.helpers.datetime import convert_to_s3_folder
from src.helpers.io import read_gzipped_json_records


def fetch_events(
    site_bucket_name: str, timestamps: list[datetime], path_to_data_dir: str, num_processes: int
) -> Generator[pd.DataFrame, None, None]:
    """
    Given config inputs, including a site's bucket name and a list of date-hour
    timestamps, fetches corresponding S3 Snowplow event files and returns them
    as a generator of pandas DataFrames, each corresponding to one timestamp.
    """
    processes: dict[datetime, subprocess.Popen] = {}
    timestamps_to_download = iter(timestamps)

    # Start fetching for the first num_processes timestamps
    for _ in range(num_processes):
        timestamp = next(timestamps_to_download, None)
        if timestamp:
            processes[timestamp] = _fetch_folder(site_bucket_name, timestamp, path_to_data_dir)

    for timestamp in timestamps:
        # Wait for fetch to finish
        processes[timestamp].wait()

        # Start next fetch job in the queue
        timestamp_next = next(timestamps_to_download, None)
        if timestamp_next:
            processes[timestamp_next] = _fetch_folder(site_bucket_name, timestamp, path_to_data_dir)

        # Read fetched data into a DataFrame
        df = _read_folder(site_bucket_name, timestamp, path_to_data_dir)

        # TODO: Delete local data folder from disk

        # Yield the fetched data for transformation
        yield df


def _fetch_folder(site_bucket_name: str, timestamp: datetime, path_to_data_dir: str) -> subprocess.Popen:
    """
    Runs a command downloading an S3 folder of enriched Snowplow events in a site's
    bucket corresponding to a given date and hour.
    """
    timestamp_folder = convert_to_s3_folder(timestamp)

    path_local = Path(path_to_data_dir) / timestamp_folder
    # Create the path directory. It shouldn't have already existed because it's
    # deleted at the end of a job run, but setting exist_ok=True prevents error if
    # it already exists
    path_local.mkdir(parents=True, exist_ok=True)

    # Probably fine to put the "enriched/good" string here since we're only using enriched data,
    # it can be passed to as a function parameter if needed
    # TODO: When Site is a full-fledged class, probably still worth passing just a bucket name,
    # e.g., site_bucket_name = Site.s3_bucket_name instead of passing the entire Site instance
    path_s3 = Path(site_bucket_name) / "enriched/good" / timestamp_folder
    uri_s3 = f"s3://{path_s3}"

    command = f"aws s3 sync {uri_s3} {path_local}"

    # TODO: Maybe it's worth configuring our own way of logging stuff, like this:
    # https://github.com/LocalAtBrown/experiment-semantic-similarity/blob/main/notebooks/util/logging.py?
    # Adding file name, function & line number of where the log happens might be useful
    logging.info(f"Running this command in a separate processor: {command}")

    # Run the command
    return subprocess.Popen(command.split(" "), stdout=subprocess.DEVNULL)


def _read_folder(site_bucket_name: str, timestamp: datetime, path_to_data_dir: str) -> pd.DataFrame:
    """
    Opens all .gz files in a local data folder given its corresponding site bucket
    name and date-hour timestamp and combine all their data into a single pandas DataFrame.
    """
    # Local directory holding all data files fetched from the remote S3 folder
    path_local = Path(path_to_data_dir) / convert_to_s3_folder(timestamp)

    # Probably only need *.gz instead of **/*.gz because there should be no directory
    # within the local data folder, but using the latter for good measure
    dfs = [read_gzipped_json_records(path_file) for path_file in path_local.glob("**/*.gz")]

    # Combine into a single DataFrame
    return pd.concat(dfs)
