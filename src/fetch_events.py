import logging
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.helpers.datetime import convert_to_s3_folder


def fetch_events(
    site_bucket_name: str, timestamps: list[datetime], path_to_intermediate_files: str, num_processes: int
) -> pd.DataFrame:
    """
    Given config inputs, including a site's bucket name and a list of date-hour
    timestamps, fetches corresponding S3 Snowplow event files and returns them
    as a single DataFrame.
    """
    # dfs: list[pd.DataFrame] = []
    processes: dict[datetime, subprocess.Popen] = {}
    timestamps_to_download = iter(timestamps)

    for _ in range(num_processes):
        timestamp = next(timestamps_to_download, None)
        if timestamp:
            processes[timestamp] = _fetch_folder(site_bucket_name, timestamp, path_to_intermediate_files)

    for timestamp in timestamps:
        # Wait for download to finish
        processes[timestamp].wait()

        # Start next download job in the queue
        timestamp_next = next(timestamps_to_download, None)
        if timestamp_next:
            processes[timestamp_next] = _fetch_folder(site_bucket_name, timestamp, path_to_intermediate_files)


def _fetch_folder(site_bucket_name: str, timestamp: datetime, path_to_intermediate_files: str) -> subprocess.Popen:
    """
    Runs a command downloading an S3 folder of enriched Snowplow events in a site's bucket corresponding
    to a given date and hour.
    """
    timestamp_folder = convert_to_s3_folder(timestamp)

    path_local = Path(path_to_intermediate_files) / timestamp_folder
    # Create the path. It shouldn't have already existed because it's deleted at the end of the run,
    # but setting exist_ok=True prevents error if it already exists
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
