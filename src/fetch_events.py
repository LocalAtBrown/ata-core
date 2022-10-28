import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Generator, List, Union, cast

import pandas as pd

from src.helpers.datetime import convert_to_s3_folder
from src.helpers.exceptions import EventFetchError
from src.helpers.io import read_gzipped_json_records


def fetch_events(
    site_bucket_name: str,
    timestamps: List[datetime],
    path_to_data_dir: Union[str, Path],
    num_processes: int,
    delete_after_read: bool = True,
) -> Generator[pd.DataFrame, None, None]:
    """
    Given config inputs, including a site's bucket name and a list of date-hour
    timestamps, fetches corresponding S3 Snowplow event files and returns them
    as a generator of pandas DataFrames (instead of a list, which uses more memory),
    each corresponding to one timestamp.
    """
    processes: dict[datetime, subprocess.Popen] = {}
    timestamps_to_download = iter(timestamps)
    # Turn path_to_data_dir into a Path object if it's a string
    path_to_data_dir = Path(path_to_data_dir)

    # Start fetching for the first num_processes timestamps
    for _ in range(num_processes):
        timestamp = next(timestamps_to_download, None)
        if timestamp:
            processes[timestamp] = _fetch_folder(site_bucket_name, timestamp, path_to_data_dir)

    # As soon as a process finishes and is freed up, save its data into a DataFrame and fetch the next timestamp
    for i, timestamp in enumerate(timestamps):
        process = processes[timestamp]

        # Wait for fetch to finish using Popen.communicate
        _, process_error_message = process.communicate()
        # Raise exception if return code is not 0, indicating an error happened
        process_return_code = process.returncode
        process_args = cast(List[str], process.args)  # to appease mypy
        if process_return_code != 0:
            raise EventFetchError(
                f"Subprocess command failed with return code {process_return_code}. "
                + f"MESSAGE: {process_error_message.decode('utf-8').strip()}. "
                + f"COMMAND: {' '.join(process_args)}."
            )

        # Start next fetch job in the queue
        timestamp_next = next(timestamps_to_download, None)
        if timestamp_next:
            processes[timestamp_next] = _fetch_folder(site_bucket_name, timestamp_next, path_to_data_dir)

        # Read fetched data into a DataFrame
        df = _read_folder(site_bucket_name, timestamp, path_to_data_dir)

        # Delete local data folder from disk
        if delete_after_read:
            if i == len(timestamps) - 1:
                # Once reach the end of timestamp list, delete entire data directory
                # containing all local folders to remove clutter
                shutil.rmtree(path_to_data_dir)
            else:
                # Remove S3-downloaded folder we just read from
                _delete_folder(site_bucket_name, timestamp, path_to_data_dir)

        # Yield the fetched data for next step, i.e., transformation
        yield df


def _fetch_folder(site_bucket_name: str, timestamp: datetime, path_to_data_dir: Path) -> subprocess.Popen:
    """
    Runs a command downloading an S3 folder of enriched Snowplow events in a site's
    bucket corresponding to a given date and hour.
    """
    timestamp_folder = convert_to_s3_folder(timestamp)

    path_local = path_to_data_dir / timestamp_folder
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

    # TODO: Once a global logging config is set, log the exact command to the console
    # Would also be helpful to customize log message by adding file name, function & line
    # number of where the log happens, kind of like this:
    # https://github.com/LocalAtBrown/experiment-semantic-similarity/blob/main/notebooks/util/logging.py

    # Run the command
    return subprocess.Popen(command.split(" "), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def _read_folder(site_bucket_name: str, timestamp: datetime, path_to_data_dir: Path) -> pd.DataFrame:
    """
    Opens all .gz files in a local data folder given its corresponding site bucket
    name and date-hour timestamp and combine all their data into a single pandas DataFrame.
    """
    # Local directory holding all data files fetched from the remote S3 folder
    path_local = path_to_data_dir / convert_to_s3_folder(timestamp)

    # Probably only need *.gz instead of **/*.gz because there should be no directory
    # within the local data folder, but using the latter for good measure
    dfs = [read_gzipped_json_records(path_file) for path_file in path_local.glob("**/*.gz")]

    # If there are no files/DataFrames, return an empty DataFrame, since pd.concat([]) throws an error
    if len(dfs) == 0:
        # TODO: logging.info showing no data available to fetch for this particular date-hour timestamp
        return pd.DataFrame()

    # Combine into a single DataFrame
    return pd.concat(dfs)


def _delete_folder(site_bucket_name: str, timestamp: datetime, path_to_data_dir: Path) -> None:
    """
    Deletes an S3-fetched folder corresponding to the given site bucket name and date-hour timestamp.
    """
    path_local = path_to_data_dir / convert_to_s3_folder(timestamp)
    shutil.rmtree(path_local)
