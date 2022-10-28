from datetime import datetime
from typing import List

import pytest

from src.fetch_events import fetch_events


# ---------- FIXTURES ----------
@pytest.fixture(scope="module")
def site_bucket_name() -> str:
    return "lnl-snowplow-local-news-lab"


@pytest.fixture(scope="module")
def timestamps_single_empty() -> List[datetime]:
    """
    Timestamp pointing to a nonexistent S3 folder
    """
    # https://s3.console.aws.amazon.com/s3/buckets/lnl-snowplow-local-news-lab?prefix=enriched/good/2022/10/26/&region=us-east-1
    return [datetime(2022, 10, 26, 22)]


@pytest.fixture(scope="module")
def timestamps_single_nonempty() -> List[datetime]:
    """
    Timestamp pointing to an existing S3 folder
    """
    # https://s3.console.aws.amazon.com/s3/buckets/lnl-snowplow-local-news-lab?prefix=enriched/good/2022/10/26/&region=us-east-1
    return [datetime(2022, 10, 26, 20)]


@pytest.fixture(scope="module")
def timestamps_multi() -> List[datetime]:
    """
    2 timestamps: one empty, one nonempty
    """
    # https://s3.console.aws.amazon.com/s3/buckets/lnl-snowplow-local-news-lab?prefix=enriched/good/2022/10/26/&region=us-east-1
    return [datetime(2022, 10, 26, 20), datetime(2022, 10, 26, 22)]


# ---------- TESTS ----------
def test_single_ts_empty(site_bucket_name, timestamps_single_empty, tmp_path) -> None:
    """
    1 timestamp pointing to a nonexistent S3 folder, 1 process
    """
    dfs = list(
        fetch_events(site_bucket_name, timestamps_single_empty, tmp_path, num_processes=1, delete_after_read=False)
    )
    assert len(dfs) == 1
    assert dfs[0].shape == (0, 0)


def test_single_ts_nonempty(site_bucket_name, timestamps_single_nonempty, tmp_path):
    """
    1 timestamp pointing to an existing S3 folder, 1 process
    """
    dfs = list(
        fetch_events(site_bucket_name, timestamps_single_nonempty, tmp_path, num_processes=1, delete_after_read=False)
    )
    assert len(dfs) == 1
    # If restrict to specific fields, this will break, but the number of rows should stay the same
    assert dfs[0].shape == (9, 63)


def test_multi_ts_single_proc(site_bucket_name, timestamps_multi, tmp_path):
    """
    2 timestamps, 1 process
    """
    dfs = list(fetch_events(site_bucket_name, timestamps_multi, tmp_path, num_processes=1, delete_after_read=False))
    assert len(dfs) == 2
    # Assuming order of DataFrames received is the same as order between timestamps
    # If restrict to specific fields, column shape check might break, but the number of rows should stay the same
    assert dfs[0].shape == (9, 63)
    assert dfs[1].shape == (0, 0)


def test_multi_ts_multi_procs(site_bucket_name, timestamps_multi, tmp_path):
    """
    2 timestamps, 2 processes
    """
    dfs = list(fetch_events(site_bucket_name, timestamps_multi, tmp_path, num_processes=2, delete_after_read=False))
    assert len(dfs) == 2
    # Assuming order of DataFrames received is the same as order between timestamps
    # If restrict to specific fields, column shape check might break, but the number of rows should stay the same
    assert dfs[0].shape == (9, 63)
    assert dfs[1].shape == (0, 0)
