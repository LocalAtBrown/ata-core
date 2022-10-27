import gzip
import json
from collections import deque
from pathlib import Path
from typing import Optional, Union

import pandas as pd


def read_gzipped_json_records(path: Union[str, Path], fields: Optional[set[str]] = None) -> pd.DataFrame:
    """
    Reads gzipped file of json records as rows, e.g., in a Snowplow event file, into a pandas DataFrame.
    This is faster and less error-prone than using pandas to open Snowplow .gz files directly.
    """
    with gzip.open(path, "rt") as f:
        # records is a queue instead of a list to make append operations faster
        # (refer to https://www.geeksforgeeks.org/deque-in-python/)
        records = deque()

        for line in f:
            record = json.loads(line)
            if fields is None:
                # Add all fields if there's no specified list of fields
                records.append(record)
            else:
                # TODO: Doing this instead of the try-except block in
                # https://github.com/LocalAtBrown/article-rec-training-job/blob/5d3f556760a047b03128e4ec1ffc36e6665bf27d/job/steps/fetch_data.py#L53-L58
                # We'll see what kind of error appears and adjust accordingly
                records.append({field: record.get(field) for field in fields})

    return pd.DataFrame.from_records(records)
