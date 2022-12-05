from datetime import datetime, timedelta

import pytest

from ata_pipeline0.helpers.datetime import get_timestamps


@pytest.mark.unit
def test_get_timestamps() -> None:
    start_date = datetime.strptime("2022-10-10", format("%Y-%m-%d")) - timedelta(days=1)
    days = 1
    result = get_timestamps(start_date=start_date, days=days)

    expected = [start_date.replace(hour=hour) for hour in range(24)]

    assert result == expected
