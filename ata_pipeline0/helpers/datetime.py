from datetime import datetime, timedelta
from typing import List


def get_timestamps(start_date: datetime, days: int) -> List[datetime]:
    return [start_date + timedelta(hours=hour_diff) for hour_diff in range(days * 24)]
