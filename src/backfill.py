"""
This needs to:
1. accept args
  a. site
  b. start date
  c. end date

2. fetch events from s3 (existing functionality, will need to convert from 2 datetimes to explicit specific datetimes
so we will need a function to take 2 datetimes and grab all paths between them for the given site)
3. process them (existing functionality)
4. store them (being written right now)

Notes:
- Might need to batch
- Run locally, or run on cloud? Start with local
"""

from datetime import datetime, timedelta
from typing import List

import click


@click.command()
@click.option(
    "--start_date",
    type=click.DateTime,
    default=datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
    help="Start date to run the backfill from.",
)
@click.option("--days", type=click.INT, default=1, help="How many days of data to grab after the start date.")
@click.argument("site", type=click.Choice(["afro-la", "dallas-free-press", "open-vallejo", "the-19th"]))
def backfill(start_date: datetime, days: int, site: str):
    pass


def get_timestamps(start_date: datetime, days: int) -> List[datetime]:
    return [start_date + timedelta(hours=hour_diff) for hour_diff in range(days * 24)]
