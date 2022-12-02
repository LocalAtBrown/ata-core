from datetime import datetime, timedelta
from typing import List

import click

from ata_pipeline0.helpers.site import SiteName
from ata_pipeline0.main import run_pipeline


@click.command()
@click.option(
    "--start-date",
    type=click.DateTime(),
    default=datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
    help="Start date to run the backfill from.",
)
@click.option("--days", type=int, default=1, help="How many days of data to grab after the start date.")
@click.argument("site", type=SiteName)
def backfill(start_date: datetime, days: int, site: SiteName):
    timestamps = get_timestamps(start_date=start_date, days=days)
    # call function that calls the whole process; handler should call the same fn
    run_pipeline(site_name=site, timestamps=timestamps)


def get_timestamps(start_date: datetime, days: int) -> List[datetime]:
    return [start_date + timedelta(hours=hour_diff) for hour_diff in range(days * 24)]


if __name__ == "__main__":
    backfill()
