from datetime import datetime, timedelta

import click

from ata_pipeline0.helpers.datetime import get_timestamps
from ata_pipeline0.main import run_pipeline
from ata_pipeline0.site.names import SiteName
from ata_pipeline0.site.sites import SITES


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
    run_pipeline(site=SITES[site], timestamps=timestamps)


if __name__ == "__main__":
    backfill()
