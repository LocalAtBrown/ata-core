from datetime import datetime, timedelta

import click

from ata_pipeline0.helpers.datetime import get_timestamps
from ata_pipeline0.helpers.logging import logging
from ata_pipeline0.helpers.site import SiteName
from ata_pipeline0.main import run_pipeline

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--start-date",
    type=click.DateTime(),
    default=datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
    help="Start date to run the backfill from.",
)
@click.option("--days", type=int, default=1, help="How many days of data to grab after the start date.")
@click.option("--batch", is_flag=True, default=False, help="Set to batch runs per-timestamp; use for larger ranges.")
@click.option("--batch-size", type=int, default=1, help="Number of hours to batch by.")
@click.argument("site", type=SiteName)
def backfill(start_date: datetime, days: int, batch: bool, batch_size: int, site: SiteName):
    exec_start = datetime.now()
    timestamps = get_timestamps(start_date=start_date, days=days)
    # call function that calls the whole process; handler should call the same fn
    if batch:
        for batch_num, idx in enumerate(range(0, len(timestamps), batch_size)):
            current_timestamps = timestamps[idx : idx + batch_size]
            logger.info(f"Running batch #{batch_num} for the following timestamps:")
            for ts in current_timestamps:
                logger.info(f"--> {ts}")
            run_pipeline(site_name=site, timestamps=current_timestamps)
    else:
        run_pipeline(site_name=site, timestamps=timestamps)
    exec_end = datetime.now()
    exec_total = exec_end - exec_start
    logger.info(f"Backfill finished, took: {exec_total} long.")


if __name__ == "__main__":
    backfill()
