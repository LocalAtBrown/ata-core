import pandas as pd
from ata_db_models.models import Event
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.helpers.logging import logging

logger = logging.getLogger(__name__)


def write_events(df: pd.DataFrame, engine: Engine) -> None:
    """
    Writes preprocessed events to database.
    """
    data = df.to_dict(orient="records")

    # Wrap execution within a begin-commit-rollback block in the form of two
    # context managers (see: https://docs.sqlalchemy.org/en/14/orm/session_basics.html#framing-out-a-begin-commit-rollback-block)
    with Session(engine) as session, session.begin():
        # Create statement to bulk-insert event rows
        # Insert.on_conflict_do_nothing skips through events whose [event_id, site_name]
        # composite key already exists in the DB
        statement = insert(Event).values(data).on_conflict_do_nothing(index_elements=[Event.site_name, Event.event_id])

        # Execute statement
        result = session.execute(statement)

    # Count number of rows/events inserted
    num_rows_inserted = result.rowcount

    # Log message
    logger.info(
        f"Inserted {num_rows_inserted} rows into the {Event.__name__} table. "
        + f"Skipped {df.shape[0] - num_rows_inserted} rows that violated unique key constraint."
    )
