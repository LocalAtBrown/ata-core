from contextlib import contextmanager

import pandas as pd
import pytest
from ata_db_models.helpers import get_conn_string
from ata_db_models.models import Event, SQLModel
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from src.helpers.fields import FieldNew, FieldSnowplow
from src.helpers.preprocessors import ConvertFieldTypes
from src.helpers.site import SiteName
from src.write_events import write_events


# ---------- FIXTURES ----------
@pytest.fixture(scope="module")
def df() -> pd.DataFrame:
    """
    Returns a dummy DataFrame for testing.
    """
    df = pd.DataFrame(
        [
            [
                "736.0",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 15_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6.1 Mobile/15E148 Safari/604.1",
                "47a36759-7b59-4171-8db2-59fdbb0cde9a",
                "https://duckduckgo.com/",
                "page_ping",
                "DuckDuckGo",
                "/author/Diante-Marigny/",
                "414.0",
                None,
                None,
                "530.0",
                "2022-11-23 00:18:14.427000+00:00",
                "1",
                "3784c768-0eaf-407e-ac35-baa285bfba53",
                "search",
                None,
                "9c6e4788-b05f-4521-a589-52dc6d250e8f",
                "2654.0",
                SiteName.DALLAS_FREE_PRESS,
            ],
            [
                "600.0",
                "Mozilla/5.0 (compatible; AhrefsBot/7.0;  http://ahrefs.com/robot/)",
                "70366c22-fff7-4ce1-8242-d7367a51dae7",
                None,
                "page_view",
                None,
                "/west-dallas/west-dallas-history-environmental-injustice-timeline-concrete/",
                "800.0",
                None,
                None,
                None,
                "2022-11-23 00:01:52.024000+00:00",
                "1",
                "74309984-1b10-445a-9b1a-e05ee448abd9",
                None,
                None,
                "d4681118-3b33-47d7-8baa-4213cdb9eecd",
                "16582.0",
                SiteName.AFRO_LA,
            ],
            [
                "900.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Safari/605.1.15",
                "00ffcb05-8fe9-46b2-8c7f-0556012d617d",
                None,
                "submit_form",
                None,
                "/",
                "1440.0",
                None,
                "{'formId': 'FORM', 'formClasses': ['search-form'], 'elements': [{'name': 's', 'value': 'south dallas resource map', 'nodeName': 'INPUT', 'type': 'search'}]}",
                None,
                "2022-11-18 16:08:43.527000+00:00",
                "43",
                "95d7ea22-76f9-4f34-8c6c-9197cbb4e9d4",
                None,
                None,
                "e52fd234-77a3-416d-92c3-f9692af4a34b",
                "3265.0",
                SiteName.DALLAS_FREE_PRESS,
            ],
        ],
        columns=[
            FieldSnowplow.DVCE_SCREENHEIGHT,
            FieldSnowplow.USERAGENT,
            FieldSnowplow.NETWORK_USERID,
            FieldSnowplow.PAGE_REFERRER,
            FieldSnowplow.EVENT_NAME,
            FieldSnowplow.REFR_SOURCE,
            FieldSnowplow.PAGE_URLPATH,
            FieldSnowplow.DVCE_SCREENWIDTH,
            FieldSnowplow.SEMISTRUCT_FORM_CHANGE,
            FieldSnowplow.SEMISTRUCT_FORM_SUBMIT,
            FieldSnowplow.PP_YOFFSET_MAX,
            FieldSnowplow.DERIVED_TSTAMP,
            FieldSnowplow.DOMAIN_SESSIONIDX,
            FieldSnowplow.EVENT_ID,
            FieldSnowplow.REFR_MEDIUM,
            FieldSnowplow.SEMISTRUCT_FORM_FOCUS,
            FieldSnowplow.DOMAIN_USERID,
            FieldSnowplow.DOC_HEIGHT,
            FieldNew.SITE_NAME,
        ],
    )

    df = ConvertFieldTypes(
        fields_int={FieldSnowplow.DOMAIN_SESSIONIDX},
        fields_float={
            FieldSnowplow.DOC_HEIGHT,
            FieldSnowplow.DVCE_SCREENHEIGHT,
            FieldSnowplow.DVCE_SCREENWIDTH,
            FieldSnowplow.PP_YOFFSET_MAX,
        },
        fields_datetime={FieldSnowplow.DERIVED_TSTAMP},
        fields_categorical={FieldSnowplow.EVENT_NAME, FieldSnowplow.REFR_MEDIUM, FieldSnowplow.REFR_SOURCE},
    )(df)

    return df


@pytest.fixture(scope="module")
def df_duplicate_key(df) -> pd.DataFrame:
    df = df.copy()
    # Change last event's ID to that of the first. Since both events have the same
    # site nam, the last one will fail key-uniqueness check
    df.loc[df.shape[0] - 1, FieldSnowplow.EVENT_ID] = df.loc[0, FieldSnowplow.EVENT_ID]
    return df


@pytest.fixture(scope="module")
def db_name() -> str:
    # Assuming the local DB name is "postgres"
    return "postgres"


@pytest.fixture(autouse=True, scope="module")
def engine(db_name) -> Engine:
    return create_engine(get_conn_string(db_name))


@pytest.fixture(scope="module")
def Session(engine) -> sessionmaker:
    return sessionmaker(engine)


# ---------- HELPERS ----------
@contextmanager
def create_and_drop_tables(engine: Engine):
    """
    Context manager to safely create and drop tables before and after each test.
    """
    SQLModel.metadata.create_all(engine)
    # The try-finally block ensures tables are still dropped in the event of an exception
    # (see: https://realpython.com/python-with-statement/#opening-files-for-writing-second-version)
    try:
        yield
    finally:
        SQLModel.metadata.drop_all(engine)


# ---------- TESTS ----------
@pytest.mark.integration
def test_write_events(df, engine, Session) -> None:
    with create_and_drop_tables(engine):
        # Write mock events to mock DB
        write_events(df, Session)

        # Assert all rows were written
        with Session() as session, session.begin():
            assert session.query(Event).count() == df.shape[0]


@pytest.mark.integration
def test_write_events_duplicate_key(df_duplicate_key, engine, Session) -> None:
    num_unique_keys = df_duplicate_key.groupby([FieldSnowplow.EVENT_ID, FieldNew.SITE_NAME]).ngroups

    with create_and_drop_tables(engine):
        # Write all but the last event to DB. They have unique keys so should all go through
        write_events(df_duplicate_key.iloc[:-1], Session)

        # Write last event to DB. Its composite key already exists, so it should not go through
        num_rows_written = write_events(df_duplicate_key.iloc[[-1]], Session)
        assert num_rows_written == 0

        # Assert all rows except the last one were written
        with Session() as session, session.begin():
            assert session.query(Event).count() == num_unique_keys
