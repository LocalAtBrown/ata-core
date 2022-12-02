import json
from datetime import datetime
from typing import List

import boto3
from ata_db_models.helpers import get_conn_string
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.fetch_events import fetch_events
from src.helpers.fields import FieldNew, FieldSnowplow
from src.helpers.preprocessors import (
    AddFieldSiteName,
    ConvertFieldTypes,
    DeleteRowsDuplicateKey,
    DeleteRowsEmpty,
    SelectFieldsRelevant,
)
from src.helpers.site import SiteName
from src.preprocess_events import preprocess_events
from src.write_events import write_events


def handler(event, context):
    # Note: this is invoked by an event-driven, async method (s3 trigger) so the return value is discarded
    print("request: {}".format(json.dumps(event)))
    return {"hi": "there"}


def run_pipeline(site_name: SiteName, timestamps: List[datetime], concurrency: int = 4):
    # Fetch from S3
    s3 = boto3.resource("s3")
    df = fetch_events(s3, site_name, timestamps, concurrency)

    # Preprocess
    df = preprocess_events(
        df,
        preprocessors=[
            SelectFieldsRelevant(fields_relevant={*FieldSnowplow}),
            DeleteRowsEmpty(
                fields_required={
                    FieldSnowplow.DERIVED_TSTAMP,
                    FieldSnowplow.DOC_HEIGHT,
                    FieldSnowplow.DOMAIN_SESSIONIDX,
                    FieldSnowplow.DOMAIN_USERID,
                    FieldSnowplow.DVCE_SCREENHEIGHT,
                    FieldSnowplow.DVCE_SCREENWIDTH,
                    FieldSnowplow.EVENT_ID,
                    FieldSnowplow.EVENT_NAME,
                    FieldSnowplow.NETWORK_USERID,
                    FieldSnowplow.PAGE_URLPATH,
                }
            ),
            ConvertFieldTypes(
                fields_int={FieldSnowplow.DOMAIN_SESSIONIDX},
                fields_float={
                    FieldSnowplow.DOC_HEIGHT,
                    FieldSnowplow.DVCE_SCREENHEIGHT,
                    FieldSnowplow.DVCE_SCREENWIDTH,
                    FieldSnowplow.PP_YOFFSET_MAX,
                },
                fields_datetime={FieldSnowplow.DERIVED_TSTAMP},
                fields_categorical={FieldSnowplow.EVENT_NAME, FieldSnowplow.REFR_MEDIUM, FieldSnowplow.REFR_SOURCE},
            ),
            DeleteRowsDuplicateKey(
                field_primary_key=FieldSnowplow.EVENT_ID, field_timestamp=FieldSnowplow.DERIVED_TSTAMP
            ),
            AddFieldSiteName(site_name, field_site_name=FieldNew.SITE_NAME),
        ],
    )

    # Write to DB
    engine = create_engine(get_conn_string())
    session_factory = sessionmaker(engine)

    write_events(df, session_factory)
