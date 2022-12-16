import re
from datetime import datetime
from typing import List, Optional

import boto3
from ata_db_models.helpers import get_conn_string
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ata_pipeline0.fetch_events import fetch_events
from ata_pipeline0.helpers.fields import FieldNew, FieldSnowplow
from ata_pipeline0.helpers.preprocessors import (
    AddFieldFormSubmitIsNewsletter,
    AddFieldSiteName,
    ConvertFieldTypes,
    DeleteRowsBot,
    DeleteRowsDuplicateKey,
    DeleteRowsEmpty,
    ReplaceNaNs,
    SelectFieldsRelevant,
)
from ata_pipeline0.preprocess_events import preprocess_events
from ata_pipeline0.site.sites import SITES, Site
from ata_pipeline0.write_events import write_events


def handler(event, context):
    # Note: this is invoked by an event-driven, async method (s3 trigger) so the return value is discarded
    # see here for example event structure: https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    site = SITES[re.findall("lnl-snowplow-(.*)", bucket_name)[0]]
    object_key = event["Records"][0]["s3"]["object"]["key"]
    run_pipeline(site=site, object_key=object_key, concurrency=1)


def run_pipeline(
    site: Site,
    timestamps: Optional[List[datetime]] = None,
    object_key: Optional[str] = None,
    concurrency: int = 4,
):
    # Fetch from S3
    s3 = boto3.resource("s3")
    df = fetch_events(
        s3_resource=s3,
        site_name=site.name,
        timestamps=timestamps,
        object_key=object_key,
        num_concurrent_downloads=concurrency,
    )

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
                fields_json={
                    FieldSnowplow.SEMISTRUCT_FORM_CHANGE,
                    FieldSnowplow.SEMISTRUCT_FORM_FOCUS,
                    FieldSnowplow.SEMISTRUCT_FORM_SUBMIT,
                },
            ),
            # This happens after converting field type because timestamps need to be in datetime format
            DeleteRowsDuplicateKey(
                field_primary_key=FieldSnowplow.EVENT_ID, field_timestamp=FieldSnowplow.DERIVED_TSTAMP
            ),
            DeleteRowsBot(field_useragent=FieldSnowplow.USERAGENT),
            AddFieldSiteName(site_name=site.name, field_site_name=FieldNew.SITE_NAME),
            ReplaceNaNs(replace_with=None),
            AddFieldFormSubmitIsNewsletter(
                site_newsletter_signup_validator=site.newsletter_signup_validator,
                field_event_name=FieldSnowplow.EVENT_NAME,
                field_form_submit_is_newsletter=FieldNew.FORM_SUBMIT_IS_NEWSLETTER,
            ),
        ],
    )

    # Write to DB
    engine = create_engine(get_conn_string())
    session_factory = sessionmaker(engine)

    write_events(df, session_factory)
