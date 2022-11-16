from enum import Enum
from typing import Set

import pandas as pd

from src.helpers.logging import logging

logger = logging.getLogger(__name__)


class Field(str, Enum):
    """
    Enum for Snowplow fields of interest.
    Snowplow documentation of these fields can be found here: https://docs.snowplow.io/docs/understanding-your-pipeline/canonical-event/.

    This class also subclasses from str so that, e.g., Field.DERIVED_TSTAMP == "derived_tstamp",
    which means we can pass Field.DERIVED_TSTAMP into a pandas DataFrame directly without having
    to grab its string value (Field.DERIVED_TSTAMP.value) first. (See use case as well as caveats:
    https://stackoverflow.com/questions/58608361/string-based-enum-in-python.)
    """

    # [DATETIME] Timestamp making allowance for innaccurate device clock
    DERIVED_TSTAMP = "derived_tstamp"

    # [FLOAT] The page's height in pixels
    DOC_HEIGHT = "doc_height"

    # [INT] Number of the current user session, e.g. first session is 1, next session is 2, etc. Dependent on domain_userid
    DOMAIN_SESSIONIDX = "domain_sessionidx"

    # [STR, CATEGORICAL if needed] User ID set by Snowplow using 1st party cookie
    DOMAIN_USERID = "domain_userid"

    # [FLOAT] Screen height in pixels. Almost 1-to-1 relationship with domain_userid (there are exceptions)
    DVCE_SCREENHEIGHT = "dvce_screenheight"

    # [FLOAT] Screen width in pixels. Almost 1-to-1 relationship with domain_userid (there are exceptions)
    DVCE_SCREENWIDTH = "dvce_screenwidth"

    # [STR] ID of event. This would be the primary key within the site DataFrame,
    # and part of the [site_name, event_id] composite key in the database table
    EVENT_ID = "event_id"

    # [STR, CATEGORICAL] Name of event. Can be "page_view", "page_ping", "focus_form", "change_form", "submit_form"
    EVENT_NAME = "event_name"

    # [STR, CATEGORICAL if needed] User ID set by Snowplow using 3rd party cookie
    NETWORK_USERID = "network_userid"

    # [STR, CATEGORICAL if needed] Path to page, e.g., /event-directory/ in https://dallasfreepress.com/event-directory/
    PAGE_URLPATH = "page_urlpath"

    # [STR] URL of the referrer
    PAGE_REFERRER = "page_referrer"

    # [FLOAT] Maximum page y-offset seen in the last ping period. Depends on event_name == "page_ping"
    PP_YOFFSET_MAX = "pp_yoffset_max"

    # [STR, CATEGORICAL] Type of referer. Can be "social", "search", "internal", "unknown", "email"
    # (read: https://docs.snowplow.io/docs/enriching-your-data/available-enrichments/referrer-parser-enrichment/)
    REFR_MEDIUM = "refr_medium"

    # [STR, CATEGORICAL] Name of referer if recognised, e.g., "Google" or "Bing"
    REFR_SOURCE = "refr_source"

    # [STR (JSON)] Data/attributes of HTML input and its form in JSON format. Only present if event_name == "change_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/change_form/jsonschema/1-0-0)
    SEMISTRUCT_FORM_CHANGE = "unstruct_event_com_snowplowanalytics_snowplow_change_form_1"

    # [STR (JSON)] Data/attributes of HTML input and its form in JSON format. Only present if event_name == "focus_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/focus_form/jsonschema/1-0-0)
    SEMISTRUCT_FORM_FOCUS = "unstruct_event_com_snowplowanalytics_snowplow_focus_form_1"

    # [STR (JSON)] Data/attributes of HTML form and all its inputs in JSON format. Only present if event_name == "submit_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    SEMISTRUCT_FORM_SUBMIT = "unstruct_event_com_snowplowanalytics_snowplow_submit_form_1"

    # [STR] Raw useragent
    USERAGENT = "useragent"


def preprocess_events(
    df: pd.DataFrame,
    fields_relevant: Set[Field],
    fields_required: Set[Field],
    field_primary_key: Field,
    fields_int: Set[Field],
    fields_float: Set[Field],
    fields_datetime: Set[Field],
    fields_categorical: Set[Field],
) -> pd.DataFrame:
    """
    Main Snowplow events DataFrame preprocessing function.
    """
    # Select fields we're interested in
    df = _select_fields_relevant(df, fields_relevant)
    logger.info("Selected relevant fields")

    # Delete rows with duplicate primary key
    num_rows = df.shape[0]
    df = _delete_rows_duplicate_key(df, field_primary_key)
    logger.info(f"Deleted {num_rows - df.shape[0]} rows with duplicate {field_primary_key.value} from staged DataFrame")

    # Delete rows with empty data where required
    num_rows = df.shape[0]
    df = _delete_rows_empty(df, fields_required)
    logger.info(
        f"Deleted {num_rows - df.shape[0]} rows with at least 1 empty cell in a required field from staged DataFrame"
    )

    # Convert field data types
    df = _convert_field_types(df, fields_int, fields_float, fields_datetime, fields_categorical)
    logger.info("Converted field data types")

    logger.info(f"Preprocessed DataFrame shape: {df.shape}")
    return df


def _select_fields_relevant(df: pd.DataFrame, fields_relevant: Set[Field]) -> pd.DataFrame:
    """
    Select relevant fields from an events DataFrame. If a field doesn't exist,
    it'll be added to the result DataFrame as an empty column.
    """
    # Sometimes, df doesn't have all the fields in fields_relevant, so we create
    # an empty DataFrame with all the fields we'd like to have and concatenate df to it
    df_empty_with_all_fields = pd.DataFrame(columns=[*fields_relevant])
    # Get a list of fields in fields_relevant that are actually in df, because we
    # don't want to query for nonexistent fields and have pandas raise a KeyError
    fields_available = df.columns.intersection([*fields_relevant])

    # Query for fields in fields_available and perform said concatenation, so that
    # the final DataFrame will have all the fields in fields_relevant
    return pd.concat([df_empty_with_all_fields, df[[*fields_available]]])


def _delete_rows_empty(df: pd.DataFrame, fields_required: Set[Field]) -> pd.DataFrame:
    """
    Given a list of fields that cannot have empty or null data, remove all rows
    with null values in any of these fields.
    """
    return df.dropna(subset=[*fields_required])


def _delete_rows_duplicate_key(df: pd.DataFrame, field_primary_key: Field) -> pd.DataFrame:
    """
    Delete all rows whose primary key is repeated in the DataFrame.
    """
    return df.drop_duplicates(subset=[field_primary_key], keep=False)


def _convert_field_types(
    df: pd.DataFrame,
    fields_int: Set[Field],
    fields_float: Set[Field],
    fields_datetime: Set[Field],
    fields_categorical: Set[Field],
) -> pd.DataFrame:
    """
    Changes data types in a Snowplow events DataFrame to those desired.
    """
    # Make a copy of the original so that it's not affected, but can remove
    # this if memory is an issue
    df = df.copy()

    df[[*fields_int]] = df[[*fields_int]].astype(int)
    df[[*fields_float]] = df[[*fields_float]].astype(float)

    # pd.to_datetime can only turn pandas Series to datetime, so need to convert
    # one Series/column at a time
    # All timestamps should already be in UTC: https://discourse.snowplow.io/t/what-timezones-are-the-timestamps-set-in/622,
    # but setting utc=True just to be safe
    for field in fields_datetime:
        df[field] = pd.to_datetime(df[field], utc=True)

    df[[*fields_categorical]] = df[[*fields_categorical]].astype("category")

    return df
