from enum import Enum
from typing import Set

import pandas as pd


class Field(Enum):
    """
    Enum for Snowplow fields of interest.
    Snowplow documentation of these fields can be found here: https://docs.snowplow.io/docs/understanding-your-pipeline/canonical-event/.
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

    # [STR] ID of event. Ideally, this would be the primary key.
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
    # Can be loaded into Redshift as-is since Redshift allows inserting and querying with semistructured data
    SEMISTRUCT_FORM_CHANGE = "unstruct_event_com_snowplowanalytics_snowplow_change_form_1"

    # [STR (JSON)] Data/attributes of HTML input and its form in JSON format. Only present if event_name == "focus_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/focus_form/jsonschema/1-0-0)
    # Can be loaded into Redshift as-is since Redshift allows inserting and querying with semistructured data
    SEMISTRUCT_FORM_FOCUS = "unstruct_event_com_snowplowanalytics_snowplow_focus_form_1"

    # [STR (JSON)] Data/attributes of HTML form and all its inputs in JSON format. Only present if event_name == "submit_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    # Can be loaded into Redshift as-is since Redshift allows inserting and querying with semistructured data
    SEMISTRUCT_FORM_SUBMIT = "unstruct_event_com_snowplowanalytics_snowplow_submit_form_1"

    # [STR] Raw useragent
    USERAGENT = "useragent"


def _delete_fields(df: pd.DataFrame, fields_to_keep: Set[Field]) -> pd.DataFrame:
    """
    Remove unncessary fields from an events DataFrame.
    """
    return df[[f.value for f in fields_to_keep]]


def _delete_rows_empty(df: pd.DataFrame, fields_required: Set[Field]) -> pd.DataFrame:
    """
    Given a list of fields that cannot have empty or null data, remove all rows
    with null values in any of these fields.
    """
    return df.dropna(subset=[f.value for f in fields_required])


def _delete_rows_duplicate_key(df: pd.DataFrame, field_primary_key: Field) -> pd.DataFrame:
    """
    Delete all rows whose primary key is repeated in the DataFrame.
    """
    return df.drop_duplicates(subset=[field_primary_key.value])


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

    fields_int = [f.value for f in fields_int]
    fields_float = [f.value for f in fields_float]
    fields_datetime = [f.value for f in fields_datetime]
    fields_categorical = [f.value for f in fields_categorical]

    df[fields_int] = df[fields_int].astype(int)
    df[fields_float] = df[fields_float].astype(float)
    # pd.to_datetime can only turn pandas Series to datetime
    for field in fields_datetime:
        df[field] = pd.to_datetime(df[field])
    df[fields_categorical] = df[fields_categorical].astype("category")

    return df
