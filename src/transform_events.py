from enum import Enum
from typing import Dict, Set

import pandas as pd


class EventFields(Enum):
    """
    Enum for Snowplow fields of interest.
    Snowplow documentation of these fields can be found here: https://docs.snowplow.io/docs/understanding-your-pipeline/canonical-event/.
    """

    # Timestamp making allowance for innaccurate device clock
    DERIVED_TSTAMP = "derived_tstamp"

    # The page's height in pixels
    DOC_HEIGHT = "doc_height"

    # Number of the current user session, e.g. first session is 1, next session is 2, etc. Dependent on domain_userid
    DOMAIN_SESSIONIDX = "domain_sessionidx"

    # User ID set by Snowplow using 1st party cookie. Our primary identification key
    DOMAIN_USERID = "domain_userid"

    # Screen height in pixels. Almost 1-to-1 relationship with domain_userid (there are exceptions)
    DVCE_SCREENHEIGHT = "dvce_screenheight"

    # Screen width in pixels. Almost 1-to-1 relationship with domain_userid (there are exceptions)
    DVCE_SCREENWIDTH = "dvce_screenwidth"

    # Name of event. Can be "page_view", "page_ping", "focus_form", "change_form", "submit_form"
    EVENT_NAME = "event_name"

    # User ID set by Snowplow using 3rd party cookie
    NETWORK_USERID = "network_userid"

    # Path to page, e.g., /event-directory/ in https://dallasfreepress.com/event-directory/
    PAGE_URLPATH = "page_urlpath"

    # URL of the referrer
    PAGE_REFERRER = "page_referrer"

    # Maximum page y-offset seen in the last ping period. Depends on event_name == "page_ping"
    PP_YOFFSET_MAX = "pp_yoffset_max"

    # Type of referer. Can be "social", "search", "internal", "unknown", "email" (read: https://docs.snowplow.io/docs/enriching-your-data/available-enrichments/referrer-parser-enrichment/)
    REFR_MEDIUM = "refr_medium"

    # Name of referer if recognised, e.g., "Google" or "Bing"
    REFR_SOURCE = "refr_source"

    # Data/attributes of HTML input and its form in JSON format. Only present if event_name == "change_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/change_form/jsonschema/1-0-0)
    UNSTRUCT_FORM_CHANGE = "unstruct_event_com_snowplowanalytics_snowplow_change_form_1"

    # Data/attributes of HTML input and its form in JSON format. Only present if event_name == "focus_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/focus_form/jsonschema/1-0-0)
    UNSTRUCT_FORM_FOCUS = "unstruct_event_com_snowplowanalytics_snowplow_focus_form_1"

    # Data/attributes of HTML form and all its inputs in JSON format. Only present if event_name == "submit_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    UNSTRUCT_FORM_SUBMIT = "unstruct_event_com_snowplowanalytics_snowplow_submit_form_1"

    # Raw useragent
    USERAGENT = "useragent"


def delete_rows(df: pd.DataFrame, fields_required: Set[EventFields]) -> pd.DataFrame:
    df = df.dropna(subset=[field.value for field in fields_required])
    df = df.drop_duplicates()


def transform_fields(df: pd.DataFrame) -> pd.DataFrame:
    return df


def add_fields(df: pd.DataFrame) -> pd.DataFrame:
    return df


def split_into_tables(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    return {"hi": df}
