from enum import Enum, auto
from typing import Any


class _StrEnum(str, Enum):
    """
    StrEnum class. Replace with built-in version after upgrading to Python 3.10.
    """

    @staticmethod
    def _generate_next_value_(name: str, *args: Any, **kwargs: Any) -> str:
        return name.lower()

    def __str__(self) -> str:
        return f"{self.value}"


class FieldSnowplow(_StrEnum):
    """
    Enum for Snowplow fields of interest.
    Snowplow documentation of these fields can be found here: https://docs.snowplow.io/docs/understanding-your-pipeline/canonical-event/.

    This class also subclasses from str so that, e.g., Field.DERIVED_TSTAMP == "derived_tstamp",
    which means we can pass Field.DERIVED_TSTAMP into a pandas DataFrame directly without having
    to grab its string value (Field.DERIVED_TSTAMP.value) first. (See use case as well as caveats:
    https://stackoverflow.com/questions/58608361/string-based-enum-in-python.)
    """

    # [FLOAT] Browser viewport height in pixels
    BR_VIEWHEIGHT = auto()

    # [FLOAT] Browser viewport width in pixels
    BR_VIEWWIDTH = auto()

    # [DATETIME] Timestamp making allowance for innaccurate device clock
    DERIVED_TSTAMP = auto()

    # [FLOAT] The page's height in pixels
    DOC_HEIGHT = auto()

    # [INT] Number of the current user session, e.g. first session is 1, next session is 2, etc. Dependent on domain_userid
    DOMAIN_SESSIONIDX = auto()

    # [STR, CATEGORICAL if needed] User ID set by Snowplow using 1st party cookie
    DOMAIN_USERID = auto()

    # [FLOAT] Screen height in pixels. Almost 1-to-1 relationship with domain_userid (there are exceptions)
    DVCE_SCREENHEIGHT = auto()

    # [FLOAT] Screen width in pixels. Almost 1-to-1 relationship with domain_userid (there are exceptions)
    DVCE_SCREENWIDTH = auto()

    # [STR] ID of event. This would be the primary key within the site DataFrame,
    # and part of the [site_name, event_id] composite key in the database table
    EVENT_ID = auto()

    # [STR, CATEGORICAL] Name of event. Can be "page_view", "page_ping", "focus_form", "change_form", "submit_form"
    EVENT_NAME = auto()

    # [STR] URL of the referrer
    PAGE_REFERRER = auto()

    # [STR] URL fragment of page, e.g., about in https://dallasfreepress.com/#about
    PAGE_URLFRAGMENT = auto()

    # [STR, CATEGORICAL if needed] Path to page, e.g., /event-directory/ in https://dallasfreepress.com/event-directory/
    PAGE_URLPATH = auto()

    # [STR] Query string of page URL, e.g., ?utm_source=google&utm_medium=cpc&utm_campaign=brand
    PAGE_URLQUERY = auto()

    # [FLOAT] Maximum page y-offset seen in the last ping period. Depends on event_name == "page_ping"
    PP_YOFFSET_MAX = auto()

    # [STR, CATEGORICAL] Type of referer. Can be "social", "search", "internal", "unknown", "email"
    # (read: https://docs.snowplow.io/docs/enriching-your-data/available-enrichments/referrer-parser-enrichment/)
    REFR_MEDIUM = auto()

    # [STR, CATEGORICAL] Name of referer if recognised, e.g., "Google" or "Bing"
    REFR_SOURCE = auto()

    # [STR] Referrer URL host
    REFR_URLHOST = auto()

    # [STR] Referrer URL fragment
    REFR_URLFRAGMENT = auto()

    # [STR] Referrer URL path
    REFR_URLPATH = auto()

    # [STR] Referrer URL query string
    REFR_URLQUERY = auto()

    # [STR (JSON)] Data/attributes of HTML form and all its inputs in JSON format. Only present if event_name == "submit_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    SEMISTRUCT_FORM_SUBMIT = "unstruct_event_com_snowplowanalytics_snowplow_submit_form_1"

    # [STR] Raw useragent
    USERAGENT = auto()


class FieldNew(_StrEnum):
    """
    Enum for non-Snowplow fields to be added.
    """

    # [STR] Site partner's name (as a slug corresponding to its S3 bucket)
    SITE_NAME = auto()
