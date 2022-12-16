from enum import Enum


class EventName(str, Enum):
    """
    Enum for Snowplow event names.
    """

    PAGE_VIEW = "page_view"
    PAGE_PING = "page_ping"
    FOCUS_FORM = "focus_form"
    CHANGE_FORM = "change_form"
    SUBMIT_FORM = "submit_form"
