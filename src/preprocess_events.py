from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Set

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


class Preprocessor(ABC):
    """
    Base preprocessor abstract class. Its children should be dataclasses storing
    variables needed for the specific transformation.
    """

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calls an instance of a child class as if it's a (preprocessing) function
        """
        df_out = self.transform(df)
        self.log_result(df, df_out)
        return df_out

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms a Snowplow DataFrame using parameters predefined in the dataclass.
        """
        pass

    @abstractmethod
    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        """
        Logs useful post-transformation messages
        """
        pass


@dataclass
class SelectFieldsRelevant(Preprocessor):
    """
    Select relevant fields from an events DataFrame. If a field doesn't exist,
    it'll be added to the result DataFrame as an empty column.
    """

    fields_relevant: Set[Field]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Sometimes, df doesn't have all the fields in fields_relevant, so we create
        # an empty DataFrame with all the fields we'd like to have and concatenate df to it
        df_empty_with_all_fields = pd.DataFrame(columns=[*self.fields_relevant])
        # Get a list of fields in fields_relevant that are actually in df, because we
        # don't want to query for nonexistent fields and have pandas raise a KeyError
        fields_available = df.columns.intersection([*self.fields_relevant])

        # Query for fields in fields_available and perform said concatenation, so that
        # the final DataFrame will have all the fields in fields_relevant
        df = pd.concat([df_empty_with_all_fields, df[[*fields_available]]])

        return df

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Selected relevant fields")


@dataclass
class DeleteRowsEmpty(Preprocessor):
    """
    Given a list of fields that cannot have empty or null data, remove all rows
    with null values in any of these fields.
    """

    fields_required: Set[Field]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(subset=[*self.fields_required])

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info(
            f"Deleted {df_in.shape[0] - df_out.shape[0]} rows with at least 1 empty cell in a required field from staged DataFrame"
        )


@dataclass
class DeleteRowsDuplicateKey(Preprocessor):
    """
    Delete all rows whose primary key is repeated in the DataFrame.
    """

    field_primary_key: Field

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=[self.field_primary_key], keep=False)

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info(
            f"Deleted {df_in.shape[0] - df_out.shape[0]} rows with duplicate {self.field_primary_key} from staged DataFrame"
        )


@dataclass
class ConvertFieldTypes(Preprocessor):

    """
    Changes data types in a Snowplow events DataFrame to those desired.
    """

    fields_int: Set[Field]
    fields_float: Set[Field]
    fields_datetime: Set[Field]
    fields_categorical: Set[Field]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Make a copy of the original so that it's not affected, but can remove
        # this if memory is an issue
        df = df.copy()

        df[[*self.fields_int]] = df[[*self.fields_int]].astype(int)
        df[[*self.fields_float]] = df[[*self.fields_float]].astype(float)

        # pd.to_datetime can only turn pandas Series to datetime, so need to convert
        # one Series/column at a time
        # All timestamps should already be in UTC: https://discourse.snowplow.io/t/what-timezones-are-the-timestamps-set-in/622,
        # but setting utc=True just to be safe
        for field in self.fields_datetime:
            df[field] = pd.to_datetime(df[field], utc=True)

        df[[*self.fields_categorical]] = df[[*self.fields_categorical]].astype("category")

        return df

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Converted field data types")


def preprocess_events(df: pd.DataFrame, preprocessors: List[Preprocessor]) -> pd.DataFrame:
    """
    Main Snowplow events DataFrame preprocessing function, taking in a list of
    predefined preprocessors as above. Since the output DataFrame of one preprocessor
    becomes the input DataFrame of the one after it, the order of preprocessors matters.
    """
    for preprocessor in preprocessors:
        df = preprocessor(df)

    logger.info(f"Preprocessed DataFrame shape: {df.shape}")
    return df
