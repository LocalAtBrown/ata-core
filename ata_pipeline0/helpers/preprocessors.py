import ast
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Set

import numpy as np
import pandas as pd

from ata_pipeline0.helpers.fields import FieldNew, FieldSnowplow
from ata_pipeline0.helpers.logging import logging
from ata_pipeline0.helpers.site import SiteName

logger = logging.getLogger(__name__)


class Preprocessor(ABC):
    """
    Base preprocessor abstract class. Its children should be dataclasses storing
    variables needed for the specific transformation.
    """

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calls an instance of a child class as if it's a (preprocessing) function.
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
        Logs useful post-transformation messages.
        """
        pass


@dataclass
class SelectFieldsRelevant(Preprocessor):
    """
    Select relevant fields from an events DataFrame. If a field doesn't exist,
    it'll be added to the result DataFrame as an empty column.
    """

    fields_relevant: Set[FieldSnowplow]

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

    fields_required: Set[FieldSnowplow]

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

    field_primary_key: FieldSnowplow
    field_timestamp: FieldSnowplow

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Sort values by timestamp so the first event kept is the earliest,
        # which is most likely to be a parent (if its key doesn't already exist
        # in the DB)
        # (see: https://snowplow.io/blog/dealing-with-duplicate-event-ids/)
        df = df.sort_values(self.field_timestamp)
        return df.drop_duplicates(subset=[self.field_primary_key], keep="first")

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info(
            f"Deleted {df_in.shape[0] - df_out.shape[0]} rows with duplicate {self.field_primary_key} from staged DataFrame"
        )


@dataclass
class ConvertFieldTypes(Preprocessor):

    """
    Changes data types in a Snowplow events DataFrame to those desired.
    """

    fields_int: Set[FieldSnowplow]
    fields_float: Set[FieldSnowplow]
    fields_datetime: Set[FieldSnowplow]
    fields_categorical: Set[FieldSnowplow]
    fields_json: Set[FieldSnowplow]

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

        # df = df.replace([np.nan], [None])

        for field in self.fields_json:
            df[field] = df[field].apply(self._convert_to_json)
        return df

    @staticmethod
    def _convert_to_json(value: str) -> Dict:
        try:
            # if valid json, will convert to a dictionary
            return ast.literal_eval(value)
        except ValueError:
            # if invalid, will throw a ValueError and we just want it to return None
            return None  # type: ignore

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Converted field data types")


@dataclass
class AddFieldSiteName(Preprocessor):
    """
    Adds a constant field holding partner's name to the Snowplow events DataFrame.
    """

    site_name: SiteName
    field_site_name: FieldNew

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Make a copy of the original so that it's not affected, but can remove
        # this if memory is an issue
        df = df.copy()

        df[self.field_site_name] = self.site_name

        return df

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info(f"Added site name {self.site_name} as a new field")


@dataclass
class ReplaceNaNs(Preprocessor):
    """
    Replaces all `np.nan` instances in the DataFrame with a specified alternative.
    """

    replace_with: Any

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace([np.nan], [self.replace_with])

        return df

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info(f"Replaced all NaNs with {self.replace_with}")
