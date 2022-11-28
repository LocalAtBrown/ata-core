from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Set

import pandas as pd

from src.helpers.fields import FieldSnowplow
from src.helpers.logging import logging

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

    fields_int: Set[FieldSnowplow]
    fields_float: Set[FieldSnowplow]
    fields_datetime: Set[FieldSnowplow]
    fields_categorical: Set[FieldSnowplow]

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
