from typing import Set

import pandas as pd
import pytest
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_ns_dtype,
    is_float_dtype,
    is_int64_dtype,
)

from src.helpers.fields import FieldNew, FieldSnowplow
from src.helpers.preprocessors import (
    AddFieldSiteName,
    ConvertFieldTypes,
    DeleteRowsDuplicateKey,
    DeleteRowsEmpty,
    SelectFieldsRelevant,
)
from src.helpers.site import SiteName


# ---------- FIXTURES ----------
@pytest.fixture(scope="module")
def df() -> pd.DataFrame:
    """
    Creates a dummy events DataFrame for testing purposes. It doesn't reflect
    real data by any means.
    """
    return pd.DataFrame(
        [
            ["A0", "1", "1500", None, "2022-11-01T00:00:01.051Z", "page_ping"],
            ["A0", "1", None, "400", "2022-11-02T00:00:01.051Z", "submit_form"],
            ["B1", "2", "2000", "200", "2022-12-01T00:00:01.051Z", "focus_form"],
            ["C2", "1", "1500", "400", None, None],
        ],
        columns=[
            FieldSnowplow.EVENT_ID,
            FieldSnowplow.DOMAIN_SESSIONIDX,
            FieldSnowplow.DOC_HEIGHT,
            FieldSnowplow.PP_YOFFSET_MAX,
            FieldSnowplow.DERIVED_TSTAMP,
            FieldSnowplow.EVENT_NAME,
        ],
    )


@pytest.fixture(scope="module")
def fields_relevant() -> Set[FieldSnowplow]:
    return {FieldSnowplow.EVENT_ID, FieldSnowplow.DERIVED_TSTAMP, FieldSnowplow.PAGE_URLPATH}


@pytest.fixture(scope="module")
def fields_required() -> Set[FieldSnowplow]:
    # FieldSnowplow.Event_NAME isn't included in the dummy DataFrame, but it should
    # still be included in the output DataFrame as an empty column
    return {FieldSnowplow.EVENT_ID, FieldSnowplow.DOC_HEIGHT, FieldSnowplow.DERIVED_TSTAMP, FieldSnowplow.EVENT_NAME}


@pytest.fixture(scope="module")
def field_primary_key() -> FieldSnowplow:
    return FieldSnowplow.EVENT_ID


@pytest.fixture(scope="module")
def fields_int() -> Set[FieldSnowplow]:
    return {FieldSnowplow.DOMAIN_SESSIONIDX}


@pytest.fixture(scope="module")
def fields_float() -> Set[FieldSnowplow]:
    return {FieldSnowplow.DOC_HEIGHT, FieldSnowplow.PP_YOFFSET_MAX}


@pytest.fixture(scope="module")
def fields_datetime() -> Set[FieldSnowplow]:
    return {FieldSnowplow.DERIVED_TSTAMP}


@pytest.fixture(scope="module")
def fields_categorical() -> Set[FieldSnowplow]:
    return {FieldSnowplow.EVENT_NAME}


@pytest.fixture(scope="module")
def site_name() -> SiteName:
    return SiteName.AFRO_LA


@pytest.fixture(scope="module")
def field_site_name() -> FieldNew:
    return FieldNew.SITE_NAME


# ---------- TESTS ----------
@pytest.mark.unit
def test_select_fields_relevant(df, fields_relevant) -> None:
    df = SelectFieldsRelevant(fields_relevant)(df)
    assert set(df.columns) == fields_relevant


@pytest.mark.unit
def test_delete_rows_empty(df, fields_required) -> None:
    df = DeleteRowsEmpty(fields_required)(df)
    # doc_height is not required, so the first row is off the hook
    assert df.shape[0] == 2
    # isna() should return False for all cells under required fields;
    # these false values sum up to 0
    assert df[[*fields_required]].isna().to_numpy().sum() == 0


@pytest.mark.unit
def test_delete_rows_duplicate_key(df, field_primary_key) -> None:
    df = DeleteRowsDuplicateKey(field_primary_key)(df)
    # First 2 rows should be removed
    assert df.shape[0] == 2
    # Check primary key uniqueness
    assert df[field_primary_key].is_unique


@pytest.mark.unit
def test_convert_field_types(df, fields_int, fields_float, fields_datetime, fields_categorical) -> None:
    df = ConvertFieldTypes(fields_int, fields_float, fields_datetime, fields_categorical)(df)

    for f in fields_int:
        assert is_int64_dtype(df[f])

    for f in fields_float:
        assert is_float_dtype(df[f])

    for f in fields_datetime:
        assert is_datetime64_ns_dtype(df[f])

    for f in fields_categorical:
        assert is_categorical_dtype(df[f])


@pytest.mark.unit
def test_add_field_site_name(df, site_name, field_site_name) -> None:
    df = AddFieldSiteName(site_name, field_site_name)(df)
    # pd.Series.all returns True if all of its boolean values are True
    assert (df[field_site_name] == site_name).all()
