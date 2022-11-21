from typing import Set

import pandas as pd
import pytest
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_ns_dtype,
    is_float_dtype,
    is_int64_dtype,
)

from src.preprocess_events import (
    Field,
    _convert_field_types,
    _delete_rows_duplicate_key,
    _delete_rows_empty,
    _select_fields_relevant,
)


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
            Field.EVENT_ID,
            Field.DOMAIN_SESSIONIDX,
            Field.DOC_HEIGHT,
            Field.PP_YOFFSET_MAX,
            Field.DERIVED_TSTAMP,
            Field.EVENT_NAME,
        ],
    )


@pytest.fixture(scope="module")
def fields_relevant() -> Set[Field]:
    return {Field.EVENT_ID, Field.DERIVED_TSTAMP, Field.PAGE_URLPATH}


@pytest.fixture(scope="module")
def fields_required() -> Set[Field]:
    # Field.Event_NAME isn't included in the dummy DataFrame, but it should
    # still be included in the output DataFrame as an empty column
    return {Field.EVENT_ID, Field.DOC_HEIGHT, Field.DERIVED_TSTAMP, Field.EVENT_NAME}


@pytest.fixture(scope="module")
def field_primary_key() -> Field:
    return Field.EVENT_ID


@pytest.fixture(scope="module")
def fields_int() -> Set[Field]:
    return {Field.DOMAIN_SESSIONIDX}


@pytest.fixture(scope="module")
def fields_float() -> Set[Field]:
    return {Field.DOC_HEIGHT, Field.PP_YOFFSET_MAX}


@pytest.fixture(scope="module")
def fields_datetime() -> Set[Field]:
    return {Field.DERIVED_TSTAMP}


@pytest.fixture(scope="module")
def fields_categorical() -> Set[Field]:
    return {Field.EVENT_NAME}


# ---------- TESTS ----------
def test__select_fields_relevant(df, fields_relevant) -> None:
    df = _select_fields_relevant(df, fields_relevant)
    assert set(df.columns) == fields_relevant


def test__delete_rows_empty(df, fields_required) -> None:
    df = _delete_rows_empty(df, fields_required)
    # doc_height is not required, so the first row is off the hook
    assert df.shape[0] == 2
    # isna() should return False for all cells under required fields;
    # these false values sum up to 0
    assert df[[*fields_required]].isna().to_numpy().sum() == 0


def test__delete_rows_duplicate_key(df, field_primary_key) -> None:
    df = _delete_rows_duplicate_key(df, field_primary_key)
    # First 2 rows should be removed
    assert df.shape[0] == 2
    # Check primary key uniqueness
    assert df[field_primary_key].is_unique


def test__convert_field_types(df, fields_int, fields_float, fields_datetime, fields_categorical) -> None:
    df = _convert_field_types(df, fields_int, fields_float, fields_datetime, fields_categorical)

    for f in fields_int:
        assert is_int64_dtype(df[f])

    for f in fields_float:
        assert is_float_dtype(df[f])

    for f in fields_datetime:
        assert is_datetime64_ns_dtype(df[f])

    for f in fields_categorical:
        assert is_categorical_dtype(df[f])
