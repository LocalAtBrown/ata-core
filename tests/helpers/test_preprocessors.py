from typing import Set

import numpy as np
import pandas as pd
import pytest
import user_agents as ua
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_ns_dtype,
    is_float_dtype,
    is_int64_dtype,
)

from ata_pipeline0.helpers.events import EventName
from ata_pipeline0.helpers.fields import FieldNew, FieldSnowplow
from ata_pipeline0.helpers.preprocessors import (
    AddFieldFormSubmitIsNewsletter,
    AddFieldSiteName,
    ConvertFieldTypes,
    DeleteRowsBot,
    DeleteRowsDuplicateKey,
    DeleteRowsEmpty,
    ReplaceNaNs,
    SelectFields,
)
from ata_pipeline0.site.names import SiteName
from ata_pipeline0.site.newsletter import SiteNewsletterSignupValidator


@pytest.mark.unit
class TestAddFieldFormSubmitIsNewsletter:
    """
    Unit tests for the `AddFieldFormSubmitIsNewsletter` preprocessor.

    These only test the logic of the class's transformation method itself. Refer
    to the tests in `tests/site/test_newsletter.py` for unit tests of site-specific
    newsletter-signup validation logic.
    """

    # ---------- FIXTURES ---------
    @pytest.fixture(scope="class")
    def df(self, dummy_email) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                [EventName.PAGE_PING, None],
                [
                    EventName.SUBMIT_FORM,
                    "{'formId': '', 'formClasses': [], 'elements': [{'name': 'email', 'value': '%s', 'nodeName': 'INPUT', 'type': 'email'}]}"
                    % dummy_email,
                ],
                [EventName.SUBMIT_FORM, "{'formId': '', 'formClasses': [], 'elements': []}"],
            ],
            columns=[FieldSnowplow.EVENT_NAME, FieldSnowplow.SEMISTRUCT_FORM_SUBMIT],
        )
        return ConvertFieldTypes(
            fields_int=set(),
            fields_float=set(),
            fields_datetime=set(),
            fields_categorical=set(),
            fields_json={FieldSnowplow.SEMISTRUCT_FORM_SUBMIT},
        )(df)

    class DummyNsv(SiteNewsletterSignupValidator):
        def validate(self, event: pd.Series) -> bool:
            return super().validate(event)

    @pytest.fixture(scope="class")
    def site_newsletter_signup_validator(self) -> SiteNewsletterSignupValidator:
        return self.DummyNsv()

    @pytest.fixture(scope="class")
    def field_event_name(self) -> FieldSnowplow:
        return FieldSnowplow.EVENT_NAME

    @pytest.fixture(scope="class")
    def field_form_submit_is_newsletter(self) -> FieldNew:
        return FieldNew.FORM_SUBMIT_IS_NEWSLETTER

    # ---------- TESTS ----------
    def test(self, df, site_newsletter_signup_validator, field_event_name, field_form_submit_is_newsletter) -> None:
        df = AddFieldFormSubmitIsNewsletter(
            site_newsletter_signup_validator, field_event_name, field_form_submit_is_newsletter
        )(df)

        assert df[field_form_submit_is_newsletter].equals(pd.Series([None, True, False]))


@pytest.mark.unit
class TestAddFieldSiteName:
    """
    Unit tests for the `AddFieldSiteName` preprocessor.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def df(self) -> pd.DataFrame:
        return pd.DataFrame()

    @pytest.fixture(scope="class")
    def site_name(self) -> SiteName:
        return SiteName.AFRO_LA

    @pytest.fixture(scope="class")
    def field_site_name(self) -> FieldNew:
        return FieldNew.SITE_NAME

    # ---------- TESTS ----------
    def test(self, df, site_name, field_site_name) -> None:
        df = AddFieldSiteName(site_name, field_site_name)(df)
        # pd.Series.all returns True if all of its boolean values are True
        assert (df[field_site_name] == site_name).all()


@pytest.mark.unit
class TestConvertFieldTypes:
    """
    Unit tests for the `ConvertFieldTypes` preprocessor.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [
                    "1",
                    "1500",
                    None,
                    "2022-11-02T00:00:01.051Z",
                    EventName.PAGE_PING,
                    None,
                ],
                [
                    "1",
                    None,
                    "400",
                    "2022-11-01T00:00:01.051Z",
                    EventName.SUBMIT_FORM,
                    "{'field': 'value'}",
                ],
                [
                    "2",
                    "2000",
                    "200",
                    "2022-12-01T00:00:01.051Z",
                    EventName.FOCUS_FORM,
                    None,
                ],
                [
                    "1",
                    "1500",
                    "400",
                    None,
                    None,
                    None,
                ],
            ],
            columns=[
                FieldSnowplow.DOMAIN_SESSIONIDX,
                FieldSnowplow.DOC_HEIGHT,
                FieldSnowplow.PP_YOFFSET_MAX,
                FieldSnowplow.DERIVED_TSTAMP,
                FieldSnowplow.EVENT_NAME,
                FieldSnowplow.SEMISTRUCT_FORM_FOCUS,
            ],
        )

    @pytest.fixture(scope="class")
    def fields_int(self) -> Set[FieldSnowplow]:
        return {FieldSnowplow.DOMAIN_SESSIONIDX}

    @pytest.fixture(scope="class")
    def fields_float(self) -> Set[FieldSnowplow]:
        return {FieldSnowplow.DOC_HEIGHT, FieldSnowplow.PP_YOFFSET_MAX}

    @pytest.fixture(scope="class")
    def fields_datetime(self) -> Set[FieldSnowplow]:
        return {FieldSnowplow.DERIVED_TSTAMP}

    @pytest.fixture(scope="class")
    def fields_categorical(self) -> Set[FieldSnowplow]:
        return {FieldSnowplow.EVENT_NAME}

    @pytest.fixture(scope="class")
    def fields_json(self) -> Set[FieldSnowplow]:
        return {FieldSnowplow.SEMISTRUCT_FORM_FOCUS}

    # ---------- TESTS ----------
    def test(self, df, fields_int, fields_float, fields_datetime, fields_categorical, fields_json) -> None:
        df = ConvertFieldTypes(fields_int, fields_float, fields_datetime, fields_categorical, fields_json)(df)

        for f in fields_int:
            assert is_int64_dtype(df[f])

        for f in fields_float:
            assert is_float_dtype(df[f])

        for f in fields_datetime:
            assert is_datetime64_ns_dtype(df[f])

        for f in fields_categorical:
            assert is_categorical_dtype(df[f])

        for f in fields_json:
            assert df[f].apply(lambda x: x is None or isinstance(x, dict)).sum() == df.shape[0]


@pytest.mark.unit
class TestDeleteRowsBot:
    """
    Unit tests for the `DeleteRowsBot` preprocessor.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [
                    "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.5249.119 Mobile Safari/537.36 (compatible; Googlebot/2.1;  http://www.google.com/bot.html)",  # this is a bot
                ],
                [
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",  # this is NOT a bot
                ],
                [
                    "Mozilla/5.0 (Linux; Android 11; 5087Z Build/RP1A.200720.011; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/107.0.5304.105 Mobile Safari/537.36",  # this is NOT a bot
                ],
                [
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",  # this is NOT a bot
                ],
            ],
            columns=[
                FieldSnowplow.USERAGENT,
            ],
        )

    @pytest.fixture(scope="class")
    def field_useragent(self) -> FieldSnowplow:
        return FieldSnowplow.USERAGENT

    # ---------- TESTS ----------
    def test(self, df, field_useragent) -> None:
        df = DeleteRowsBot(field_useragent)(df)
        # Only first row should be removed
        assert df.shape[0] == 3
        # All other rows should be non-bot events
        assert df[field_useragent].apply(lambda x: ua.parse(x).is_bot).sum() == 0


@pytest.mark.unit
class TestDeleteRowsDuplicateKey:
    """
    Unit tests for the `DeleteRowsDuplicateKey` preprocessor.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def key_duplicate(self) -> str:
        return "A0"

    @pytest.fixture(scope="class")
    def field_primary_key(self) -> FieldSnowplow:
        return FieldSnowplow.EVENT_ID

    @pytest.fixture(scope="class")
    def field_timestamp(self) -> FieldSnowplow:
        return FieldSnowplow.DERIVED_TSTAMP

    @pytest.fixture(scope="class")
    def df(self, key_duplicate, field_timestamp) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                [key_duplicate, "2022-11-02T00:00:01.051Z"],
                [key_duplicate, "2022-11-01T00:00:01.051Z"],
                ["B1", "2022-12-01T00:00:01.051Z"],
                ["C2", "2022-12-12T22:00:01.051Z"],
            ],
            columns=[FieldSnowplow.EVENT_ID, FieldSnowplow.DERIVED_TSTAMP],
        )
        return ConvertFieldTypes(
            fields_int=set(),
            fields_float=set(),
            fields_datetime={field_timestamp},
            fields_categorical=set(),
            fields_json=set(),
        )(df)

    # ---------- TESTS ----------
    def test(self, df, field_primary_key, field_timestamp, key_duplicate) -> None:
        duplicate_timestamp_min = df[df[field_primary_key] == key_duplicate][field_timestamp].min()

        df = DeleteRowsDuplicateKey(field_primary_key, field_timestamp)(df)

        # 1 row (second row) should be removed
        assert df.shape[0] == 3

        # Remaining event with duplicated key should have the earliest timestamp
        # so that it's most likely to be an actual/parent event
        assert df[df[field_primary_key] == key_duplicate][field_timestamp].shape[0] == 1
        deduped_timestamp = df[df[field_primary_key] == key_duplicate][field_timestamp].iloc[0]
        assert deduped_timestamp == duplicate_timestamp_min

        # Check primary key uniqueness
        assert df[field_primary_key].is_unique


@pytest.mark.unit
class TestDeleteRowsEmpty:
    """
    Unit tests for the `DeleteRowsEmpty` preprocessor.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [
                    "1",
                    "1500",
                    None,
                    "2022-11-02T00:00:01.051Z",
                    EventName.PAGE_PING,
                ],
                [
                    "1",
                    None,
                    "400",
                    "2022-11-01T00:00:01.051Z",
                    EventName.SUBMIT_FORM,
                ],
                [
                    "2",
                    "2000",
                    "200",
                    "2022-12-01T00:00:01.051Z",
                    EventName.FOCUS_FORM,
                ],
                [
                    "1",
                    "1500",
                    "400",
                    None,
                    None,
                ],
            ],
            columns=[
                FieldSnowplow.DOMAIN_SESSIONIDX,
                FieldSnowplow.DOC_HEIGHT,
                FieldSnowplow.PP_YOFFSET_MAX,
                FieldSnowplow.DERIVED_TSTAMP,
                FieldSnowplow.EVENT_NAME,
            ],
        )

    @pytest.fixture(scope="class")
    def fields_required(self) -> Set[FieldSnowplow]:
        return {FieldSnowplow.DOC_HEIGHT, FieldSnowplow.DERIVED_TSTAMP, FieldSnowplow.EVENT_NAME}

    # ---------- TESTS ----------
    def test(self, df, fields_required) -> None:
        df = DeleteRowsEmpty(fields_required)(df)
        # pp_yoffset_max is not required, so the first row is off the hook
        assert df.shape[0] == 2
        # isna() should return False for all cells under required fields;
        # these false values sum up to 0
        assert df[[*fields_required]].isna().to_numpy().sum() == 0


@pytest.mark.unit
class TestReplaceNaNs:
    """
    Unit tests for the `TestReplaceNaNs` preprocessor.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [[np.nan, EventName.PAGE_PING], [np.nan, np.nan]], columns=[FieldSnowplow.EVENT_ID, FieldSnowplow.EVENT_NAME]
        )

    @pytest.fixture(scope="class")
    def replace_with(self) -> str:
        return "woo"

    # ---------- TESTS ----------
    def test(self, df, replace_with) -> None:
        df = ReplaceNaNs(replace_with)(df)
        df_check = df.dropna()
        assert df.shape == df_check.shape


@pytest.mark.unit
class TestSelectFields:
    """
    Unit tests for the `SelectFields` preprocessor.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [
                    "2022-11-02T00:00:01.051Z",
                    EventName.PAGE_PING,
                    None,
                ]
            ],
            columns=[FieldSnowplow.DERIVED_TSTAMP, FieldSnowplow.EVENT_NAME, FieldSnowplow.SEMISTRUCT_FORM_SUBMIT],
        )

    @pytest.fixture(scope="class")
    def fields(self) -> Set[FieldSnowplow]:
        return {FieldSnowplow.DERIVED_TSTAMP, FieldSnowplow.EVENT_NAME}

    # ---------- TESTS ----------
    def test(self, df, fields) -> None:
        df = SelectFields(fields)(df)
        assert set(df.columns) == fields
