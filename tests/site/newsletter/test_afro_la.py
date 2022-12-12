import pandas as pd
import pytest

from ata_pipeline0.helpers.fields import FieldNew, FieldSnowplow
from ata_pipeline0.helpers.preprocessors import ConvertFieldTypes
from ata_pipeline0.site.newsletter import AfroLaNewsletterSubmissionValidator


@pytest.fixture(scope="class")
def nsv() -> AfroLaNewsletterSubmissionValidator:
    return AfroLaNewsletterSubmissionValidator()


@pytest.fixture(scope="class")
def event() -> pd.Series:
    df = pd.DataFrame(
        [
            [
                "https://www.afrolanews.org/",
                "afro-la",
                "2022-11-03 05:39:13.16",
                "697",
                "4",
                "7292d425-2424-4b00-9894-b31a87a770bd",
                "768",
                "1366",
                "2bba4051-c7f9-46cd-90bc-9b869a5fe187",
                "submit_form",
                "156f6713-4722-49cc-8335-721742f66525",
                "/subscribe",
                None,
                "unknown",
                None,
                None,
                None,
                "{'formId': 'FORM', 'formClasses': ['group', 'w-full', 'rounded-wt', 'bg-transparent', 'shadow-none', 'sm:shadow-md'], 'elements': [{'name': 'ref', 'value': '', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'redirect_path', 'value': '/', 'nodeName': 'INPUT', 'type': 'hidden'}, {'name': 'double_opt', 'value': 'true', 'nodeName': 'INPUT', 'type': 'hidden'}, {'name': 'origin', 'value': '/subscribe', 'nodeName': 'INPUT', 'type': 'hidden'}, {'name': 'visit_token', 'value': '004abfd4-ea3e-4246-87a4-83d0e153d383', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'email', 'value': 'dummyemail@dummydomain.com', 'nodeName': 'INPUT', 'type': 'email'}]}",
                "Mozilla/5.0 (X11; CrOS x86_64 14816.131.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
            ]
        ],
        columns=[
            FieldSnowplow.PAGE_REFERRER,
            FieldNew.SITE_NAME,
            FieldSnowplow.DERIVED_TSTAMP,
            FieldSnowplow.DOC_HEIGHT,
            FieldSnowplow.DOMAIN_SESSIONIDX,
            FieldSnowplow.DOMAIN_USERID,
            FieldSnowplow.DVCE_SCREENHEIGHT,
            FieldSnowplow.DVCE_SCREENWIDTH,
            FieldSnowplow.EVENT_ID,
            FieldSnowplow.EVENT_NAME,
            FieldSnowplow.NETWORK_USERID,
            FieldSnowplow.PAGE_URLPATH,
            FieldSnowplow.PP_YOFFSET_MAX,
            FieldSnowplow.REFR_MEDIUM,
            FieldSnowplow.REFR_SOURCE,
            FieldSnowplow.SEMISTRUCT_FORM_CHANGE,
            FieldSnowplow.SEMISTRUCT_FORM_FOCUS,
            FieldSnowplow.SEMISTRUCT_FORM_SUBMIT,
            FieldSnowplow.USERAGENT,
        ],
    )

    # TODO: Make preprocessors like these site class attributes so that we don't
    # have to do this all the time
    preprocessor = ConvertFieldTypes(
        fields_int={FieldSnowplow.DOMAIN_SESSIONIDX},
        fields_float={
            FieldSnowplow.DOC_HEIGHT,
            FieldSnowplow.DVCE_SCREENHEIGHT,
            FieldSnowplow.DVCE_SCREENWIDTH,
            FieldSnowplow.PP_YOFFSET_MAX,
        },
        fields_datetime={FieldSnowplow.DERIVED_TSTAMP},
        fields_categorical={FieldSnowplow.EVENT_NAME, FieldSnowplow.REFR_MEDIUM, FieldSnowplow.REFR_SOURCE},
        fields_json={
            FieldSnowplow.SEMISTRUCT_FORM_CHANGE,
            FieldSnowplow.SEMISTRUCT_FORM_FOCUS,
            FieldSnowplow.SEMISTRUCT_FORM_SUBMIT,
        },
    )

    return preprocessor(df).iloc[0]


@pytest.mark.unit
class TestAfroLaNewsletterSubmissionValidators:
    def test_has_nonemtpy_data(self, nsv, event) -> None:
        assert nsv.has_nonempty_data(event)

    def test_has_email_input(self, nsv, event) -> None:
        assert nsv.has_email_input(event)

    def test_has_correct_urlpath(self, nsv, event) -> None:
        assert nsv.has_correct_urlpath(event)
