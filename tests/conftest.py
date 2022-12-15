from typing import List, Union

import pytest

from ata_pipeline0.helpers.fields import FieldNew, FieldSnowplow
from ata_pipeline0.helpers.preprocessors import ConvertFieldTypes


@pytest.fixture(scope="package")
def all_fields() -> List[Union[FieldSnowplow, FieldNew]]:
    return [
        FieldSnowplow.DERIVED_TSTAMP,
        FieldSnowplow.DOC_HEIGHT,
        FieldSnowplow.DOMAIN_SESSIONIDX,
        FieldSnowplow.DOMAIN_USERID,
        FieldSnowplow.DVCE_SCREENHEIGHT,
        FieldSnowplow.DVCE_SCREENWIDTH,
        FieldSnowplow.EVENT_ID,
        FieldSnowplow.EVENT_NAME,
        FieldSnowplow.NETWORK_USERID,
        FieldSnowplow.PAGE_REFERRER,
        FieldSnowplow.PAGE_URLPATH,
        FieldSnowplow.PP_YOFFSET_MAX,
        FieldSnowplow.REFR_MEDIUM,
        FieldSnowplow.REFR_SOURCE,
        FieldNew.SITE_NAME,
        FieldSnowplow.SEMISTRUCT_FORM_CHANGE,
        FieldSnowplow.SEMISTRUCT_FORM_FOCUS,
        FieldSnowplow.SEMISTRUCT_FORM_SUBMIT,
        FieldSnowplow.USERAGENT,
    ]


@pytest.fixture(scope="package")
def preprocessor_convert_all_field_types() -> ConvertFieldTypes:
    return ConvertFieldTypes(
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


@pytest.fixture(scope="package")
def dummy_email() -> str:
    return "dummy@dummydomain.com"
