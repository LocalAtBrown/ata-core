import pandas as pd
import pytest

from ata_pipeline0.helpers.fields import FieldSnowplow
from ata_pipeline0.site.names import SiteName
from ata_pipeline0.site.newsletter import (
    AfroLaNewsletterSignupValidator,
    DallasFreePressNewsletterSignupValidator,
    OpenVallejoNewsletterSignupValidator,
    SiteNewsletterSignupValidator,
    The19thNewsletterSignupValidator,
)


@pytest.fixture(scope="module")
def dummy_email() -> str:
    return "dummy@dummydomain.com"


@pytest.mark.unit
class TestSite:
    """
    Unit tests for base site validators.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def event(site, all_fields, preprocessor_convert_all_field_types, dummy_email) -> pd.Series:
        df = pd.DataFrame(
            [
                [
                    "2022-11-03 05:39:13.16",
                    "697",
                    "4",
                    "7292d425-2424-4b00-9894-b31a87a770bd",
                    "768",
                    "1366",
                    "2bba4051-c7f9-46cd-90bc-9b869a5fe187",
                    "submit_form",
                    "156f6713-4722-49cc-8335-721742f66525",
                    "https://www.afrolanews.org/",
                    "/subscribe",
                    None,
                    "unknown",
                    None,
                    SiteName.AFRO_LA,
                    None,
                    None,
                    "{'formId': 'FORM', 'formClasses': ['group', 'w-full', 'rounded-wt', 'bg-transparent', 'shadow-none', 'sm:shadow-md'], 'elements': [{'name': 'ref', 'value': '', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'redirect_path', 'value': '/', 'nodeName': 'INPUT', 'type': 'hidden'}, {'name': 'double_opt', 'value': 'true', 'nodeName': 'INPUT', 'type': 'hidden'}, {'name': 'origin', 'value': '/subscribe', 'nodeName': 'INPUT', 'type': 'hidden'}, {'name': 'visit_token', 'value': '004abfd4-ea3e-4246-87a4-83d0e153d383', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'email', 'value': '%s', 'nodeName': 'INPUT', 'type': 'email'}]}"
                    % dummy_email,
                    "Mozilla/5.0 (X11; CrOS x86_64 14816.131.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
                ]
            ],
            columns=all_fields,
        )

        return preprocessor_convert_all_field_types(df).iloc[0]

    # ---------- TESTS ----------
    def test_has_data_true(self, event) -> None:
        assert SiteNewsletterSignupValidator.has_data(event)

    def test_has_data_false(self, event) -> None:
        event = event.copy()
        event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT] = None
        assert SiteNewsletterSignupValidator.has_data(event) is False

    def test_has_email_input_true(self, event) -> None:
        assert SiteNewsletterSignupValidator.has_email_input(event)

    def test_has_email_input_false(self, event, dummy_email) -> None:
        event = event.copy()
        event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT] = {
            "formId": "",
            "formClasses": [],
            "elements": [
                {
                    "name": "email",
                    "value": dummy_email,
                    "nodeName": "INPUT",
                    "type": "text",  # must be "type": "email"
                }
            ],
        }
        assert SiteNewsletterSignupValidator.has_email_input(event) is False


@pytest.mark.unit
class TestAfroLa:
    """
    Unit tests for AfroLA validators.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def nsv(self) -> AfroLaNewsletterSignupValidator:
        return AfroLaNewsletterSignupValidator()

    @pytest.fixture(scope="class")
    def event(self, all_fields, preprocessor_convert_all_field_types, dummy_email) -> pd.Series:
        df = pd.DataFrame(
            [
                [
                    "2022-11-03 05:39:13.16",
                    "697",
                    "4",
                    "7292d425-2424-4b00-9894-b31a87a770bd",
                    "768",
                    "1366",
                    "2bba4051-c7f9-46cd-90bc-9b869a5fe187",
                    "submit_form",
                    "156f6713-4722-49cc-8335-721742f66525",
                    "https://www.afrolanews.org/",
                    "/subscribe",
                    None,
                    "unknown",
                    None,
                    SiteName.AFRO_LA,
                    None,
                    None,
                    "{'formId': 'FORM', 'formClasses': ['group', 'w-full', 'rounded-wt', 'bg-transparent', 'shadow-none', 'sm:shadow-md'], 'elements': [{'name': 'ref', 'value': '', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'redirect_path', 'value': '/', 'nodeName': 'INPUT', 'type': 'hidden'}, {'name': 'double_opt', 'value': 'true', 'nodeName': 'INPUT', 'type': 'hidden'}, {'name': 'origin', 'value': '/subscribe', 'nodeName': 'INPUT', 'type': 'hidden'}, {'name': 'visit_token', 'value': '004abfd4-ea3e-4246-87a4-83d0e153d383', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'email', 'value': '%s', 'nodeName': 'INPUT', 'type': 'email'}]}"
                    % dummy_email,
                    "Mozilla/5.0 (X11; CrOS x86_64 14816.131.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
                ]
            ],
            columns=all_fields,
        )

        return preprocessor_convert_all_field_types(df).iloc[0]

    # ---------- TESTS ----------
    def test_is_in_newsletter_page_true(self, nsv, event) -> None:
        assert nsv.is_in_newsletter_page(event)

    def test_is_in_newsletter_page_false(self, nsv, event) -> None:
        event = event.copy()
        event[FieldSnowplow.PAGE_URLPATH] = "/"
        assert nsv.is_in_newsletter_page(event) is False


@pytest.mark.unit
class TestDallasFreePress:
    """
    Unit tests for DFP validators.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def nsv(self) -> DallasFreePressNewsletterSignupValidator:
        return DallasFreePressNewsletterSignupValidator()

    @pytest.fixture(scope="class")
    def event(self, all_fields, preprocessor_convert_all_field_types, dummy_email) -> pd.Series:
        df = pd.DataFrame(
            [
                [
                    "2022-11-04 15:33:57.85",
                    "1502",
                    "1",
                    "a1ee0745-f471-4695-8b3d-a2320587f5ed",
                    "800",
                    "1280",
                    "8c62f083-539a-4158-bddd-ec83271639cd",
                    "submit_form",
                    "957e4dc1-2754-4213-949b-d5928d446911",
                    "https://dallasfreepress.com/dallas-news/dallas-forgot-remember-reclaim-lost-black-schools-education-history/",
                    "/text-and-email-notifications/",
                    None,
                    "internal",
                    None,
                    SiteName.DALLAS_FREE_PRESS,
                    None,
                    None,
                    "{'formId': 'mc-embedded-subscribe-form', 'formClasses': ['validate'], 'elements': [{'name': 'EMAIL', 'value': '%s', 'nodeName': 'INPUT', 'type': 'email'}, {'name': 'FNAME', 'value': 'Libby', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'LNAME', 'value': 'Daniels', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'MMERGE3', 'value': '75231', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'MMERGE4', 'value': '9729253923', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'group[8368][1]', 'value': '1', 'nodeName': 'INPUT', 'type': 'checkbox'}, {'name': 'group[8368][2]', 'value': '2', 'nodeName': 'INPUT', 'type': 'checkbox'}, {'name': 'group[8368][4]', 'value': None, 'nodeName': 'INPUT', 'type': 'checkbox'}, {'name': 'b_6cbf3c038f5cc4d279f4da4ed_37f4ad3cfe', 'value': '', 'nodeName': 'INPUT', 'type': 'text'}]}"
                    % dummy_email,
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
                ]
            ],
            columns=all_fields,
        )

        return preprocessor_convert_all_field_types(df).iloc[0]

    # ---------- TESTS ----------
    def test_is_newsletter_inline_form_true(self, nsv, event) -> None:
        assert nsv.is_newsletter_inline_form(event)

    def test_is_newsletter_inline_form_false(self, nsv, event, dummy_email) -> None:
        event = event.copy()
        event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT] = {
            "formId": "dummy-id",  # must be "mc-embedded-subscribe-form"
            "formClasses": [],
            "elements": [
                {
                    "name": "email",
                    "value": dummy_email,
                    "nodeName": "INPUT",
                    "type": "email",
                }
            ],
        }
        assert nsv.is_newsletter_inline_form(event) is False


@pytest.mark.unit
class TestOpenVallejoNewsletterSignupValidators:
    """
    Unit tests for OpenVallejo validators.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def nsv(self) -> OpenVallejoNewsletterSignupValidator:
        return OpenVallejoNewsletterSignupValidator()

    @pytest.fixture(scope="class")
    def event_inline(self, all_fields, preprocessor_convert_all_field_types, dummy_email) -> pd.Series:
        df = pd.DataFrame(
            [
                [
                    "2022-11-04 17:04:17.234",
                    "4284",
                    "2",
                    "f7a02cc7-e9f1-40bc-99ca-9d1aad111070",
                    "693",
                    "320",
                    "dc2295e6-7e64-4f17-9df3-53466045eac9",
                    "submit_form",
                    "173295ed-93ac-4504-8dd8-d880f8d2c74e",
                    "https://openvallejo.org/donate/?mc_cid=396eb8ce49&mc_eid=3327a7bf29&utm_campaign=396eb8ce49-Vallejo+patrol+staffing+story_COPY_01&utm_medium=email&utm_source=Open+Vallejo&utm_term=0_5c634b5220-396eb8ce49-600953573",
                    "/newsletter/",
                    None,
                    "internal",
                    None,
                    SiteName.OPEN_VALLEJO,
                    None,
                    None,
                    "{'formId': 'mc-embedded-subscribe-form', 'formClasses': ['validate'], 'elements': [{'name': 'EMAIL', 'value': '%s', 'nodeName': 'INPUT', 'type': 'email'}, {'name': 'b_625f546ae539f2396949b95f4_5c634b5220', 'value': '', 'nodeName': 'INPUT', 'type': 'text'}]}"
                    % dummy_email,
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6,2 Mobile/15E148 Safari/604.1",
                ]
            ],
            columns=all_fields,
        )

        return preprocessor_convert_all_field_types(df).iloc[0]

    @pytest.fixture(scope="class")
    def event_popup(self) -> pd.Series:
        # TODO once we have data for this kind of form in S3
        pass

    # ---------- TESTS ----------
    def test_is_newsletter_inline_form_true(self, nsv, event_inline) -> None:
        assert nsv.is_newsletter_inline_form(event_inline)

    def test_is_newsletter_inline_form_false(self, nsv, event_inline, dummy_email) -> None:
        event_inline = event_inline.copy()
        event_inline[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT] = {
            "formId": "dummy-id",  # must include the "mc-embedded-subscribe-form" string
            "formClasses": [],
            "elements": [
                {
                    "name": "email",
                    "value": dummy_email,
                    "nodeName": "INPUT",
                    "type": "email",
                }
            ],
        }
        assert nsv.is_newsletter_inline_form(event_inline) is False

    def test_is_newsletter_popup_form_true(self) -> None:
        # TODO once we have data for this kind of form in S3
        pass

    def test_is_newsletter_popup_form_false(self) -> None:
        # TODO once we have data for this kind of form in S3
        pass


@pytest.mark.unit
class TestThe19thNewsletterSignupValidators:
    """
    Unit tests for The 19th validators.
    """

    # ---------- FIXTURES ----------
    @pytest.fixture(scope="class")
    def nsv(self) -> The19thNewsletterSignupValidator:
        return The19thNewsletterSignupValidator()

    @pytest.fixture(scope="class")
    def event(self, all_fields, preprocessor_convert_all_field_types, dummy_email) -> pd.Series:
        df = pd.DataFrame(
            [
                [
                    "2022-11-07 17:31:18.216",
                    "6991",
                    "1",
                    "7434e1bd-3539-42fa-948d-96801d807476",
                    "1024",
                    "768",
                    "4d768aef-addc-4a3a-a133-1ea7fbd184f1",
                    "submit_form",
                    "b800111b-fe37-4f49-b0c9-61fbbcc5ff4a",
                    "https://www.google.com/",
                    "/2022/06/mayra-flores-record-women-congress/",
                    None,
                    "search",
                    "Google",
                    SiteName.THE_19TH,
                    None,
                    None,
                    "{'formId': 'newsletter-form-block_62b24d43296c5', 'formClasses': ['newsletter-form', 'align', 'js-newsletter-form'], 'elements': [{'name': 'EMAIL', 'value': '%s', 'nodeName': 'INPUT', 'type': 'email'}, {'name': 'subscribe-confirmation-block_62b24d43296c5', 'value': 'on', 'nodeName': 'INPUT', 'type': 'checkbox'}, {'name': 'NEWSLETTER', 'value': 'e35d7d0333', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'group[70588][1]', 'value': '1', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'b_8c4d626920c5131bb82226529_a35c3279be', 'value': '', 'nodeName': 'INPUT', 'type': 'text'}]}"
                    % dummy_email,
                    "Mozilla/5.0 (iPad; CPU OS 11_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.0 Mobile/15E148 Safari/604.1",
                ]
            ],
            columns=all_fields,
        )

        return preprocessor_convert_all_field_types(df).iloc[0]

    # ---------- TESTS ----------
    def test_is_newsletter_form_true(self, nsv, event) -> None:
        assert nsv.is_newsletter_form(event)

    def test_is_newsletter_form_false(self, nsv, event, dummy_email) -> None:
        event = event.copy()
        event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT] = {
            "formId": "dummy-id",  # must include the "newsletter" string
            "formClasses": [],
            "elements": [
                {
                    "name": "email",
                    "value": dummy_email,
                    "nodeName": "INPUT",
                    "type": "email",
                }
            ],
        }
        assert nsv.is_newsletter_form(event) is False
