import functools
from abc import ABC
from dataclasses import dataclass
from typing import Callable, List, Optional, cast

import pandas as pd

from ata_pipeline0.helpers.fields import FieldSnowplow


# ---------- SITE FORM-SUBMISSION EVENT UTILITIES ----------
@dataclass
class FormElement:
    """
    JSON data schema of an element as part of Snowplow form-submission event data
    (see: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    """

    # These constraints are looser than what the schema requires. If we need
    # them to be stricter, consider pydantic.
    name: str
    node_name: str
    value: Optional[str] = None
    type: Optional[str] = None


@dataclass
class FormSubmitData:
    """
    JSON data schema of a Snowplow form-submission event
    (see: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    """

    # These constraints are looser than what the schema requires. If we need
    # them to be stricter, consider pydantic.
    form_id: str
    form_classes: List[str]
    elements: List[FormElement]


@functools.cache
def parse_form_submit_dict(data: dict) -> FormSubmitData:
    """
    Creates a dataclass from a corresponding `dict` of form-submission data.
    """
    return FormSubmitData(
        form_id=data["formId"], form_classes=data["formClasses"], elements=[FormElement(*e) for e in data["elements"]]
    )


# ---------- SITE NEWSLETTER-FORM-SUBMISSION validatorS ----------
class SiteNewsletterSubmissionValidator(ABC):
    """
    Base class storing common newsletter-form-submission validators across all of our
    partners.
    """

    @staticmethod
    def has_nonempty_data(event: pd.Series) -> bool:
        """
        Checks if a form-submission event actually has form HTML data.
        """
        # Should only be either dict or None because we'll perform this check
        # after the ConvertFieldTypes and ReplaceNaNs preprocessors
        form_data_raw = cast(Optional[dict], event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return form_data_raw is not None

    @staticmethod
    def has_email_input(event: pd.Series) -> bool:
        """
        Checks if the HTML form of a form-submission event has an `<input type="email">`
        element, which is the case in all of our partners' newsletter forms.
        """
        form_data = parse_form_submit_dict(event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return any([e.node_name == "INPUT" and e.type == "email" for e in form_data.elements])

    @property
    def validators(self) -> List[Callable[[pd.Series], bool]]:
        """
        List of individual validators used to check if a form-submission event is of a newsletter form.
        It's supposed (but not required) to be extended (or superseded) by child classes of `SiteNewsletterFormValidator`.
        """
        return [self.has_nonempty_data, self.has_email_input]

    def validate(self, event: pd.Series) -> bool:
        """
        Main verification method.

        Checks if a form-submission event is of a newsletter form using a pre-specified
        list of individual validators. If one validator fails, it automatically fails.
        """
        return all([validate(event) for validate in self.validators])


class AfroLaNewsletterSubmissionValidator(SiteNewsletterSubmissionValidator):
    """
    Newsletter-form-submission validation logic for AfroLA.
    """

    @staticmethod
    def has_correct_urlpath(event: pd.Series) -> bool:
        """
        Checks if the URL path where the form submission happens is correct.
        """
        return event[FieldSnowplow.PAGE_URLPATH] == "/subscribe"

    @property
    def validators(self) -> List[Callable[[pd.Series], bool]]:
        return [*super().validators, self.has_correct_urlpath]
