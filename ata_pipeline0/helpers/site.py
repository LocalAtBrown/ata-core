import functools
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional, cast

import pandas as pd

from ata_pipeline0.helpers.fields import FieldSnowplow


class SiteName(str, Enum):
    """
    Enum of partner slugs corresponding to the S3 buckets
    """

    AFRO_LA = "afro-la"
    DALLAS_FREE_PRESS = "dallas-free-press"
    OPEN_VALLEJO = "open-vallejo"
    THE_19TH = "the-19th"


@dataclass
class FormElement:
    name: str
    node_name: str
    value: Optional[str] = None
    type: Optional[str] = None


@dataclass
class FormSubmitData:
    form_id: str
    form_classes: List[str]
    elements: List[FormElement]


@dataclass
class Site(ABC):
    name: SiteName

    @staticmethod
    @functools.cache
    def parse_form_submit_data(data: dict) -> FormSubmitData:
        """
        Creates a form-data dataclass from a corresponding dict.
        """
        return FormSubmitData(
            form_id=data["formId"], form_classes=data["formClasses"], elements=[FormElement(*e) for e in data["elements"]]
        )

    @staticmethod
    def verify_newsletter_form_submit_nonempty_data(event: pd.Series) -> bool:
        """
        Checks if a form-submission event actually has form HTML data.
        """
        # Should only be either dict or None because we'll perform this check
        # after the ConvertFieldTypes and ReplaceNaNs preprocessors
        form_data_raw = cast(Optional[dict], event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return form_data_raw is not None

    def verify_newsletter_form_submit_has_email_input(self, event: pd.Series) -> bool:
        """
        Checks if the HTML form of a form-submission event has an `<input type="email">`
        element, which is the case in all of our partners' newsletter forms.
        """
        form_data = self.parse_form_submit_data(event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return any([e.node_name == "INPUT" and e.type == "email" for e in form_data.elements])

    @property
    def newsletter_form_submit_verifiers(self) -> List[Callable[[pd.Series], bool]]:
        """
        List of verifiers used to check if a form-submission event is of a newsletter form.
        It's supposed (but not required) to be extended (or superseded) by child classes of `Site`.
        """
        return [self.verify_newsletter_form_submit_nonempty_data, self.verify_newsletter_form_submit_has_email_input]

    def verify_newsletter_form_submit(self, event: pd.Series) -> bool:
        """
        Checks if a form-submission event is of a newsletter form using a pre-specified
        list of individual verifiers. If one verifier fails, it automatically fails.
        """
        return all([verify(event) for verify in self.newsletter_form_submit_verifiers])
