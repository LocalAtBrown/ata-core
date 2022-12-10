import functools
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional, cast

import pandas as pd

from ata_pipeline0.helpers.fields import FieldSnowplow


# ---------- SITE NAMES ----------
class SiteName(str, Enum):
    """
    Enum of partner slugs corresponding to the S3 buckets
    """

    AFRO_LA = "afro-la"
    DALLAS_FREE_PRESS = "dallas-free-press"
    OPEN_VALLEJO = "open-vallejo"
    THE_19TH = "the-19th"


# ---------- SITE FORM-SUBMISSION EVENT UTILITIES ----------
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


class SiteNewsletterFormVerifier(ABC):
    """
    Base class storing common newsletter-form-submission verifiers across all of our
    partners.
    """

    @staticmethod
    @functools.cache
    def parse_dict(data: dict) -> FormSubmitData:
        """
        Creates a form-data dataclass from a corresponding dict.
        """
        return FormSubmitData(
            form_id=data["formId"], form_classes=data["formClasses"], elements=[FormElement(*e) for e in data["elements"]]
        )

    @staticmethod
    def has_nonempty_data(event: pd.Series) -> bool:
        """
        Checks if a form-submission event actually has form HTML data.
        """
        # Should only be either dict or None because we'll perform this check
        # after the ConvertFieldTypes and ReplaceNaNs preprocessors
        form_data_raw = cast(Optional[dict], event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return form_data_raw is not None

    def has_email_input(self, event: pd.Series) -> bool:
        """
        Checks if the HTML form of a form-submission event has an `<input type="email">`
        element, which is the case in all of our partners' newsletter forms.
        """
        form_data = self.parse_dict(event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return any([e.node_name == "INPUT" and e.type == "email" for e in form_data.elements])

    @property
    def verifiers(self) -> List[Callable[[pd.Series], bool]]:
        """
        List of individual verifiers used to check if a form-submission event is of a newsletter form.
        It's supposed (but not required) to be extended (or superseded) by child classes of `SiteNewsletterFormVerifier`.
        """
        return [self.has_nonempty_data, self.has_email_input]

    def verify(self, event: pd.Series) -> bool:
        """
        Main verification method.

        Checks if a form-submission event is of a newsletter form using a pre-specified
        list of individual verifiers. If one verifier fails, it automatically fails.
        """
        return all([verify(event) for verify in self.verifiers])


class AfroLANewsletterFormVerifier(SiteNewsletterFormVerifier):
    """
    Newsletter-form-submission verification logic for AfroLA.
    """

    @staticmethod
    def has_correct_urlpath(event: pd.Series) -> bool:
        """
        Checks if the URL path where the form submission happens is correct.
        """
        return event[FieldSnowplow.PAGE_URLPATH] == "/subscribe"

    @property
    def verifiers(self) -> List[Callable[[pd.Series], bool]]:
        return [*super().verifiers, self.has_correct_urlpath]


# ---------- SITE CLASSES ----------
@dataclass
class Site(ABC):
    """
    Base site class.
    """

    name: SiteName
    newsletter_form_verifier: SiteNewsletterFormVerifier


class AfroLA(Site):
    """
    AfroLA site class.
    """

    name = SiteName.AFRO_LA
    newsletter_form_verifier = AfroLANewsletterFormVerifier()
