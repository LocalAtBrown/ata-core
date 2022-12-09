from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

import numpy as np
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
class FormSubmitData:
    form_id: str
    form_classes: List[str]
    elements: List[FormElement]


@dataclass
class FormElement:
    name: str
    node_name: str
    value: Optional[str] = None
    type: Optional[str] = None


@dataclass
class Site(ABC):
    name: SiteName

    @staticmethod
    def parse_form_submit_data(data: dict) -> FormSubmitData:
        return FormSubmitData(
            form_id=data["formId"], form_classes=data["formClasses"], elements=[FormElement(*e) for e in data["elements"]]
        )

    @abstractmethod
    def check_form_submit_is_newsletter(self, event: pd.Series) -> bool:
        form_data_raw = event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT]

        # If form data is missing/null, not a newsletter
        if not form_data_raw or np.isnan(form_data_raw):
            return False

        form_data = self.parse_form_submit_data(form_data_raw)

        # If form
        if not any([e.node_name == "INPUT" and e.type == "email" for e in form_data.elements]):
            return False

        return True
