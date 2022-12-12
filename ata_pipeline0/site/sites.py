from abc import ABC
from dataclasses import dataclass

from ata_pipeline0.site.names import SiteName
from ata_pipeline0.site.newsletter import (
    AfroLaNewsletterSubmissionValidator,
    SiteNewsletterSubmissionValidator,
)


# ---------- SITE CLASSES ----------
@dataclass
class Site(ABC):
    """
    Base site class.
    """

    name: SiteName
    newsletter_submission_validator: SiteNewsletterSubmissionValidator


class AfroLA(Site):
    """
    AfroLA site class.
    """

    name = SiteName.AFRO_LA
    newsletter_submission_validator = AfroLaNewsletterSubmissionValidator()
