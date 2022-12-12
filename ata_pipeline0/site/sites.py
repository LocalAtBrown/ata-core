from abc import ABC
from dataclasses import dataclass

from ata_pipeline0.site.names import SiteName
from ata_pipeline0.site.newsletter import (
    AfroLaNewsletterSubmissionValidator,
    DallasFreePressNewsletterSubmissionValidator,
    OpenVallejoNewsletterSubmissionValidator,
    SiteNewsletterSubmissionValidator,
    The19thNewsletterSubmissionValidator,
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


class DallasFreePress(Site):
    """
    Dallas Free Press site class.
    """

    name = SiteName.DALLAS_FREE_PRESS
    newsletter_submission_validator = DallasFreePressNewsletterSubmissionValidator()


class OpenVallejo(Site):
    """
    OpenVallejo site class.
    """

    name = SiteName.OPEN_VALLEJO
    newsletter_submission_validator = OpenVallejoNewsletterSubmissionValidator()


class The19th(Site):
    """
    The 19th site class.
    """

    name = SiteName.THE_19TH
    newsletter_submission_validator = The19thNewsletterSubmissionValidator()
