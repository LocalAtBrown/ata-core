from abc import ABC
from dataclasses import dataclass

from ata_pipeline0.site.names import SiteName
from ata_pipeline0.site.newsletter import (
    AfroLaNewsletterSignupValidator,
    DallasFreePressNewsletterSignupValidator,
    OpenVallejoNewsletterSignupValidator,
    SiteNewsletterSignupValidator,
    The19thNewsletterSignupValidator,
)


# ---------- SITE CLASSES ----------
@dataclass
class Site(ABC):
    """
    Base site class.
    """

    name: SiteName
    newsletter_signup_validator: SiteNewsletterSignupValidator


class AfroLa(Site):
    """
    AfroLA site class.
    """

    name = SiteName.AFRO_LA
    newsletter_signup_validator = AfroLaNewsletterSignupValidator()


class DallasFreePress(Site):
    """
    Dallas Free Press site class.
    """

    name = SiteName.DALLAS_FREE_PRESS
    newsletter_signup_validator = DallasFreePressNewsletterSignupValidator()


class OpenVallejo(Site):
    """
    OpenVallejo site class.
    """

    name = SiteName.OPEN_VALLEJO
    newsletter_signup_validator = OpenVallejoNewsletterSignupValidator()


class The19th(Site):
    """
    The 19th site class.
    """

    name = SiteName.THE_19TH
    newsletter_signup_validator = The19thNewsletterSignupValidator()
