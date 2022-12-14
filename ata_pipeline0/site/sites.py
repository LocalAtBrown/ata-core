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
class AfroLa:
    """
    AfroLA site class.
    """

    name: SiteName = SiteName.AFRO_LA
    newsletter_signup_validator: SiteNewsletterSignupValidator = AfroLaNewsletterSignupValidator()


@dataclass
class DallasFreePress:
    """
    Dallas Free Press site class.
    """

    name: SiteName = SiteName.DALLAS_FREE_PRESS
    newsletter_signup_validator: SiteNewsletterSignupValidator = DallasFreePressNewsletterSignupValidator()


@dataclass
class OpenVallejo:
    """
    OpenVallejo site class.
    """

    name: SiteName = SiteName.OPEN_VALLEJO
    newsletter_signup_validator: SiteNewsletterSignupValidator = OpenVallejoNewsletterSignupValidator()


@dataclass
class The19th:
    """
    The 19th site class.
    """

    name: SiteName = SiteName.THE_19TH
    newsletter_signup_validator: SiteNewsletterSignupValidator = The19thNewsletterSignupValidator()


SITES = {
    SiteName.AFRO_LA: AfroLa(),
    SiteName.DALLAS_FREE_PRESS: DallasFreePress(),
    SiteName.OPEN_VALLEJO: OpenVallejo(),
    SiteName.THE_19TH: The19th(),
}
