import pytest


@pytest.fixture(scope="package")
def dummy_email() -> str:
    return "dummy@dummydomain.com"
