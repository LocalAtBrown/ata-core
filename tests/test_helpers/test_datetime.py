from datetime import datetime

from src.helpers.datetime import convert_to_s3_folder


def test_convert_to_s3_folder() -> None:
    assert convert_to_s3_folder(datetime(2022, 10, 26, 20)) == "2022/10/26/20"
