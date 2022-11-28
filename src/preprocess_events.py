from typing import List

import pandas as pd

from src.helpers.logging import logging
from src.helpers.preprocessors import Preprocessor

logger = logging.getLogger(__name__)


def preprocess_events(df: pd.DataFrame, preprocessors: List[Preprocessor]) -> pd.DataFrame:
    """
    Main Snowplow events DataFrame preprocessing function, taking in a list of
    predefined preprocessors as above. Since the output DataFrame of one preprocessor
    becomes the input DataFrame of the one after it, the order of preprocessors matters.
    """
    for preprocessor in preprocessors:
        df = preprocessor(df)

    logger.info(f"Preprocessed DataFrame shape: {df.shape}")
    return df
