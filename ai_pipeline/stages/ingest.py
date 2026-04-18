import logging

import pandas as pd


logger = logging.getLogger(__name__)


def load_data(filepath: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath)
        logger.info("[Ingest] Loaded %s rows, %s columns from %s.", len(df), len(df.columns), filepath)
        return df
    except Exception as e:
        logger.exception("[Ingest] Failed to load file: %s", filepath)
        raise RuntimeError(f"Ingestion failed: {e}") from e
