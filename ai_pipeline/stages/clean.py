import logging

import pandas as pd


logger = logging.getLogger(__name__)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        before = len(df)

        # Normalize string columns to avoid accidental mismatches from whitespace.
        object_columns = df.select_dtypes(include=["object"]).columns
        for column in object_columns:
            df[column] = df[column].astype(str).str.strip()

        df.columns = df.columns.str.strip()
        df = df.dropna()
        df = df.drop_duplicates()

        removed = before - len(df)
        logger.info("[Processing] Removed %s rows. %s rows remain.", removed, len(df))
        return df
    except Exception as e:
        logger.exception("[Processing] Cleaning failed.")
        raise RuntimeError(f"Processing failed: {e}") from e
