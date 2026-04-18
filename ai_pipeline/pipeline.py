import logging
from typing import Any

import pandas as pd

from stages.ingest import load_data
from stages.clean import clean_data
from stages.analyze import analyze_data
from stages.ai_insights import get_structured_insights
from stages.output import save_output
from config import INPUT_FILE, OUTPUT_DIR


logger = logging.getLogger(__name__)


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lookup = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate in lookup:
            return lookup[candidate]
    return None


def _build_columns_info(df: pd.DataFrame) -> str:
    numeric_columns = df.select_dtypes(include=["number"]).columns.tolist()
    datetime_columns = df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns.tolist()
    categorical_columns = [
        column for column in df.columns if column not in numeric_columns and column not in datetime_columns
    ]

    revenue_column = _find_column(df, ["revenue", "sales", "amount", "total", "total_sales"])
    product_column = _find_column(df, ["product", "item", "sku", "product_name"])
    region_column = _find_column(df, ["region", "area", "zone", "market"])
    date_column = _find_column(df, ["date", "order_date", "transaction_date", "timestamp"])

    schema = {
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "datetime_columns": datetime_columns,
        "revenue_column": revenue_column,
        "product_column": product_column,
        "region_column": region_column,
        "date_column": date_column,
        "column_names": df.columns.tolist(),
        "dtypes": {column: str(dtype) for column, dtype in df.dtypes.items()},
    }

    return pd.Series(schema).to_json(indent=2)


def run_ingestion_stage(input_file: str = INPUT_FILE):
    logger.info("[Stage: Ingestion] Loading source data.")
    return load_data(input_file)


def run_processing_stage(df):
    logger.info("[Stage: Processing] Cleaning data.")
    return clean_data(df)


def run_ai_stage(df):
    logger.info("[Stage: AI] Building summary and generating insights.")
    summary = analyze_data(df)
    columns_info = _build_columns_info(df)
    insights = get_structured_insights(summary, columns_info)
    return summary, insights


def run_output_stage(insights: Any):
    logger.info("[Stage: Output] Persisting AI insights.")
    return save_output(insights, OUTPUT_DIR)


def run_pipeline_with_file(input_file: str = INPUT_FILE) -> dict[str, Any]:
    logger.info("--- Pipeline started ---")
    raw_df = run_ingestion_stage(input_file)
    cleaned_df = run_processing_stage(raw_df)
    summary, insights = run_ai_stage(cleaned_df)
    output_path = run_output_stage(insights)
    logger.info("--- Pipeline complete --- Saved at %s", output_path)
    return {
        "success": True,
        "rows_loaded": len(raw_df),
        "rows_after_cleaning": len(cleaned_df),
        "summary": summary,
        "insights": insights,
        "output_path": output_path,
    }


def run_pipeline_from_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    logger.info("--- Pipeline started ---")
    logger.info("[Stage: Ingestion] Received uploaded DataFrame with %s rows.", len(df))
    cleaned_df = run_processing_stage(df)
    summary, insights = run_ai_stage(cleaned_df)
    output_path = run_output_stage(insights)
    logger.info("--- Pipeline complete --- Saved at %s", output_path)
    return {
        "success": True,
        "rows_loaded": len(df),
        "rows_after_cleaning": len(cleaned_df),
        "summary": summary,
        "insights": insights,
        "output_path": output_path,
    }


def run_pipeline() -> bool:
    logger.info("--- Pipeline started ---")
    try:
        run_pipeline_with_file(INPUT_FILE)
        return True
    except Exception:
        logger.exception("Pipeline failed.")
        return False
