import logging

import pandas as pd


logger = logging.getLogger(__name__)


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lookup = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate in lookup:
            return lookup[candidate]
    return None


def _build_business_summary(df: pd.DataFrame) -> str:
    revenue_col = _find_column(df, ["revenue", "sales", "amount", "total", "total_sales"])
    region_col = _find_column(df, ["region", "area", "zone", "market"])
    product_col = _find_column(df, ["product", "item", "sku", "product_name"])

    lines = [f"Rows analyzed: {len(df)}"]

    if revenue_col is not None:
        revenue_series = pd.to_numeric(df[revenue_col], errors="coerce")
        total_revenue = float(revenue_series.sum(skipna=True))
        lines.append(f"Total revenue: {total_revenue:.2f}")
        lines.append(f"Average revenue per row: {float(revenue_series.mean(skipna=True)):.2f}")

        if region_col is not None:
            grouped = (
                df.assign(_revenue=revenue_series)
                .groupby(region_col, dropna=False)["_revenue"]
                .sum()
                .sort_values(ascending=False)
            )
            if not grouped.empty and total_revenue > 0:
                top_region = grouped.index[0]
                top_value = float(grouped.iloc[0])
                top_share = (top_value / total_revenue) * 100
                lines.append(
                    f"Top region by revenue: {top_region} ({top_value:.2f}, {top_share:.2f}% of total revenue)"
                )

    if product_col is not None:
        if revenue_col is not None:
            revenue_series = pd.to_numeric(df[revenue_col], errors="coerce")
            product_grouped = (
                df.assign(_revenue=revenue_series)
                .groupby(product_col, dropna=False)["_revenue"]
                .sum()
                .sort_values(ascending=False)
            )
            if not product_grouped.empty:
                best_product = product_grouped.index[0]
                lines.append(f"Best product by revenue: {best_product} ({float(product_grouped.iloc[0]):.2f})")
        else:
            counts = df[product_col].value_counts(dropna=False)
            if not counts.empty:
                lines.append(f"Most frequent product: {counts.index[0]} ({int(counts.iloc[0])} records)")

    return "\n".join(lines)


def analyze_data(df: pd.DataFrame) -> str:
    try:
        if df.empty:
            logger.warning("[AI] Dataset is empty after processing.")
            return "Dataset is empty after cleaning. No statistics available."

        business_summary = _build_business_summary(df)
        summary = df.describe(include="all").to_string()
        preview = df.head(3).to_string(index=False)
        logger.info("[AI] Summary generated for model input.")
        return (
            f"Business Summary:\n{business_summary}\n\n"
            f"Dataset Summary:\n{summary}\n\n"
            f"Sample Rows:\n{preview}"
        )
    except Exception as e:
        logger.exception("[AI] Failed to generate summary.")
        raise RuntimeError(f"Summary generation failed: {e}") from e
