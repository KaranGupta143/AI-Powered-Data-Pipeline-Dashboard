import io
import json
from typing import Any

import pandas as pd
import streamlit as st

from logging_config import setup_logging
from pipeline import run_ai_stage, run_output_stage, run_processing_stage


setup_logging()

st.set_page_config(page_title="AI Data Pipeline", page_icon="📊", layout="wide")
st.markdown(
    """
    <style>
    .card {
        padding: 20px 18px;
        border-radius: 16px;
        background: linear-gradient(180deg, #111827 0%, #0b1220 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        box-shadow: 0 10px 28px rgba(0, 0, 0, 0.28);
        text-align: center;
        min-height: 132px;
    }
    .card h3 {
        margin: 0 0 10px 0;
        font-size: 0.95rem;
        font-weight: 700;
        color: #cbd5e1;
        letter-spacing: 0.02em;
    }
    .card h2 {
        margin: 0;
        font-size: 1.8rem;
        font-weight: 800;
        color: #ffffff;
    }
    .card h4 {
        margin: 0;
        font-size: 1.05rem;
        font-weight: 700;
        color: #ffffff;
        line-height: 1.35;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("AI-Powered Data Pipeline Dashboard")
st.caption("AI-powered automated data pipeline with real-time analytics and insights")


def _read_uploaded_csv(uploaded_file) -> tuple[pd.DataFrame, str]:
    file_bytes = uploaded_file.getvalue()
    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    last_error = None

    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(io.BytesIO(file_bytes), encoding=encoding)
            return df, encoding
        except UnicodeDecodeError as e:
            last_error = e
            continue

    raise ValueError(
        "Could not decode CSV with common encodings (utf-8, utf-8-sig, cp1252, latin-1). "
        f"Last error: {last_error}"
    )


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lookup = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate in lookup:
            return lookup[candidate]
    return None


def _prepare_sales_fields(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()

    sales_col = _find_column(working, ["sales", "revenue", "amount", "total", "total_sales"])
    quantity_col = _find_column(working, ["quantity", "qty", "units", "unit_sold"])
    unit_price_col = _find_column(working, ["unitprice", "unit_price", "price", "unit cost"])

    if quantity_col is not None:
        working[quantity_col] = pd.to_numeric(working[quantity_col], errors="coerce")
    if unit_price_col is not None:
        working[unit_price_col] = pd.to_numeric(working[unit_price_col], errors="coerce")

    if sales_col is None and quantity_col is not None and unit_price_col is not None:
        # Canonical sales derivation for retail-like datasets.
        working["Sales"] = working[quantity_col] * working[unit_price_col]
    elif sales_col is not None:
        working[sales_col] = pd.to_numeric(working[sales_col], errors="coerce")

    invoice_date_col = _find_column(working, ["invoicedate", "invoice_date", "date", "order_date", "transaction_date", "timestamp"])
    if invoice_date_col is not None:
        working[invoice_date_col] = pd.to_datetime(working[invoice_date_col], errors="coerce")

    return working


def get_best_columns(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    numeric = df.select_dtypes(include="number").columns.tolist()
    categorical = df.select_dtypes(include=["object", "category"]).columns.tolist()
    categorical = [col for col in categorical if df[col].nunique(dropna=True) < 50]
    return numeric, categorical


def _build_fallback_business_metrics(df: pd.DataFrame) -> dict[str, str]:
    working = _prepare_sales_fields(df)
    revenue_col = _find_column(working, ["sales", "revenue", "amount", "total", "total_sales"])
    region_col = _find_column(working, ["country", "region", "area", "zone", "market"])
    product_col = _find_column(working, ["description", "product", "item", "sku", "product_name"])

    metrics = {
        "Total Revenue": "N/A",
        "Top Region": "N/A",
        "Top Product": "N/A",
    }

    if revenue_col is not None:
        revenue = pd.to_numeric(working[revenue_col], errors="coerce")
        total_revenue = float(revenue.sum(skipna=True))
        metrics["Total Revenue"] = f"{total_revenue:,.2f}"

        if region_col is not None and total_revenue > 0:
            region_sales = working.assign(_revenue=revenue).groupby(region_col, dropna=False)["_revenue"].sum()
            if not region_sales.empty:
                top_region = region_sales.idxmax()
                share = (float(region_sales.max()) / total_revenue) * 100
                metrics["Top Region"] = f"{top_region} ({share:.1f}% share)"

        if product_col is not None:
            product_sales = working.assign(_revenue=revenue).groupby(product_col, dropna=False)["_revenue"].sum()
            if not product_sales.empty:
                top_product = product_sales.idxmax()
                metrics["Top Product"] = f"{top_product} ({float(product_sales.max()):,.2f})"
    elif product_col is not None:
        counts = working[product_col].value_counts(dropna=False)
        if not counts.empty:
            metrics["Top Product"] = f"{counts.index[0]} ({int(counts.iloc[0])} rows)"

    return metrics


def _render_metric_cards(business_summary: dict[str, Any], fallback_metrics: dict[str, str]) -> None:
    total_revenue = business_summary.get("total_revenue") or fallback_metrics["Total Revenue"]
    top_category = business_summary.get("top_category_product") or fallback_metrics["Top Product"]
    key_region = business_summary.get("key_region") or fallback_metrics["Top Region"]

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            f"<div class='card'><h3>💰 Revenue</h3><h2>{total_revenue}</h2></div>",
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f"<div class='card'><h3>🏆 Product</h3><h4>{top_category}</h4></div>",
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f"<div class='card'><h3>🌍 Region</h3><h4>{key_region}</h4></div>",
            unsafe_allow_html=True,
        )


def _render_bullet_section(title: str, items: list[Any], empty_message: str) -> None:
    st.markdown(f"### {title}")
    if items:
        for item in items:
            st.markdown(f"- {item}")
    else:
        st.caption(empty_message)


def _render_chart_config(df: pd.DataFrame, chart: dict[str, Any]) -> None:
    chart_type = str(chart.get("type", "")).lower()
    x_column = chart.get("x")
    y_column = chart.get("y")

    if not x_column or not y_column or x_column not in df.columns or y_column not in df.columns:
        return

    x_series = df[x_column]
    y_series = pd.to_numeric(df[y_column], errors="coerce")

    if chart_type in {"line", "area"} and any(token in str(x_column).lower() for token in ["date", "time", "timestamp"]):
        x_series = pd.to_datetime(x_series, errors="coerce")

    if chart_type == "bar":
        if pd.api.types.is_datetime64_any_dtype(x_series):
            x_frame = pd.DataFrame({"x": x_series, "y": y_series}).dropna().sort_values("x")
            chart_data = x_frame.groupby("x")["y"].sum()
        else:
            chart_data = (
                pd.DataFrame({"x": x_series.astype(str), "y": y_series})
                .dropna()
                .groupby("x")["y"]
                .sum()
                .sort_values(ascending=False)
            )
            if chart_data.shape[0] > 10:
                chart_data = chart_data.head(10)
        st.bar_chart(chart_data)
    elif chart_type == "line":
        if pd.api.types.is_datetime64_any_dtype(x_series):
            x_frame = pd.DataFrame({"x": x_series, "y": y_series}).dropna().sort_values("x")
            chart_data = x_frame.groupby("x")["y"].sum()
        else:
            chart_data = (
                pd.DataFrame({"x": x_series, "y": y_series})
                .dropna()
                .groupby("x")["y"]
                .sum()
            )
        st.line_chart(chart_data)
    elif chart_type == "area":
        if pd.api.types.is_datetime64_any_dtype(x_series):
            x_frame = pd.DataFrame({"x": x_series, "y": y_series}).dropna().sort_values("x")
            chart_data = x_frame.groupby("x")["y"].sum()
        else:
            chart_data = (
                pd.DataFrame({"x": x_series.astype(str), "y": y_series})
                .dropna()
                .groupby("x")["y"]
                .sum()
            )
        st.area_chart(chart_data)


def _render_fallback_charts(df: pd.DataFrame) -> None:
    working = _prepare_sales_fields(df)
    revenue_col = _find_column(working, ["sales", "revenue", "amount", "total", "total_sales"])
    region_col = _find_column(working, ["country", "region", "area", "zone", "market"])
    date_col = _find_column(working, ["invoicedate", "invoice_date", "date", "order_date", "transaction_date", "timestamp"])
    product_col = _find_column(working, ["description", "product", "item", "sku", "product_name"])

    if revenue_col is not None and region_col is not None:
        revenue = pd.to_numeric(working[revenue_col], errors="coerce")
        by_region = (
            working.assign(_revenue=revenue)
            .groupby(region_col, dropna=False)["_revenue"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        st.bar_chart(by_region)
    if revenue_col is not None and date_col is not None:
        trend_df = working[[date_col, revenue_col]].copy()
        trend_df[revenue_col] = pd.to_numeric(trend_df[revenue_col], errors="coerce")
        trend = (
            trend_df.dropna(subset=[date_col])
            .groupby(trend_df[date_col].dt.to_period("M"))[revenue_col]
            .sum()
            .sort_index()
        )
        trend.index = trend.index.astype(str)
        st.subheader("📅 Revenue Trend")
        st.line_chart(trend)
    if product_col is not None:
        if revenue_col is not None:
            revenue = pd.to_numeric(working[revenue_col], errors="coerce")
            top_products = (
                working.assign(_revenue=revenue)
                .groupby(product_col, dropna=False)["_revenue"]
                .sum()
                .sort_values(ascending=False)
                .nlargest(10)
            )
            st.subheader("🏆 Top Products")
            st.bar_chart(top_products)
        else:
            top_products = working[product_col].value_counts(dropna=False).head(10)
            st.subheader("🏆 Top Products")
            st.bar_chart(top_products)
    if revenue_col is not None and region_col is not None:
        revenue = pd.to_numeric(working[revenue_col], errors="coerce")
        country_sales = (
            working.assign(_revenue=revenue)
            .groupby(region_col, dropna=False)["_revenue"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        st.subheader("🌍 Sales by Country")
        st.bar_chart(country_sales)


def _render_final_charts(df: pd.DataFrame) -> bool:
    working = _prepare_sales_fields(df)
    sales_col = _find_column(working, ["sales", "revenue", "amount", "total", "total_sales"])
    country_col = _find_column(working, ["country", "region", "area", "zone", "market"])
    description_col = _find_column(working, ["description", "product", "item", "sku", "product_name"])
    date_col = _find_column(working, ["invoicedate", "invoice_date", "date", "order_date", "transaction_date", "timestamp"])

    if sales_col is None:
        return False

    has_any_chart = False
    sales_series = pd.to_numeric(working[sales_col], errors="coerce")

    if date_col is not None:
        trend = (
            working.assign(_sales=sales_series)
            .dropna(subset=[date_col])
            .groupby(working[date_col].dt.to_period("M"))["_sales"]
            .sum()
            .sort_index()
        )
        if not trend.empty:
            trend.index = trend.index.astype(str)
            st.line_chart(trend)
            has_any_chart = True

    if description_col is not None:
        top_products = (
            working.assign(_sales=sales_series)
            .groupby(description_col, dropna=False)["_sales"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        if not top_products.empty:
            st.bar_chart(top_products)
            has_any_chart = True

    if country_col is not None:
        country_sales = (
            working.assign(_sales=sales_series)
            .groupby(country_col, dropna=False)["_sales"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        if not country_sales.empty:
            st.bar_chart(country_sales)
            has_any_chart = True

    if not has_any_chart:
        numeric_columns, categorical_columns = get_best_columns(working)
        if numeric_columns and categorical_columns:
            grouped = (
                working.groupby(categorical_columns[0], dropna=False)[numeric_columns[0]]
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )
            st.bar_chart(grouped)
            has_any_chart = True

    return has_any_chart


def _columns_info(df: pd.DataFrame) -> str:
    numeric_columns, categorical_columns = get_best_columns(df)
    date_like_columns = [
        column for column in df.columns if "date" in str(column).lower() or "time" in str(column).lower() or "timestamp" in str(column).lower()
    ]
    return (
        f"numeric_columns: {numeric_columns}\n"
        f"categorical_columns: {categorical_columns}\n"
        f"date_like_columns: {date_like_columns}\n"
        f"all_columns: {df.columns.tolist()}"
    )


uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

if uploaded_file is not None:
    try:
        uploaded_df, detected_encoding = _read_uploaded_csv(uploaded_file)
    except Exception as e:
        st.error(f"Failed to read CSV: {e}")
        st.stop()

    st.caption(f"Detected CSV encoding: {detected_encoding}")

    st.markdown("## 📂 Data Preview")
    st.dataframe(uploaded_df.head(10), width="stretch")
    st.divider()

    uploaded_df = _prepare_sales_fields(uploaded_df)
    fallback_metrics = _build_fallback_business_metrics(uploaded_df)
    fallback_metrics["Rows"] = f"{len(uploaded_df):,}"

    st.info("Click Run Pipeline to generate production insights and dashboard charts.")

    business_summary: dict[str, Any] = {}
    final_metrics = fallback_metrics
    insights: dict[str, Any] = {}
    chart_source_df = uploaded_df
    summary = ""

    if st.button("Run Pipeline", type="primary"):
        status_box = st.container(border=True)
        status_box.subheader("⚙️ Pipeline Status")

        try:
            status_box.success("✔ Ingestion Complete")

            cleaned_df = run_processing_stage(uploaded_df.copy())
            cleaned_df = _prepare_sales_fields(cleaned_df)
            status_box.success("✔ Data Cleaned")

            summary, insights = run_ai_stage(cleaned_df)
            status_box.success("✔ AI Analysis Done")

            output_path = run_output_stage(insights)
            status_box.success("✔ Output Generated")
        except Exception as e:
            status_box.error(f"Pipeline failed: {e}")
            st.stop()

        business_summary = insights.get("business_summary", {}) if isinstance(insights, dict) else {}
        final_metrics = _build_fallback_business_metrics(cleaned_df)
        final_metrics["Rows"] = f"{len(cleaned_df):,}"
        chart_source_df = cleaned_df

        st.success(f"Saved output to: {output_path}")
        st.download_button(
            label="Download insights",
            data=json.dumps(insights, indent=2, ensure_ascii=False) if isinstance(insights, dict) else str(insights),
            file_name="insights.json" if isinstance(insights, dict) else "insights.txt",
            mime="application/json" if isinstance(insights, dict) else "text/plain",
        )

    st.markdown("## 📌 Overview")
    _render_metric_cards(business_summary, final_metrics)
    st.divider()

    st.markdown("## 📈 Charts")
    st.markdown("### Analytics Dashboard")
    st.markdown("---")

    chart_left, chart_right = st.columns(2)
    if chart_source_df is not None:
        working_df = _prepare_sales_fields(chart_source_df)
        sales_col = _find_column(working_df, ["sales", "revenue", "amount", "total", "total_sales"])
        country_col = _find_column(working_df, ["country", "region", "area", "zone", "market"])
        date_col = _find_column(working_df, ["invoicedate", "invoice_date", "date", "order_date", "transaction_date", "timestamp"])
        description_col = _find_column(working_df, ["description", "product", "item", "sku", "product_name"])

        if sales_col is not None and date_col is not None:
            monthly_trend = (
                working_df.assign(_sales=pd.to_numeric(working_df[sales_col], errors="coerce"))
                .dropna(subset=[date_col])
                .groupby(working_df[date_col].dt.to_period("M"))["_sales"]
                .sum()
                .sort_index()
            )
            monthly_trend.index = monthly_trend.index.astype(str)
        else:
            monthly_trend = None

        if sales_col is not None and country_col is not None:
            country_sales = (
                working_df.assign(_sales=pd.to_numeric(working_df[sales_col], errors="coerce"))
                .groupby(country_col, dropna=False)["_sales"]
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )
        else:
            country_sales = None

        if sales_col is not None and description_col is not None:
            top_products = (
                working_df.assign(_sales=pd.to_numeric(working_df[sales_col], errors="coerce"))
                .groupby(description_col, dropna=False)["_sales"]
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )
        else:
            top_products = None

        with chart_left:
            st.subheader("📅 Revenue Trend")
            if monthly_trend is not None and not monthly_trend.empty:
                st.line_chart(monthly_trend)
            else:
                st.caption("Monthly revenue trend is unavailable for this dataset.")

        with chart_right:
            st.subheader("🌍 Sales by Country")
            if country_sales is not None and not country_sales.empty:
                st.bar_chart(country_sales)
            else:
                st.caption("Country sales chart is unavailable for this dataset.")

        st.markdown("### 🏆 Top Products")
        if top_products is not None and not top_products.empty:
            st.bar_chart(top_products)
        else:
            st.caption("Top products chart is unavailable for this dataset.")
    else:
        rendered = _render_final_charts(chart_source_df)
        if not rendered:
            st.info("Not enough compatible columns for the primary chart set. Showing safe fallback charts.")
            _render_fallback_charts(chart_source_df)
    st.divider()

    st.markdown("## 🤖 AI Insights")
    if isinstance(insights, dict) and insights.get("insights"):
        _render_bullet_section("🔍 Key Insights", insights.get("insights", []), "No insights returned yet.")
        _render_bullet_section("💡 Recommendations", insights.get("recommendations", []), "No recommendations returned yet.")
    else:
        st.caption("Run the pipeline to populate AI insights and recommendations.")

    if summary:
        with st.expander("Dataset Summary Sent to AI"):
            st.text(summary)
            st.text(_columns_info(chart_source_df))
else:
    st.info("Upload a CSV file to start.")
