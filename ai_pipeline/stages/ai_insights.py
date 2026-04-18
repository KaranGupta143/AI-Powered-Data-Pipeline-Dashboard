import json
import logging
from typing import Any

from openai import OpenAI

from config import AI_MODEL, OPENAI_API_KEY


client = OpenAI(api_key=OPENAI_API_KEY)
logger = logging.getLogger(__name__)


SYSTEM_PROMPT = (
    "You are a senior data analyst and BI expert. "
    "Write business-ready insights that are specific, quantitative, and concise. "
    "Prefer exact metrics, percentages, comparisons, and dataset-aware chart suggestions. "
    "For visualizations, prioritize revenue/Sales over Quantity when both are available."
)

USER_PROMPT_TEMPLATE = """You are a senior data analyst and BI expert.

Analyze the dataset structure and summary below:

{summary}

Dataset schema context:
{columns_info}

Your task:
1. Identify important columns (numeric, categorical, date)
2. Automatically detect:
   - revenue/sales columns
   - category/product columns
   - date/time columns
   - geographic/region columns (if any)

3. Generate:
   A. Business Summary:
      - Total revenue (if possible)
      - Top category/product
      - Key region (if exists)

   B. Insights:
      - Key trends with numbers/percentages
      - Anomalies or outliers

   C. Recommended Visualizations:
      - Which charts to create
      - Which columns to use

   D. Recommendations:
      - Business actions based on data

IMPORTANT:
- Be specific with numbers and percentages
- Adapt to ANY dataset (do not assume column names)
- If column not found, intelligently infer from context
- Prefer Sales/revenue as the y-axis metric for charts whenever possible
- If Sales is missing but Quantity and UnitPrice exist, treat Sales as Quantity * UnitPrice

Output in structured format:
{{
  "business_summary": {{
    "total_revenue": "...",
    "top_category_product": "...",
    "key_region": "..."
  }},
  "insights": ["..."],
  "visualizations": [
    {{"type": "bar", "x": "...", "y": "..."}}
  ],
  "recommendations": ["..."]
}}

SECOND PROMPT (FOR CHART LOGIC):
Based on this dataset schema:

{columns_info}

Return chart configurations in JSON:

- bar charts
- line charts
- top categories

Each chart must include:
- chart_type
- x_column
- y_column

IMPORTANT:
- Choose best columns automatically
- Prefer meaningful business relationships
- Prefer Sales/revenue for y_column instead of Quantity unless revenue columns are unavailable
- For categorical bar charts, choose grouped business dimensions (e.g., Country/Region, Product/Description)
- Keep high-cardinality categorical charts focused on top categories (top 10)
- For time trends, use date/time columns on x and Sales/revenue on y

Output format:
[
  {{
    "type": "bar",
    "x": "column_name",
    "y": "column_name"
  }}
]
"""


def _normalize_chart_configs(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    normalized: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        chart_type = item.get("type") or item.get("chart_type")
        x_column = item.get("x") or item.get("x_column")
        y_column = item.get("y") or item.get("y_column")
        if chart_type and x_column and y_column:
            normalized.append(
                {
                    "type": str(chart_type),
                    "x": str(x_column),
                    "y": str(y_column),
                }
            )
    return normalized


def _coerce_output(parsed: dict[str, Any]) -> dict[str, Any]:
    business_summary = parsed.get("business_summary", {})
    if not isinstance(business_summary, dict):
        business_summary = {}

    insights = parsed.get("insights", [])
    recommendations = parsed.get("recommendations", [])
    visualizations = _normalize_chart_configs(parsed.get("visualizations", []))

    return {
        "business_summary": business_summary,
        "insights": insights if isinstance(insights, list) else [str(insights)],
        "visualizations": visualizations,
        "recommendations": recommendations if isinstance(recommendations, list) else [str(recommendations)],
    }


def _format_structured_output(parsed: dict[str, Any]) -> str:
    business_summary = parsed.get("business_summary", {})
    insights = parsed.get("insights", [])
    recommendations = parsed.get("recommendations", [])
    visualizations = parsed.get("visualizations", [])

    def as_bullets(items: list) -> str:
        if not items:
            return "- No items returned."
        return "\n".join(f"- {item}" for item in items)

    return (
        f"Business Summary\n{json.dumps(business_summary, indent=2)}\n\n"
        f"Insights\n{as_bullets(insights)}\n\n"
        f"Visualizations\n{json.dumps(visualizations, indent=2)}\n\n"
        f"Recommendations\n{as_bullets(recommendations)}"
    )


def get_structured_insights(summary_text: str, columns_info: str) -> dict[str, Any]:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not set. Add it in .env before running AI insights.")

    try:
        response = client.chat.completions.create(
            model=AI_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": USER_PROMPT_TEMPLATE.format(summary=summary_text, columns_info=columns_info),
                },
            ],
        )

        content = response.choices[0].message.content or "{}"
        parsed = json.loads(content)
        logger.info("[AI] Insight generation completed.")
        return _coerce_output(parsed)
    except Exception as e:
        logger.exception("[AI] Insight generation failed.")
        raise RuntimeError(f"AI generation failed: {e}") from e


def get_insights(summary_text: str, columns_info: str = "") -> str:
    structured = get_structured_insights(summary_text, columns_info)
    return _format_structured_output(structured)
