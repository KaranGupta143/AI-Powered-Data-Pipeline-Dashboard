import logging
import os
import json
from datetime import datetime
from typing import Any


logger = logging.getLogger(__name__)


def save_output(insights: Any, output_dir: str) -> str:
    try:
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(output_dir, f"insights_{timestamp}.json" if isinstance(insights, dict) else f"insights_{timestamp}.txt")

        if isinstance(insights, dict):
            content = json.dumps(insights, indent=2, ensure_ascii=False)
        else:
            content = str(insights)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info("[Output] Saved insights to %s", filepath)
        return filepath
    except Exception as e:
        logger.exception("[Output] Failed to save insights.")
        raise RuntimeError(f"Output write failed: {e}") from e
