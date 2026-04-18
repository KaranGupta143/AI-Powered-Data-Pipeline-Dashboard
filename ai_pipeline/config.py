import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
INPUT_FILE = str(BASE_DIR / "data" / "input.csv")
OUTPUT_DIR = str(BASE_DIR / "data" / "outputs")
SCHEDULE_HOURS = int(os.getenv("SCHEDULE_HOURS", "1"))
SCHEDULE_MINS = int(os.getenv("SCHEDULE_MINS", str(SCHEDULE_HOURS * 60)))
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
LOG_FILE = str(BASE_DIR / "logs" / "pipeline.log")
