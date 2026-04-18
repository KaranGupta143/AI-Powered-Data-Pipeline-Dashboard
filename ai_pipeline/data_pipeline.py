import pprint
from pathlib import Path
import pandas as pd


class DataPipeline:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.data = None
        self.cleaned_data = None

    # Step 1: Load CSV
    def load_data(self):
        try:
            self.data = pd.read_csv(self.file_path)
            return self.data
        except Exception as e:
            raise Exception(f"Error loading data: {e}")

    # Step 2: Clean Data (remove nulls)
    def clean_data(self):
        if self.data is None:
            raise ValueError("Data not loaded. Run load_data() first.")

        self.cleaned_data = self.data.dropna()
        return self.cleaned_data

    # Step 3: Generate Summary Statistics
    def generate_summary(self):
        if self.cleaned_data is None:
            raise ValueError("Data not cleaned. Run clean_data() first.")

        summary = {
            "shape": self.cleaned_data.shape,
            "columns": list(self.cleaned_data.columns),
            "data_types": self.cleaned_data.dtypes.astype(str).to_dict(),
            "summary_stats": self.cleaned_data.describe().to_dict(),
        }
        return summary

    # Step 4: Run Full Pipeline
    def run_pipeline(self):
        self.load_data()
        self.clean_data()
        summary = self.generate_summary()
        return summary


if __name__ == "__main__":
    project_dir = Path(__file__).resolve().parent
    input_path = project_dir / "data" / "input.csv"
    pipeline = DataPipeline(str(input_path))
    result = pipeline.run_pipeline()

    print("Structured Summary:\n")
    pprint.pprint(result)
