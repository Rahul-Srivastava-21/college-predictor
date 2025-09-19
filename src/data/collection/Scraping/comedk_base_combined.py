import pandas as pd
import tabula
import logging
import os
import re
from typing import List, Dict, Tuple

# ------------------ Logging Config ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("cutoff_processing.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ------------------ Base Processor ------------------
class BaseProcessor:
    def __init__(self, pdf_file=None, year=None, round_no=None, exam=None):
        self.pdf_file = pdf_file
        self.year = year
        self.round_no = round_no
        self.exam = exam
        self.logger = logging.getLogger(__name__)

    def clean_value(self, value) -> str:
        if pd.isna(value):
            return ""
        return str(value).strip()

    def extract_numeric_value(self, value):
        if pd.isna(value):
            return None
        value_str = str(value).strip()
        if value_str in ["", "--", "-", "nan"]:
            return None
        try:
            return int(re.findall(r"\d+", value_str)[0])
        except Exception:
            return None

    def parse_pdf(self, pdf_path: str, exam_type: str) -> List[Dict]:
        logger.info(f"Parsing {exam_type} PDF: {os.path.basename(pdf_path)}")
        filename = os.path.basename(pdf_path).lower()

        year_match = re.search(r"(\d{4})", filename)
        year = int(year_match.group(1)) if year_match else 2024
        round_match = re.search(r"round(\d+)", filename)
        round_num = int(round_match.group(1)) if round_match else 1

        # Try lattice first, fallback to stream
        try:
            tables = tabula.read_pdf(
                pdf_path, pages="all", lattice=True,
                multiple_tables=True, pandas_options={"header": None}
            )
            if not tables or all(t.empty for t in tables):
                logger.warning("Lattice gave no usable tables, retrying with stream mode...")
                tables = tabula.read_pdf(
                    pdf_path, pages="all", stream=True,
                    multiple_tables=True, pandas_options={"header": None}
                )
        except Exception as e:
            logger.error(f"PDF read failed: {e}")
            return []

        logger.info(f"{exam_type}: Detected {len(tables)} tables from {pdf_path}")

        all_records = []
        for idx, df in enumerate(tables):
            if df.shape[0] < 2:
                logger.debug(f"Skipping tiny table {idx} (rows={df.shape[0]}, cols={df.shape[1]})")
                continue

            records = self.process_table(df, year, round_num, exam_type, idx)
            all_records.extend(records)

        logger.info(f"Extracted {len(all_records)} records from {exam_type}")
        return all_records

    def process_table(self, df: pd.DataFrame, year: int, round_num: int, exam_type: str, table_idx: int) -> List[Dict]:
        # To be implemented by subclasses
        raise NotImplementedError("Subclasses must implement process_table method")

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        if not df.empty:
            df.to_csv(filename, index=False)
            logger.info(f"Saved {len(df)} rows to {filename}")

def find_pdf_files():
    return [f for f in os.listdir(".") if f.lower().endswith(".pdf")]

# ------------------ COMEDK Processor ------------------
class COMEDKProcessor(BaseProcessor):
    def __init__(self):
        super().__init__()

    def process_table(self, df: pd.DataFrame, year: int, round_num: int, exam_type: str, table_idx: int) -> List[Dict]:
        """Process COMEDK table using the working logic"""
        records = []
        
        if df.empty or df.shape[1] < 4:
            return records

        # Save debug table for first few tables
        if table_idx < 3:
            try:
                df.to_csv(f"comedk_debug_table_{table_idx}.csv", index=False)
                logger.info(f"Saved debug table {table_idx}")
            except Exception as e:
                logger.warning(f"Failed to save debug table: {e}")

        headers = df.iloc[0].fillna("").tolist()
        df = df.iloc[1:]

        for _, row in df.iterrows():
            college_code = self.clean_value(row[0])
            college_name = self.clean_value(row[1])
            category = self.clean_value(row[2])

            for col_idx, branch in enumerate(headers[3:], start=3):
                branch_name = self.clean_value(branch)
                cutoff_val = self.extract_numeric_value(row[col_idx])

                if cutoff_val is not None and branch_name:
                    records.append({
                        "College_Code": college_code,
                        "College_Name": college_name,
                        "Category": category,
                        "Branch": branch_name,
                        "Cutoff_Rank": cutoff_val,
                        "Year": year,
                        "Round": round_num,
                        "Exam_Type": "COMEDK",
                    })

        logger.info(f"Extracted {len(records)} COMEDK records from table {table_idx}")
        if records:
            logger.debug(f"Sample COMEDK records: {records[:2]}")
            
        return records

    def process_files(self, file_paths: List[str]) -> pd.DataFrame:
        all_records = []
        
        for file_path in file_paths:
            filename = os.path.basename(file_path).lower()
            if "comedk" in filename:
                all_records.extend(self.parse_pdf(file_path, "COMEDK"))
        
        return pd.DataFrame(all_records)

# ------------------ Main ------------------
def main():
    processor = COMEDKProcessor()
    pdf_files = find_pdf_files()
    
    if not pdf_files:
        print("No PDF files found!")
        return
    
    comedk_df = processor.process_files(pdf_files)
    print("COMEDK:", len(comedk_df), "records")
    
    if not comedk_df.empty:
        processor.save_to_csv(comedk_df, "comedk_cutoffs.csv")
        print("COMEDK data saved successfully!")
    else:
        print("No COMEDK records extracted. Check the debug CSV files.")

if __name__ == "__main__":
    main()