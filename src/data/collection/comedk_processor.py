import pandas as pd
import logging
import os
import re
from typing import List, Dict
from base_processor import BaseCutoffProcessor, find_pdf_files, logger

class COMEDKProcessor(BaseCutoffProcessor):
    def __init__(self):
        super().__init__()

    def process_table(self, df: pd.DataFrame, year: int, round_num: int, exam_type: str, table_idx: int) -> List[Dict]:
        """Process COMEDK table using the working logic from the original file"""
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
