import os
import re
import csv
from docx import Document

# ------------------ Config ------------------
INPUT_DIR = r"D:\Courses\Global Academy of Technology\kcet-college-pred\src\data\collection\Scraping\kcet_docs"
OUTPUT_FILE = "kcet_cutoffs_all_optimized.csv"

# ------------------ Helper Functions ------------------

def parse_college_header(text):
    """Detect lines like: E001 College Name. Returns (code, name) or None"""
    text = text.strip()
    match = re.match(r"^(E\d+)\s+(.*)$", text)
    if match:
        return match.group(1), match.group(2).strip()
    return None

def clean_cell_text(text):
    """Normalize multi-line cell text into single line with single spaces."""
    return ' '.join(text.split()) if text else ''

# ------------------ Optimized DOCX Processing ------------------

def process_docx_optimized(filepath, year, round_num, debug=False):
    print(f"\n📂 Processing {filepath}...")
    doc = Document(filepath)
    records = []

    current_college_code = None
    current_college_name = None
    seen_rows = set()
    processed_tables = set()  # track processed table IDs

    # Walk elements in order
    for block in doc.element.body:
        if block.tag.endswith('p'):
            text = ''.join([t.text for t in block.iter() if t.tag.endswith('t')]).strip()
            header = parse_college_header(text)
            if header:
                current_college_code, current_college_name = header
                if debug:
                    print(f"🏫 Found College: {current_college_code} {current_college_name}")

        elif block.tag.endswith('tbl'):
            table = None
            # find the docx.table object matching this xml <tbl>
            for t in doc.tables:
                if t._element == block and id(t) not in processed_tables:
                    table = t
                    processed_tables.add(id(t))
                    break
            if not table:
                continue  # already used

            if current_college_code is None:
                continue  # table before first college header

            if len(table.rows) <= 1:
                continue

            categories = [clean_cell_text(c.text) for c in table.rows[0].cells[1:] if clean_cell_text(c.text)]
            if not categories:
                continue

            for row in table.rows[1:]:
                cells = [clean_cell_text(c.text) for c in row.cells]
                if not cells or not cells[0]:
                    continue

                branch = ' '.join(cells[0].splitlines()).strip()
                cutoffs = cells[1:len(categories)+1]

                for cat, cutoff in zip(categories, cutoffs):
                    if cutoff and cutoff != "--":
                        key = (current_college_code, branch, cat, cutoff)
                        if key not in seen_rows:
                            records.append([
                                current_college_code,
                                current_college_name,
                                cat,
                                branch,
                                cutoff,
                                year,
                                round_num,
                                "CET"
                            ])
                            seen_rows.add(key)

    if debug:
        print(f"✅ Extracted {len(records)} records from {os.path.basename(filepath)}")

    return records

# ------------------ Main Script ------------------

def main():
    all_records = []

    for filename in os.listdir(INPUT_DIR):
        if filename.endswith(".docx") and filename.startswith("kcet-round"):
            match = re.match(r"kcet-round(\d+)-(\d{4})\.docx", filename)
            if not match:
                continue
            round_num, year = match.groups()
            filepath = os.path.join(INPUT_DIR, filename)

            records = process_docx_optimized(filepath, year, round_num, debug=True)
            all_records.extend(records)

            print(f"✅ Extracted {len(records)} records from {filename}")
            if records:
                print("🔎 Preview (first 5 rows):")
                for r in records[:5]:
                    print("   ", r)

    if all_records:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["College_Code", "College_Name", "Category", "Branch",
                             "Cutoff_Rank", "Year", "Round", "Exam_Type"])
            writer.writerows(all_records)
        print(f"\n✨ Done! Extracted {len(all_records)} records into {OUTPUT_FILE}")
    else:
        print("\n⚠️ No records extracted.")

if __name__ == "__main__":
    main()