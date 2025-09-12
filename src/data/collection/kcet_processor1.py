# kcet_parse.py

# 1) Install dependencies (local Python with pip)
#    pip install "camelot-py[cv]" pandas

import os
import re
import sys
import argparse
import camelot
import pandas as pd

# Defaults; can be overridden by CLI
PDF_PATH = "kcet-round0-2024.pdf"          # put the attached PDF in the same folder
OUT_CSV  = "kcet_round0_2024_round0_mock_CET.csv"

# Known branch code → branch name mappings frequently listed in the PDF header blocks
branch_map = {
    "AI": "Artificial Intelligence",
    "AD": "Artificial Intel, Data Sc",
    "AE": "Aeronaut.Engg",
    "AM": "B Tech in AM",
    "AR": "Architecture",
    "AU": "Automobile",
    "BB": "B Tech in EC",
    "BG": "B Tech in AD",
    "BH": "B Tech in AI",
    "BJ": "B Tech in EE",
    "BL": "B Tech in AS",
    "BP": "B Tech in CE",
    "BW": "B Tech in CS",
    "BZ": "B Tech in DS",
    "CA": "CS (AI, Machine Learning)",
    "CB": "Comp. Sc. and Bus Sys.",
    "CD": "Computer Sc. and Design",
    "CE": "Civil",
    "CF": "CS(Artificial Intel.)",
    "CH": "Chemical",
    "CM": "B Tech in EV",
    "CO": "Computer Engineering",
    "CR": "Ceramics",
    "CS": "Computers",
    "CT": "Const. Tech. Mgmt.",
    "CY": "CS- Cyber Security",
    "DA": "B Tech in MC",
    "DB": "B Tech in ME",
    "DC": "Data Sciences",
    "DD": "B Tech in MS",
    "DE": "B Tech in PE",
    "DF": "B Tech in RA",
    "DG": "DESIGN",
    "DH": "B Tech in RAI",
    "DJ": "B Tech in RO",
    "DK": "B Tech in SS",
    "DM": "B.TECH IN CS NW",
    "DN": "B.Tech in VLSI",
    "DS": "Comp. Sc. Engg-Data Sc.",
    "EB": "EAT",
    "EC": "Electronics",
    "EE": "Electrical",
    "EI": "Elec. Inst. Engg",
    "EL": "Electronics, Instr. Tech.",
    "EN": "Environmental",
    "ES": "Electronics and Computer",
    "ET": "Elec. Telecommn. Engg.",
    "EV": "EC Engg(VLSI Design)",
    "IC": "CS-IoT, Cyber Security",
    "IE": "Info.Science",
    "IM": "Ind. Engg. Mgmt.",
    "IO": "CS- Internet of Things",
    "IP": "Ind.Prodn.",
    "LD": "B Tech in DS",
    "LE": "B Tech in AIML",
    "LF": "B Tech in CC",
    "LG": "B Tech in CS",
    "LH": "B Tech in IS",
    "LJ": "B Tech in BS",
    "LK": "B Tech in IOT",
    "MD": "Med.Elect.",
    "ME": "Mechanical",
    "MI": "Mining Engineering",
    "MK": "Mechanical Engg (Kannada)",
    "MM": "Mechanical, Smart Manf.",
    "MR": "Marine Engineering",
    "MT": "Mechatronics",
    "OT": "Industrial IOT",
    "PT": "Polymer Tech.",
    "RA": "Robotics and Automation",
    "RI": "Robotics and AI",
    "RO": "Auto. And Robot.",
    "SE": "Aero Space Engg.",
    "ST": "Silk Tech.",
    "TC": "Telecommn.",
    "TX": "Textiles",
    "UP": "Planning",
    "UR": "Planning",
    "ZA": "B TECH IN AERONAUT. ENGG.",
    "ZC": "CSC",
    "ZH": "B TECH IN COMP.SC.ART.INT",
    "ZO": "B.TECH IN CS.AND BUSI.SYS",
    "ZT": "B.TECH. IN MECH.SMAR.MANU",
    "ZR": "COMP.SCE.AND ENG(ART.INT)",
    "ZU": "CYBER SECURITY",
}

# Quota labels commonly used in the PDF
quota_labels = {
    "GM","GMK","GMR","SCG","SCK","SCR","STG","STK","STR",
    "1G","1K","1R","2AG","2AK","2AR","2BG","2BK","2BR",
    "3AG","3AK","3AR","3BG","3BK","3BR"
}

def norm(s: str) -> str:
    """Normalize Camelot cell text: collapse NBSP and strip."""
    if s is None:
        return ""
    return s.replace("\xa0", " ").strip()

def parse_college_header(line: str):
    """
    Match college header lines like: E001  University of ...
    """
    line = norm(line)
    m = re.match(r"^(E\d{3})\s+(.*)$", line)
    if m:
        return m.group(1), m.group(2).strip()
    return None, None

def looks_numeric(v: str) -> bool:
    """True for rank-like strings or '--'."""
    v = norm(v)
    if v == "--":
        return True
    v2 = v.replace(",", "")
    return v2.isdigit()

def main():
    parser = argparse.ArgumentParser(
        description="Extract KCET cutoff ranks from KCET-2024 mock allotment PDF into CSV."
    )
    parser.add_argument(
        "--pdf", "-p",
        default=PDF_PATH,
        help=f"Path to KCET mock allotment PDF (default: {PDF_PATH})"
    )
    parser.add_argument(
        "--out", "-o",
        default=OUT_CSV,
        help=f"Output CSV file path (default: {OUT_CSV})"
    )
    parser.add_argument(
        "--pages",
        default="all",
        help='Pages to parse (Camelot syntax, e.g. "1-5,7,9" or "all"; default: all)'
    )
    parser.add_argument(
        "--flavor",
        choices=["lattice", "stream"],
        default="lattice",
        help="Camelot extraction flavor (default: lattice)"
    )
    parser.add_argument(
        "--min-quota-hits",
        type=int,
        default=5,
        help="Minimum number of quota labels required to treat a row as the quota header (default: 5)"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Admission year to stamp in rows (default: 2024)"
    )
    parser.add_argument(
        "--round",
        default="Round0",
        help='Round label to stamp in rows (default: "Round0")'
    )
    parser.add_argument(
        "--exam-type",
        default="CET",
        help='Exam type label to stamp in rows (default: "CET")'
    )
    args = parser.parse_args()

    if not os.path.exists(args.pdf):
        print(f"ERROR: PDF not found: {args.pdf}", file=sys.stderr)
        sys.exit(1)

    # Extract tables
    try:
        tables = camelot.read_pdf(
            args.pdf,
            pages=args.pages,
            flavor=args.flavor,
            strip_text="\n",
        )
    except Exception as e:
        print(f"ERROR: Camelot failed to read PDF: {e}", file=sys.stderr)
        sys.exit(2)

    if not tables or len(tables) == 0:
        print("WARNING: No tables extracted from the PDF (check pages/flavor).", file=sys.stderr)

    records = []
    current_college_code = None
    current_college_name = None

    # Iterate tables
    for t in tables:
        df = t.df

        # Track the last seen college header within this table
        for _, row in df.iterrows():
            line = norm(" ".join([c for c in row if isinstance(c, str)]))
            ccode, cname = parse_college_header(line)
            if ccode:
                current_college_code = ccode
                current_college_name = cname

        # Identify header row candidates that list many quota labels
        header_candidates = []
        for idx, row in df.iterrows():
            tokens = [norm(c) for c in row.tolist() if c and isinstance(c, str)]
            hits = sum(1 for c in tokens if c in quota_labels)
            if hits >= args.min_quota_hits:
                header_candidates.append(idx)

        if not header_candidates:
            continue

        for hidx in header_candidates:
            header_row = df.iloc[hidx].tolist()
            header = [norm(c) if isinstance(c, str) else "" for c in header_row]

            # Map column index → quota label
            col_quota = {}
            for ci, val in enumerate(header):
                if val in quota_labels:
                    col_quota[ci] = val

            # Walk downward to collect cutoff values
            last_branch_code = None
            for ridx in range(hidx + 1, len(df)):
                row_vals = [norm(c) if isinstance(c, str) else "" for c in df.iloc[ridx].tolist()]
                row_text = norm(" ".join(row_vals))

                # Stop if a new college header starts
                ccode2, _ = parse_college_header(row_text)
                if ccode2:
                    break

                # Try to detect branch code on this row
                seen_branch_code = None
                for val in row_vals:
                    if val in branch_map:
                        seen_branch_code = val
                        break
                    m2 = re.match(r"^([A-Z]{2})\s+(.+)$", val)
                    if m2 and m2.group(1) in branch_map:
                        seen_branch_code = m2.group(1)
                        break

                if seen_branch_code:
                    last_branch_code = seen_branch_code
                else:
                    # Fall back to previously seen branch code (or look up a few rows)
                    if not last_branch_code:
                        for k in range(max(hidx, ridx - 5), ridx):
                            up_vals = [norm(c) if isinstance(c, str) else "" for c in df.iloc[k].tolist()]
                            for uv in up_vals:
                                if uv in branch_map:
                                    last_branch_code = uv
                                    break
                            if last_branch_code:
                                break

                # Determine if the row looks like numeric cutoffs under quota columns
                if not any(looks_numeric(row_vals[ci]) for ci in col_quota if ci < len(row_vals)):
                    continue

                # If still missing a branch code, skip saving this row
                if not last_branch_code:
                    continue

                # Emit records for each quota column with a present value
                for ci, q in col_quota.items():
                    if ci < len(row_vals):
                        cutoff = row_vals[ci]
                        if cutoff == "":
                            continue
                        records.append({
                            "College_Code": current_college_code,
                            "College_Name": current_college_name,
                            "Branch_Code": last_branch_code,
                            "Branch": branch_map.get(last_branch_code, None),
                            "Quota": q,
                            "Cutoff_Rank": cutoff,
                            "Year": args.year,
                            "Round": args.round,
                            "Exam_Type": args.exam_type,
                        })

    out = pd.DataFrame.from_records(records)
    # Mandatory fields
    if not out.empty:
        out = out.dropna(subset=["College_Code", "Branch_Code", "Quota", "Cutoff_Rank"])
    out.to_csv(args.out, index=False)
    print(f"Saved: {args.out}")
    if not out.empty:
        print(out.head(20))

if __name__ == "__main__":
    main()
