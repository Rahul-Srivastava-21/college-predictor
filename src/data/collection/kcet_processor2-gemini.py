# K.C.E.T. 2024 Mock Allotment PDF Data Extractor
# This script reads the provided PDF file, extracts the engineering cutoff rank tables,
# and exports the data to a CSV file.
#
# Requirements:
# - Python 3
# - pandas library (`pip install pandas`)
# - pypdf library (`pip install pypdf`)

import pypdf
import re
import pandas as pd

def clean_header(header_list):
    """Cleans the extracted header list to remove extraneous text and known issues."""
    cleaned = []
    for h in header_list:
        h = h.replace('ЗАК', '3AK').replace('ЗВЯ', '3BR')
        if re.match(r'^[123][A-Z]{1,2}$|^GM[KR]?$|^S[CT][GKR]?$', h):
            cleaned.append(h)
    return cleaned

def split_concatenated_ranks(rank_string):
    """
    Heuristically splits long strings of digits into individual rank numbers.
    This version uses regex to find all sequences of digits or '--'.
    """
    # Normalize by replacing '--' with spaces for easier splitting
    s = rank_string.replace('--', ' -- ')
    
    # Extract all number sequences or '--'
    ranks = re.findall(r'\d+|--', s)
    
    final_ranks = []
    temp_long_str = ""
    
    for rank in ranks:
        if rank.isdigit() and len(rank) > 6 and not re.match(r'^\d{4,6}-\d{4,6}$', rank):
            temp_long_str += rank
        else:
            if temp_long_str:
                while len(temp_long_str) > 4:
                    chunk_len = 6 if len(temp_long_str) > 10 else 5
                    final_ranks.append(temp_long_str[:chunk_len])
                    temp_long_str = temp_long_str[chunk_len:]
                if temp_long_str:
                     final_ranks.append(temp_long_str)
                temp_long_str = ""
            final_ranks.append(rank)

    if temp_long_str:
        while len(temp_long_str) > 4:
            chunk_len = 6 if len(temp_long_str) > 10 else 5
            final_ranks.append(temp_long_str[:chunk_len])
            temp_long_str = temp_long_str[chunk_len:]
        if temp_long_str:
            final_ranks.append(temp_long_str)

    return final_ranks

def parse_pdf_data(pdf_path):
    """
    Main function to parse the KCET PDF. This is a complete rewrite with a more
    robust line-by-line parsing logic that is less prone to formatting errors.
    """
    all_rows_data = []
    
    try:
        reader = pypdf.PdfReader(pdf_path)
    except FileNotFoundError:
        print(f"Error: The file '{pdf_path}' was not found.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred while reading the PDF: {e}")
        return pd.DataFrame()

    full_text = ""
    for page in reader.pages:
        try:
            text = page.extract_text()
            if text:
                full_text += text
        except Exception:
            continue

    # Split the document by college headers (e.g., "E001 ...")
    college_blocks = re.split(r'(?=\nE\d{3,}\s)', full_text)
    
    if len(college_blocks) < 2:
        print("Error: Could not find any college data blocks in the PDF.")
        return pd.DataFrame()

    for block in college_blocks:
        if not block.strip():
            continue

        lines = block.strip().split('\n')
        
        college_header = lines[0]
        match = re.match(r'(E\d{3,})\s+(.*)', college_header)
        if not match:
            continue
            
        current_college_code = match.group(1)
        current_college_name = match.group(2).replace('(PUBLIC UNIV.)', '').strip()
        
        current_headers = []
        
        line_idx = 1
        while line_idx < len(lines):
            line = lines[line_idx].strip()
            
            # Identify header row
            parts = line.split()
            potential_headers = clean_header(parts)
            if len(potential_headers) > 5 and potential_headers[0] in ('1G', '1K', 'GM'):
                current_headers = potential_headers
                line_idx += 1
                continue

            # Identify data/branch row
            if current_headers and (re.match(r'^[A-Z]{2,}', line) or re.search(r'\d', line)):
                branch_name_parts = []
                rank_line = ""
                
                # Check if the line contains a branch name
                branch_match = re.match(r'([A-Z][A-Za-z\.\s\(\)-]+)', line)
                if branch_match:
                    branch_candidate = branch_match.group(1).strip()
                    # Check if there is rank data on the same line
                    rank_match = re.search(r'\s{2,}(\d{3,}|--)', line)
                    if rank_match:
                        branch_name_parts.append(line[:rank_match.start()].strip())
                        rank_line = line[rank_match.start():].strip()
                    else:
                        branch_name_parts.append(branch_candidate)
                
                # Check for rank data on the next line if not found on the current one
                if not rank_line and (line_idx + 1 < len(lines)):
                    next_line = lines[line_idx + 1].strip()
                    if re.match(r'^\s*(\d{3,}|--)', next_line):
                        rank_line = next_line
                        line_idx += 1 # Skip the rank line in the next iteration
                    # Also check for continuations of the branch name
                    elif re.match(r'^[a-z]', next_line) and not re.search(r'\d',next_line):
                         branch_name_parts.append(next_line)
                         line_idx += 1
                         if (line_idx + 1 < len(lines)) and re.match(r'^\s*(\d{3,}|--)', lines[line_idx+1].strip()):
                             rank_line = lines[line_idx+1].strip()
                             line_idx += 1


                if branch_name_parts and rank_line:
                    branch_name = " ".join(branch_name_parts).strip()
                    branch_code_match = re.match(r'^[A-Z]{2,}', branch_name)
                    branch_code = branch_code_match.group(0) if branch_code_match else "N/A"
                    
                    ranks = split_concatenated_ranks(rank_line)
                    
                    for idx, rank in enumerate(ranks):
                        if idx < len(current_headers):
                            quota = current_headers[idx]
                            cutoff_rank = rank.replace('--', '').strip() if rank.isdigit() else None
                            
                            if cutoff_rank:
                                all_rows_data.append({
                                    'College_Code': current_college_code,
                                    'College_Name': current_college_name,
                                    'Branch_Code': branch_code,
                                    'Branch': branch_name,
                                    'Quota': quota,
                                    'Cutoff_Rank': int(cutoff_rank),
                                    'Year': 2024,
                                    'Round': 'Mock Allotment',
                                    'Exam_Type': 'KCET-CET'
                                })
            
            line_idx += 1

    return pd.DataFrame(all_rows_data)

# --- Main execution block ---
if __name__ == "__main__":
    pdf_file_path = 'kcet-round0-2024.pdf'
    output_csv_path = 'kcet_extracted_data.csv'
    
    print(f"Starting data extraction from '{pdf_file_path}'...")
    
    df = parse_pdf_data(pdf_file_path)
    
    if not df.empty:
        df.to_csv(output_csv_path, index=False)
        print(f"Data extraction complete. {len(df)} records saved to '{output_csv_path}'")
    else:
        print("No data was extracted. Please ensure the PDF file path is correct and the file is accessible.")

