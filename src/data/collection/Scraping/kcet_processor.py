import pandas as pd
import tabula
import re
import os
from typing import List, Dict, Optional


class ImprovedKCETExtractor:
    def __init__(self):
        self.branch_codes = {
    'AD': 'Artificial Intel, Data Sc',
    'AE': 'Aeronaut.Engg',
    'AI': 'Artificial Intelligence',
    'AM': 'B Tech in AM',
    'AR': 'Architecture',
    'AT': 'Automotive Engg.',
    'AU': 'Automobile',
    'BA': 'B.Tech(Agri.Engg)',
    'BB': 'B Tech in EC',
    'BF': 'B Tech in DS',
    'BG': 'B Tech in AD',
    'BH': 'B Tech in AI',
    'BJ': 'B Tech in EE',
    'BK': 'B Tech in EN',
    'BL': 'B Tech in AS',
    'BM': 'Bio Medical',
    'BN': 'B Tech in BD',
    'BO': 'B Tech in BT',
    'BP': 'B Tech in CE',
    'BQ': 'B Tech in CG',
    'BR': 'BioMed. and Robotic Engg',
    'BT': 'Bio Technology',
    'BU': 'B Tech in CI',
    'BV': 'B Tech in CO',
    'BW': 'B Tech in CS',
    'BX': 'B Tech in CY',
    'BY': 'B Tech in DO',
    'BZ': 'B Tech in DS',
    'CA': 'CS (AI, Machine Learning)',
    'CB': 'Comp. Sc. and Bus Sys.',
    'CC': 'Computer and Comm. Engg',
    'CD': 'Computer Sc. and Design',
    'CE': 'Civil',
    'CF': 'CS(Artificial Intel.)',
    'CG': 'Computer Science and Tech',
    'CH': 'Chemical',
    'CK': 'Civil Engg (Kannada)',
    'CL': 'B Tech in EO',
    'CM': 'B Tech in EV',
    'CN': 'B Tech in IB',
    'CO': 'Computer Engineering',
    'CQ': 'B Tech in IO',
    'CR': 'Ceramics',
    'CS': 'Computers',
    'CT': 'Const. Tech. Mgmt.',
    'CU': 'B Tech in IS',
    'CV': 'Civil Environment Engg',
    'CW': 'B Tech in IT',
    'CX': 'B Tech in IY',
    'CY': 'CS- Cyber Security',
    'CZ': 'B Tech in LC',
    'DA': 'B Tech in MC',
    'DB': 'B Tech in ME',
    'DC': 'Data Sciences',
    'DD': 'B Tech in MS',
    'DE': 'B Tech in PE',
    'DF': 'B Tech in RA',
    'DG': 'DESIGN',
    'DH': 'B Tech in RAI',
    'DI': 'B Tech in RE',
    'DJ': 'B Tech in RO',
    'DK': 'B Tech in SS',
    'DL': 'B.TECH IN CS',
    'DM': 'B.TECH IN CS NW',
    'DN': 'B.Tech in VLSI',
    'DS': 'Comp. Sc. Engg- Data Sc.',
    'EA': 'Agriculture Engineering',
    'EB': 'EAT',
    'EC': 'Electronics',
    'EE': 'Electrical',
    'EI': 'Elec. Inst. Engg',
    'EL': 'Electronics, Instr. Tech.',
    'EN': 'Environmental',
    'ER': 'Electrical and Computer',
    'ES': 'Electronics and Computer',
    'ET': 'Elec. Telecommn. Engg.',
    'EV': 'EC Engg(VLSI Design)',
    'EZ': 'ELECTRONICS AND COMPUT',
    'IC': 'CS-IoT, Cyber Security',
    'IE': 'Information Science',
    'IM': 'Ind. Engg. Mgmt.',
    'IO': 'CS- Internet of Things',
    'IP': 'Ind.Prodn.',
    'IZ': 'INFORMATION SCIENCE',
    'LA': 'B Plan',
    'LD': 'B Tech in DS',
    'LE': 'B Tech in AIML',
    'LF': 'B Tech in CC',
    'LG': 'B Tech in CS',
    'LH': 'B Tech in IS',
    'LJ': 'B Tech in BS',
    'LK': 'B Tech in IOT',
    'MD': 'Med.Elect.',
    'ME': 'Mechanical',
    'MI': 'Mining Engineering',
    'MK': 'Mechanical Engg (Kannada)',
    'MM': 'Mechanical, Smart Manf.',
    'MR': 'Marine Engineering',
    'MT': 'Mechatronics',
    'OT': 'Industrial IOT',
    'PT': 'Polymer Tech.',
    'RA': 'Robotics and Automation',
    'RI': 'Robotics and AI',
    'RO': 'Auto. And Robot.',
    'SE': 'Aero Space Engg.',
    'ST': 'Silk Tech.',
    'TC': 'Telecommn.',
    'TX': 'Textiles',
    'UP': 'Planning',
    'UR': 'Planning',
    'YA': 'B.TEHIN COM.SCE.ENG(ROBO)',
    'YB': 'B.TECH IN CS.ENG(DAT ANA)',
    'YC': 'B.TECH IN EMD. SYS. VLSI',
    'YD': 'B.TECH IN CS AND ARTI INT',
    'YE': 'B.TECH IN CIV.CONST.SUST.',
    'YF': 'B.TECH IN ELECT ENGG. CS.',
    'YG': 'B.TECH IN EC.ENG.VSLI EMD',
    'YH': 'ENGINEERING DESIGN',
    'YI': 'B.TECH IN MECHANICAL',
    'ZA': 'B TECH IN AERONAUT. ENGG.',
    'ZC': 'CSC',
    'ZH': 'B TECH IN COMP.SC.ART.INT',
    'ZL': 'CIVIL ENG. WITH COMP.APPL',
    'ZM': 'B.TECH IN COMP.SCI. DESI.',
    'ZN': 'B.TECH IN PHARM. ENGG.',
    'ZO': 'B.TECH IN CS.AND BUSI.SYS',
    'ZQ': 'B.TECH IN IN.TCH.DAT.ANAL',
    'ZR': 'COMP.SCE.AND ENG(ART.INT)',
    'ZT': 'B.TECH. IN MECH.SMAR.MANU',
    'ZU': 'CYBER SECURITY',
    'ZV': 'B.TECH.IN INF.TEC.AUG.REA',
    'ZW': 'COMP. SCE. AND ENG (AIML)'}

        self.category_codes = ['1G','1K','1R','2AG','2AK','2AR','2BG','2BK','2BR','3AG','3AK','3AR','3BG','3BK','3BR','GM','GMK','GMR','SCG','SCK','SCR','STG','STK','STR']

    
    def extract_tables_with_tabula(self, pdf_path: str) -> pd.DataFrame:
        """Extract tables using multiple Tabula strategies"""
        print("Extracting tables with improved Tabula settings...")
        
        all_data = []
        
        # Strategy 1: Lattice method with specific area detection
        try:
            tables_lattice = tabula.read_pdf(
                pdf_path, 
                pages='all',
                multiple_tables=True,
                lattice=True,  # Force lattice detection
                pandas_options={'header': None}
            )
            print(f"Lattice method found {len(tables_lattice)} tables")
            
            for table in tables_lattice:
                processed_data = self._process_table_improved(table)
                all_data.extend(processed_data)
                
        except Exception as e:
            print(f"Lattice method failed: {e}")
        
        # Strategy 2: Stream method with better parsing
        try:
            tables_stream = tabula.read_pdf(
                pdf_path,
                pages='all',
                multiple_tables=True,
                stream=True,
                guess=True,  # Let tabula guess the format
                pandas_options={'header': None}
            )
            print(f"Stream method found {len(tables_stream)} tables")
            
            for table in tables_stream:
                processed_data = self._process_table_improved(table)
                all_data.extend(processed_data)
                
        except Exception as e:
            print(f"Stream method failed: {e}")
        
        # Strategy 3: Area-specific extraction (if you know table coordinates)
        try:
            tables_area = tabula.read_pdf(
                pdf_path,
                pages='all',
                multiple_tables=True,
                area=[50, 50, 750, 550],  # Adjust coordinates as needed
                pandas_options={'header': None}
            )
            print(f"Area method found {len(tables_area)} tables")
            
            for table in tables_area:
                processed_data = self._process_table_improved(table)
                all_data.extend(processed_data)
                
        except Exception as e:
            print(f"Area method failed: {e}")
        
        if all_data:
            # Remove duplicates and clean data
            df = pd.DataFrame(all_data)
            df = self._clean_dataframe(df)
            return df
        else:
            return pd.DataFrame()
    
    def _process_table_improved(self, table: pd.DataFrame) -> List[Dict]:
        """Improved table processing with better college detection"""
        if table.empty:
            return []
        
        data_rows = []
        current_college_info = None
        
        for idx, row in table.iterrows():
            # Convert row to text, handling NaN values
            row_cells = [str(cell).strip() for cell in row if pd.notna(cell) and str(cell).strip() != 'nan']
            if not row_cells:
                continue
            
            row_text = ' '.join(row_cells)
            
            # Skip header-like rows
            if self._is_header_row(row_text):
                continue
            
            # Detect college information (E001, E002, etc.)
            college_info = self._extract_college_info(row_text)
            if college_info:
                current_college_info = college_info
                continue
            
            # Process data rows if we have college context
            if current_college_info:
                branch_data = self._extract_branch_data(row_text, current_college_info)
                if branch_data:
                    data_rows.append(branch_data)
        
        return data_rows
    
    def _is_header_row(self, text: str) -> bool:
        """Check if row is a header/category row"""
        header_indicators = ['1G', '1K', '1R', '2AG', '2AK', '2AR', 'GM', 'GMK', 'GMR']
        # If most of the text consists of category codes, it's likely a header
        words = text.split()
        header_count = sum(1 for word in words if word in header_indicators)
        return header_count > len(words) * 0.6
    
    def _extract_college_info(self, text: str) -> Optional[Dict]:
        """Extract college code and name"""
        # Look for pattern: E001, E002, etc. followed by college name
        college_match = re.search(r'(E\d{3})\s+(.+?)(?:\s+[A-Z]{2}\s+|$)', text)
        if college_match:
            code = college_match.group(1)
            name = college_match.group(2).strip()
            # Clean up the name (remove trailing branch codes)
            name = re.sub(r'\s+[A-Z]{2}(\s+[A-Z].*)?$', '', name)
            return {'code': code, 'name': name}
        return None
    
    def _extract_branch_data(self, text: str, college_info: Dict) -> Optional[Dict]:
        """Extract branch and rank data from a row"""
        words = text.split()
        if len(words) < 3:
            return None
        
        # Find branch code
        branch_code = None
        branch_name = None
        start_idx = 0
        
        for i, word in enumerate(words):
            if word in self.branch_codes:
                branch_code = word
                branch_name = self.branch_codes[word]
                start_idx = i + 1
                break
        
        if not branch_code:
            return None
        
        # Extract numerical values (ranks)
        ranks = {}
        rank_values = []
        
        for word in words[start_idx:]:
            if self._is_valid_rank(word):
                try:
                    rank_values.append(int(word))
                except ValueError:
                    rank_values.append(None)
            elif word == '--':
                rank_values.append(None)
        
        # Map ranks to categories
        for i, category in enumerate(self.category_codes):
            if i < len(rank_values):
                ranks[category] = rank_values[i]
            else:
                ranks[category] = None
        
        return {
            'college_code': college_info['code'],
            'college_name': college_info['name'],
            'branch_code': branch_code,
            'branch_name': branch_name,
            **ranks
        }
    
    def _is_valid_rank(self, value: str) -> bool:
        """Check if a string represents a valid rank"""
        try:
            num = int(value)
            return 1 <= num <= 300000  # Reasonable rank range
        except ValueError:
            return False
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate the extracted data"""
        if df.empty:
            return df
        
        # Remove rows with invalid college codes
        df = df[df['college_code'] != 'UNKNOWN']
        
        # Remove duplicate rows
        df = df.drop_duplicates()
        
        # Clean rank columns - ensure they're within reasonable ranges
        rank_columns = self.category_codes
        for col in rank_columns:
            if col in df.columns:
                # Convert to numeric and filter unreasonable values
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].where((df[col] >= 1) & (df[col] <= 300000))
        
        return df
    
    def save_to_csv(self, df: pd.DataFrame, filename: str) -> None:
        """Save cleaned data to CSV"""
        try:
            if not df.empty:
                # Ensure proper column order
                columns_order = ['college_code', 'college_name', 'branch_code', 'branch_name'] + self.category_codes
                df = df.reindex(columns=columns_order)
            
            df.to_csv(filename, index=False)
            print(f"Clean data saved to {filename}")
            print(f"Total records: {len(df)}")
            
            if not df.empty:
                print(f"Unique colleges: {df['college_code'].nunique()}")
                print(f"Unique branches: {df['branch_code'].nunique()}")
            
        except Exception as e:
            print(f"Error saving to CSV: {e}")


def main():
    pdf_path = r"D:\Courses\Global Academy of Technology\kcet-college-pred\src\data\collection\kcet-round0-2024.pdf"
    
    extractor = ImprovedKCETExtractor()
    
    # Extract data with improved method
    df = extractor.extract_tables_with_tabula(pdf_path)
    
    print(f"Extraction complete. Shape: {df.shape}")
    
    if not df.empty:
        print("Sample extracted data:")
        print(df.head())
        
        # Save to CSV
        output_file = "kcet_cutoffs_2024_r0_cleaned1.csv"
        extractor.save_to_csv(df, output_file)
    else:
        print("No meaningful data extracted. Check PDF format and table structure.")


if __name__ == "__main__":
    main()