import pandas as pd

def combine_csv(files, output_file="combined_cutoffs.csv"):
    # Load and combine all CSV files
    dfs = [pd.read_csv(f) for f in files]
    combined_df = pd.concat(dfs, ignore_index=True)

    # Save to output file
    combined_df.to_csv(output_file, index=False)
    print(f"✅ Combined CSV saved as: {output_file}")

if __name__ == "__main__":
    # Replace these with your actual CSV filenames (must be in same folder as script)
    input_files = [
        "comedk_cutoffs1.csv",
        "comedk_cutoffs.csv",
        "kcet_cutoffs_all_streamed.csv",
        "kcet_cutoffs_all_streamed1.csv"
    ]

    combine_csv(input_files, "combined_cutoffs.csv")
