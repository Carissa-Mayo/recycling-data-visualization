from pathlib import Path
import pandas as pd

RAW = Path("data_raw")

def inspect_xlsx(path: Path, n = 5):
    xls = pd.ExcelFile(path)
    print(f"\n=== {path.name} ===")
    print("Sheets:", xls.sheet_names)
    for s in xls.sheet_names[:3]:
        df = pd.read_excel(path, sheet_name=s)
        print(f"\n-- Sheet: {s} --")
        print("Shape:", df.shape)
        print("Columns:", list(df.columns))
        print(df.head(n))

inspect_xlsx(RAW / "rdrs_1.xlsx")
inspect_xlsx(RAW / "rdrs_8.xlsx")

for p in sorted((RAW / "rdrs_3").glob("*.xlsx")):
    inspect_xlsx(p, n=3)
    break