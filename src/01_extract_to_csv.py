from pathlib import Path
import pandas as pd

RAW = Path("data_raw")
OUT = Path("data_intermediate")
OUT.mkdir(exist_ok=True)

def pick_first_nonempty_sheet(xlsx_path: Path) -> str:
    xls = pd.ExcelFile(xlsx_path)
    for s in xls.sheet_names:
        df = pd.read_excel(xlsx_path, sheet_name = s)
        if df.shape[0] > 0 and df.shape[1] > 0:
            return s
    return xls.sheet_names[0]

def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def to_long_from_quarter_cols(df: pd.DataFrame, id_cols, q_cols=("Q1","Q2","Q3","Q4")):
    df = df[id_cols + list(q_cols)].copy()
    df = df.melt(id_vars=id_cols, value_vars=list(q_cols), var_name="quarter", value_name="tons")
    df["quarter"] = df["quarter"].str.replace("Q", "", regex=False).astype(int)
    df["tons"] = pd.to_numeric(df["tons"], errors="coerce").fillna(0.0)
    return df

def to_long_by_unpivot(df: pd.DataFrame, id_cols):
    # Unpivot everything else into "stream" + "tons"
    value_cols = [c for c in df.columns if c not in id_cols]
    df = df.melt(id_vars=id_cols, value_vars=value_cols, var_name="stream", value_name="tons")
    df["tons"] = pd.to_numeric(df["tons"], errors="coerce").fillna(0.0)
    return df

def standardize(df: pd.DataFrame, source: str, entity_col: str, year_col: str, quarter_col: str | None, stream_col: str, tons_col: str):
    df = df.copy()
    df["source"] = source
    df["entity"] = df[entity_col].astype(str)
    df["year"] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")
    df["quarter"] = pd.to_numeric(df[quarter_col], errors="coerce").astype("Int64") if quarter_col else df["quarter"].astype("Int64")
    df["stream"] = df[stream_col].astype(str)
    df["tons"] = pd.to_numeric(df[tons_col], errors="coerce").fillna(0.0)

    df = df.dropna(subset=["year","quarter"])
    df["yearquarter"] = df["year"].astype(int).astype(str) + "-Q" + df["quarter"].astype(int).astype(str)

    return df[["year","quarter","yearquarter","source","entity","stream","tons"]]

CFG = {
    "rdrs_1": {
        "file": RAW / "rdrs_1.xlsx",
        "sheet": None,            # set to sheet name if needed
        "year_col": "Year",
        "quarter_col": "Quarter",
        "entity_col": "Jurisdiction",  # change after inspection
        "id_cols": ["Year","Quarter","Jurisdiction"],  # change after inspection
        "unpivot": "streams",      # "streams" means melt non-id columns into stream/tons
        "source": "RDRS_1_Jurisdiction"
    },
    "rdrs_8": {
        "file": RAW / "rdrs_8.xlsx",
        "sheet": None,
        "year_col": "Year",
        "quarter_col": "Quarter",
        "entity_value": "California",
        "id_cols": ["Year","Quarter"],  # usually just these; entity is constant
        "unpivot": "streams",
        "source": "RDRS_8_Statewide"
    }
}

def extract_rdrs_1():
    c = CFG["rdrs_1"]
    sheet = c["sheet"] or pick_first_nonempty_sheet(c["file"])
    df = clean_cols(pd.read_excel(c["file"], sheet_name=sheet))

    df_long = to_long_by_unpivot(df, id_cols=c["id_cols"])

    # clean stream names (optional but highly recommended)
    df_long["stream"] = (
        df_long["stream"]
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.rstrip(".")
    )

    df_std = standardize(
        df_long, c["source"],
        entity_col=c["entity_col"],
        year_col=c["year_col"],
        quarter_col=c["quarter_col"],
        stream_col="stream",
        tons_col="tons"
    )
    df_std.to_csv(OUT / "rdrs_1_long.csv", index=False)


def extract_rdrs_8():
    c = CFG["rdrs_8"]
    sheet = c["sheet"] or pick_first_nonempty_sheet(c["file"])
    df = clean_cols(pd.read_excel(c["file"], sheet_name=sheet))
    df["Entity"] = c["entity_value"]
    id_cols = c["id_cols"] + ["Entity"]
    df_long = to_long_by_unpivot(df, id_cols=id_cols)
    df_long["stream"] = (
        df_long["stream"]
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    df_std = standardize(
        df_long, c["source"],
        entity_col="Entity",
        year_col=c["year_col"],
        quarter_col=c["quarter_col"],
        stream_col="stream",
        tons_col="tons"
    )
    df_std.to_csv(OUT / "rdrs_8_long.csv", index=False)

def extract_rdrs_3_folder():
    folder = RAW / "rdrs_3"
    all_rows = []

    for p in sorted(folder.glob("*.xlsx")):
        sheet = pick_first_nonempty_sheet(p)
        df = clean_cols(pd.read_excel(p, sheet_name=sheet))

        # exact columns from your file
        entity_col = "Reporting Entity ( RDRS #)"
        entity_id_col = "RDRSID"
        stream_col = "Total Tons by Material Stream"
        year_col = "Year"
        q_cols = ["Q1", "Q2", "Q3", "Q4"]

        # keep only what we need (ignore %Change and Tons Difference columns)
        keep = [entity_col, entity_id_col, stream_col, year_col] + q_cols
        df = df[keep].copy()

        # drop empty rows
        df = df.dropna(subset=[entity_col, stream_col, year_col])

        # wide Q1..Q4 -> long
        df_long = df.melt(
            id_vars=[entity_col, entity_id_col, stream_col, year_col],
            value_vars=q_cols,
            var_name="quarter",
            value_name="tons"
        )

        # quarter "Q1"->1 etc
        df_long["quarter"] = df_long["quarter"].str.replace("Q", "", regex=False).astype(int)

        # clean numeric
        df_long["tons"] = pd.to_numeric(df_long["tons"], errors="coerce").fillna(0.0)

        # build standard columns
        df_long["source"] = "RDRS_3_Facility"
        df_long["year"] = pd.to_numeric(df_long[year_col], errors="coerce").astype("Int64")
        df_long = df_long.dropna(subset=["year"])
        df_long["yearquarter"] = df_long["year"].astype(int).astype(str) + "-Q" + df_long["quarter"].astype(str)

        # make entity stable + unique (name + id)
        df_long["entity"] = (
            df_long[entity_col].astype(str).str.strip()
            + " (RDRSID " + df_long[entity_id_col].astype(str).str.strip() + ")"
        )

        # stream is already provided in this report
        df_long["stream"] = (
            df_long[stream_col]
            .astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.rstrip(".")
        )

        out = df_long[["year","quarter","yearquarter","source","entity","stream","tons"]]
        all_rows.append(out)

    out_all = pd.concat(all_rows, ignore_index=True)
    out_all = out_all[out_all["tons"] != 0].copy()
    out_all.to_csv(OUT / "rdrs_3_long.csv", index=False)


if __name__ == "__main__":
    extract_rdrs_1()
    extract_rdrs_8()
    extract_rdrs_3_folder()
    print("Wrote:", list(OUT.glob("*.csv")))