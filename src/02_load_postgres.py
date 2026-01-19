import pandas as pd
import psycopg2
from pathlib import Path

OUT = Path("data_intermediate")
conn = psycopg2.connect(host="localhost", dbname="rdrs", user="postgres", password="postgres", port=5432)

def load_csv(csv_path):
    df = pd.read_csv(csv_path)
    with conn, conn.cursor() as cur:
        # insert using COPY via psycopg2
        import io
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        cur.copy_expert(
            "COPY staging.rdrs_long (year,quarter,yearquarter,source,entity,stream,tons) FROM STDIN WITH (FORMAT csv)",
            buf
        )

if __name__ == "__main__":
    for p in ["rdrs_1_long.csv", "rdrs_8_long.csv", "rdrs_3_long.csv"]:
        load_csv(OUT / p)
    print("Loaded staging.rdrs_long")
