import json
import pandas as pd

def read_supplier_json(file_path: str) -> pd.DataFrame:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return pd.DataFrame(data)
