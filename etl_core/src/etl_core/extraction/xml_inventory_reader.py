import xml.etree.ElementTree as ET
import pandas as pd

def read_inventory_xml(file_path: str) -> pd.DataFrame:
    tree = ET.parse(file_path)
    root = tree.getroot()

    rows = []
    for item in root.findall("item"):
        rows.append({
            "product_id": int(item.find("product_id").text),
            "store_id": int(item.find("store_id").text),
            "quantity_on_hand": int(item.find("quantity_on_hand").text),
            "last_updated": item.find("last_updated").text,
        })

    return pd.DataFrame(rows)
