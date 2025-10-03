import pandas as pd
from typing import List, Dict

def process_excel(file_path: str) -> List[Dict]:
    df = pd.read_excel(file_path)
    return df.to_dict(orient='records')
