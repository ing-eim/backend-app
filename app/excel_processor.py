import pandas as pd
import logging
from typing import List, Dict

def process_excel(file_path: str) -> List[Dict]:
    logging.info(f"Procesando archivo Excel: {file_path}")
    try:
        df = pd.read_excel(file_path)
        # Reemplaza NaN, inf y -inf por None para compatibilidad con JSON
        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)
        logging.info(f"Archivo procesado correctamente: {file_path}, filas: {len(df)}")
        return df.to_dict(orient='records')
    except Exception as e:
        logging.error(f"Error procesando archivo Excel {file_path}: {str(e)}")
        raise
