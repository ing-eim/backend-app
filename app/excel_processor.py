import os
import re
import pandas as pd
import logging
from typing import List, Dict

def process_excel(file_path: str) -> List[Dict]:
    logging.info(f"Procesando archivo Excel: {file_path}")
    try:
        # Detectar si el archivo sigue la nomenclatura OnTime_acumulado_AAAA
        filename = os.path.basename(file_path)
        logging.info(f"{filename}")

        name_only, _ext = os.path.splitext( f"{filename}")
        logging.info(f"{name_only}")

        is_ontime = bool(re.match(r"^temp_OnTime_acumulado_\d{4}$", name_only, re.IGNORECASE))
        logging.info(f"is_ontime: {is_ontime}")
        if is_ontime:
            logging.info(f"Archivo detectado como OnTime, leyendo hoja 'OCT25' desde fila 7, columna 1: {filename}")
            # Verificar que la hoja 'OCT25' exista (case-insensitive)
            xls = pd.ExcelFile(file_path)
            sheet_to_use = None
            for s in xls.sheet_names:
                #logging.info(f"HOJA {s}")
                if s.upper() == 'OCT25':
                    sheet_to_use = s
                    break
            if sheet_to_use is None:
                raise ValueError("Hoja 'OCT25' no encontrada en el archivo Excel")
            # skiprows=6 hace que la lectura comience en la fila 7 (1-based)
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, skiprows=6)
            # Para archivos OnTime la última columna de datos válida es la número 79 (1-based).
            # Recortamos todas las columnas después de la columna 79 y seguimos con la siguiente fila.
            # df.iloc uses 0-based indexing, por lo que usamos :79 para obtener las primeras 79 columnas.
            if df.shape[1] > 79:
                logging.info(f"OnTime file has {df.shape[1]} columns; trimming to 79 columns")
                df = df.iloc[:, :79]
            elif df.shape[1] < 79:
                logging.warning(f"OnTime file {filename} tiene solo {df.shape[1]} columnas; se esperaban al menos 79")
        else:
            df = pd.read_excel(file_path)
        # Omitir filas que inicien con un campo vacío o nulo (primera columna)
        if df.shape[1] > 0:
            first_col = df.columns[0]
            before_count = len(df)
            # máscara: valor no nulo y no vacío al convertir a string y hacer strip
            try:
                non_empty_mask = df[first_col].notnull() & (df[first_col].astype(str).str.strip() != '')
            except Exception:
                # si la conversión a str falla por algún tipo inusual, sólo filtrar nulos
                non_empty_mask = df[first_col].notnull()
            df = df[non_empty_mask]
            after_count = len(df)
            dropped = before_count - after_count
            if dropped > 0:
                logging.info(f"Omitidas {dropped} filas que iniciaban con campo vacío en columna '{first_col}'")

        # Reemplaza NaN, inf y -inf por None para compatibilidad con JSON
        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)
        logging.info(f"Archivo procesado correctamente: {file_path}, filas: {len(df)}")
        return df.to_dict(orient='records')
    except Exception as e:
        logging.error(f"Error procesando archivo Excel {file_path}: {str(e)}")
        raise
