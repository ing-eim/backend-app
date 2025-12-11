import os
import re
import unicodedata
import pandas as pd
import logging
from app.logging_utils import SizeAndTimedRotatingFileHandler, ensure_logs_dir
from typing import List, Dict, Optional
from sqlalchemy import text
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime as _dt
import re


# Module logging: rely on the application to configure handlers for the
# 'operations' logger (avoid creating handlers here to prevent duplicate
# file handles which cause rotation failures on Windows).
ops_logger = logging.getLogger('operations')


def _bulk_insert_with_fallback(db, insert_sql: str, all_params_list: list, table_name: str = "tabla") -> int:
    """
    Función helper para ejecutar BULK INSERT optimizado con fallback automático.
    
    Estrategia de 3 niveles:
    1. fast_executemany con pyodbc (óptimo, 10-20x más rápido)
    2. SQLAlchemy execute con lista de parámetros (rápido, 3-5x más rápido)
    3. Row-by-row tradicional (lento pero garantizado)
    
    Args:
        db: Sesión de base de datos SQLAlchemy
        insert_sql: SQL INSERT con named parameters (:param)
        all_params_list: Lista de diccionarios con parámetros
        table_name: Nombre de la tabla para logging
    
    Returns:
        Número de registros insertados
    """
    if len(all_params_list) == 0:
        ops_logger.warning(f"{table_name}: No hay registros para insertar")
        return 0
    
    ops_logger.info(f"{table_name}: Ejecutando BULK INSERT de {len(all_params_list)} registros...")
    bulk_success = False
    total_inserted = 0
    
    # NIVEL 1: Intentar fast_executemany de pyodbc
    try:
        raw_conn = None
        if hasattr(db, 'bind'):
            engine = db.bind
            raw_conn = engine.raw_connection()
        elif hasattr(db, 'connection'):
            conn_obj = db.connection()
            if hasattr(conn_obj, 'connection'):
                raw_conn = conn_obj.connection
        
        if raw_conn is not None:
            cursor = raw_conn.cursor()
            cursor.fast_executemany = True
            
            # Convertir SQL de named params (:param) a placeholders (?)
            import_sql_bulk = insert_sql
            param_names = []
            for match in re.finditer(r':(\w+)', insert_sql):
                param_names.append(match.group(1))
            
            # Reemplazar :param con ?
            insert_sql_bulk = re.sub(r':\w+', '?', insert_sql)
            
            # Convertir diccionarios a tuplas en el orden correcto
            values_list = []
            for params in all_params_list:
                values_tuple = tuple(params.get(pname) for pname in param_names)
                values_list.append(values_tuple)
            
            ops_logger.info(f"{table_name}: Usando fast_executemany con lotes de 500...")
            batch_size_bulk = 500
            
            for batch_start in range(0, len(values_list), batch_size_bulk):
                batch_end = min(batch_start + batch_size_bulk, len(values_list))
                batch_values = values_list[batch_start:batch_end]
                
                try:
                    cursor.executemany(insert_sql_bulk, batch_values)
                    total_inserted += len(batch_values)
                    
                    if batch_end % 5000 == 0 or batch_end == len(values_list):
                        ops_logger.info(f"{table_name}: fast_executemany {batch_end}/{len(values_list)} registros...")
                except MemoryError:
                    ops_logger.error(f"{table_name}: MemoryError en lote {batch_start}-{batch_end}, abortando fast_executemany")
                    del values_list
                    del batch_values
                    import gc
                    gc.collect()
                    raise
            
            raw_conn.commit()
            bulk_success = True
            ops_logger.info(f"{table_name}: BULK INSERT (fast_executemany) completado: {total_inserted} registros")
            return total_inserted
        else:
            ops_logger.debug(f"{table_name}: No se pudo obtener raw_connection de pyodbc")
    except Exception as bulk_err:
        import traceback
        ops_logger.warning(f"{table_name}: fast_executemany falló: {str(bulk_err)[:200]}")
        ops_logger.debug(traceback.format_exc())
    
    # NIVEL 2: SQLAlchemy execute con lista de parámetros
    if not bulk_success:
        ops_logger.info(f"{table_name}: Usando SQLAlchemy bulk execute...")
        try:
            chunk_size = 2000
            for i in range(0, len(all_params_list), chunk_size):
                chunk = all_params_list[i:i + chunk_size]
                db.execute(text(insert_sql), chunk)
                
                actual_processed = min(i + chunk_size, len(all_params_list))
                if actual_processed % 10000 == 0 or actual_processed == len(all_params_list):
                    ops_logger.info(f"{table_name}: Bulk execute {actual_processed}/{len(all_params_list)} filas...")
            
            total_inserted = len(all_params_list)
            ops_logger.info(f"{table_name}: SQLAlchemy bulk execute completado: {total_inserted} registros")
            return total_inserted
        except Exception as bulk_fallback_err:
            ops_logger.warning(f"{table_name}: Bulk execute falló: {str(bulk_fallback_err)[:200]}, usando row-by-row...")
    
    # NIVEL 3: Row-by-row (último recurso)
    ops_logger.info(f"{table_name}: Usando INSERT row-by-row...")
    for idx, params in enumerate(all_params_list, start=1):
        db.execute(text(insert_sql), params)
        
        if idx % 5000 == 0 or idx == len(all_params_list):
            ops_logger.info(f"{table_name}: Row-by-row {idx}/{len(all_params_list)} filas...")
    
    total_inserted = len(all_params_list)
    ops_logger.info(f"{table_name}: INSERT row-by-row completado: {total_inserted} registros")
    return total_inserted

def process_excel(file_path: str, db: Optional[object] = None, username: Optional[str] = None) -> List[Dict]:
    """Process an Excel file and optionally send OnTime rows to a stored procedure.

    Args:
        file_path: path to the Excel file
        db: optional SQLAlchemy Session. If provided and file is OnTime, each record
            will be sent to dbo.sp_proc_registros in the expected parameter order.

    Returns:
        List of record dicts parsed from the file.
    """
    logging.getLogger('operations').info(f"Procesando archivo Excel: {file_path}")
    try:
        logging.getLogger('operations').info(f"Procesando archivo Excel: {file_path}")
    except Exception:
        pass
    try:
        # Detectar si el archivo sigue la nomenclatura OnTime_acumulado_AAAA
        filename = os.path.basename(file_path)
        logging.getLogger('operations').info(f"{filename}")

        name_only, _ext = os.path.splitext( f"{filename}")
        logging.getLogger('operations').info(f"{name_only}")

        is_ontime = bool(re.match(r"^temp_OnTime_acumulado_\d{4}$", name_only, re.IGNORECASE))
        logging.getLogger('operations').info(f"is_ontime: {is_ontime}")
        if is_ontime:
            logging.getLogger('operations').info(f"Archivo detectado como OnTime, buscando hojas con formato MMMYY que contengan columna DATAWERHOUSE: {filename}")
            # Buscar hojas que cumplan:
            # 1. Formato MMMYY (3 letras del mes + 2 dígitos del año)
            # 2. Contengan la columna "DATAWERHOUSE"
            
            # Pattern para detectar hojas MMMYY (ej: OCT25, ENE24, DIC25)
            month_year_pattern = re.compile(r'^[A-Z]{3}\d{2}$', re.IGNORECASE)
            
            sheets_to_process = []
            # Abrir el archivo Excel una sola vez y mantenerlo abierto para todas las lecturas
            with pd.ExcelFile(file_path, engine='openpyxl') as xls:
                for sheet_name in xls.sheet_names:
                    # Verificar si cumple con el patrón MMMYY
                    if month_year_pattern.match(sheet_name.strip()):
                        # Leer preview de la hoja para verificar si contiene DATAWERHOUSE
                        try:
                            # Usar el objeto xls abierto en lugar de file_path para evitar reabrir
                            df_preview = pd.read_excel(xls, sheet_name=sheet_name, skiprows=6, nrows=0)
                            # Normalizar nombres de columnas para búsqueda
                            normalized_cols = [re.sub(r"\s+", " ", str(c).replace('\xa0', ' ')).strip().upper() for c in df_preview.columns]
                            if 'DATAWERHOUSE' in normalized_cols:
                                sheets_to_process.append(sheet_name)
                                logging.getLogger('operations').info(f"Hoja '{sheet_name}' cumple criterios: formato MMMYY y contiene DATAWERHOUSE")
                            else:
                                logging.getLogger('operations').info(f"Hoja '{sheet_name}' tiene formato MMMYY pero no contiene columna DATAWERHOUSE")
                        except Exception as preview_err:
                            logging.getLogger('operations').warning(f"No se pudo verificar hoja '{sheet_name}': {preview_err}")
            
            if not sheets_to_process:
                raise ValueError("No se encontraron hojas con formato MMMYY que contengan la columna DATAWERHOUSE")
            
            logging.getLogger('operations').info(f"Se procesarán {len(sheets_to_process)} hoja(s): {', '.join(sheets_to_process)}")
            
            # Procesar todas las hojas que cumplan los criterios
            all_records = []
            all_columns = None
            
            # Abrir el archivo una vez y leer todas las hojas
            with pd.ExcelFile(file_path, engine='openpyxl') as xls:
                for sheet_to_use in sheets_to_process:
                    logging.getLogger('operations').info(f"Procesando hoja: '{sheet_to_use}'")
                    
                    # skiprows=6 hace que la lectura comience en la fila 7 (1-based)
                    # Usar el objeto xls abierto para evitar reabrir el archivo
                    df_sheet = pd.read_excel(xls, sheet_name=sheet_to_use, skiprows=6)
                    
                    # Para archivos OnTime la última columna de datos válida es la número 79 (1-based).
                    # Recortamos todas las columnas después de la columna 79 y seguimos con la siguiente fila.
                    # df.iloc uses 0-based indexing, por lo que usamos :79 para obtener las primeras 79 columnas.
                    if df_sheet.shape[1] > 79:
                        logging.getLogger('operations').info(f"Hoja '{sheet_to_use}' tiene {df_sheet.shape[1]} columnas; recortando a 79 columnas")
                        df_sheet = df_sheet.iloc[:, :79]
                    elif df_sheet.shape[1] < 79:
                        logging.getLogger('operations').warning(f"Hoja '{sheet_to_use}' tiene solo {df_sheet.shape[1]} columnas; se esperaban al menos 79")
                    
                    # Omitir filas que inicien con un campo vacío o nulo (primera columna)
                    if df_sheet.shape[1] > 0:
                        first_col = df_sheet.columns[0]
                        before_count = len(df_sheet)
                        # máscara: valor no nulo y no vacío al convertir a string y hacer strip
                        try:
                            non_empty_mask = df_sheet[first_col].notnull() & (df_sheet[first_col].astype(str).str.strip() != '')
                        except Exception:
                            # si la conversión a str falla por algún tipo inusual, sólo filtrar nulos
                            non_empty_mask = df_sheet[first_col].notnull()
                        df_sheet = df_sheet[non_empty_mask]
                        after_count = len(df_sheet)
                        dropped = before_count - after_count
                        if dropped > 0:
                            logging.getLogger('operations').info(f"Hoja '{sheet_to_use}': omitidas {dropped} filas que iniciaban con campo vacío en columna '{first_col}'")
                    
                    # Reemplaza NaN, inf y -inf por None para compatibilidad con JSON
                    df_sheet = df_sheet.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
                    df_sheet = df_sheet.where(pd.notnull(df_sheet), None)
                    
                    sheet_records = df_sheet.to_dict(orient='records')
                    all_records.extend(sheet_records)
                    
                    # Guardar las columnas de la primera hoja procesada para mapping
                    if all_columns is None:
                        all_columns = list(df_sheet.columns)
                    
                    logging.getLogger('operations').info(f"Hoja '{sheet_to_use}' procesada: {len(sheet_records)} filas")
            
            logging.getLogger('operations').info(f"Total de registros combinados de todas las hojas: {len(all_records)}")
            
            # Usar los registros y columnas combinados
            records = all_records
            original_columns = all_columns
            
            # Crear un DataFrame consolidado para mantener compatibilidad con el resto del código
            df = pd.DataFrame(records) if records else pd.DataFrame()
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
                    logging.getLogger('operations').info(f"Omitidas {dropped} filas que iniciaban con campo vacío en columna '{first_col}'")

            # Reemplaza NaN, inf y -inf por None para compatibilidad con JSON
            df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
            df = df.where(pd.notnull(df), None)
            logging.getLogger('operations').info(f"Archivo procesado correctamente: {file_path}, filas: {len(df)}")
            records = df.to_dict(orient='records')
            # Keep a copy of original columns for mapping; we'll support fuzzy header matching
            original_columns = list(df.columns)

        def _write_acumulado_file(target_file_path: str, name_only_local: str, count: int) -> None:
            """Write acumulado_<AAAA>.txt with the number of rows (count) next to the uploaded file.

            The year is inferred from the filename (trailing 4 digits) or falls back to current year.
            """
            try:
                # try to get year from filename like OnTime_acumulado_2025
                ym = re.search(r"(\d{4})$", name_only_local)
                if ym:
                    year = ym.group(1)
                else:
                    year = str(_dt.now().year)
                out_dir = os.path.dirname(target_file_path) or '.'
                out_path = os.path.join(out_dir, f"acumulado_{year}.txt")
                with open(out_path, 'w', encoding='utf-8') as fh:
                    fh.write(str(int(count)))
                logging.getLogger('operations').info(f"Escribido acumulado: {out_path} con {count} filas")
            except Exception as write_err:
                logging.getLogger('operations').error(f"No se pudo escribir acumulado_{year}.txt: {write_err}")

        def normalize_name(s: str) -> str:
            if s is None:
                return ''
            # replace non-breaking spaces, collapse whitespace, strip and uppercase
            return re.sub(r"\s+", " ", str(s).replace('\xa0', ' ')).strip().upper()

        # If OnTime and a DB session was provided, send each record to the stored procedure
        if is_ontime and db is not None and len(records) > 0:
            # Columns order expected by the stored procedure (must match exactly)
            ordered_cols = [
                "FECHA DE OFERTA",
                "# viaje",
                "T.UNIDAD",
                "CONCEPTO",
                "ESTATUS",
                "CLIENTE",
                "ORIGEN",
                "DESTINO",
                "DIRECCION DE CARGA",
                "CITA DE CARGA",
                "HORA CARGA",
                "CLIENTE DESTINO",
                "DIRECCION DE DESCARGA",
                "CITA DESCARGA",
                "HORA DESCARGA",
                "NUMERO DE CARGA",
                "CONFIRMACION CITA",
                "ECONOMICOS",
                "OPERADOR",
                "CELULAR",
                "LINEA",
                "correo ccp",
                "PLATAFORMA ejecutiva",
                "PLATAFORMA  monitoreo",
                "link TRACTO",
                "usuario",
                "contraseña",
                "Eco. unidad",
                "LINK CAJA",
                "USUARIO CAJA",
                "CONTRASEÑA CAJA",
                "Eco. CAJA",
                "TARIFA TRANSP.",
                "ACCESORIOS TRANSP",
                "IVA",
                "RETENCION",
                "TOTAL .L",
                "TARIFA CLIENTE",
                "ACCESORIOS CTE",
                "IVA CTE",
                "RETENCION CTE",
                "TOTAL CLIENTE",
                "UTILIDAD",
                "%",
                "REVISION ADELA",
                "NOMBRE EJECUTIVA",
                "DATAWERHOUSE",
                "COMENTARIO DEL ACCESORIO (OPCIONAL)",
                "ESTADIAS",
                "MANIOBRAS",
                "REPARTO",
                "DIF DE FLETE",
                "PISTAS",
                "PENSION",
                "MOV EN FALSO",
                "COBRO X LOG INV",
                "RECHAZO TOTAL",
                "DEVOLUCIONES",
                "FALTANTES",
                "SOBRANTES",
                "VTA CANCELADA",
                "INCIDENCIA MONITOREO",
                "INCIDENCIA EJECUTIVA",
                "INCIDENCIA LINEA TRANSPORTE",
                "INCIDENCIA CLIENTE",
                "INCIDENCIA PLATAFORMA",
                "LLEGADA A CARGAR",
                "SALIDA DE CARGA",
                "LLEGADA A DESCARGA",
                "SALIDA DESCARGA",
                "HORAS CARGA",
                "HORAS DESCARGA",
            ]

            param_names = [f"p{i+1}" for i in range(len(ordered_cols))]
            exec_sql = "EXEC dbo.sp_ins_ontime " + ", ".join(f":{n}" for n in param_names)

            # Build normalized map of record keys to original keys for fuzzy lookup
            normalized_key_map = {normalize_name(k): k for k in original_columns}

            # Filter records: only insert rows where DATAWERHOUSE contains "CERRADO ddmmmYY" pattern
            # Pattern: CERRADO followed by 2-digit day, 3-letter month, 2-digit year (e.g., "CERRADO 10NOV24")
            datawerhouse_pattern = re.compile(r"CERRADO\s+\d{2}[A-Z]{3}\d{2}", re.IGNORECASE)
            
            # Find the DATAWERHOUSE column (try exact match first, then normalized)
            datawerhouse_col = None
            if "DATAWERHOUSE" in original_columns:
                datawerhouse_col = "DATAWERHOUSE"
            else:
                normalized_dw = normalize_name("DATAWERHOUSE")
                if normalized_dw in normalized_key_map:
                    datawerhouse_col = normalized_key_map[normalized_dw]
            
            # Apply filter
            original_count = len(records)
            if datawerhouse_col:
                filtered_records = [
                    rec for rec in records
                    if datawerhouse_col in rec and 
                    rec[datawerhouse_col] is not None and
                    datawerhouse_pattern.search(str(rec[datawerhouse_col]))
                ]
                records = filtered_records
                filtered_count = len(records)
                ops_logger.info(f"OnTime filter: {original_count} records read, {filtered_count} match DATAWERHOUSE pattern 'CERRADO ddmmmYY', {original_count - filtered_count} skipped")
            else:
                ops_logger.warning("DATAWERHOUSE column not found, inserting all records without filter")

            # Single-transaction strategy: execute SP for all rows and commit once.
            # Log the target database and current user for debugging where inserts go
            try:
                try:
                    db_name = db.execute(text("SELECT DB_NAME()")).scalar()
                except Exception:
                    db_name = None
                try:
                    db_user = db.execute(text("SELECT SUSER_SNAME()")).scalar()
                except Exception:
                    db_user = None
                logging.getLogger('operations').info(f"DB context: DB_NAME={db_name}, SUSER_SNAME={db_user}")
            except Exception:
                pass
            
            # Pre-compilar conjuntos para verificaciones rápidas
            date_columns_set = {
                "FECHA DE OFERTA", "CITA DE CARGA", "CITA DESCARGA",
                "LLEGADA A CARGAR", "SALIDA DE CARGA", "LLEGADA A DESCARGA", "SALIDA DESCARGA"
            }
            numeric_columns_set = {
                "TARIFA TRANSP.", "ACCESORIOS TRANSP", "IVA", "RETENCION", "TOTAL .L",
                "TARIFA CLIENTE", "ACCESORIOS CTE", "IVA CTE", "RETENCION CTE", "TOTAL CLIENTE",
                "UTILIDAD", "%"
            }
            
            current_idx = None
            try:
                total_affected = 0
                
                # Preparar todos los parámetros en una lista para BULK INSERT
                ops_logger.info("Preparando datos para BULK INSERT...")
                all_params_list = []
                
                for idx, rec in enumerate(records, start=1):
                    current_idx = idx
                    # prepare parameters by position
                    params = {}
                    for i, col in enumerate(ordered_cols):
                        # try exact match first; if not present, try normalized header match
                        val = rec.get(col)
                        if val is None:
                            nk = normalize_name(col)
                            if nk in normalized_key_map:
                                val = rec.get(normalized_key_map[nk])

                        # Convert pandas Timestamp to python datetime
                        try:
                            if hasattr(val, "to_pydatetime"):
                                val = val.to_pydatetime()
                        except Exception:
                            pass

                        # Normalize empty strings and whitespace
                        if isinstance(val, str):
                            val = val.replace('\xa0', ' ').strip()
                            if val == '':
                                val = None

                        # Date/datetime columns that must be parsed to a datetime
                        if val is not None and col in date_columns_set:
                            # If it's already a datetime/date, keep it; else try to parse
                            try:
                                if isinstance(val, _dt):
                                    pass
                                else:
                                    val = val.replace('.', ':')
                                    val = val.replace('hrs', '')
                                    parsed = pd.to_datetime(val, errors='coerce', dayfirst=True)
                                    if not pd.isna(parsed):
                                        val = parsed.to_pydatetime()
                                    else:
                                        # log inability to parse; leave as-is so SP will error explicitly
                                        logging.debug(f"No se pudo parsear fecha para columna {col}: {val}")
                            except Exception:
                                logging.debug(f"Exception parsing date for column {col}: {val}")

                        # Columns that must be numeric in the DB - try to coerce
                        if val is not None and col in numeric_columns_set:
                            # attempt to sanitize and convert strings to Decimal
                            if isinstance(val, (int, float, Decimal)):
                                try:
                                    val = Decimal(str(val))
                                except (InvalidOperation, Exception):
                                    pass
                            else:
                                s = re.sub(r"[^0-9.,\-]", "", str(val))
                                if s == '':
                                    val = None
                                else:
                                    try:
                                        if s.count(',') > 0 and s.count('.') == 0:
                                            s = s.replace(',', '.')
                                        elif s.count(',') > 0 and s.count('.') > 0:
                                            if s.rfind('.') > s.rfind(','):
                                                s = s.replace(',', '')
                                            else:
                                                s = s.replace('.', '').replace(',', '.')
                                        val = Decimal(s)
                                        # Quantize to 2 decimals for DECIMAL(12,2) fields
                                        try:
                                            val = val.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                                        except Exception:
                                            pass
                                    except (InvalidOperation, Exception):
                                        try:
                                            val = float(s.replace(',', '.'))
                                        except Exception:
                                            pass

                        params[param_names[i]] = val

                    # Final normalization: convert floats to Decimal (quantized), normalize common placeholders to None
                    for pk, pv in list(params.items()):
                        # Treat common placeholder strings as NULL
                        if isinstance(pv, str) and pv.strip() in {'', '-', 'NA', 'N/A', 'NONE'}:
                            params[pk] = None
                            continue
                        # Convert floats to Decimal with 2 decimal places
                        if isinstance(pv, float):
                            try:
                                d = Decimal(str(pv)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                                params[pk] = d
                            except Exception:
                                pass
                        elif isinstance(pv, Decimal):
                            try:
                                params[pk] = pv.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                            except Exception:
                                pass
                    
                    all_params_list.append(params)
                    
                    if idx % 500 == 0:
                        logging.getLogger('operations').info(f"Preparadas {idx}/{len(records)} filas...")
                
                # Ejecutar BULK INSERT usando executemany
                ops_logger.info(f"Ejecutando BULK INSERT de {len(all_params_list)} registros...")
                
                # Procesar en lotes de 1000 para evitar límites de parámetros
                batch_size = 1000
                for batch_start in range(0, len(all_params_list), batch_size):
                    batch_end = min(batch_start + batch_size, len(all_params_list))
                    batch_params = all_params_list[batch_start:batch_end]
                    
                    # Ejecutar el SP para cada registro en el lote
                    for params in batch_params:
                        db.execute(text(exec_sql), params)
                    
                    ops_logger.info(f"Lote procesado: {batch_start+1} a {batch_end} de {len(all_params_list)}")
                
                ops_logger.info("BULK INSERT completado exitosamente")

                # If we reach here, all executions for dbo.sp_ins_ontime succeeded.
                # If a username was provided, call dbo.sp_procesa_ontime_complet with the user and processed filename
                try:
                    if username:
                        processed_name = name_only
                        if processed_name.lower().startswith('temp_'):
                            processed_name = processed_name[5:]
                        sp2_sql = "EXEC dbo.sp_procesa_ontime_complet :nombre_usuario, :name_file_procesado"
                        logging.getLogger('operations').info(f"Ejecutando procedure para usuario={username}, archivo={processed_name}")
                        db.execute(text(sp2_sql), {"nombre_usuario": username, "name_file_procesado": processed_name})
                        try:
                            sp2_affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
                        except Exception:
                            sp2_affected = None
                        logging.getLogger('operations').info(f"Procedure @@ROWCOUNT={sp2_affected}")
                    else:
                        logging.getLogger('operations').info("No se proporcionó nombre de usuario; se omite la ejecución de procedure")
                except Exception as sp2_err:
                    logging.getLogger('operations').error(f"Error ejecutando procedure: {sp2_err}")
                    raise

                # -- NEW: after processing OCT25, look for a PPTO sheet for current year (e.g. 'PPTO 25')
                try:
                    yy2 = str(_dt.now().year % 100).zfill(2)
                    ppto_sheet = None
                    with pd.ExcelFile(file_path) as xls:
                        for s in xls.sheet_names:
                            if re.match(rf'^PPTO\s*{yy2}$', str(s).strip(), re.IGNORECASE):
                                ppto_sheet = s
                                break

                    if ppto_sheet:
                        logging.getLogger('operations').info(f"Se encontró hoja PPTO: '{ppto_sheet}'. Comprobando bitácora antes de insertar en dbo.presupuesto_tmp")
                        # Check mi_bitacora_operaciones for prior load of this sheet name
                        try:
                            cnt = db.execute(text("SELECT COUNT(1) FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :sheetname"), {"sheetname": ppto_sheet}).scalar()

                            logging.getLogger('operations').info(f"Bitácora operaciones: {cnt} registros encontrados para la hoja '{ppto_sheet}'")
                        except Exception:
                            cnt = None

                        if cnt is not None and int(cnt) > 0:
                            logging.getLogger('operations').info(f"La hoja '{ppto_sheet}' ya figura en dbo.mi_bitacora_operaciones (count={cnt}), se omite la carga de presupuesto.")
                        else:
                            # Read PPTO sheet and map to presupuesto_tmp
                            try:
                                df_p = pd.read_excel(file_path, sheet_name=ppto_sheet, header=0)
                                logging.getLogger('operations').info(f"Reading presupuesto sheet '{ppto_sheet}' with header=0")
                            except Exception as read_p_err:
                                logging.getLogger('operations').error(f"No se pudo leer la hoja {ppto_sheet}: {read_p_err}")
                                raise

                            df_p = df_p.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
                            df_p = df_p.where(pd.notnull(df_p), None)

                            # Omitir filas donde la primera columna esté vacía
                            if df_p.shape[1] > 0:
                                first_col_p = df_p.columns[0]
                                before_p = len(df_p)
                                try:
                                    non_empty_mask_p = df_p[first_col_p].notnull() & (df_p[first_col_p].astype(str).str.strip() != '')
                                except Exception:
                                    non_empty_mask_p = df_p[first_col_p].notnull()
                                df_p = df_p[non_empty_mask_p]
                                dropped_p = before_p - len(df_p)
                                if dropped_p > 0:
                                    logging.getLogger('operations').info(f"Presupuesto: omitidas {dropped_p} filas que iniciaban con campo vacío en columna '{first_col_p}'")

                            records_p = df_p.to_dict(orient='records')

                            insert_sql_p = (
                                "INSERT INTO dbo.presupuesto_tmp (Mes, Anio, Venta_Anio_Anterior, Escenario_Pesimista, Escenario_Conservador, Escenario_Optimista, Usuario_Creacion, Fecha_Creacion) "
                                "VALUES (:Mes, :Anio, :Venta_Anio_Anterior, :Escenario_Pesimista, :Escenario_Conservador, :Escenario_Optimista, :Usuario_Creacion, :Fecha_Creacion)"
                            )

                            # helper to coerce numeric to Decimal
                            def _to_decimal(v):
                                if v is None:
                                    return None
                                try:
                                    if isinstance(v, (int, float, Decimal)):
                                        d = Decimal(str(v))
                                        return d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                                    s = re.sub(r"[^0-9.,\-]", "", str(v))
                                    if s == '':
                                        return None
                                    if s.count(',') > 0 and s.count('.') == 0:
                                        s = s.replace(',', '.')
                                    elif s.count(',') > 0 and s.count('.') > 0:
                                        if s.rfind('.') > s.rfind(','):
                                            s = s.replace(',', '')
                                        else:
                                            s = s.replace('.', '').replace(',', '.')
                                    d = Decimal(s)
                                    return d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                                except Exception:
                                    try:
                                        return Decimal(str(float(str(v).replace(',', '.')))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                                    except Exception:
                                        return None

                            # Build normalized header map for scenario columns
                            norm_map_p = {normalize_name(c): c for c in list(df_p.columns)}
                            h_esc_pes = normalize_name('ESCENARIO PESIMISTA')
                            h_esc_cons = normalize_name('ESCENARIO CONSERVADOR')
                            h_esc_opt = normalize_name('ESCENARIO OPTIMISTA')

                            total_inserted_p = 0
                            current_year = _dt.now().year
                            for idx_p, rec_p in enumerate(records_p, start=1):
                                params_p = {
                                    'Mes': None,
                                    'Anio': current_year,
                                    'Venta_Anio_Anterior': None,
                                    'Escenario_Pesimista': None,
                                    'Escenario_Conservador': None,
                                    'Escenario_Optimista': None,
                                    'Usuario_Creacion': username,
                                    'Fecha_Creacion': None
                                }

                                # Mes from first physical column
                                try:
                                    mval = rec_p.get(first_col_p)
                                except Exception:
                                    mval = None
                                if isinstance(mval, str):
                                    mval = mval.replace('\xa0', ' ').strip()
                                    if mval == '':
                                        mval = None
                                params_p['Mes'] = mval

                                # Venta Año Anterior -> second column if present
                                if df_p.shape[1] >= 2:
                                    sec_col = df_p.columns[1]
                                    v = rec_p.get(sec_col)
                                    params_p['Venta_Anio_Anterior'] = _to_decimal(v)

                                # Scenario columns by header name if present
                                try:
                                    if h_esc_pes in norm_map_p:
                                        params_p['Escenario_Pesimista'] = _to_decimal(rec_p.get(norm_map_p[h_esc_pes]))
                                except Exception:
                                    params_p['Escenario_Pesimista'] = None
                                try:
                                    if h_esc_cons in norm_map_p:
                                        params_p['Escenario_Conservador'] = _to_decimal(rec_p.get(norm_map_p[h_esc_cons]))
                                except Exception:
                                    params_p['Escenario_Conservador'] = None
                                try:
                                    if h_esc_opt in norm_map_p:
                                        params_p['Escenario_Optimista'] = _to_decimal(rec_p.get(norm_map_p[h_esc_opt]))
                                except Exception:
                                    params_p['Escenario_Optimista'] = None

                                # Log insert params for debugging
                                try:
                                    def _serialize_val(v):
                                        if v is None:
                                            return None
                                        try:
                                            if isinstance(v, _dt):
                                                return v.isoformat()
                                        except Exception:
                                            pass
                                        try:
                                            if isinstance(v, Decimal):
                                                return str(v)
                                        except Exception:
                                            pass
                                        return v

                                    loggable_p = {k: _serialize_val(v) for k, v in params_p.items()}
                                   # logging.getLogger('operations').info(f"Presupuesto insert fila {idx_p}: {loggable_p}")
                                except Exception:
                                    pass

                                db.execute(text(insert_sql_p), params_p)
                                try:
                                    affected_p = db.execute(text("SELECT @@ROWCOUNT")).scalar()
                                except Exception:
                                    affected_p = None
                                try:
                                    total_inserted_p += int(affected_p) if affected_p is not None else 0
                                except Exception:
                                    pass

                            logging.getLogger('operations').info(f"Presupuesto: insertadas aprox {total_inserted_p} filas desde hoja '{ppto_sheet}'")

                            # After inserting presupuesto rows, call sp_proc_ontime with sheet name as processed file
                            try:
                                sp_ppto_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado,14"
                                logging.getLogger('operations').info(f"Ejecutando Procedure para Presupuesto usuario={username}, hoja={ppto_sheet}")
                                db.execute(text(sp_ppto_sql), {"nombre_usuario": username, "name_file_procesado": ppto_sheet})
                                try:
                                    sp_p_af = db.execute(text("SELECT @@ROWCOUNT")).scalar()
                                except Exception:
                                    sp_p_af = None
                                logging.getLogger('operations').info(f"Procedure (Presupuesto) @@ROWCOUNT={sp_p_af}")
                            except Exception as sp_p_err:
                                logging.getLogger('operations').error(f"Error ejecutando procedure para Presupuesto: {sp_p_err}")
                                raise

                    else:
                        logging.getLogger('operations').info(f"No se encontró hoja PPTO {yy2} en el libro; se omite carga de presupuesto.")
                except Exception:
                    # Any exception here should bubble up to outer handler to trigger rollback
                    raise

                # Commit once for the whole file (includes both SP calls and presupuesto inserts)
                db.commit()
                logging.getLogger('operations').info(f"Envío a SP completado. Enviadas: {len(records)}, total_affected_calc={total_affected}")

                # Opción B: escribir archivo acumulado_<AAAA>.txt con el número de filas
                #try:
                #    _write_acumulado_file(file_path, name_only, len(records))
                #except Exception:
                    # _write_acumulado_file ya hace logging; no hacer fallar el flujo principal
                 #   pass

            except Exception as sp_err:
                # Rollback entire transaction on any failure
                try:
                    db.rollback()
                except Exception:
                    pass

                # Log detailed error including the failing row index and non-null params
                failing_params = None
                try:
                    failing_params = {k: params[k] for k in params if params[k] is not None}
                except Exception:
                    failing_params = None

                logging.getLogger('operations').error(f"Error ejecutando SP en fila {current_idx}: {sp_err} -- datos: {failing_params}")
                # Try to still write the acumulado file even if SP failed (option B)
                try:
                    _write_acumulado_file(file_path, name_only, len(records))
                except Exception:
                    pass

                # Propagate exception to caller to indicate the file-level failure
                raise

        return records
    except Exception as e:
        logging.getLogger('operations').error(f"Error procesando archivo Excel {file_path}: {str(e)}")
        try:
            logging.getLogger('operations').error(f"Error procesando archivo Excel {file_path}: {str(e)}")
        except Exception:
            pass
        raise


def process_incidencias(file_path: str, db: object, username: Optional[str] = None, original_name: Optional[str] = None) -> int:
    """Process an incidencias Excel file: insert rows into dbo.incidencias_tmp and call dbo.sp_proc_ontime.

    Returns the number of rows inserted.
    """
    logging.getLogger('operations').info(f"Procesando archivo de incidencias: {file_path}")
    try:
        logging.getLogger('operations').info(f"Procesando archivo de incidencias: {file_path}")
    except Exception:
        pass
    try:
        # Read sheet (first sheet) and normalize
        df = pd.read_excel(file_path)
        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        # Omitir filas que inicien con un campo vacío o nulo (primera columna)
        processed_count = 0
        if df.shape[1] > 0:
            first_col = df.columns[0]
            before_count = len(df)
            try:
                non_empty_mask = df[first_col].notnull() & (df[first_col].astype(str).str.strip() != '')
            except Exception:
                non_empty_mask = df[first_col].notnull()
            df = df[non_empty_mask]
            after_count = len(df)
            dropped = before_count - after_count
            if dropped > 0:
                logging.getLogger('operations').info(f"Incidencias: omitidas {dropped} filas que iniciaban con campo vacío en columna '{first_col}'")

        records = df.to_dict(orient='records')
        processed_count = len(records)

        expected = [
            "CARTA PORTE", "NÚMERO ENVÍO", "CLIENTE", "LÍNEA TRANSPORTISTA",
            "OPERADOR", "ORIGEN", "DESTINO", "ANOMALÍA", "FECHA",
            "COORDENADAS LAT", "COORDENADAS LON", "UBICACIÓN", "COMENTARIOS"
        ]

        def normalize_name_simple(s: str) -> str:
            if s is None:
                return ''
            return re.sub(r"\s+", " ", str(s).replace('\xa0', ' ')).strip().upper().replace('Ñ','N')

        normalized_key_map = {normalize_name_simple(k): k for k in list(df.columns)}

        # Build insert SQL with proper column names (use brackets for special names)
        insert_sql = (
            "INSERT INTO dbo.incidencias_tmp (Carta_Porte, [Número_Envío], Cliente, [Línea_Transportista], "
            "Operador, Origen, Destino, Anomalía, Fecha, Coordenadas_Lat, Coordenadas_Lon, Ubicación, Comentarios,creado_por) "
            "VALUES (:Carta_Porte, :Numero_Envio, :Cliente, :Linea_Transportista, :Operador, :Origen, :Destino, :Anomalia, :Fecha, :Coordenadas_Lat, :Coordenadas_Lon, :Ubicacion, :Comentarios,:Usuario_Creacion)"
        )
        # Preparar todos los parámetros para BULK INSERT
        all_params_list = []
        for idx, rec in enumerate(records, start=1):
            params = {
                'Carta_Porte': None,
                'Numero_Envio': None,
                'Cliente': None,
                'Linea_Transportista': None,
                'Operador': None,
                'Origen': None,
                'Destino': None,
                'Anomalia': None,
                'Fecha': None,
                'Coordenadas_Lat': None,
                'Coordenadas_Lon': None,
                'Ubicacion': None,
                'Comentarios': None,
                'Usuario_Creacion': username if username else None
            }

            for key_norm, col in normalized_key_map.items():
                if key_norm in expected:
                    val = rec.get(col)
                    if isinstance(val, str):
                        val = val.replace('\xa0', ' ').strip()
                        if val == '':
                            val = None
                    if val is not None and key_norm == 'FECHA':
                        try:
                            if hasattr(val, 'to_pydatetime'):
                                val = val.to_pydatetime()
                            elif not isinstance(val, _dt):
                                parsed = pd.to_datetime(val, errors='coerce', dayfirst=True)
                                if not pd.isna(parsed):
                                    val = parsed.to_pydatetime()
                                else:
                                    val = None
                        except Exception:
                            val = None
                    if val is not None and key_norm in ('COORDENADAS_LAT', 'COORDENADAS_LON'):
                        try:
                            val = Decimal(str(val))
                            val = val.quantize(Decimal('0.0000001'))
                        except Exception:
                            try:
                                val = float(str(val).replace(',', '.'))
                            except Exception:
                                val = None

                    if key_norm == 'CARTA PORTE':
                        params['Carta_Porte'] = val
                    elif key_norm == 'NÚMERO ENVÍO':
                        params['Numero_Envio'] = val
                    elif key_norm == 'CLIENTE':
                        params['Cliente'] = val
                    elif key_norm == 'LÍNEA TRANSPORTISTA':
                        params['Linea_Transportista'] = val
                    elif key_norm == 'OPERADOR':
                        params['Operador'] = val
                    elif key_norm == 'ORIGEN':
                        params['Origen'] = val
                    elif key_norm == 'DESTINO':
                        params['Destino'] = val
                    elif key_norm == 'ANOMALÍA':
                        params['Anomalia'] = val
                    elif key_norm == 'FECHA':
                        params['Fecha'] = val
                    elif key_norm == 'COORDENADAS LAT':
                        params['Coordenadas_Lat'] = val
                    elif key_norm == 'COORDENADAS LON':
                        params['Coordenadas_Lon'] = val
                    elif key_norm == 'UBICACIÓN':
                        params['Ubicacion'] = val
                    elif key_norm == 'COMENTARIOS':
                        params['Comentarios'] = val

            for k in list(params.keys()):
                v = params[k]
                if isinstance(v, int) and k not in ('Semana', 'Dias_Pipeline'):
                    try:
                        params[k] = str(v)
                    except Exception:
                        pass
            
            all_params_list.append(params)
        
        # Ejecutar BULK INSERT optimizado
        total_inserted = _bulk_insert_with_fallback(db, insert_sql, all_params_list, "incidencias_tmp")

        # After inserts, call sp_proc_ontime if username provided
        if username and original_name:
            processed_name = original_name
            if processed_name.lower().startswith('temp_'):
                processed_name = processed_name[5:]
            sp2_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado,15"
            logging.getLogger('operations').info(f"Ejecutando Procedure para incidencias usuario={username}, archivo={processed_name}")
            db.execute(text(sp2_sql), {"nombre_usuario": username, "name_file_procesado": processed_name})
            try:
                sp2_af = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                sp2_af = None
            logging.getLogger('operations').info(f"Procedure (incidencias) @@ROWCOUNT={sp2_af}")

        logging.getLogger('operations').info(f"Incidencias: procesadas {processed_count} filas, inserts afectaron aprox: {total_inserted}")
        return processed_count
    except Exception as e:
        logging.getLogger('operations').error(f"Error procesando incidencias {file_path}: {e}")
        try:
            logging.getLogger('operations').error(f"Error procesando incidencias {file_path}: {e}")
        except Exception:
            pass
        raise


def process_pipeline_transporte(file_path: str, db: object, username: Optional[str] = None, original_name: Optional[str] = None) -> int:
    """Process a pipelineTransporte Excel file.

    Rules:
    - Validate that the sheet to read contains 'Data_Historico' (case-insensitive) in its name.
    - Omit rows whose first column is empty/null.
    - Insert rows (no commit) into dbo.pipeline_transporte_tmp following the column order defined in the schema.
    - After inserts, call dbo.sp_proc_ontime(:nombre_usuario, :name_file_procesado) if username and original_name provided.

    Returns number of rows processed (omitting empty-first-column rows).
    """
    logging.getLogger('operations').info(f"V2.0 - Procesando pipeline transporte")
    logging.getLogger('operations').info(f"Procesando pipeline transporte: {file_path}")
    try:
        logging.getLogger('operations').info(f"Procesando pipeline transporte: {file_path}")
    except Exception:
        pass
    try:
        # Find sheet with Data_Historico
        sheet_to_use = None
        with pd.ExcelFile(file_path) as xls:
            for s in xls.sheet_names:
                if 'DATA_HISTORICO' in s.upper():
                    sheet_to_use = s
                    break
        if sheet_to_use is None:
            raise ValueError("Hoja que contiene 'Data_Historico' no encontrada en el archivo Excel")

        # Expected headers (normalized) to map; we'll do fuzzy matching
        expected = [
            'Proveedor', 'Fecha de Prospección', 'Semana', 'Fuente de Prospecto', 'Responsable',
            'Fases Pipeline', 'Medio de Contacto', 'Fecha último contacto', 'Días Pipeline', 'Nombre de Contacto 1',
            'Número Telefono 1', 'Correo Electrónico 1', 'Nombre de Contacto 2', 'Número Telefono 2', 'Correo Electrónico 2',
            'Ubicación', 'Tipo de unidad', 'Capacidad instalada', 'Requisitos básicos de carga', 'Ruta estrategica', 
            'Cliente estrategico', 'Comentarios'
        ]
        
        def nk(s: str) -> str:
            """Normalize column name for comparison."""
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()

        # OPTIMIZACIÓN: Leer archivo UNA SOLA VEZ con header fijo (header=0 es más común)
        # Si el archivo tiene formato diferente, ajustar aquí en lugar de auto-detectar
        logging.getLogger('operations').info(f"Leyendo hoja '{sheet_to_use}' con header=0...")
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, header=0)
            logging.getLogger('operations').info(f"Archivo leído: {len(df)} filas, {len(df.columns)} columnas")
        except Exception as read_err:
            logging.getLogger('operations').error(f"Error leyendo archivo: {read_err}")
            raise
        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        # Omitir filas con primer campo vacío
        if df.shape[1] > 0:
            first_col = df.columns[0]
            before_count = len(df)
            try:
                non_empty_mask = df[first_col].notnull() & (df[first_col].astype(str).str.strip() != '')
            except Exception:
                non_empty_mask = df[first_col].notnull()
            df = df[non_empty_mask]
            after_count = len(df)
            dropped = before_count - after_count
            if dropped > 0:
                logging.getLogger('operations').info(f"PipelineTransporte: omitidas {dropped} filas que iniciaban con campo vacío en columna '{first_col}'")

        records = df.to_dict(orient='records')
        processed_count = len(records)

       

      

        normalized_key_map = {nk(k): k for k in list(df.columns)}
        # Normalize expected list so comparisons are accent-insensitive
        normalized_expected_map = {nk(e): e for e in expected}

        insert_sql = (
            "INSERT INTO dbo.pipeline_transporte_tmp (Proveedor, Fecha_Prospeccion, Semana, Fuente_Prospecto, Responsable, "
            "Fases_Pipeline, Medio_Contacto, Fecha_Ultimo_Contacto, Dias_Pipeline, Nombre_Contacto1, Numero_Telefono1, Correo_Electronico1, "
            "Nombre_Contacto2, Numero_Telefono2, Correo_Electronico2, "
            "Ubicacion, Tipo_Unidad, Capacidad_Instalada, Requisitos_Basicos_Carga, Ruta_Estrategica, Cliente_Estrategico, Comentarios, Usuario_Creacion) "
            "VALUES (:Proveedor, :Fecha_Prospeccion, :Semana, :Fuente_Prospecto, :Responsable, :Fases_Pipeline, :Medio_Contacto, :Fecha_Ultimo_Contacto, :Dias_Pipeline, :Nombre_Contacto1, :Numero_Telefono1, :Correo_Electronico1, :Nombre_Contacto2, :Numero_Telefono2, :Correo_Electronico2, :Ubicacion, :Tipo_Unidad, :Capacidad_Instalada, :Requisitos_Basicos_Carga, :Ruta_Estrategica, :Cliente_Estrategico, :Comentarios, :Usuario_Creacion)"
        )

        total_inserted = 0

        # Build a mapping from expected (normalized) -> actual column name when present
        matched_cols = {exp_norm: normalized_key_map.get(exp_norm) for exp_norm in normalized_expected_map.keys()}
        
        # Pre-compilar TODOS los conjuntos normalizados para ELIMINAR llamadas a nk() dentro del loop
        nk_proveedor = nk('Proveedor')
        nk_fecha_prospeccion = nk('Fecha de prospección')
        nk_semana = nk('Semana')
        nk_fuente_prospecto = nk('Fuente de prospecto')
        nk_responsable = nk('Responsable')
        nk_fases_pipeline = nk('Fases Pipeline')
        nk_medio_contacto = nk('Medio de contacto')
        nk_fecha_ultimo_contacto = nk('Fecha último contacto')
        nk_dias_pipeline = nk('Días Pipeline')
        nk_nombre_contacto1 = nk('Nombre de Contacto 1')
        nk_numero_telefono1 = nk('Número Telefono 1')
        nk_correo_electronico1 = nk('Correo Electrónico 1')
        nk_nombre_contacto2 = nk('Nombre de Contacto 2')
        nk_numero_telefono2 = nk('Número Telefono 2')
        nk_correo_electronico2 = nk('Correo Electrónico 2')
        nk_ubicacion = nk('Ubicación')
        nk_tipo_unidad = nk('Tipo de unidad')
        nk_capacidad_instalada = nk('Capacidad instalada')
        nk_requisitos_basicos = nk('Requisitos básicos de carga')
        nk_ruta_estrategica = nk('Ruta estrategica')
        nk_cliente_estrategico = nk('Cliente estrategico')
        nk_comentarios = nk('Comentarios')
        
        date_columns_set = {nk_fecha_prospeccion, nk_fecha_ultimo_contacto}
        
        # Crear un diccionario de mapeo directo para asignación rápida (evitar cascada de if/elif)
        param_mapping = {
            nk_proveedor: 'Proveedor',
            nk_fecha_prospeccion: 'Fecha_Prospeccion',
            nk_semana: 'Semana',
            nk_fuente_prospecto: 'Fuente_Prospecto',
            nk_responsable: 'Responsable',
            nk_fases_pipeline: 'Fases_Pipeline',
            nk_medio_contacto: 'Medio_Contacto',
            nk_fecha_ultimo_contacto: 'Fecha_Ultimo_Contacto',
            nk_dias_pipeline: 'Dias_Pipeline',
            nk_nombre_contacto1: 'Nombre_Contacto1',
            nk_numero_telefono1: 'Numero_Telefono1',
            nk_correo_electronico1: 'Correo_Electronico1',
            nk_nombre_contacto2: 'Nombre_Contacto2',
            nk_numero_telefono2: 'Numero_Telefono2',
            nk_correo_electronico2: 'Correo_Electronico2',
            nk_ubicacion: 'Ubicacion',
            nk_tipo_unidad: 'Tipo_Unidad',
            nk_capacidad_instalada: 'Capacidad_Instalada',
            nk_requisitos_basicos: 'Requisitos_Basicos_Carga',
            nk_ruta_estrategica: 'Ruta_Estrategica',
            nk_cliente_estrategico: 'Cliente_Estrategico',
            nk_comentarios: 'Comentarios'
        }
        
        # BULK INSERT: preparar todos los parámetros primero, ejecutar en lotes después
        logging.getLogger('operations').info(f"Preparando datos para BULK INSERT de {len(records)} registros...")
        all_params_list = []
        
        for idx, rec in enumerate(records, start=1):
            params = {
                'Proveedor': None, 'Fecha_Prospeccion': None, 'Semana': None, 'Fuente_Prospecto': None, 'Responsable': None,
                'Fases_Pipeline': None, 'Medio_Contacto': None, 'Fecha_Ultimo_Contacto': None, 'Dias_Pipeline': None, 'Nombre_Contacto1': None,
                'Numero_Telefono1': None, 'Correo_Electronico1': None,  'Nombre_Contacto2': None, 'Numero_Telefono2': None, 'Correo_Electronico2': None, 'Ubicacion': None, 'Tipo_Unidad': None, 'Capacidad_Instalada': None,
                'Requisitos_Basicos_Carga': None, 'Ruta_Estrategica': None, 'Cliente_Estrategico': None, 'Comentarios': None, 'Usuario_Creacion': username
            }

            # Iterate over expected normalized keys (stable order) and extract value from actual column if present
            for exp_norm, actual_col in matched_cols.items():
                if actual_col is None:
                    continue
                    
                val = rec.get(actual_col)
                
                # Normalize strings
                if isinstance(val, str):
                    val = val.replace('\xa0', ' ').strip()
                    if val == '':
                        val = None

                if val is None:
                    continue

                # Date coercion: use normalized keys for matching
                if exp_norm in date_columns_set:
                    try:
                        if hasattr(val, 'to_pydatetime'):
                            val = val.to_pydatetime()
                        elif isinstance(val, _dt):
                            pass
                        else:
                            parsed = pd.to_datetime(val, errors='coerce', dayfirst=False)
                            if pd.isna(parsed):
                                parsed = pd.to_datetime(val, errors='coerce', dayfirst=True)
                            if not pd.isna(parsed):
                                val = parsed.to_pydatetime()
                            else:
                                val = None
                    except Exception:
                        val = None

                # Numeric coercion for Semana
                elif exp_norm == nk_semana:
                    try:
                        s = str(val)
                        m = re.search(r'(\d+)', s)
                        val = int(m.group(1)) if m else None
                    except Exception:
                        val = None

                # Numeric coercion for Dias Pipeline
                elif exp_norm == nk_dias_pipeline:
                    try:
                        val = int(float(str(val).replace(',', '.')))
                    except Exception:
                        val = None

                # Numeric coercion for Capacidad Instalada
                elif exp_norm == nk_capacidad_instalada:
                    try:
                        s = re.sub(r"[^0-9.,\-]", "", str(val))
                        if s == '':
                            val = None
                        else:
                            if s.count(',') > 0 and s.count('.') == 0:
                                s = s.replace(',', '.')
                            elif s.count(',') > 0 and s.count('.') > 0:
                                if s.rfind('.') > s.rfind(','):
                                    s = s.replace(',', '')
                                else:
                                    s = s.replace('.', '').replace(',', '.')
                            d = Decimal(s)
                            val = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    except Exception:
                        try:
                            val = float(str(val).replace(',', '.'))
                        except Exception:
                            val = None

                # Usar mapeo directo en lugar de cascada if/elif
                param_key = param_mapping.get(exp_norm)
                if param_key and val is not None:
                    params[param_key] = val

            all_params_list.append(params)
            
            if idx % 1000 == 0:
                logging.getLogger('operations').info(f"Preparadas {idx}/{len(records)} filas...")
        
        # Ejecutar BULK INSERT usando fast_executemany de pyodbc
        logging.getLogger('operations').info(f"Ejecutando BULK INSERT de {len(all_params_list)} registros...")
        
        if len(all_params_list) > 0:
            bulk_success = False
            try:
                # Intentar obtener la conexión raw de pyodbc para fast_executemany
                raw_conn = None
                
                # Navegación a través de las capas de SQLAlchemy para llegar a pyodbc
                if hasattr(db, 'bind'):
                    # db es una Session, obtener engine
                    engine = db.bind
                    raw_conn = engine.raw_connection()
                elif hasattr(db, 'connection'):
                    # db ya tiene método connection
                    conn_obj = db.connection()
                    if hasattr(conn_obj, 'connection'):
                        raw_conn = conn_obj.connection
                
                if raw_conn is not None:
                    cursor = raw_conn.cursor()
                    cursor.fast_executemany = True
                    
                    logging.getLogger('operations').info(f"fast_executemany habilitado, construyendo valores...")
                    
                    # SQL con placeholders ? para pyodbc
                    insert_sql_bulk = (
                        "INSERT INTO dbo.pipeline_transporte_tmp (Proveedor, Fecha_Prospeccion, Semana, Fuente_Prospecto, Responsable, "
                        "Fases_Pipeline, Medio_Contacto, Fecha_Ultimo_Contacto, Dias_Pipeline, Nombre_Contacto1, Numero_Telefono1, Correo_Electronico1, "
                        "Nombre_Contacto2, Numero_Telefono2, Correo_Electronico2, "
                        "Ubicacion, Tipo_Unidad, Capacidad_Instalada, Requisitos_Basicos_Carga, Ruta_Estrategica, Cliente_Estrategico, Comentarios, Usuario_Creacion) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    )
                    
                    # Convertir a tuplas
                    values_list = [
                        (
                            p['Proveedor'], p['Fecha_Prospeccion'], p['Semana'], 
                            p['Fuente_Prospecto'], p['Responsable'], p['Fases_Pipeline'],
                            p['Medio_Contacto'], p['Fecha_Ultimo_Contacto'], p['Dias_Pipeline'],
                            p['Nombre_Contacto1'], p['Numero_Telefono1'], p['Correo_Electronico1'],
                            p['Nombre_Contacto2'], p['Numero_Telefono2'], p['Correo_Electronico2'],
                            p['Ubicacion'], p['Tipo_Unidad'], p['Capacidad_Instalada'],
                            p['Requisitos_Basicos_Carga'], p['Ruta_Estrategica'], p['Cliente_Estrategico'],
                            p['Comentarios'], p['Usuario_Creacion']
                        ) for p in all_params_list
                    ]
                    
                    # Usar lotes pequeños para evitar MemoryError (500 registros por lote es seguro)
                    logging.getLogger('operations').info(f"Ejecutando executemany en lotes de 500 registros...")
                    batch_size_bulk = 500
                    total_inserted = 0
                    
                    for batch_start in range(0, len(values_list), batch_size_bulk):
                        batch_end = min(batch_start + batch_size_bulk, len(values_list))
                        batch_values = values_list[batch_start:batch_end]
                        
                        try:
                            cursor.executemany(insert_sql_bulk, batch_values)
                            total_inserted += len(batch_values)
                            
                            if batch_end % 5000 == 0 or batch_end == len(values_list):
                                logging.getLogger('operations').info(f"fast_executemany: {batch_end}/{len(values_list)} registros...")
                        except MemoryError:
                            # Si aún hay MemoryError, liberar memoria y continuar con fallback
                            logging.getLogger('operations').error(f"MemoryError persistente en lote {batch_start}-{batch_end}, abortando fast_executemany")
                            del values_list
                            del batch_values
                            import gc
                            gc.collect()
                            raise
                    
                    raw_conn.commit()
                    bulk_success = True
                    logging.getLogger('operations').info(f"BULK INSERT (fast_executemany) completado: {total_inserted} registros en {(len(values_list) + batch_size_bulk - 1) // batch_size_bulk} lotes")
                else:
                    logging.getLogger('operations').warning("No se pudo obtener raw_connection de pyodbc")
                    
            except Exception as bulk_err:
                import traceback
                tb = traceback.format_exc()
                logging.getLogger('operations').warning(f"BULK INSERT con fast_executemany falló: {bulk_err}\n{tb}")
            
            # Fallback: usar SQLAlchemy bulk_insert_mappings (más rápido que row-by-row)
            if not bulk_success:
                logging.getLogger('operations').info("Usando SQLAlchemy bulk_insert_mappings...")
                try:
                    from sqlalchemy.orm import Session
                    if isinstance(db, Session):
                        # bulk_insert_mappings es significativamente más rápido que execute individual
                        # Procesar en chunks para evitar problemas de memoria
                        chunk_size = 2000
                        for i in range(0, len(all_params_list), chunk_size):
                            chunk = all_params_list[i:i + chunk_size]
                            # Necesitamos mapear a objetos ORM o usar Core insert
                            # Como no tenemos modelo ORM, usar execute con bindparam es mejor
                            
                            # Construir un multi-row VALUES statement
                            from sqlalchemy import bindparam
                            stmt = text(insert_sql)
                            db.execute(stmt, chunk)
                            
                            if (i + chunk_size) % 10000 == 0 or (i + chunk_size) >= len(all_params_list):
                                actual_processed = min(i + chunk_size, len(all_params_list))
                                logging.getLogger('operations').info(f"Bulk insert: {actual_processed}/{len(all_params_list)} filas...")
                        
                        total_inserted = len(all_params_list)
                        logging.getLogger('operations').info(f"SQLAlchemy bulk insert completado: {total_inserted} registros")
                    else:
                        raise Exception("db no es una Session de SQLAlchemy")
                except Exception as bulk_fallback_err:
                    logging.getLogger('operations').warning(f"Bulk insert mappings falló: {bulk_fallback_err}, usando row-by-row...")
                    # Último recurso: row by row (más lento pero garantizado)
                    for idx, params in enumerate(all_params_list, start=1):
                        db.execute(text(insert_sql), params)
                        
                        if idx % 5000 == 0 or idx == len(all_params_list):
                            logging.getLogger('operations').info(f"Row-by-row: {idx}/{len(all_params_list)} filas...")
                    
                    total_inserted = len(all_params_list)
                    logging.getLogger('operations').info("INSERT row-by-row completado")
        else:
            total_inserted = 0
            logging.getLogger('operations').warning("No hay registros para insertar")

        # After inserts, call post-processing SP if username/original_name provided
        if username and original_name:
            processed_name = original_name
            if processed_name.lower().startswith('temp_'):
                processed_name = processed_name[5:]
            sp2_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado,3"
            logging.getLogger('operations').info(f"Ejecutando Procedure para pipeline usuario={username}, archivo={processed_name}")
            db.execute(text(sp2_sql), {"nombre_usuario": username, "name_file_procesado": processed_name})
            try:
                sp2_af = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                sp2_af = None
            logging.getLogger('operations').info(f"Procedure (pipeline) @@ROWCOUNT={sp2_af}")

        logging.getLogger('operations').info(f"PipelineTransporte: procesadas {processed_count} filas, inserts afectaron aprox: {total_inserted}")
        return processed_count
    except Exception as e:
        logging.getLogger('operations').error(f"Error procesando pipeline transporte {file_path}: {e}")
        try:
            logging.getLogger('operations').error(f"Error procesando pipeline transporte {file_path}: {e}")
        except Exception:
            pass
        raise


def process_pipeline_comercial(file_path: str, db: object, username: Optional[str] = None, original_name: Optional[str] = None) -> int:
    """Process a pipelineComercial Excel file into dbo.pipeline_comercial_tmp.

    Validations:
    - filename must match pipelineComercial_semXX_DD-MM-AAAA (week, day, month, year)
    - sheet name must contain 'PIPELINE' (case-insensitive)
    - caller provides a DB session; inserts are executed but not committed here (caller should commit once)

    Returns number of rows processed (omitting rows whose first column is empty).
    """
    logging.getLogger('operations').info(f"Procesando pipeline comercial: {file_path}")
    try:
        logging.getLogger('operations').info(f"Procesando pipeline comercial: {file_path}")
    except Exception:
        pass
    try:
        # Load workbook and pick sheet containing 'PIPELINE'
        sheet_to_use = None
        with pd.ExcelFile(file_path) as xls:
            for s in xls.sheet_names:
                if 'PIPELINE' in s.upper():
                    sheet_to_use = s
                    break
        if sheet_to_use is None:
            raise ValueError("Hoja que contiene 'PIPELINE' no encontrada en el archivo Excel")

        # Expected headers (human names) - used by the header-detection heuristic
        expected = [
            'No', 'Semana', 'Fuente de Prospecto', 'Cliente', 'Bloque de prospección', 'Tipo de cliente', 'ZONA GEOGRAFICA',
            'Segmento', 'Clasificación de la oportunidad %', 'FUNNEL', 'Contacto 1','Contacto 2', 'Correo Electronico 1','Correo Electronico2', 'Telefono', 'Puesto',
            'Fecha Contacto Inicial', 'Fecha Ultimo contacto', 'Evento Ultimo Contacto', 'Dias en Pipeline', 'Responsable de Seguimiento',
            'Status', 'Producto a Transportar', 'Tipo de cliente (por su actividad)', 'Nombre de intermediario', 'Segmento',
            'Proveedor Actual', 'Ubicación de Negociación', 'Proyecto Cross Selling / Quien Genero la oportunidad',
            'IMPO', 'EXPO', 'NAC', 'DED', 'INTMDL', 'Mudanza', 'SPOT', 'CIRCUITO', 'PUERTOS', 'Origen', 'Destino', 'Bitacora de seguimiento'
        ]

        def nk(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()

        normalized_expected_map = {nk(e): e for e in expected}

        # For pipeline comercial files we expect the real header to be on row 2 (so data starts on row 3)
        # and the first physical column is empty and should be skipped. Read deterministically with
        # header=1 and then drop the first physical column (column index 0). This avoids heuristic
        # ambiguity and matches the provided file convention.
        try:
            # Read deterministically: headers are on Excel row 3 (header=2), so data starts on row 4.
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, header=0)
            logging.getLogger('operations').info(f"Reading pipeline comercial sheet '{sheet_to_use}' with header=2 (headers on row 3, data starts on row 4)")
        except Exception as read_err:
            logging.getLogger('operations').error(f"No se pudo leer sheet {sheet_to_use} con header=2: {read_err}")
            raise

        # If the first physical column is empty/placeholder, drop it so logical columns start at physical col 2
        try:
            if df.shape[1] >= 2:
                # Drop the first physical column because it's always empty per file convention.
                df = df.iloc[:, 1:].copy()
                logging.getLogger('operations').info(f"Dropped first physical column; columns now: {list(df.columns)}")
            else:
                logging.getLogger('operations').warning(f"Sheet {sheet_to_use} tiene menos de 2 columnas; no se eliminó la primera columna")
        except Exception as drop_err:
            logging.getLogger('operations').warning(f"No se pudo eliminar la primera columna física: {drop_err}")

        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        # Omitir filas con primer campo vacío
        if df.shape[1] > 0:
            # after dropping the first physical column, logical first column is at index 0
            first_col = df.columns[0]
            before_count = len(df)
            try:
                non_empty_mask = df[first_col].notnull() & (df[first_col].astype(str).str.strip() != '')
            except Exception:
                non_empty_mask = df[first_col].notnull()
            df = df[non_empty_mask]
            after_count = len(df)
            dropped = before_count - after_count
            if dropped > 0:
                logging.getLogger('operations').info(f"PipelineComercial: omitidas {dropped} filas que iniciaban con campo vacío en columna '{first_col}'")

        records = df.to_dict(orient='records')
        processed_count = len(records)

        
        normalized_key_map = {nk(k): k for k in list(df.columns)}
        

        insert_sql = (
            "INSERT INTO dbo.pipeline_comercial_tmp (No, Semana, Fuente_Prospecto, Cliente, Bloque_Prospeccion, Tipo_Cliente, Zona_Geografica, Segmento, Clasificacion_Oportunidad, Funnel, Contacto1, Contacto2, Correo_Electronico1, Correo_Electronico2, Telefono, Puesto, Fecha_Contacto_Inicial, Fecha_Ultimo_Contacto, Evento_Ultimo_Contacto, Dias_en_Pipeline, Responsable_Seguimiento, Status, Producto_a_Transportar, Tipo_Cliente_Actividad, Nombre_Intermediario, Segmento_Secundario, Proveedor_Actual, Ubicacion_Negociacion, Proyecto_Cross_Selling, IMPO, EXPO, NAC, DED, INTMDL, Mudanza, SPOT, CIRCUITO, PUERTOS, Origen, Destino, Bitacora_Seguimiento, Usuario_Creacion) "
            "VALUES (:No, :Semana, :Fuente_Prospecto, :Cliente, :Bloque_Prospeccion, :Tipo_Cliente, :Zona_Geografica, :Segmento, :Clasificacion_Oportunidad, :Funnel, :Contacto1, :Contacto2, :Correo_Electronico1, :Correo_Electronico2, :Telefono, :Puesto, :Fecha_Contacto_Inicial, :Fecha_Ultimo_Contacto, :Evento_Ultimo_Contacto, :Dias_en_Pipeline, :Responsable_Seguimiento, :Status, :Producto_a_Transportar, :Tipo_Cliente_Actividad, :Nombre_Intermediario, :Segmento_Secundario, :Proveedor_Actual, :Ubicacion_Negociacion, :Proyecto_Cross_Selling, :IMPO, :EXPO, :NAC, :DED, :INTMDL, :Mudanza, :SPOT, :CIRCUITO, :PUERTOS, :Origen, :Destino, :Bitacora_Seguimiento, :Usuario_Creacion)"
        )

        # Preparar todos los parámetros para BULK INSERT
        all_params_list = []
        for idx, rec in enumerate(records, start=1):
            params = {
                'No': None, 'Semana': None, 'Fuente_Prospecto': None, 'Cliente': None, 'Bloque_Prospeccion': None,
                'Tipo_Cliente': None, 'Zona_Geografica': None, 'Segmento': None, 'Clasificacion_Oportunidad': None, 'Funnel': None,
                'Contacto1': None, 'Contacto2': None, 'Correo_Electronico1': None, 'Correo_Electronico2': None, 'Telefono': None, 'Puesto': None, 'Fecha_Contacto_Inicial': None,
                'Fecha_Ultimo_Contacto': None, 'Evento_Ultimo_Contacto': None, 'Dias_en_Pipeline': None, 'Responsable_Seguimiento': None,
                'Status': None, 'Producto_a_Transportar': None, 'Tipo_Cliente_Actividad': None, 'Nombre_Intermediario': None,
                'Segmento_Secundario': None, 'Proveedor_Actual': None, 'Ubicacion_Negociacion': None, 'Proyecto_Cross_Selling': None,
                'IMPO': 0, 'EXPO': 0, 'NAC': 0, 'DED': 0, 'INTMDL': 0, 'Mudanza': 0, 'SPOT': 0, 'CIRCUITO': 0, 'PUERTOS': 0,
                'Origen': None, 'Destino': None, 'Bitacora_Seguimiento': None, 'Usuario_Creacion': username
            }

            for norm_key, col in normalized_key_map.items():
                if norm_key in normalized_expected_map:
                    val = rec.get(col)
                    if isinstance(val, str):
                        val = val.replace('\xa0', ' ').strip()
                        if val == '':
                            val = None

                    # Dates: try month-first then day-first
                    if val is not None and norm_key in (nk('Fecha Contacto Inicial'), nk('Fecha Ultimo contacto')):
                        try:
                            if hasattr(val, 'to_pydatetime'):
                                val = val.to_pydatetime()
                            elif isinstance(val, _dt):
                                pass
                            else:
                                parsed = pd.to_datetime(val, errors='coerce', dayfirst=False)
                                if pd.isna(parsed):
                                    parsed = pd.to_datetime(val, errors='coerce', dayfirst=True)
                                if not pd.isna(parsed):
                                    val = parsed.to_pydatetime()
                                else:
                                    val = None
                        except Exception:
                            val = None

                    # Numeric coercion
                    if val is not None and norm_key == nk('Clasificación de la oportunidad %'):
                        try:
                            s = re.sub(r"[^0-9.,\-]", "", str(val))
                            if s == '':
                                val = None
                            else:
                                if s.count(',') > 0 and s.count('.') == 0:
                                    s = s.replace(',', '.')
                                elif s.count(',') > 0 and s.count('.') > 0:
                                    if s.rfind('.') > s.rfind(','):
                                        s = s.replace(',', '')
                                    else:
                                        s = s.replace('.', '').replace(',', '.')
                                d = Decimal(s)
                                val = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        except Exception:
                            try:
                                val = float(str(val).replace(',', '.'))
                            except Exception:
                                val = None

                    if val is not None and norm_key in (nk('Semana'),):
                        try:
                            s = str(val)
                            m = re.search(r'(\d+)', s)
                            if m:
                                val = int(m.group(1))
                            else:
                                val = None
                        except Exception:
                            val = None

                    if val is not None and norm_key == nk('Dias en Pipeline'):
                        try:
                            val = int(float(str(val).replace(',', '.')))
                        except Exception:
                            val = None

                    # Flags: coerce common yes/si/1/TRUE to 1
                    if val is not None and norm_key in (nk('IMPO'), nk('EXPO'), nk('NAC'), nk('DED'), nk('INTMDL'), nk('Mudanza'), nk('SPOT'), nk('CIRCUITO'), nk('PUERTOS')):
                        try:
                            sval = str(val).strip().upper()
                            if sval in ('1', 'YES', 'Y', 'SI', 'S', 'TRUE', 'T'):
                                val = 1
                            else:
                                val = 0
                        except Exception:
                            val = 0

                    # Assign to params by normalized name
                    if norm_key == nk('No'):
                        params['No'] = val
                    elif norm_key == nk('Semana'):
                        params['Semana'] = val
                    elif norm_key == nk('Fuente de Prospecto'):
                        params['Fuente_Prospecto'] = val
                    elif norm_key == nk('Cliente'):
                        params['Cliente'] = val
                    elif norm_key == nk('Bloque de prospección'):
                        params['Bloque_Prospeccion'] = val
                    elif norm_key == nk('Tipo de cliente'):
                        params['Tipo_Cliente'] = val
                    elif norm_key == nk('ZONA GEOGRAFICA'):
                        params['Zona_Geografica'] = val
                    elif norm_key == nk('Segmento'):
                        # Ambiguity: map to Segmento_Secundario if Segmento_Secundario already filled; else Segmento
                        if params.get('Segmento') is None:
                            params['Segmento'] = val
                        else:
                            params['Segmento_Secundario'] = val
                    elif norm_key == nk('Clasificación de la oportunidad %'):
                        params['Clasificacion_Oportunidad'] = val
                    elif norm_key == nk('FUNNEL'):
                        params['Funnel'] = val
                    elif norm_key == nk('Contacto 1'):
                        params['Contacto1'] = val
                    elif norm_key == nk('Contacto 2'):
                        params['Contacto2'] = val
                    elif norm_key == nk('Correo Electronico 1'):
                        params['Correo_Electronico1'] = val
                    elif norm_key == nk('Correo Electronico2'):
                        params['Correo_Electronico2'] = val
                    elif norm_key == nk('Telefono'):
                        params['Telefono'] = val
                    elif norm_key == nk('Puesto'):
                        params['Puesto'] = val
                    elif norm_key == nk('Fecha Contacto Inicial'):
                        params['Fecha_Contacto_Inicial'] = val
                    elif norm_key == nk('Fecha Ultimo contacto'):
                        params['Fecha_Ultimo_Contacto'] = val
                    elif norm_key == nk('Evento Ultimo Contacto'):
                        params['Evento_Ultimo_Contacto'] = val
                    elif norm_key == nk('Dias en Pipeline'):
                        params['Dias_en_Pipeline'] = val
                    elif norm_key == nk('Responsable de Seguimiento'):
                        params['Responsable_Seguimiento'] = val
                    elif norm_key == nk('Status'):
                        params['Status'] = val
                    elif norm_key == nk('Producto a Transportar'):
                        params['Producto_a_Transportar'] = val
                    elif norm_key == nk('Tipo de cliente (por su actividad)'):
                        params['Tipo_Cliente_Actividad'] = val
                    elif norm_key == nk('Nombre de intermediario'):
                        params['Nombre_Intermediario'] = val
                    elif norm_key == nk('Proveedor Actual'):
                        params['Proveedor_Actual'] = val
                    elif norm_key == nk('Ubicación de Negociación'):
                        params['Ubicacion_Negociacion'] = val
                    elif norm_key == nk('Proyecto Cross Selling / Quien Genero la oportunidad'):
                        params['Proyecto_Cross_Selling'] = val
                    elif norm_key == nk('IMPO'):
                        params['IMPO'] = int(val) if val is not None else 0
                    elif norm_key == nk('EXPO'):
                        params['EXPO'] = int(val) if val is not None else 0
                    elif norm_key == nk('NAC'):
                        params['NAC'] = int(val) if val is not None else 0
                    elif norm_key == nk('DED'):
                        params['DED'] = int(val) if val is not None else 0
                    elif norm_key == nk('INTMDL'):
                        params['INTMDL'] = int(val) if val is not None else 0
                    elif norm_key == nk('Mudanza'):
                        params['Mudanza'] = int(val) if val is not None else 0
                    elif norm_key == nk('SPOT'):
                        params['SPOT'] = int(val) if val is not None else 0
                    elif norm_key == nk('CIRCUITO'):
                        params['CIRCUITO'] = int(val) if val is not None else 0
                    elif norm_key == nk('PUERTOS'):
                        params['PUERTOS'] = int(val) if val is not None else 0
                    elif norm_key == nk('Origen'):
                        params['Origen'] = val
                    elif norm_key == nk('Destino'):
                        params['Destino'] = val
                    elif norm_key == nk('Bitacora de seguimiento'):
                        params['Bitacora_Seguimiento'] = val

            # Safety: avoid int->text misbindings
            for k in list(params.keys()):
                v = params[k]
                if isinstance(v, int) and k not in ('Semana', 'Dias_en_Pipeline', 'IMPO', 'EXPO', 'NAC', 'DED', 'INTMDL', 'Mudanza', 'SPOT', 'CIRCUITO', 'PUERTOS', 'No'):
                    try:
                        params[k] = str(v)
                    except Exception:
                        pass

            # Log the parameters being inserted (serialize datetimes/decimals) to help debugging
            try:
                def _serialize_val(v):
                    if v is None:
                        return None
                    try:
                        if isinstance(v, _dt):
                            return v.isoformat()
                    except Exception:
                        pass
                    try:
                        if isinstance(v, Decimal):
                            return str(v)
                    except Exception:
                        pass
                    return v

                loggable = {k: _serialize_val(v) for k, v in params.items()}
                #logging.getLogger('operations').info(f"PipelineComercial insert fila {idx}: {loggable}")
            except Exception as log_ex:
                logging.debug(f"No se pudo serializar params para logging en fila {idx}: {log_ex}")

            all_params_list.append(params)
        
        # Ejecutar BULK INSERT optimizado
        total_inserted = _bulk_insert_with_fallback(db, insert_sql, all_params_list, "pipeline_comercial_tmp")

        # After inserts, call sp_proc_ontime if provided
        if username and original_name:
            processed_name = original_name
            if processed_name.lower().startswith('temp_'):
                processed_name = processed_name[5:]
            sp2_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado, 8"
            logging.getLogger('operations').info(f"Ejecutando procedure para pipeline comercial usuario={username}, archivo={processed_name}")
            db.execute(text(sp2_sql), {"nombre_usuario": username, "name_file_procesado": processed_name})
            try:
                sp2_af = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                sp2_af = None
            logging.getLogger('operations').info(f"Procedure (pipeline_comercial) @@ROWCOUNT={sp2_af}")

        logging.getLogger('operations').info(f"PipelineComercial: procesadas {processed_count} filas, inserts afectaron aprox: {total_inserted}")
        return processed_count
    except Exception as e:
        logging.getLogger('operations').error(f"Error procesando pipeline comercial {file_path}: {e}")
        try:
            logging.getLogger('operations').error(f"Error procesando pipeline comercial {file_path}: {e}")
        except Exception:
            pass
        raise


def process_disponibilidad_transporte(file_path: str, db: object, username: Optional[str] = None, original_name: Optional[str] = None) -> int:
    """Process disponibilidadTransporte files into dbo.disponibilidad_transporte_tmp.

    Rules:
    - Filename convention verified by caller (endpoint); this function expects to receive
      the saved temp file path and a DB session.
    - Select sheet whose name contains 'OCT25' (case-insensitive).
    - Read deterministically with header=2 (headers on Excel row 3) and drop first physical column
      so logical columns start at physical column 2.
    - Omit rows whose first logical column (after drop) is empty/null.
    - Insert rows (no commit) into dbo.disponibilidad_transporte_tmp in the exact column order:
      Fecha, Capacidad, LT, Origen, Destino, Ruta, Disponibilidad, Ejecutiva, Cliente,
      Ofertado_Desde, Clasificacion_PQ_No_Cargo, No_Cargo_Por, Incidencias_Ejecutivas, Usuario_Creacion
    - After successful inserts, call dbo.sp_proc_ontime(:nombre_usuario, :name_file_procesado) if username and original_name provided.

    Returns number of rows inserted (omitting empty-first-column rows).
    """
    logging.getLogger('operations').info(f"Procesando disponibilidad transporte: {file_path}")
    try:
        logging.getLogger('operations').info(f"Procesando disponibilidad transporte: {file_path}")
    except Exception:
        pass
    try:
        # Find OCT25 sheet
        sheet_to_use = None
        with pd.ExcelFile(file_path) as xls:
            for s in xls.sheet_names:
                if 'OCT25' in s.upper() or 'OCT 25' in s.upper():
                    sheet_to_use = s
                    break
        if sheet_to_use is None:
            raise ValueError("Hoja 'OCT25' no encontrada en el archivo Excel")

        # Read with header=0 (Excel row 1 is header) per validation request: header row = 1, first record = row 2
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, header=0)
            logging.getLogger('operations').info(f"Reading disponibilidad sheet '{sheet_to_use}' with header=0 (headers on row 1)")
        except Exception as read_err:
            logging.getLogger('operations').error(f"No se pudo leer sheet {sheet_to_use} con header=0: {read_err}")
            raise

        # Helper to normalize strings (used by validation)
        def nk(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()

        # Validate that the header is on Excel row 1 and the first data row is row 2.
        # Heuristic: if the first data row (df.iloc[0]) contains mostly values equal to the header names
        # (after normalization), it's likely the file has header on row 2 instead. In that case we raise.
        try:
            if df.shape[0] < 1:
                raise ValueError("El sheet no contiene filas de datos")
            cols = list(df.columns)
            # Normalize column names and first data row values
            normalized_cols = {nk(c) for c in cols}
            first_row_vals = df.iloc[0]
            match_count = 0
            total_checked = 0
            for c in cols:
                try:
                    v = first_row_vals.get(c)
                except Exception:
                    v = None
                if v is None:
                    # empty cell doesn't count
                    continue
                total_checked += 1
                if isinstance(v, str):
                    if nk(v) in normalized_cols:
                        match_count += 1
                else:
                    # non-string values are unlikely to be headers
                    pass

            # If more than half of non-empty first-row cells match header names, suspect header is on row 2
            if total_checked > 0 and match_count > (len(cols) / 2):
                raise ValueError("Se esperaba que la fila 1 contenga los encabezados y la fila 2 el primer registro; el archivo parece tener el encabezado en otra fila")
        except Exception as v_err:
            logging.getLogger('operations').error(f"Validación de encabezado falló: {v_err}")
            raise

        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        # Omitir filas con primer campo vacío
        processed_count = 0
        if df.shape[1] > 0:
            first_col = df.columns[0]
            before_count = len(df)
            try:
                non_empty_mask = df[first_col].notnull() & (df[first_col].astype(str).str.strip() != '')
            except Exception:
                non_empty_mask = df[first_col].notnull()
            df = df[non_empty_mask]
            after_count = len(df)
            dropped = before_count - after_count
            if dropped > 0:
                logging.getLogger('operations').info(f"Disponibilidad: omitidas {dropped} filas que iniciaban con campo vacío en columna '{first_col}'")

        records = df.to_dict(orient='records')
        processed_count = len(records)

        # Expected logical headers (approximate human names) - we'll do normalization
        expected = [
            'FECHA', 'CAPACIDAD', 'LT', 'ORIGEN', 'DESTINO', 'RUTA', 'DISPONIBILIDAD', 'EJECUTIVA', 'CLIENTE',
            'OFERTADO DESDE', 'CLASIFICACION PQ NO CARGO', 'NO CARGO POR', 'INCIDENCIAS EJECUTIVAS'
        ]

        def nk(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()

        normalized_expected = {nk(e): e for e in expected}
        normalized_key_map = {nk(k): k for k in list(df.columns)}

        insert_sql = (
            "INSERT INTO dbo.disponibilidad_transporte_tmp (Fecha, Capacidad, LT, Origen, Destino, Ruta, Disponibilidad, Ejecutiva, Cliente, Ofertado_Desde, Clasificacion_PQ_No_Cargo, No_Cargo_Por, Incidencias_Ejecutivas, Usuario_Creacion) "
            "VALUES (:Fecha, :Capacidad, :LT, :Origen, :Destino, :Ruta, :Disponibilidad, :Ejecutiva, :Cliente, :Ofertado_Desde, :Clasificacion_PQ_No_Cargo, :No_Cargo_Por, :Incidencias_Ejecutivas, :Usuario_Creacion)"
        )

        # Preparar todos los parámetros para BULK INSERT
        all_params_list = []
        for idx, rec in enumerate(records, start=1):
            params = {
                'Fecha': None, 'Capacidad': None, 'LT': None, 'Origen': None, 'Destino': None, 'Ruta': None,
                'Disponibilidad': None, 'Ejecutiva': None, 'Cliente': None, 'Ofertado_Desde': None,
                'Clasificacion_PQ_No_Cargo': None, 'No_Cargo_Por': None, 'Incidencias_Ejecutivas': None,
                'Usuario_Creacion': username
            }

            for norm_key, col in normalized_key_map.items():
                if norm_key in normalized_expected:
                    val = rec.get(col)
                    # Normalize strings
                    if isinstance(val, str):
                        val = val.replace('\xa0', ' ').strip()
                        if val == '':
                            val = None

                    # Fecha parsing
                    if val is not None and norm_key == nk('FECHA'):
                        try:
                            if hasattr(val, 'to_pydatetime'):
                                val = val.to_pydatetime()
                            elif isinstance(val, _dt):
                                pass
                            else:
                                parsed = pd.to_datetime(val, errors='coerce', dayfirst=False)
                                if pd.isna(parsed):
                                    parsed = pd.to_datetime(val, errors='coerce', dayfirst=True)
                                if not pd.isna(parsed):
                                    val = parsed.to_pydatetime()
                                else:
                                    logging.debug(f"No se pudo parsear Fecha en fila {idx} columna {col}: {val}")
                                    val = None
                        except Exception:
                            val = None

                    # Capacidad numeric -> Decimal(12,2)
                    if val is not None and norm_key == nk('CAPACIDAD'):
                        try:
                            s = re.sub(r"[^0-9.,\-]", "", str(val))
                            if s == '':
                                val = None
                            else:
                                if s.count(',') > 0 and s.count('.') == 0:
                                    s = s.replace(',', '.')
                                elif s.count(',') > 0 and s.count('.') > 0:
                                    if s.rfind('.') > s.rfind(','):
                                        s = s.replace(',', '')
                                    else:
                                        s = s.replace('.', '').replace(',', '.')
                                d = Decimal(s)
                                val = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        except Exception:
                            try:
                                val = float(str(val).replace(',', '.'))
                            except Exception:
                                val = None

                    # Ofertado_Desde -> date parsing
                    if val is not None and norm_key == nk('OFERTADO DESDE'):
                        try:
                            if hasattr(val, 'to_pydatetime'):
                                val = val.to_pydatetime()
                            elif isinstance(val, _dt):
                                pass
                            else:
                                parsed = pd.to_datetime(val, errors='coerce', dayfirst=False)
                                if pd.isna(parsed):
                                    parsed = pd.to_datetime(val, errors='coerce', dayfirst=True)
                                if not pd.isna(parsed):
                                    val = parsed.to_pydatetime()
                                else:
                                    val = None
                        except Exception:
                            val = None

                    # Assign to params
                    if norm_key == nk('FECHA'):
                        params['Fecha'] = val
                    elif norm_key == nk('CAPACIDAD'):
                        params['Capacidad'] = val
                    elif norm_key == nk('LT'):
                        params['LT'] = val
                    elif norm_key == nk('ORIGEN'):
                        params['Origen'] = val
                    elif norm_key == nk('DESTINO'):
                        params['Destino'] = val
                    elif norm_key == nk('RUTA'):
                        params['Ruta'] = val
                    elif norm_key == nk('DISPONIBILIDAD'):
                        params['Disponibilidad'] = val
                    elif norm_key == nk('EJECUTIVA'):
                        params['Ejecutiva'] = val
                    elif norm_key == nk('CLIENTE'):
                        params['Cliente'] = val
                    elif norm_key == nk('OFERTADO DESDE'):
                        params['Ofertado_Desde'] = val
                    elif norm_key == nk('CLASIFICACION PQ NO CARGO'):
                        params['Clasificacion_PQ_No_Cargo'] = val
                    elif norm_key == nk('NO CARGO POR'):
                        params['No_Cargo_Por'] = val
                    elif norm_key == nk('INCIDENCIAS EJECUTIVAS'):
                        params['Incidencias_Ejecutivas'] = val

            # Safety: convert ints to strings for textual columns where appropriate
            for k in list(params.keys()):
                v = params[k]
                if isinstance(v, int) and k not in ('Capacidad',):
                    try:
                        params[k] = str(v)
                    except Exception:
                        pass

            # Log params for debugging (serialize datetimes/decimals)
            try:
                def _serialize_val(v):
                    if v is None:
                        return None
                    try:
                        if isinstance(v, _dt):
                            return v.isoformat()
                    except Exception:
                        pass
                    try:
                        if isinstance(v, Decimal):
                            return str(v)
                    except Exception:
                        pass
                    return v

                loggable = {k: _serialize_val(v) for k, v in params.items()}
                #logging.getLogger('operations').info(f"Disponibilidad insert fila {idx}: {loggable}")
            except Exception as log_ex:
                logging.debug(f"No se pudo serializar params para logging en fila {idx}: {log_ex}")

            all_params_list.append(params)
        
        # Ejecutar BULK INSERT optimizado
        total_inserted = _bulk_insert_with_fallback(db, insert_sql, all_params_list, "disponibilidad_transporte_tmp")

        # After inserts, call sp_proc_ontime if username/original_name provided
        if username and original_name:
            processed_name = original_name
            if processed_name.lower().startswith('temp_'):
                processed_name = processed_name[5:]
            sp2_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado,13"
            logging.getLogger('operations').info(f"Ejecutando Procedure para disponibilidad usuario={username}, archivo={processed_name}")
            db.execute(text(sp2_sql), {"nombre_usuario": username, "name_file_procesado": processed_name})
            try:
                sp2_af = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                sp2_af = None
            logging.getLogger('operations').info(f"Procedure (disponibilidad) @@ROWCOUNT={sp2_af}")

        logging.getLogger('operations').info(f"DisponibilidadTransporte: procesadas {processed_count} filas, inserts afectaron aprox: {total_inserted}")
        return processed_count
    except Exception as e:
        logging.getLogger('operations').error(f"Error procesando disponibilidad transporte {file_path}: {e}")
        try:
            logging.getLogger('operations').error(f"Error procesando disponibilidad transporte {file_path}: {e}")
        except Exception:
            pass
        raise


def process_factoraje(file_path: str, db: object, username: Optional[str] = None, original_name: Optional[str] = None) -> int:
    """Process factoraje files into dbo.factoraje_tmp.

    - Reads the FIRST sheet of the workbook (sheet index 0)
    - Expects headers on the first row. Omits rows whose first column is empty.
    - Inserts rows (no commit) into dbo.factoraje_tmp following the exact column order described by the user.
    - Calls dbo.sp_proc_ontime(:nombre_usuario, :name_file_procesado) after successful inserts if username and original_name provided.

    Returns number of rows inserted (omitting empty-first-column rows).
    """
    logging.getLogger('operations').info(f"Procesando factoraje: {file_path}")
    try:
        logging.getLogger('operations').info(f"Procesando factoraje: {file_path}")
    except Exception:
        pass
    try:
        # Buscar la fila que contiene "Nombre" como inicio de encabezados
        # Leer sin encabezado para buscar en todas las celdas
        df_search = pd.read_excel(file_path, sheet_name=0, header=None)
        
        # Función de normalización para búsqueda
        def nk_local(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()
        
        # Buscar "Nombre" en las primeras 20 filas y columnas
        nombre_row = None
        nombre_col_idx = None
        cliente_col_idx = None
        max_search_rows = min(20, len(df_search))
        max_search_cols = min(20, df_search.shape[1])
        
        for row_idx in range(max_search_rows):
            for col_idx in range(max_search_cols):
                cell_value = df_search.iloc[row_idx, col_idx]
                if cell_value is not None and nk_local(str(cell_value)) == 'NOMBRE':
                    nombre_row = row_idx
                    nombre_col_idx = col_idx
                    logging.getLogger('operations').info(f"Encontrado 'Nombre' en fila {nombre_row} (0-based), columna {nombre_col_idx} (0-based)")
                    
                    # Buscar "CLIENTE" en la misma fila para determinar el rango de columnas
                    for end_col_idx in range(col_idx + 1, min(col_idx + 20, df_search.shape[1])):
                        end_cell = df_search.iloc[row_idx, end_col_idx]
                        if end_cell is not None and nk_local(str(end_cell)) == 'CLIENTE':
                            cliente_col_idx = end_col_idx
                            logging.getLogger('operations').info(f"Encontrado 'CLIENTE' en columna {cliente_col_idx} (0-based)")
                            break
                    break
            if nombre_row is not None:
                break
        
        if nombre_row is None:
            raise ValueError("No se encontró la celda con texto 'Nombre' en la primera hoja")
        
        if cliente_col_idx is None:
            logging.getLogger('operations').warning("No se encontró 'CLIENTE', usando todas las columnas desde 'Nombre'")
            cliente_col_idx = df_search.shape[1] - 1
        
        # Construir lista de columnas a leer (desde nombre_col_idx hasta cliente_col_idx inclusive)
        cols_to_read = list(range(nombre_col_idx, cliente_col_idx + 1))
        
        # Leer usando header=nombre_row para que esa fila sea el encabezado
        try:
            df = pd.read_excel(
                file_path, 
                sheet_name=0, 
                header=nombre_row,
                usecols=cols_to_read
            )
            logging.getLogger('operations').info(f"Leyendo factoraje: encabezado en fila Excel {nombre_row + 1} (0-based: {nombre_row}), datos desde fila Excel {nombre_row + 2}, columnas {nombre_col_idx} a {cliente_col_idx}")
            logging.getLogger('operations').info(f"Columnas leídas: {list(df.columns)}")
        except Exception as read_err:
            logging.getLogger('operations').error(f"No se pudo leer con usecols: {read_err}")
            # Fallback sin usecols
            df = pd.read_excel(file_path, sheet_name=0, header=nombre_row)
            # Recortar columnas manualmente
            if nombre_col_idx > 0 or cliente_col_idx < df.shape[1] - 1:
                df = df.iloc[:, nombre_col_idx:cliente_col_idx + 1]
                logging.getLogger('operations').info(f"Recortadas columnas en fallback: {list(df.columns)}")

        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        # Filtrar filas donde la primera columna (Nombre) esté vacía
        if df.shape[1] > 0:
            first_col = df.columns[0]
            before_count = len(df)
            try:
                non_empty_mask = df[first_col].notnull() & (df[first_col].astype(str).str.strip() != '')
            except Exception:
                non_empty_mask = df[first_col].notnull()
            df = df[non_empty_mask]
            after_count = len(df)
            dropped = before_count - after_count
            if dropped > 0:
                logging.getLogger('operations').info(f"Factoraje: omitidas {dropped} filas con columna Nombre vacía")

        records = df.to_dict(orient='records')
        processed_count = len(records)
        
        # Log de muestra del primer registro
        if len(records) > 0:
            sample_rec = records[0]
            logging.getLogger('operations').info(f"Primer registro de ejemplo - Nombre: '{sample_rec.get(df.columns[0])}', Columnas: {list(sample_rec.keys())}")

        # expected headers (human readable) - normalized comparison
        expected = ['Nombre', 'No Viaje', 'No Factura', 'Flete', 'Maniobras', 'Otros', 'Subtotal', 'IVA', 'ISR', 'Total', 'FECHA FACT', 'CLIENTE']

        normalized_key_map = {nk_local(k): k for k in list(df.columns)}
        normalized_expected = {nk_local(e): e for e in expected}

        insert_sql = (
            "INSERT INTO dbo.factoraje_tmp (Nombre, No_Viaje, No_Factura, Flete, Maniobras, Otros, Subtotal, IVA, ISR, Total, Fecha_Fact, Cliente, Usuario_Creacion) "
            "VALUES (:Nombre, :No_Viaje, :No_Factura, :Flete, :Maniobras, :Otros, :Subtotal, :IVA, :ISR, :Total, :Fecha_Fact, :Cliente, :Usuario_Creacion)"
        )

        # Preparar todos los parámetros para BULK INSERT
        all_params_list = []
        for idx, rec in enumerate(records, start=1):
            params = {
                'Nombre': None, 'No_Viaje': None, 'No_Factura': None, 'Flete': None, 'Maniobras': None, 'Otros': None,
                'Subtotal': None, 'IVA': None, 'ISR': None, 'Total': None, 'Fecha_Fact': None, 'Cliente': None, 'Usuario_Creacion': username
            }

            for norm_key, col in normalized_key_map.items():
                if norm_key in normalized_expected:
                    val = rec.get(col)
                    # Normalize strings
                    if isinstance(val, str):
                        val = val.replace('\xa0', ' ').strip()
                        if val == '':
                            val = None

                    # Dates
                    if val is not None and norm_key == nk_local('FECHA FACT'):
                        try:
                            if hasattr(val, 'to_pydatetime'):
                                val = val.to_pydatetime()
                            elif isinstance(val, _dt):
                                pass
                            else:
                                parsed = pd.to_datetime(val, errors='coerce', dayfirst=False)
                                if pd.isna(parsed):
                                    parsed = pd.to_datetime(val, errors='coerce', dayfirst=True)
                                if not pd.isna(parsed):
                                    val = parsed.to_pydatetime()
                                else:
                                    val = None
                        except Exception:
                            val = None

                    # Numeric coercion for monetary fields
                    if val is not None and norm_key in {nk_local('Flete'), nk_local('Maniobras'), nk_local('Otros'), nk_local('Subtotal'), nk_local('IVA'), nk_local('ISR'), nk_local('Total')}:
                        try:
                            s = re.sub(r"[^0-9.,\-]", "", str(val))
                            if s == '':
                                val = None
                            else:
                                if s.count(',') > 0 and s.count('.') == 0:
                                    s = s.replace(',', '.')
                                elif s.count(',') > 0 and s.count('.') > 0:
                                    if s.rfind('.') > s.rfind(','):
                                        s = s.replace(',', '')
                                    else:
                                        s = s.replace('.', '').replace(',', '.')
                                d = Decimal(s)
                                val = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        except Exception:
                            try:
                                val = float(str(val).replace(',', '.'))
                            except Exception:
                                val = None

                    # Assign to params by normalized expected key
                    if norm_key == nk_local('NOMBRE'):
                        params['Nombre'] = val
                    elif norm_key == nk_local('NO VIAJE'):
                        params['No_Viaje'] = val
                    elif norm_key == nk_local('NO FACTURA'):
                        params['No_Factura'] = val
                    elif norm_key == nk_local('FLETE'):
                        params['Flete'] = val
                    elif norm_key == nk_local('MANIOBRAS'):
                        params['Maniobras'] = val
                    elif norm_key == nk_local('OTROS'):
                        params['Otros'] = val
                    elif norm_key == nk_local('SUBTOTAL'):
                        params['Subtotal'] = val
                    elif norm_key == nk_local('IVA'):
                        params['IVA'] = val
                    elif norm_key == nk_local('ISR'):
                        params['ISR'] = val
                    elif norm_key == nk_local('TOTAL'):
                        params['Total'] = val
                    elif norm_key == nk_local('FECHA FACT'):
                        params['Fecha_Fact'] = val
                    elif norm_key == nk_local('CLIENTE'):
                        params['Cliente'] = val

            # Safety: convert ints to strings for textual columns where appropriate
            for k in list(params.keys()):
                v = params[k]
                if isinstance(v, int) and k not in ('Flete', 'Maniobras', 'Otros', 'Subtotal', 'IVA', 'ISR', 'Total'):
                    try:
                        params[k] = str(v)
                    except Exception:
                        pass

            # Log params
            try:
                def _serialize_val(v):
                    if v is None:
                        return None
                    try:
                        if isinstance(v, _dt):
                            return v.isoformat()
                    except Exception:
                        pass
                    try:
                        if isinstance(v, Decimal):
                            return str(v)
                    except Exception:
                        pass
                    return v

                loggable = {k: _serialize_val(v) for k, v in params.items()}
                #logging.getLogger('operations').info(f"Factoraje insert fila {idx}: {loggable}")
            except Exception as log_ex:
                logging.debug(f"No se pudo serializar params para logging en fila {idx}: {log_ex}")

            all_params_list.append(params)
        
        # Ejecutar BULK INSERT optimizado
        total_inserted = _bulk_insert_with_fallback(db, insert_sql, all_params_list, "factoraje_tmp")

        # After inserts, call post-processing SP if provided
        if username and original_name:
            processed_name = original_name
            if processed_name.lower().startswith('temp_'):
                processed_name = processed_name[5:]
            sp2_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado,1"
            logging.getLogger('operations').info(f"Ejecutando Procedure para factoraje usuario={username}, archivo={processed_name}")
            db.execute(text(sp2_sql), {"nombre_usuario": username, "name_file_procesado": processed_name})
            try:
                sp2_af = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                sp2_af = None
            logging.getLogger('operations').info(f"Procedure (factoraje) @@ROWCOUNT={sp2_af}")

        logging.getLogger('operations').info(f"Factoraje: procesadas {processed_count} filas, inserts afectaron aprox: {total_inserted}")
        return processed_count
    except Exception as e:
        logging.getLogger('operations').error(f"Error procesando factoraje {file_path}: {e}")
        try:
            logging.getLogger('operations').error(f"Error procesando factoraje {file_path}: {e}")
        except Exception:
            pass
        raise


def process_relacion_pago(file_path: str, db: object, username: Optional[str] = None, original_name: Optional[str] = None) -> int:
    """Process relacion_pago files into dbo.relacion_pago_tmp.

    - Reads the FIRST sheet of the workbook (sheet index 0)
    - Expects headers on the first row. Omits rows whose first column is empty.
    - Inserts rows (no commit) into dbo.relacion_pago_tmp following the exact column order described by the user.
    - Calls dbo.sp_proc_ontime(:nombre_usuario, :name_file_procesado) after successful inserts if username and original_name provided.

    Returns number of rows inserted (omitting empty-first-column rows).
    """
    logging.getLogger('operations').info(f"Procesando relacion_pago: {file_path}")
    try:
        logging.getLogger('operations').info(f"Procesando relacion_pago: {file_path}")
    except Exception:
        pass
    try:
        # Read first sheet with header=0
        try:
            df = pd.read_excel(file_path, sheet_name=0, header=1)
            logging.getLogger('operations').info("Reading relacion_pago first sheet with header=1")
        except Exception as read_err:
            logging.getLogger('operations').error(f"No se pudo leer la primera hoja: {read_err}")
            raise

        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        # Omit rows where first column is empty
        if df.shape[1] > 0:
            first_col = df.columns[0]
            before_count = len(df)
            try:
                non_empty_mask = df[first_col].notnull() & (df[first_col].astype(str).str.strip() != '')
            except Exception:
                non_empty_mask = df[first_col].notnull()
            df = df[non_empty_mask]
            after_count = len(df)
            dropped = before_count - after_count
            if dropped > 0:
                logging.getLogger('operations').info(f"Relacion Pago: omitidas {dropped} filas que iniciaban con campo vacío en columna '{first_col}'")

        records = df.to_dict(orient='records')
        processed_count = len(records)

        def nk_local(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()

        # Log columnas leídas por pandas (con sufijos automáticos para duplicados)
        logging.getLogger('operations').info(f"Columnas detectadas en relacion_pago: {list(df.columns)}")

        # Mapeo por posición de columna (0-based) según el orden esperado del Excel:
        # Nombre, No Viaje, No Factura, Flete, Maniobras, Otros, Subtotal, IVA, Subtotal, ISR, IVA, SUBTOTAL, TOTAL, FECHA FACT, CLIENTE
        # Pandas renombra duplicados: Subtotal, IVA, Subtotal.1, ISR, IVA.1, SUBTOTAL.2, TOTAL
        column_mapping = {}
        cols = list(df.columns)
        
        # Normalizar nombres de columnas para búsqueda
        normalized_cols = {nk_local(k): k for k in cols}
        
        # Buscar FECHA_FACT y CLIENTE por nombre normalizado
        fecha_fact_col = None
        cliente_col = None
        
        for norm_key, orig_col in normalized_cols.items():
            if 'FECHA' in norm_key and 'FACT' in norm_key:
                fecha_fact_col = orig_col
                logging.getLogger('operations').info(f"Encontrada columna FECHA_FACT: '{orig_col}'")
            elif norm_key == 'CLIENTE':
                cliente_col = orig_col
                logging.getLogger('operations').info(f"Encontrada columna CLIENTE: '{orig_col}'")
        
        # Construir mapeo: primeras 13 columnas por posición, FECHA_FACT y CLIENTE por búsqueda
        if len(cols) >= 13:
            column_mapping = {
                'Nombre': cols[0],           # Nombre
                'No_Viaje': cols[1],         # No Viaje
                'No_Factura': cols[2],       # No Factura
                'Flete': cols[3],            # Flete
                'Maniobras': cols[4],        # Maniobras
                'Otros': cols[5],            # Otros
                'Subtotal': cols[6],         # Subtotal (1ra)
                'IVA': cols[7],              # IVA (1ra)
                'Subtotal_IVA': cols[8],     # Subtotal (2da) → Subtotal_IVA
                'ISR': cols[9],              # ISR
                'IVA_ISR': cols[10],         # IVA (2da) → IVA_ISR
                'Subtotal_ISR': cols[11],    # SUBTOTAL (3ra) → Subtotal_ISR
                'Total': cols[12]            # TOTAL (índice 12)
            }
            
            # Agregar FECHA_FACT y CLIENTE si se encontraron
            if fecha_fact_col:
                column_mapping['Fecha_Fact'] = fecha_fact_col
            if cliente_col:
                column_mapping['Cliente'] = cliente_col
                
            logging.getLogger('operations').info(f"Mapeo completado: {len(column_mapping)} columnas mapeadas")
        else:
            logging.getLogger('operations').error(f"Número de columnas insuficiente: {len(cols)}, se esperan al menos 13 columnas")
            raise ValueError(f"El archivo debe tener al menos 13 columnas, pero tiene {len(cols)}")
            
        insert_sql = (
            "INSERT INTO dbo.relacion_pago_tmp (Nombre, No_Viaje, No_Factura, Flete, Maniobras, Otros, Subtotal, IVA, Subtotal_IVA, ISR, IVA_ISR, Subtotal_ISR, Total, Fecha_Fact, Cliente, Usuario_Creacion) "
            "VALUES (:Nombre, :No_Viaje, :No_Factura, :Flete, :Maniobras, :Otros, :Subtotal, :IVA, :Subtotal_IVA, :ISR, :IVA_ISR, :Subtotal_ISR, :Total, :Fecha_Fact, :Cliente, :Usuario_Creacion)"
        )

        # Preparar todos los parámetros para BULK INSERT
        all_params_list = []
        for idx, rec in enumerate(records, start=1):
            params = {
                'Nombre': None, 'No_Viaje': None, 'No_Factura': None, 'Flete': None, 'Maniobras': None, 'Otros': None,
                'Subtotal': None, 'IVA': None, 'Subtotal_IVA': None, 'ISR': None, 'IVA_ISR': None, 'Subtotal_ISR': None,
                'Total': None, 'Fecha_Fact': None, 'Cliente': None, 'Usuario_Creacion': username
            }

            # Mapear columnas usando el mapeo por posición
            for param_name, excel_col in column_mapping.items():
                val = rec.get(excel_col)
                
                # Normalize strings
                if isinstance(val, str):
                    val = val.replace('\xa0', ' ').strip()
                    if val == '':
                        val = None

                # Dates (solo para Fecha_Fact)
                if val is not None and param_name == 'Fecha_Fact':
                    try:
                        if hasattr(val, 'to_pydatetime'):
                            val = val.to_pydatetime()
                        elif isinstance(val, _dt):
                            pass
                        else:
                            parsed = pd.to_datetime(val, errors='coerce', dayfirst=False)
                            if pd.isna(parsed):
                                parsed = pd.to_datetime(val, errors='coerce', dayfirst=True)
                            if not pd.isna(parsed):
                                val = parsed.to_pydatetime()
                            else:
                                val = None
                    except Exception:
                        val = None

                # Numeric coercion for monetary fields
                if val is not None and param_name in {'Flete', 'Maniobras', 'Otros', 'Subtotal', 'IVA', 'Subtotal_IVA', 'ISR', 'IVA_ISR', 'Subtotal_ISR', 'Total'}:
                    try:
                        s = re.sub(r"[^0-9.,\-]", "", str(val))
                        if s == '':
                            val = None
                        else:
                            if s.count(',') > 0 and s.count('.') == 0:
                                s = s.replace(',', '.')
                            elif s.count(',') > 0 and s.count('.') > 0:
                                if s.rfind('.') > s.rfind(','):
                                    s = s.replace(',', '')
                                else:
                                    s = s.replace('.', '').replace(',', '.')
                            d = Decimal(s)
                            val = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    except Exception:
                        try:
                            val = float(str(val).replace(',', '.'))
                        except Exception:
                            val = None

                # Asignar al parámetro correspondiente
                params[param_name] = val

            # Safety: convert ints to strings for textual columns where appropriate
            for k in list(params.keys()):
                v = params[k]
                if isinstance(v, int) and k not in ('Flete', 'Maniobras', 'Otros', 'Subtotal', 'IVA', 'Subtotal_IVA', 'ISR', 'IVA_ISR', 'Subtotal_ISR', 'Total'):
                    try:
                        params[k] = str(v)
                    except Exception:
                        pass

            # Log params
            try:
                def _serialize_val(v):
                    if v is None:
                        return None
                    try:
                        if isinstance(v, _dt):
                            return v.isoformat()
                    except Exception:
                        pass
                    try:
                        if isinstance(v, Decimal):
                            return str(v)
                    except Exception:
                        pass
                    return v

                loggable = {k: _serialize_val(v) for k, v in params.items()}
               # logging.getLogger('operations').info(f"Relacion Pago insert fila {idx}: {loggable}")
            except Exception as log_ex:
                logging.debug(f"No se pudo serializar params para logging en fila {idx}: {log_ex}")

            all_params_list.append(params)
        
        # Limpiar tabla temporal antes de insertar
        logging.getLogger('operations').info("Limpiando tabla relacion_pago_tmp antes de insertar...")
        try:
            db.execute(text("DELETE FROM dbo.relacion_pago_tmp"))
            db.commit()
            logging.getLogger('operations').info("Tabla relacion_pago_tmp limpiada exitosamente")
        except Exception as delete_ex:
            logging.getLogger('operations').error(f"Error limpiando tabla relacion_pago_tmp: {delete_ex}")
            raise
        
        # Ejecutar BULK INSERT optimizado
        total_inserted = _bulk_insert_with_fallback(db, insert_sql, all_params_list, "relacion_pago_tmp")

        # After inserts, call post-processing SP if provided
        if total_inserted > 0:
            if username and original_name:
                processed_name = original_name
                if processed_name.lower().startswith('temp_'):
                    processed_name = processed_name[5:]
                
                logging.getLogger('operations').info(f"Ejecutando Procedure para Relacion Pago usuario={username}, archivo={processed_name}")
                
                try:
                    # Hacer commit antes de ejecutar el SP para cerrar la transacción actual
                    db.commit()
                    logging.getLogger('operations').info("Commit realizado antes de ejecutar SP")
                    
                    # Obtener la conexión raw de pyodbc
                    raw_conn = db.connection().connection
                    cursor = raw_conn.cursor()
                    
                    # Ejecutar el SP
                    cursor.execute(
                        "EXEC dbo.sp_proc_ontime ?, ?, 2",
                        (username, processed_name)
                    )
                    
                    # Iterar por todos los resultsets
                    error_msg = None
                    error_severity = 0
                    error_state = 0
                    resultset_count = 0
                    
                    try:
                        while True:
                            resultset_count += 1
                            logging.getLogger('operations').info(f"Procesando resultset #{resultset_count}")
                            
                            try:
                                rows = cursor.fetchall()
                                if rows and len(rows) > 0:
                                    logging.getLogger('operations').info(f"Resultset #{resultset_count} tiene {len(rows)} fila(s)")
                                    last_row = rows[-1]
                                    
                                    if cursor.description:
                                        col_names = [col[0].lower() for col in cursor.description]
                                        logging.getLogger('operations').info(f"Columnas: {col_names}")
                                        
                                        error_msg_idx = next((i for i, name in enumerate(col_names) if 'error_msg' in name.lower()), None)
                                        error_severity_idx = next((i for i, name in enumerate(col_names) if 'error_severity' in name.lower()), None)
                                        error_state_idx = next((i for i, name in enumerate(col_names) if 'error_state' in name.lower()), None)
                                        
                                        if error_msg_idx is not None:
                                            error_msg = last_row[error_msg_idx] if error_msg_idx is not None else None
                                            error_severity = last_row[error_severity_idx] if error_severity_idx is not None else 0
                                            error_state = last_row[error_state_idx] if error_state_idx is not None else 0
                                            logging.getLogger('operations').info(f"error_msg: '{error_msg}', error_severity: {error_severity}, error_state: {error_state}")
                                else:
                                    logging.getLogger('operations').info(f"Resultset #{resultset_count} vacío")
                            except Exception as fetch_ex:
                                logging.getLogger('operations').info(f"No se pudieron obtener filas: {fetch_ex}")
                            
                            # Avanzar al siguiente resultset
                            try:
                                if not cursor.nextset():
                                    logging.getLogger('operations').info(f"No hay más resultsets. Total: {resultset_count}")
                                    break
                            except Exception as nextset_ex:
                                # nextset() puede lanzar error si hay problemas de transacción
                                logging.getLogger('operations').info(f"Error en nextset (fin de resultsets): {nextset_ex}")
                                break
                        
                        # Verificar si hubo error
                        if error_state and error_state > 0 and error_msg and str(error_msg).strip():
                            logging.getLogger('operations').error(f"=" * 80)
                            logging.getLogger('operations').error(f"ERROR EN STORED PROCEDURE sp_proc_ontime")
                            logging.getLogger('operations').error(f"Usuario: {username}, Archivo: {processed_name}")
                            logging.getLogger('operations').error(f"Error State: {error_state}")
                            logging.getLogger('operations').error(f"Error Severity: {error_severity}")
                            logging.getLogger('operations').error(f"Error Message: {error_msg}")
                            logging.getLogger('operations').error(f"=" * 80)
                            raise Exception(f"Error en sp_proc_ontime: {error_msg} (State: {error_state}, Severity: {error_severity})")
                        else:
                            logging.getLogger('operations').info(f"Procedure (Relacion Pago) ejecutado exitosamente")
                            
                    finally:
                        try:
                            cursor.close()
                        except Exception:
                            pass
                        
                except Exception as sp_ex:
                    error_msg = str(sp_ex)
                    
                    # Si es un error que ya procesamos, re-lanzar
                    if "Error en sp_proc_ontime:" in error_msg and "State:" in error_msg:
                        raise
                    
                    # Error SQL inesperado
                    logging.getLogger('operations').error(f"=" * 80)
                    logging.getLogger('operations').error(f"ERROR EJECUTANDO STORED PROCEDURE")
                    logging.getLogger('operations').error(f"Usuario: {username}, Archivo: {processed_name}")
                    logging.getLogger('operations').error(f"Error: {error_msg}")
                    logging.getLogger('operations').error(f"=" * 80)
                    raise Exception(f"Error ejecutando sp_proc_ontime: {error_msg}")

        logging.getLogger('operations').info(f"Relacion Pago: procesadas {processed_count} filas, inserts afectaron aprox: {total_inserted}")
        return processed_count
    except Exception as e:
        logging.getLogger('operations').error(f"Error procesando Relacion Pago {file_path}: {e}")
        try:
            logging.getLogger('operations').error(f"Error procesando Relacion Pago {file_path}: {e}")
        except Exception:
            pass
        raise


def process_venta_perdida(file_path: str, db: object, username: Optional[str] = None, original_name: Optional[str] = None) -> int:
    """Process venta perdida files into dbo.venta_perdida_tmp.

    - Filename validated by caller; this function expects the saved temp file path and a DB session.
    - Reads the sheet whose name equals the year present in original_name (e.g., '2025'). Falls back to first sheet.
    - Maps columns to: Ejecutivo, Cliente, Capacidad, Total_Vta_Perdida, Sin_Programa_Carga
    - Inserts rows (no commit) into dbo.venta_perdida_tmp following that exact column order and calls sp_proc_ontime after inserts.

    Returns number of rows processed.
    """
    logging.getLogger('operations').info(f"Procesando venta perdida: {file_path}")
    try:
        try:
            logging.getLogger('operations').info(f"Procesando venta perdida: {file_path}")
        except Exception:
            pass
        # Determine sheet name: prefer year from original_name
        sheet_to_use = None
        year_from_name = None
        if original_name:
            m = re.search(r"(\d{4})", original_name)
            if m:
                year_from_name = m.group(1)

        with pd.ExcelFile(file_path) as xls:
            if year_from_name:
                for s in xls.sheet_names:
                    if s.strip() == year_from_name:
                        sheet_to_use = s
                        break
            # fallback: choose first sheet
            if sheet_to_use is None:
                sheet_to_use = xls.sheet_names[0]

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, header=0)
            logging.getLogger('operations').info(f"Reading venta perdida sheet '{sheet_to_use}' with header=0")
        except Exception as read_err:
            logging.getLogger('operations').error(f"No se pudo leer la hoja {sheet_to_use}: {read_err}")
            raise

        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        # Omit rows where first column empty
        if df.shape[1] > 0:
            first_col = df.columns[0]
            before_count = len(df)
            try:
                non_empty_mask = df[first_col].notnull() & (df[first_col].astype(str).str.strip() != '')
            except Exception:
                non_empty_mask = df[first_col].notnull()
            df = df[non_empty_mask]
            after_count = len(df)
            dropped = before_count - after_count
            if dropped > 0:
                logging.getLogger('operations').info(f"VentaPerdida: omitidas {dropped} filas que iniciaban con campo vacío en columna '{first_col}'")

        records = df.to_dict(orient='records')
        processed_count = len(records)

        def nk(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()

        normalized_key_map = {nk(k): k for k in list(df.columns)}

        # Include all columns required by dbo.venta_perdida_tmp
        insert_sql = (
            "INSERT INTO dbo.venta_perdida_tmp (Fecha_De_Oferta, No_De_Carga, Origen, Destino, Ruta, Fecha_De_Carga, Ejecutivo, Cliente, Capacidad, Total_Vta_Perdida, Sin_Programa_Carga, Usuario_Creacion) "
            "VALUES (:Fecha_De_Oferta, :No_De_Carga, :Origen, :Destino, :Ruta, :Fecha_De_Carga, :Ejecutivo, :Cliente, :Capacidad, :Total_Vta_Perdida, :Sin_Programa_Carga, :Usuario_Creacion)"
        )

        # Preparar todos los parámetros para BULK INSERT
        all_params_list = []
        for idx, rec in enumerate(records, start=1):
            params = {
                'Fecha_De_Oferta': None, 'No_De_Carga': None, 'Origen': None, 'Destino': None, 'Ruta': None,
                'Fecha_De_Carga': None, 'Ejecutivo': None, 'Cliente': None, 'Capacidad': None, 'Total_Vta_Perdida': None,
                'Sin_Programa_Carga': None, 'Usuario_Creacion': username
            }

            for norm_key, col in normalized_key_map.items():
                val = rec.get(col)
                if isinstance(val, str):
                    val = val.replace('\xa0', ' ').strip()
                    if val == '':
                        val = None

                # Fecha de oferta
                if 'FECHA' in norm_key and 'OFERT' in norm_key:
                    if val is None:
                        params['Fecha_De_Oferta'] = None
                    else:
                        try:
                            if hasattr(val, 'to_pydatetime'):
                                params['Fecha_De_Oferta'] = val.to_pydatetime()
                            elif isinstance(val, _dt):
                                params['Fecha_De_Oferta'] = val
                            else:
                                params['Fecha_De_Oferta'] = pd.to_datetime(str(val), dayfirst=False, errors='coerce')
                                if pd.isna(params['Fecha_De_Oferta']):
                                    params['Fecha_De_Oferta'] = pd.to_datetime(str(val), dayfirst=True, errors='coerce')
                        except Exception:
                            params['Fecha_De_Oferta'] = None

                # No de carga (identificador)
                elif ('NO' in norm_key and ('CARGA' in norm_key or 'CARGAS' in norm_key)) or ('NO' in norm_key and 'DE' in norm_key and 'CARGA' in norm_key) or 'NOCARGA' in norm_key:
                    params['No_De_Carga'] = val

                # Origen / Destino / Ruta
                elif 'ORIGEN' in norm_key:
                    params['Origen'] = val
                elif 'DESTINO' in norm_key:
                    params['Destino'] = val
                elif 'RUTA' in norm_key:
                    params['Ruta'] = val

                # Fecha de carga
                elif 'FECHA' in norm_key and 'CARGA' in norm_key:
                    if val is None:
                        params['Fecha_De_Carga'] = None
                    else:
                        try:
                            if hasattr(val, 'to_pydatetime'):
                                params['Fecha_De_Carga'] = val.to_pydatetime()
                            elif isinstance(val, _dt):
                                params['Fecha_De_Carga'] = val
                            else:
                                params['Fecha_De_Carga'] = pd.to_datetime(str(val), dayfirst=False, errors='coerce')
                                if pd.isna(params['Fecha_De_Carga']):
                                    params['Fecha_De_Carga'] = pd.to_datetime(str(val), dayfirst=True, errors='coerce')
                        except Exception:
                            params['Fecha_De_Carga'] = None

                # Ejecutivo / Cliente
                elif 'EJECUT' in norm_key:
                    params['Ejecutivo'] = val
                elif 'CLIENT' in norm_key:
                    params['Cliente'] = val

                # Capacidad (numeric)
                elif 'CAPACIDAD' in norm_key:
                    if val is None:
                        params['Capacidad'] = None
                    else:
                        try:
                            s = re.sub(r"[^0-9.,\-]", "", str(val))
                            if s == '':
                                params['Capacidad'] = None
                            else:
                                if s.count(',') > 0 and s.count('.') == 0:
                                    s = s.replace(',', '.')
                                elif s.count(',') > 0 and s.count('.') > 0:
                                    if s.rfind('.') > s.rfind(','):
                                        s = s.replace(',', '')
                                    else:
                                        s = s.replace('.', '').replace(',', '.')
                                d = Decimal(s)
                                params['Capacidad'] = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        except Exception:
                            try:
                                params['Capacidad'] = Decimal(str(float(str(val).replace(',', '.')))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                            except Exception:
                                params['Capacidad'] = None

                # Total Vta Perdida
                elif 'TOTAL' in norm_key and ('VTA' in norm_key or 'VENTA' in norm_key or 'PERDIDA' in norm_key or 'VTA PERDIDA' in norm_key or 'VTA_PERDIDA' in norm_key):
                    if val is None:
                        params['Total_Vta_Perdida'] = None
                    else:
                        try:
                            s = re.sub(r"[^0-9.,\-]", "", str(val))
                            if s == '':
                                params['Total_Vta_Perdida'] = None
                            else:
                                if s.count(',') > 0 and s.count('.') == 0:
                                    s = s.replace(',', '.')
                                elif s.count(',') > 0 and s.count('.') > 0:
                                    if s.rfind('.') > s.rfind(','):
                                        s = s.replace(',', '')
                                    else:
                                        s = s.replace('.', '').replace(',', '.')
                                d = Decimal(s)
                                params['Total_Vta_Perdida'] = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        except Exception:
                            try:
                                params['Total_Vta_Perdida'] = Decimal(str(float(str(val).replace(',', '.')))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                            except Exception:
                                params['Total_Vta_Perdida'] = None

                # Sin Programa de Carga (texto)
                elif 'SIN' in norm_key and 'PROGRAMA' in norm_key:
                    params['Sin_Programa_Carga'] = val

            # Ensure textual fields are strings when they are ints
            for k in ('No_De_Carga', 'Origen', 'Destino', 'Ruta', 'Ejecutivo', 'Cliente', 'Sin_Programa_Carga'):
                v = params.get(k)
                if isinstance(v, int):
                    try:
                        params[k] = str(v)
                    except Exception:
                        pass

            # Serialize for logging
            try:
                def _serialize_val(v):
                    if v is None:
                        return None
                    try:
                        if isinstance(v, _dt):
                            return v.isoformat()
                    except Exception:
                        pass
                    try:
                        if isinstance(v, Decimal):
                            return str(v)
                    except Exception:
                        pass
                    return v

                loggable = {k: _serialize_val(v) for k, v in params.items()}
              #  logging.getLogger('operations').info(f"VentaPerdida insert fila {idx}: {loggable}")
            except Exception:
                pass

            all_params_list.append(params)
        
        # Ejecutar BULK INSERT optimizado
        total_inserted = _bulk_insert_with_fallback(db, insert_sql, all_params_list, "venta_perdida_tmp")

        # call sp_proc_ontime if provided
        if username and original_name:
            processed_name = original_name
            if processed_name.lower().startswith('temp_'):
                processed_name = processed_name[5:]
            sp2_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado,12"
            logging.getLogger('operations').info(f"Ejecutando Procedure para VentaPerdida usuario={username}, archivo={processed_name}")
            db.execute(text(sp2_sql), {"nombre_usuario": username, "name_file_procesado": processed_name})
            try:
                sp2_af = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                sp2_af = None
            logging.getLogger('operations').info(f"Procedure (VentaPerdida) @@ROWCOUNT={sp2_af}")

        logging.getLogger('operations').info(f"VentaPerdida: procesadas {processed_count} filas, inserts afectaron aprox: {total_inserted}")
        return processed_count
    except Exception as e:
        logging.getLogger('operations').error(f"Error procesando Venta Perdida {file_path}: {e}")
        try:
            logging.getLogger('operations').error(f"Error procesando Venta Perdida {file_path}: {e}")
        except Exception:
            pass
        raise


def process_evidencias_pendientes(file_path: str, db: object, username: Optional[str] = None, original_name: Optional[str] = None) -> int:
    """Process evidencias pendientes files into dbo.evidencias_pendientes_tmp.

    - Expects a sheet named 'TABLA' (case-insensitive).
    - Validates and reads header row (header=0).
    - Identifies the 'CLIENTE' column and treats the following columns as month-name columns.
      MES1..MES4 are taken from the first four month columns found to the right of CLIENTE.
    - Inserts rows (no commit) into dbo.evidencias_pendientes_tmp in the exact column order requested.
    - Calls dbo.sp_proc_ontime(:nombre_usuario, :name_file_procesado) after successful inserts if username and original_name provided.

    Returns number of rows processed.
    """
    logging.getLogger('operations').info(f"Procesando evidencias_pendientes: {file_path}")
    try:
        try:
            logging.getLogger('operations').info(f"Procesando evidencias_pendientes: {file_path}")
        except Exception:
            pass
        # Find sheet named 'TABLA'
        sheet_to_use = None
        with pd.ExcelFile(file_path) as xls:
            for s in xls.sheet_names:
                if s.strip().upper() == 'TABLA':
                    sheet_to_use = s
                    break
        if sheet_to_use is None:
            raise ValueError("Hoja 'TABLA' no encontrada en el archivo Excel")

        # Buscar la celda que contiene "CLIENTE" para determinar la fila y columna de inicio
        # Leer sin encabezado para buscar en todas las celdas
        df_search = pd.read_excel(file_path, sheet_name=sheet_to_use, header=None)
        
        # Normalizar función para búsqueda
        def nk(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()
        
        # Buscar "CLIENTE" en las primeras 20 filas y columnas
        cliente_row = None
        cliente_col_idx = None
        max_search_rows = min(20, len(df_search))
        max_search_cols = min(20, df_search.shape[1])
        
        for row_idx in range(max_search_rows):
            for col_idx in range(max_search_cols):
                cell_value = df_search.iloc[row_idx, col_idx]
                if cell_value is not None and nk(str(cell_value)) == 'CLIENTE':
                    cliente_row = row_idx
                    cliente_col_idx = col_idx
                    logging.getLogger('operations').info(f"Encontrado 'CLIENTE' en fila {cliente_row} (0-based), columna {cliente_col_idx} (0-based)")
                    break
            if cliente_row is not None:
                break
        
        if cliente_row is None:
            raise ValueError("No se encontró la celda con texto 'CLIENTE' en la hoja TABLA")
        
        # Leer usando header=cliente_row directamente (pandas cuenta desde 0)
        # Esto hará que pandas use la fila cliente_row como encabezado
        # y los datos comenzarán automáticamente en cliente_row + 1
        
        # Construir lista de índices de columnas a leer (desde cliente_col_idx en adelante)
        cols_to_read = list(range(cliente_col_idx, df_search.shape[1]))
        
        try:
            # header=cliente_row: pandas usará esa fila (0-based) como encabezado
            # usecols: usar solo las columnas desde cliente_col_idx en adelante
            df = pd.read_excel(
                file_path, 
                sheet_name=sheet_to_use, 
                header=cliente_row,
                usecols=cols_to_read
            )
            logging.getLogger('operations').info(f"Leyendo evidencias_pendientes: encabezado en fila Excel {cliente_row + 1} (0-based: {cliente_row}), datos desde fila Excel {cliente_row + 2}, columnas desde índice {cliente_col_idx}")
            
            # Verificar que el encabezado contenga CLIENTE
            cols_read = list(df.columns)
            logging.getLogger('operations').info(f"Columnas leídas: {cols_read[:5]}...")  # Mostrar primeras 5 columnas
        except Exception as read_err:
            logging.getLogger('operations').error(f"No se pudo leer con usecols: {read_err}")
            # Fallback sin usecols - usar header=cliente_row directamente
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, header=cliente_row)
            # Recortar columnas manualmente si cliente_col_idx > 0
            if cliente_col_idx > 0 and df.shape[1] > cliente_col_idx:
                df = df.iloc[:, cliente_col_idx:]
                logging.getLogger('operations').info(f"Recortadas {cliente_col_idx} columnas del inicio en fallback")

        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        cols = list(df.columns)
        normalized_map = {nk(c): c for c in cols}

        # Verificar que la primera columna sea CLIENTE
        if 'CLIENTE' not in normalized_map:
            raise ValueError(f"Error en lectura: primera columna esperada 'CLIENTE' pero se encontró '{cols[0] if cols else 'ninguna'}'. Verifique el formato del archivo.")
        
        cliente_col = normalized_map['CLIENTE']
        cliente_index = cols.index(cliente_col)
        
        # Validar que CLIENTE esté en la primera posición
        if cliente_index != 0:
            logging.getLogger('operations').warning(f"Columna CLIENTE encontrada en posición {cliente_index}, reordenando para que sea la primera")
            # Reordenar columnas para que CLIENTE esté primero
            cols_reordered = [cliente_col] + [c for c in cols if c != cliente_col]
            df = df[cols_reordered]
            cols = list(df.columns)
            normalized_map = {nk(c): c for c in cols}
            cliente_col = normalized_map['CLIENTE']
            cliente_index = 0



        # Month columns are the columns after the cliente column
        month_cols = cols[cliente_index + 1:]
        # Select first four month columns (if less than 4, fill with None)
        month_cols = [c for c in month_cols if c is not None and str(c).strip() != '']
        # Keep original header names for MesX
        selected_month_cols = month_cols[:4]

        # Filtrar filas donde la columna CLIENTE esté vacía o sea NULL
        if df.shape[0] > 0:
            before_filter = len(df)
            try:
                # Filtrar filas con cliente no nulo y no vacío
                non_empty_cliente = df[cliente_col].notnull() & (df[cliente_col].astype(str).str.strip() != '')
                df = df[non_empty_cliente]
                after_filter = len(df)
                dropped_empty = before_filter - after_filter
                if dropped_empty > 0:
                    logging.getLogger('operations').info(f"EvidenciasPendientes: omitidas {dropped_empty} filas con columna CLIENTE vacía")
            except Exception as filter_err:
                logging.getLogger('operations').warning(f"No se pudo filtrar filas vacías de CLIENTE: {filter_err}")
        
        records = df.to_dict(orient='records')
        processed_count = len(records)
        
        # Log de muestra de los primeros registros para debug
        if len(records) > 0:
            sample_rec = records[0]
            logging.getLogger('operations').info(f"Primer registro de ejemplo - Cliente: '{sample_rec.get(cliente_col)}', Columnas mes: {list(sample_rec.keys())[:5]}")

        insert_sql = (
            "INSERT INTO dbo.evidencias_pendientes_tmp (Cliente, Mes1, Total_Mes1, Mes2, Total_Mes2, Mes3, Total_Mes3, Mes4, Total_Mes4, Total_General, Usuario_Creacion) "
            "VALUES (:Cliente, :Mes1, :Total_Mes1, :Mes2, :Total_Mes2, :Mes3, :Total_Mes3, :Mes4, :Total_Mes4, :Total_General, :Usuario_Creacion)"
        )

        all_params_list = []
        total_inserted = 0
        # detect if there is an explicit "Total general" column in the sheet
        total_general_col = None
        for k_norm, k in normalized_map.items():
            if 'TOTAL' in k_norm and 'GENERAL' in k_norm:
                total_general_col = k
                break

        for idx, rec in enumerate(records, start=1):
            params = {
                'Cliente': None,
                'Mes1': None, 'Total_Mes1': None,
                'Mes2': None, 'Total_Mes2': None,
                'Mes3': None, 'Total_Mes3': None,
                'Mes4': None, 'Total_Mes4': None,
                'Total_General': None,
                'Usuario_Creacion': username
            }

            # Cliente value
            try:
                cval = rec.get(cliente_col)
            except Exception:
                cval = None
            if isinstance(cval, str):
                cval = cval.replace('\xa0', ' ').strip()
                if cval == '':
                    cval = None
            
            # Skip rows where Cliente is None (aunque ya deberían estar filtradas)
            if cval is None:
                logging.getLogger('operations').debug(f"Saltando fila {idx} con Cliente NULL")
                continue
                
            params['Cliente'] = cval

         
                # For each selected month column, set MesX to header name and Total_MesX to numeric value
            for i in range(4):
                header_name = None
                cell_val = None
                if i < len(selected_month_cols):
                    header_name = selected_month_cols[i]
                    cell_val = rec.get(header_name)
                # store month header (as string) into MesX
                params[f'Mes{i+1}'] = None if header_name is None else str(header_name).strip()

                # Coerce numeric amount for Total_MesX
                if cell_val is None or (isinstance(cell_val, str) and cell_val.strip() == ''):
                    params[f'Total_Mes{i+1}'] = None
                else:
                    # sanitize and convert to Decimal
                    try:
                        if hasattr(cell_val, 'to_pydatetime'):
                            # unlikely but treat as None
                            params[f'Total_Mes{i+1}'] = None
                        else:
                            s = re.sub(r"[^0-9.,\-]", "", str(cell_val))
                            if s == '':
                                params[f'Total_Mes{i+1}'] = None
                            else:
                                if s.count(',') > 0 and s.count('.') == 0:
                                    s = s.replace(',', '.')
                                elif s.count(',') > 0 and s.count('.') > 0:
                                    if s.rfind('.') > s.rfind(','):
                                        s = s.replace(',', '')
                                    else:
                                        s = s.replace('.', '').replace(',', '.')
                                d = Decimal(s)
                                params[f'Total_Mes{i+1}'] = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    except Exception:
                        try:
                            params[f'Total_Mes{i+1}'] = Decimal(str(float(str(cell_val).replace(',', '.')))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        except Exception:
                            params[f'Total_Mes{i+1}'] = None

            # If an explicit Total General column exists, prefer its value
            if total_general_col is not None:
                tg_val = rec.get(total_general_col)
                if tg_val is None or (isinstance(tg_val, str) and tg_val.strip() == ''):
                    params['Total_General'] = None
                else:
                    try:
                        s = re.sub(r"[^0-9.,\-]", "", str(tg_val))
                        if s == '':
                            params['Total_General'] = None
                        else:
                            if s.count(',') > 0 and s.count('.') == 0:
                                s = s.replace(',', '.')
                            elif s.count(',') > 0 and s.count('.') > 0:
                                if s.rfind('.') > s.rfind(','):
                                    s = s.replace(',', '')
                                else:
                                    s = s.replace('.', '').replace(',', '.')
                            d = Decimal(s)
                            params['Total_General'] = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    except Exception:
                        try:
                            params['Total_General'] = Decimal(str(float(str(tg_val).replace(',', '.')))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        except Exception:
                            params['Total_General'] = None
            else:
                # Fallback: compute total from the Total_Mes* fields we parsed
                try:
                    sum_val = Decimal('0')
                    any_num = False
                    for j in range(1,5):
                        mv = params.get(f'Total_Mes{j}')
                        if mv is not None:
                            any_num = True
                            sum_val += Decimal(mv)
                    params['Total_General'] = sum_val.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if any_num else None
                except Exception:
                    params['Total_General'] = None

            # Log params
            try:
                def _serialize_val(v):
                    if v is None:
                        return None
                    try:
                        if isinstance(v, _dt):
                            return v.isoformat()
                    except Exception:
                        pass
                    try:
                        if isinstance(v, Decimal):
                            return str(v)
                    except Exception:
                        pass
                    return v

                loggable = {k: _serialize_val(v) for k, v in params.items()}
             #   logging.getLogger('operations').info(f"EvidenciasPendientes insert fila {idx}: {loggable}")
            except Exception:
                pass

            all_params_list.append(params)

        # After loop, perform BULK INSERT
        total_inserted = _bulk_insert_with_fallback(db, insert_sql, all_params_list, "evidencias_pendientes_tmp")

        # After inserts, call post-processing SP if provided
        if username and original_name:
            processed_name = original_name
            if processed_name.lower().startswith('temp_'):
                processed_name = processed_name[5:]
            sp2_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado,5"
            logging.getLogger('operations').info(f"Ejecutando Procedure para Evidencias Pendientes usuario={username}, archivo={processed_name}")
            db.execute(text(sp2_sql), {"nombre_usuario": username, "name_file_procesado": processed_name})
            try:
                sp2_af = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                sp2_af = None
            logging.getLogger('operations').info(f"Procedure (EvidenciasPendientes) @@ROWCOUNT={sp2_af}")

        logging.getLogger('operations').info(f"EvidenciasPendientes: procesadas {processed_count} filas, inserts afectaron aprox: {total_inserted}")
        return processed_count
    except Exception as e:
        logging.getLogger('operations').error(f"Error procesando Evidencias Pendientes {file_path}: {e}")
        try:
            logging.getLogger('operations').error(f"Error procesando Evidencias Pendientes {file_path}: {e}")
        except Exception:
            pass
        raise


def process_pronostico_cobranza(file_path: str, db: object, username: Optional[str] = None, original_name: Optional[str] = None) -> int:
    """Process pronostico cobranza files into dbo.pronostico_cobranza_tmp.

    Rules:
    - Filename validation is done by the caller (endpoint). This function expects a saved temp file path and a DB session.
    - Sheet name must be exactly 'TABLA NUEVA' (case-insensitive).
    - Header row = 0. The column named CLIENTE is located and the following columns are treated as Semana columns.
      Up to 13 Semana columns are mapped to Semana1..Semana13 (header string) and Total_SemanaX (the numeric value in row).
    - If an explicit 'Total general' column exists, its numeric value is used for Total_General; otherwise Total_General is the sum of the available Total_SemanaX values.
    - Inserts are executed row-by-row (no commit here). After inserts, if username and original_name are provided, executes dbo.sp_proc_ontime.

    Returns number of rows processed.
    """
    logging.getLogger('operations').info(f"Procesando pronostico cobranza: {file_path}")
    try:
        try:
            logging.getLogger('operations').info(f"Procesando pronostico cobranza: {file_path}")
        except Exception:
            pass
        # Find sheet named 'TABLA NUEVA'
        sheet_to_use = None
        with pd.ExcelFile(file_path) as xls:
            for s in xls.sheet_names:
                if s.strip().upper() == 'TABLA NUEVA':
                    sheet_to_use = s
                    break
        if sheet_to_use is None:
            raise ValueError("Hoja 'TABLA NUEVA' no encontrada en el archivo Excel")

        # Buscar la fila y columnas que contienen los encabezados
        # Leer sin encabezado para buscar en todas las celdas
        df_search = pd.read_excel(file_path, sheet_name=sheet_to_use, header=None)
        
        # Normalizar función para búsqueda
        def nk(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()
        
        # Buscar "Etiquetas de fila" en las primeras 20 filas y columnas
        etiquetas_row = None
        etiquetas_col_idx = None
        total_general_col_idx = None
        max_search_rows = min(20, len(df_search))
        max_search_cols = min(20, df_search.shape[1])
        
        for row_idx in range(max_search_rows):
            for col_idx in range(max_search_cols):
                cell_value = df_search.iloc[row_idx, col_idx]
                if cell_value is not None and 'ETIQUETAS' in nk(str(cell_value)) and 'FILA' in nk(str(cell_value)):
                    etiquetas_row = row_idx
                    etiquetas_col_idx = col_idx
                    logging.getLogger('operations').info(f"Encontrado 'Etiquetas de fila' en fila {etiquetas_row} (0-based), columna {etiquetas_col_idx} (0-based)")
                    
                    # Buscar "Total general" en la misma fila para determinar el rango de columnas
                    for end_col_idx in range(col_idx + 1, min(col_idx + 20, df_search.shape[1])):
                        end_cell = df_search.iloc[row_idx, end_col_idx]
                        if end_cell is not None and 'TOTAL' in nk(str(end_cell)) and 'GENERAL' in nk(str(end_cell)):
                            total_general_col_idx = end_col_idx
                            logging.getLogger('operations').info(f"Encontrado 'Total general' en columna {total_general_col_idx} (0-based)")
                            break
                    break
            if etiquetas_row is not None:
                break
        
        if etiquetas_row is None:
            raise ValueError("No se encontró la celda con texto 'Etiquetas de fila' en la hoja TABLA NUEVA")
        
        if total_general_col_idx is None:
            logging.getLogger('operations').warning("No se encontró 'Total general', usando todas las columnas desde 'Etiquetas de fila'")
            total_general_col_idx = df_search.shape[1] - 1
        
        # Construir lista de columnas a leer (desde etiquetas_col_idx hasta total_general_col_idx inclusive)
        cols_to_read = list(range(etiquetas_col_idx, total_general_col_idx + 1))
        
        # Leer usando header=etiquetas_row para que esa fila sea el encabezado
        try:
            df = pd.read_excel(
                file_path, 
                sheet_name=sheet_to_use, 
                header=etiquetas_row,
                usecols=cols_to_read
            )
            logging.getLogger('operations').info(f"Leyendo pronostico cobranza: encabezado en fila Excel {etiquetas_row + 1} (0-based: {etiquetas_row}), datos desde fila Excel {etiquetas_row + 2}, columnas {etiquetas_col_idx} a {total_general_col_idx}")
            logging.getLogger('operations').info(f"Columnas leídas: {list(df.columns)}")
        except Exception as read_err:
            logging.getLogger('operations').error(f"No se pudo leer con usecols: {read_err}")
            # Fallback sin usecols
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, header=etiquetas_row)
            # Recortar columnas manualmente
            if etiquetas_col_idx > 0 or total_general_col_idx < df.shape[1] - 1:
                df = df.iloc[:, etiquetas_col_idx:total_general_col_idx + 1]
                logging.getLogger('operations').info(f"Recortadas columnas en fallback: {list(df.columns)}")

        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        # Filtrar filas: detener antes de encontrar una fila cuya primera columna contenga "Total general"
        if df.shape[1] > 0:
            first_col = df.columns[0]
            rows_to_keep = []
            for idx, row in df.iterrows():
                first_val = row[first_col]
                if first_val is not None and 'TOTAL' in nk(str(first_val)) and 'GENERAL' in nk(str(first_val)):
                    logging.getLogger('operations').info(f"Encontrada fila de 'Total general' en índice {idx}, deteniendo lectura de datos")
                    break
                rows_to_keep.append(idx)
            df = df.loc[rows_to_keep]

        # Omitir filas donde la primera columna esté vacía
        if df.shape[1] > 0:
            first_col = df.columns[0]
            before_count = len(df)
            try:
                non_empty_mask = df[first_col].notnull() & (df[first_col].astype(str).str.strip() != '')
            except Exception:
                non_empty_mask = df[first_col].notnull()
            df = df[non_empty_mask]
            after_count = len(df)
            dropped = before_count - after_count
            if dropped > 0:
                logging.getLogger('operations').info(f"PronosticoCobranza: omitidas {dropped} filas con primera columna vacía")

        records = df.to_dict(orient='records')
        processed_count = len(records)
        
        # Log de muestra del primer registro
        if len(records) > 0:
            sample_rec = records[0]
            logging.getLogger('operations').info(f"Primer registro de ejemplo - Primera columna: '{sample_rec.get(df.columns[0])}', Columnas: {list(sample_rec.keys())}")

        cols = list(df.columns)
        normalized_map = {nk(c): c for c in cols}

        # La primera columna es el cliente (Etiquetas de fila)
        cliente_col = cols[0]
        cliente_index = 0

        # Las columnas de semanas son las columnas después de la primera (cliente), excluyendo la última si es Total general
        week_cols = cols[cliente_index + 1:]
        # La última columna debe ser Total general, excluirla de las semanas
        if len(week_cols) > 0 and 'TOTAL' in nk(week_cols[-1]) and 'GENERAL' in nk(week_cols[-1]):
            total_general_col = week_cols[-1]
            week_cols = week_cols[:-1]
        else:
            total_general_col = None
        
        # Filtrar columnas vacías
        week_cols = [c for c in week_cols if c is not None and str(c).strip() != '']

        insert_sql = (
            "INSERT INTO dbo.pronostico_cobranza_tmp (Cliente, Semana1, Total_Semana1, Semana2, Total_Semana2, Semana3, Total_Semana3, Semana4, Total_Semana4, Semana5, Total_Semana5, Semana6, Total_Semana6, Semana7, Total_Semana7, Semana8, Total_Semana8, Semana9, Total_Semana9, Semana10, Total_Semana10, Semana11, Total_Semana11, Semana12, Total_Semana12, Semana13, Total_Semana13, Total_General, Usuario_Creacion, Fecha_Creacion) "
            "VALUES (:Cliente, :Semana1, :Total_Semana1, :Semana2, :Total_Semana2, :Semana3, :Total_Semana3, :Semana4, :Total_Semana4, :Semana5, :Total_Semana5, :Semana6, :Total_Semana6, :Semana7, :Total_Semana7, :Semana8, :Total_Semana8, :Semana9, :Total_Semana9, :Semana10, :Total_Semana10, :Semana11, :Total_Semana11, :Semana12, :Total_Semana12, :Semana13, :Total_Semana13, :Total_General, :Usuario_Creacion, :Fecha_Creacion)"
        )

        all_params_list = []
        total_inserted = 0

        for idx, rec in enumerate(records, start=1):
            params = {
                'Cliente': None,
                'Semana1': None, 'Total_Semana1': None,
                'Semana2': None, 'Total_Semana2': None,
                'Semana3': None, 'Total_Semana3': None,
                'Semana4': None, 'Total_Semana4': None,
                'Semana5': None, 'Total_Semana5': None,
                'Semana6': None, 'Total_Semana6': None,
                'Semana7': None, 'Total_Semana7': None,
                'Semana8': None, 'Total_Semana8': None,
                'Semana9': None, 'Total_Semana9': None,
                'Semana10': None, 'Total_Semana10': None,
                'Semana11': None, 'Total_Semana11': None,
                'Semana12': None, 'Total_Semana12': None,
                'Semana13': None, 'Total_Semana13': None,
                'Total_General': None,
                'Usuario_Creacion': username,
                'Fecha_Creacion': None
            }

            # Cliente
            try:
                cval = rec.get(cliente_col)
            except Exception:
                cval = None
            if isinstance(cval, str):
                cval = cval.replace('\xa0', ' ').strip()
                if cval == '':
                    cval = None
            params['Cliente'] = cval

            # Fill Semana headers and totals up to 13
            sum_totals = Decimal('0')
            any_total_present = False
            for i in range(13):
                header_name = None
                cell_val = None
                if i < len(week_cols):
                    header_name = week_cols[i]
                    try:
                        cell_val = rec.get(week_cols[i])
                    except Exception:
                        cell_val = None

                params[f'Semana{i+1}'] = None if header_name is None else str(header_name).strip()

                # Coerce numeric amount for Total_SemanaX
                if cell_val is None or (isinstance(cell_val, str) and cell_val.strip() == ''):
                    params[f'Total_Semana{i+1}'] = None
                else:
                    try:
                        s = re.sub(r"[^0-9.,\-]", "", str(cell_val))
                        if s == '':
                            params[f'Total_Semana{i+1}'] = None
                        else:
                            if s.count(',') > 0 and s.count('.') == 0:
                                s = s.replace(',', '.')
                            elif s.count(',') > 0 and s.count('.') > 0:
                                if s.rfind('.') > s.rfind(','):
                                    s = s.replace(',', '')
                                else:
                                    s = s.replace('.', '').replace(',', '.')
                            d = Decimal(s)
                            d = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                            params[f'Total_Semana{i+1}'] = d
                            sum_totals += d
                            any_total_present = True
                    except Exception:
                        try:
                            params[f'Total_Semana{i+1}'] = Decimal(str(float(str(cell_val).replace(',', '.')))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                            sum_totals += params[f'Total_Semana{i+1}']
                            any_total_present = True
                        except Exception:
                            params[f'Total_Semana{i+1}'] = None

            # Total general: prefer explicit column, otherwise sum
            if total_general_col is not None:
                tg_val = None
                try:
                    tg_val = rec.get(total_general_col)
                except Exception:
                    tg_val = None
                if tg_val is None or (isinstance(tg_val, str) and tg_val.strip() == ''):
                    params['Total_General'] = None
                else:
                    try:
                        s = re.sub(r"[^0-9.,\-]", "", str(tg_val))
                        if s == '':
                            params['Total_General'] = None
                        else:
                            if s.count(',') > 0 and s.count('.') == 0:
                                s = s.replace(',', '.')
                            elif s.count(',') > 0 and s.count('.') > 0:
                                if s.rfind('.') > s.rfind(','):
                                    s = s.replace(',', '')
                                else:
                                    s = s.replace('.', '').replace(',', '.')
                            d = Decimal(s)
                            params['Total_General'] = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    except Exception:
                        try:
                            params['Total_General'] = Decimal(str(float(str(tg_val).replace(',', '.')))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        except Exception:
                            params['Total_General'] = None
            else:
                if any_total_present:
                    params['Total_General'] = sum_totals.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                else:
                    params['Total_General'] = None

            # Fecha_Creacion: leave NULL (caller/DB can set) or set to current UTC
            params['Fecha_Creacion'] = None

            # Log params
            try:
                def _serialize_val(v):
                    if v is None:
                        return None
                    try:
                        if isinstance(v, _dt):
                            return v.isoformat()
                    except Exception:
                        pass
                    try:
                        if isinstance(v, Decimal):
                            return str(v)
                    except Exception:
                        pass
                    return v

                loggable = {k: _serialize_val(v) for k, v in params.items()}
              #  logging.getLogger('operations').info(f"PronosticoCobranza insert fila {idx}: {loggable}")
            except Exception:
                pass

            all_params_list.append(params)

        # After loop, perform BULK INSERT
        total_inserted = _bulk_insert_with_fallback(db, insert_sql, all_params_list, "pronostico_cobranza_tmp")

        # After inserts, call post-processing SP if provided
        if username and original_name:
            processed_name = original_name
            if processed_name.lower().startswith('temp_'):
                processed_name = processed_name[5:]
            sp2_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado,6"
            logging.getLogger('operations').info(f"Ejecutando Procedure para PronosticoCobranza usuario={username}, archivo={processed_name}")
            db.execute(text(sp2_sql), {"nombre_usuario": username, "name_file_procesado": processed_name})
            try:
                sp2_af = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                sp2_af = None
            logging.getLogger('operations').info(f"Procedure (PronosticoCobranza) @@ROWCOUNT={sp2_af}")

        logging.getLogger('operations').info(f"PronosticoCobranza: procesadas {processed_count} filas, inserts afectaron aprox: {total_inserted}")
        return processed_count
    except Exception as e:
        logging.getLogger('operations').error(f"Error procesando Pronostico Cobranza {file_path}: {e}")
        try:
            logging.getLogger('operations').error(f"Error procesando Pronostico Cobranza {file_path}: {e}")
        except Exception:
            pass
        raise