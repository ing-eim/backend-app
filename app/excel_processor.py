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


# --- Module logging: write excel_processor logs into logs/operations.log (rotated daily/size) ---
try:
    _logs_dir = ensure_logs_dir()

    # Use a stable base filename for operations; rotation will create dated/numbered backups
    _operations_base = os.path.join(_logs_dir, 'operations.log')

    # Avoid adding multiple handlers if module is re-imported
    _mod_logger = logging.getLogger(__name__)
    if not any(getattr(h, 'baseFilename', None) and os.path.abspath(getattr(h, 'baseFilename')) == os.path.abspath(_operations_base) for h in _mod_logger.handlers):
        _fmt = "%(asctime)s %(levelname)s %(message)s"

        # Use milliseconds formatter like the rest of the app
        class _MilliFormatter(logging.Formatter):
            def formatTime(self, record, datefmt=None):
                from datetime import datetime
                ct = datetime.fromtimestamp(record.created)
                s = ct.strftime("%Y-%m-%d %H:%M:%S")
                ms = int(record.msecs)
                return f"{s},{ms:03d}"

        # Rotate daily at midnight and when size exceeds 10MB, keep 14 backups
        _handler = SizeAndTimedRotatingFileHandler(_operations_base, when='midnight', backupCount=14, encoding='utf-8', maxBytes=10 * 1024 * 1024)
        _handler.setLevel(logging.getLogger('operations').info)
        _handler.setFormatter(_MilliFormatter(_fmt, datefmt="%Y-%m-%d %H:%M:%S"))
        _mod_logger.addHandler(_handler)
        _mod_logger.setLevel(logging.getLogger('operations').info)
        _mod_logger.propagate = False

    # Also ensure root logger has an api.log handler that rotates (only add if not present)
    try:
        _api_path = os.path.join(_logs_dir, 'api.log')
        _root = logging.getLogger()
        if not any(hasattr(h, 'baseFilename') and os.path.abspath(getattr(h, 'baseFilename')) == os.path.abspath(_api_path) for h in _root.handlers):
            _api_handler = SizeAndTimedRotatingFileHandler(_api_path, when='midnight', backupCount=30, encoding='utf-8', maxBytes=10 * 1024 * 1024)
            _api_handler.setLevel(logging.getLogger('operations').info)
            _api_handler.setFormatter(_MilliFormatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
            _root.addHandler(_api_handler)
    except Exception:
        pass
except Exception:
    # If logging setup fails, continue without module-specific logging
    logging.getLogger(__name__).warning("No se pudo configurar el logging específico para excel_processor")

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
            logging.getLogger('operations').info(f"Archivo detectado como OnTime, leyendo hoja 'OCT25' desde fila 7, columna 1: {filename}")
            # Verificar que la hoja 'OCT25' exista (case-insensitive)
            # Use context manager to ensure the Excel file handle is closed promptly
            sheet_to_use = None
            with pd.ExcelFile(file_path) as xls:
                for s in xls.sheet_names:
                    # logging.getLogger('operations').info(f"HOJA {s}")
                    if s.upper() == 'OCT25':
                        sheet_to_use = s
                        break
            if sheet_to_use is None:
                raise ValueError("Hoja 'OCT25' no encontrada en el archivo Excel")
            # skiprows=6 hace que la lectura comience en la fila 7 (1-based)
            # read_excel will open/close its own handle; using engine=openpyxl for .xlsx
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, skiprows=6)
            # Para archivos OnTime la última columna de datos válida es la número 79 (1-based).
            # Recortamos todas las columnas después de la columna 79 y seguimos con la siguiente fila.
            # df.iloc uses 0-based indexing, por lo que usamos :79 para obtener las primeras 79 columnas.
            if df.shape[1] > 79:
                logging.getLogger('operations').info(f"OnTime file has {df.shape[1]} columns; trimming to 79 columns")
                df = df.iloc[:, :79]
            elif df.shape[1] < 79:
                logging.getLogger('operations').warning(f"OnTime file {filename} tiene solo {df.shape[1]} columnas; se esperaban al menos 79")
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
            current_idx = None
            try:
                total_affected = 0
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
                        date_columns = {
                            "FECHA DE OFERTA",
                            "CITA DE CARGA",
                            "CITA DESCARGA",
                            "LLEGADA A CARGAR",
                            "SALIDA DE CARGA",
                            "LLEGADA A DESCARGA",
                            "SALIDA DESCARGA",
                        }

                        if val is not None and col in date_columns:
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
                        numeric_columns = {
                            "TARIFA TRANSP.", "ACCESORIOS TRANSP", "IVA", "RETENCION", "TOTAL .L",
                            "TARIFA CLIENTE", "ACCESORIOS CTE", "IVA CTE", "RETENCION CTE", "TOTAL CLIENTE",
                            "UTILIDAD","%"
                           
                        }

                        if val is not None and col in numeric_columns:
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

                    res = db.execute(text(exec_sql), params)
                    # Try to read the number of rows affected by the stored procedure (may be driver-dependent)
                    try:
                        affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
                        if affected is None:
                            affected = -1
                    except Exception:
                        affected = -1
                    logging.debug(f"Fila {idx}: @@ROWCOUNT={affected}")
                    try:
                        total_affected += int(affected)
                    except Exception:
                        pass

                # If we reach here, all executions for dbo.sp_ins_ontime succeeded.
                # If a username was provided, call dbo.sp_proc_ontime with the user and processed filename
                try:
                    if username:
                        processed_name = name_only
                        if processed_name.lower().startswith('temp_'):
                            processed_name = processed_name[5:]
                        sp2_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado,11"
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
            "Operador, Origen, Destino, Anomalía, Fecha, Coordenadas_Lat, Coordenadas_Lon, Ubicación, Comentarios) "
            "VALUES (:Carta_Porte, :Numero_Envio, :Cliente, :Linea_Transportista, :Operador, :Origen, :Destino, :Anomalia, :Fecha, :Coordenadas_Lat, :Coordenadas_Lon, :Ubicacion, :Comentarios)"
        )
        total_inserted = 0
        # Use single transaction: execute inserts and then call sp_proc_ontime, commit once in caller
        for idx, rec in enumerate(records, start=1):
            # Map values
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
            }

            for key_norm, col in normalized_key_map.items():
                if key_norm in expected:
                    val = rec.get(col)
                    # Normalize
                    if isinstance(val, str):
                        val = val.replace('\xa0', ' ').strip()
                        if val == '':
                            val = None
                    # Fecha coercion
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
                    # Coordinates coercion
                    if val is not None and key_norm in ('COORDENADAS_LAT', 'COORDENADAS_LON'):
                        try:
                            val = Decimal(str(val))
                            val = val.quantize(Decimal('0.0000001'))
                        except Exception:
                            try:
                                val = float(str(val).replace(',', '.'))
                            except Exception:
                                val = None

                    # assign to params
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

            # Final safety: ensure ints aren't sent to text columns by mistake.
            # Semana, Dias_Pipeline and Capacidad_Instalada are numeric; dates are datetime.
            for k in list(params.keys()):
                v = params[k]
                if isinstance(v, int) and k not in ('Semana', 'Dias_Pipeline'):
                    # convert ints that are clearly textual fields into strings
                    try:
                        params[k] = str(v)
                    except Exception:
                        pass

            # Execute insert
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

            db.execute(text(insert_sql), params)
            try:
                affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                affected = None
            logging.debug(f"Incidencias fila {idx} @@ROWCOUNT={affected}")
            try:
                total_inserted += int(affected) if affected is not None else 0
            except Exception:
                pass

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
            'Nombre de la LT', 'Fecha de Prospección', 'Semana', 'Fuente de Prospecto', 'Responsable',
            'Fases Pipeline', 'Medio de Contacto', 'Fecha último contacto', 'Días Pipeline', 'Nombre de contacto',
            'Número Telefono', 'Correo electrónico', 'Ubicación', 'Tipo de unidad', 'Capacidad instalada',
            'Requisitos básicos de carga', 'Ruta estrategica', 'Cliente estrategico', 'Comentarios'
        ]
        
        # Attempt to auto-detect header row: prefer header=1 (so data starts on row 3),
        # but fall back to header=0 when header=1 looks invalid (many empty or duplicate column names).
        def _read_with_header_guess(path, sheet_name):
            """More robust header detection:
            - Read the first few rows with header=None
            - For each candidate row (0..4) compute how many cells match expected normalized names
            - Choose the row with the highest match count (if > 0) as header
            - Otherwise fall back to header=0
            """
            try:
                # preview first 10 rows without treating any as header
                df_preview = pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=10)
                nrows = len(df_preview)

                def _local_nk(x):
                    if x is None:
                        return ''
                    s = str(x)
                    s = s.replace('\xa0', ' ')
                    s = unicodedata.normalize('NFKD', s)
                    s = ''.join(c for c in s if not unicodedata.combining(c))
                    return re.sub(r"\s+", " ", s).strip().upper()

                exp_norms = { _local_nk(e) for e in expected }

                best_row = None
                best_score = -1
                max_candidate = min(5, nrows)
                for r in range(max_candidate):
                    row_vals = df_preview.iloc[r].tolist()
                    normed = [_local_nk(v) for v in row_vals]
                    score = sum(1 for v in normed if v in exp_norms)
                    if score > best_score:
                        best_score = score
                        best_row = r

                if best_score is None or best_score <= 0:
                    logging.getLogger('operations').info("Header detection: no good candidate found in preview; using header=0")
                    df0 = pd.read_excel(path, sheet_name=sheet_name, header=0)
                    return df0, 0

                # Read with detected header row
                df_h = pd.read_excel(path, sheet_name=sheet_name, header=best_row)
                logging.getLogger('operations').info(f"Read with detected header={best_row} (0-based); data expected to start after that row)")
                return df_h, best_row
            except Exception as e:
                logging.getLogger('operations').warning(f"Header-detection failed: {e}; falling back to header=0")
                df0 = pd.read_excel(path, sheet_name=sheet_name, header=0)
                return df0, 0

        def nk(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()


        
        df, _used_header = _read_with_header_guess(file_path, sheet_to_use)
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
            "INSERT INTO dbo.pipeline_transporte_tmp (Nombre_LT, Fecha_Prospeccion, Semana, Fuente_Prospecto, Responsable, "
            "Fases_Pipeline, Medio_Contacto, Fecha_Ultimo_Contacto, Dias_Pipeline, Nombre_Contacto, Numero_Telefono, Correo_Electronico, "
            "Ubicacion, Tipo_Unidad, Capacidad_Instalada, Requisitos_Basicos_Carga, Ruta_Estrategica, Cliente_Estrategico, Comentarios, Usuario_Creacion) "
            "VALUES (:Nombre_LT, :Fecha_Prospeccion, :Semana, :Fuente_Prospecto, :Responsable, :Fases_Pipeline, :Medio_Contacto, :Fecha_Ultimo_Contacto, :Dias_Pipeline, :Nombre_Contacto, :Numero_Telefono, :Correo_Electronico, :Ubicacion, :Tipo_Unidad, :Capacidad_Instalada, :Requisitos_Basicos_Carga, :Ruta_Estrategica, :Cliente_Estrategico, :Comentarios, :Usuario_Creacion)"
        )

        total_inserted = 0

        # Build a mapping from expected (normalized) -> actual column name when present
        matched_cols = {exp_norm: normalized_key_map.get(exp_norm) for exp_norm in normalized_expected_map.keys()}
        #logging.getLogger('operations').info(f"PipelineTransporte normalized columns: {list(normalized_key_map.keys())}")
        #logging.getLogger('operations').info(f"PipelineTransporte expected normalized keys: {list(normalized_expected_map.keys())}")
        #
        # logging.getLogger('operations').info(f"PipelineTransporte matched expected columns: { {k:v for k,v in matched_cols.items() if v is not None} }")

        for idx, rec in enumerate(records, start=1):
            params = {
                'Nombre_LT': None, 'Fecha_Prospeccion': None, 'Semana': None, 'Fuente_Prospecto': None, 'Responsable': None,
                'Fases_Pipeline': None, 'Medio_Contacto': None, 'Fecha_Ultimo_Contacto': None, 'Dias_Pipeline': None, 'Nombre_Contacto': None,
                'Numero_Telefono': None, 'Correo_Electronico': None, 'Ubicacion': None, 'Tipo_Unidad': None, 'Capacidad_Instalada': None,
                'Requisitos_Basicos_Carga': None, 'Ruta_Estrategica': None, 'Cliente_Estrategico': None, 'Comentarios': None, 'Usuario_Creacion': username
            }

            # Iterate over expected normalized keys (stable order) and extract value from actual column if present
            for exp_norm, actual_col in matched_cols.items():
                if actual_col is None:
                    # no matching column in the sheet for this expected header
                    continue
                val = rec.get(actual_col)
                # Normalize strings
                if isinstance(val, str):
                    val = val.replace('\xa0', ' ').strip()
                    if val == '':
                        val = None

                # Date coercion: use normalized keys for matching
                if val is not None and exp_norm in {nk('Fecha de prospección'), nk('Fecha último contacto')}:
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
                                logging.getLogger('operations').debug(f"No se pudo parsear fecha en fila {idx} columna {actual_col}: {val}")
                                val = None
                    except Exception:
                        val = None

                # Numeric coercion for Semana, Dias, Capacidad
                if val is not None and exp_norm == nk('Semana'):
                    try:
                        s = str(val)
                        m = re.search(r'(\d+)', s)
                        if m:
                            val = int(m.group(1))
                        else:
                            val = None
                    except Exception:
                        val = None

                if val is not None and exp_norm == nk('Días Pipeline'):
                    try:
                        val = int(float(str(val).replace(',', '.')))
                    except Exception:
                        val = None

                if val is not None and exp_norm == nk('Capacidad instalada'):
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

                # Assign to params based on expected normalized name
                if exp_norm == nk('Nombre de la LT'):
                    params['Nombre_LT'] = val
                elif exp_norm == nk('Fecha de prospección'):
                    params['Fecha_Prospeccion'] = val
                elif exp_norm == nk('Semana'):
                    params['Semana'] = val
                elif exp_norm == nk('Fuente de prospecto'):
                    params['Fuente_Prospecto'] = val
                elif exp_norm == nk('Responsable'):
                    params['Responsable'] = val
                elif exp_norm == nk('Fases Pipeline'):
                    params['Fases_Pipeline'] = val
                elif exp_norm == nk('Medio de contacto'):
                    params['Medio_Contacto'] = val
                elif exp_norm == nk('Fecha último contacto'):
                    params['Fecha_Ultimo_Contacto'] = val
                elif exp_norm == nk('Días Pipeline'):
                    params['Dias_Pipeline'] = val
                elif exp_norm == nk('Nombre de contacto'):
                    params['Nombre_Contacto'] = val
                elif exp_norm == nk('Número Telefono'):
                    params['Numero_Telefono'] = val
                elif exp_norm == nk('Correo electrónico'):
                    params['Correo_Electronico'] = val
                elif exp_norm == nk('Ubicación'):
                    params['Ubicacion'] = val
                elif exp_norm == nk('Tipo de unidad'):
                    params['Tipo_Unidad'] = val
                elif exp_norm == nk('Capacidad instalada'):
                    params['Capacidad_Instalada'] = val
                elif exp_norm == nk('Requisitos básicos de carga'):
                    params['Requisitos_Basicos_Carga'] = val
                elif exp_norm == nk('Ruta estrategica'):
                    params['Ruta_Estrategica'] = val
                elif exp_norm == nk('Cliente estrategico'):
                    params['Cliente_Estrategico'] = val
                elif exp_norm == nk('Comentarios'):
                    params['Comentarios'] = val

            # Execute insert
            db.execute(text(insert_sql), params)
            try:
                affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                affected = None
            logging.getLogger('operations').debug(f"Pipeline fila {idx} @@ROWCOUNT={affected}")
            try:
                total_inserted += int(affected) if affected is not None else 0
            except Exception:
                pass

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
            'Segmento', 'Clasificación de la oportunidad %', 'FUNNEL', 'Contacto', 'Correo Electronico', 'Telefono', 'Puesto',
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
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, header=2)
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
            "INSERT INTO dbo.pipeline_comercial_tmp (No, Semana, Fuente_Prospecto, Cliente, Bloque_Prospeccion, Tipo_Cliente, Zona_Geografica, Segmento, Clasificacion_Oportunidad, Funnel, Contacto, Correo_Electronico, Telefono, Puesto, Fecha_Contacto_Inicial, Fecha_Ultimo_Contacto, Evento_Ultimo_Contacto, Dias_en_Pipeline, Responsable_Seguimiento, Status, Producto_a_Transportar, Tipo_Cliente_Actividad, Nombre_Intermediario, Segmento_Secundario, Proveedor_Actual, Ubicacion_Negociacion, Proyecto_Cross_Selling, IMPO, EXPO, NAC, DED, INTMDL, Mudanza, SPOT, CIRCUITO, PUERTOS, Origen, Destino, Bitacora_Seguimiento, Usuario_Creacion) "
            "VALUES (:No, :Semana, :Fuente_Prospecto, :Cliente, :Bloque_Prospeccion, :Tipo_Cliente, :Zona_Geografica, :Segmento, :Clasificacion_Oportunidad, :Funnel, :Contacto, :Correo_Electronico, :Telefono, :Puesto, :Fecha_Contacto_Inicial, :Fecha_Ultimo_Contacto, :Evento_Ultimo_Contacto, :Dias_en_Pipeline, :Responsable_Seguimiento, :Status, :Producto_a_Transportar, :Tipo_Cliente_Actividad, :Nombre_Intermediario, :Segmento_Secundario, :Proveedor_Actual, :Ubicacion_Negociacion, :Proyecto_Cross_Selling, :IMPO, :EXPO, :NAC, :DED, :INTMDL, :Mudanza, :SPOT, :CIRCUITO, :PUERTOS, :Origen, :Destino, :Bitacora_Seguimiento, :Usuario_Creacion)"
        )

        total_inserted = 0
        for idx, rec in enumerate(records, start=1):
            params = {
                'No': None, 'Semana': None, 'Fuente_Prospecto': None, 'Cliente': None, 'Bloque_Prospeccion': None,
                'Tipo_Cliente': None, 'Zona_Geografica': None, 'Segmento': None, 'Clasificacion_Oportunidad': None, 'Funnel': None,
                'Contacto': None, 'Correo_Electronico': None, 'Telefono': None, 'Puesto': None, 'Fecha_Contacto_Inicial': None,
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
                    elif norm_key == nk('Contacto'):
                        params['Contacto'] = val
                    elif norm_key == nk('Correo Electronico'):
                        params['Correo_Electronico'] = val
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

            db.execute(text(insert_sql), params)
            try:
                affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                affected = None
            try:
                total_inserted += int(affected) if affected is not None else 0
            except Exception:
                pass

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

        total_inserted = 0
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

            db.execute(text(insert_sql), params)
            try:
                affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                affected = None
            try:
                total_inserted += int(affected) if affected is not None else 0
            except Exception:
                pass

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
        # Read first sheet with header=0
        try:
            df = pd.read_excel(file_path, sheet_name=0, header=0)
            logging.getLogger('operations').info("Reading factoraje first sheet with header=0")
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
                logging.getLogger('operations').info(f"Factoraje: omitidas {dropped} filas que iniciaban con campo vacío en columna '{first_col}'")

        records = df.to_dict(orient='records')
        processed_count = len(records)

        # expected headers (human readable) - normalized comparison
        expected = ['Nombre', 'No Viaje', 'No Factura', 'Flete', 'Maniobras', 'Otros', 'Subtotal', 'IVA', 'ISR', 'Total', 'FECHA FACT', 'CLIENTE']

        def nk_local(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()

        normalized_key_map = {nk_local(k): k for k in list(df.columns)}
        normalized_expected = {nk_local(e): e for e in expected}

        insert_sql = (
            "INSERT INTO dbo.factoraje_tmp (Nombre, No_Viaje, No_Factura, Flete, Maniobras, Otros, Subtotal, IVA, ISR, Total, Fecha_Fact, Cliente, Usuario_Creacion) "
            "VALUES (:Nombre, :No_Viaje, :No_Factura, :Flete, :Maniobras, :Otros, :Subtotal, :IVA, :ISR, :Total, :Fecha_Fact, :Cliente, :Usuario_Creacion)"
        )

        total_inserted = 0
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

            db.execute(text(insert_sql), params)
            try:
                affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                affected = None
            try:
                total_inserted += int(affected) if affected is not None else 0
            except Exception:
                pass

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
            df = pd.read_excel(file_path, sheet_name=0, header=0)
            logging.getLogger('operations').info("Reading relacion_pago first sheet with header=0")
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

        # expected headers (human readable) - normalized comparison
        expected = ['Nombre', 'No Viaje', 'No Factura', 'Flete', 'Maniobras', 'Otros', 'Subtotal', 'IVA', 'ISR', 'Total', 'FECHA FACT', 'CLIENTE']

        def nk_local(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()

        normalized_key_map = {nk_local(k): k for k in list(df.columns)}
        normalized_expected = {nk_local(e): e for e in expected}

        insert_sql = (
            "INSERT INTO dbo.relacion_pago_tmp (Nombre, No_Viaje, No_Factura, Flete, Maniobras, Otros, Subtotal, IVA, ISR, Total, Fecha_Fact, Cliente, Usuario_Creacion) "
            "VALUES (:Nombre, :No_Viaje, :No_Factura, :Flete, :Maniobras, :Otros, :Subtotal, :IVA, :ISR, :Total, :Fecha_Fact, :Cliente, :Usuario_Creacion)"
        )

        total_inserted = 0
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
               # logging.getLogger('operations').info(f"Relacion Pago insert fila {idx}: {loggable}")
            except Exception as log_ex:
                logging.debug(f"No se pudo serializar params para logging en fila {idx}: {log_ex}")

            db.execute(text(insert_sql), params)
            try:
                affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                affected = None
            try:
                total_inserted += int(affected) if affected is not None else 0
            except Exception:
                pass

        # After inserts, call post-processing SP if provided
        if total_inserted > 0:
            if username and original_name:
                processed_name = original_name
                if processed_name.lower().startswith('temp_'):
                    processed_name = processed_name[5:]
                sp2_sql = "EXEC dbo.sp_proc_ontime :nombre_usuario, :name_file_procesado,2"
                logging.getLogger('operations').info(f"Ejecutando Procedure para Relacion Pago usuario={username}, archivo={processed_name}")
                db.execute(text(sp2_sql), {"nombre_usuario": username, "name_file_procesado": processed_name})
                try:
                    sp2_af = db.execute(text("SELECT @@ROWCOUNT")).scalar()
                except Exception:
                    sp2_af = None
                logging.getLogger('operations').info(f"Procedure (Relacion Pago) @@ROWCOUNT={sp2_af}")

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

        total_inserted = 0
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

            db.execute(text(insert_sql), params)
            try:
                affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                affected = None
            try:
                total_inserted += int(affected) if affected is not None else 0
            except Exception:
                pass

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

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, header=0)
            logging.getLogger('operations').info(f"Reading evidencias_pendientes sheet '{sheet_to_use}' with header=0")
        except Exception as read_err:
            logging.getLogger('operations').error(f"No se pudo leer la hoja TABLA: {read_err}")
            raise

        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        # Normalize header keys
        def nk(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()

        cols = list(df.columns)
        normalized_map = {nk(c): c for c in cols}

        if 'CLIENTE' not in normalized_map:
            # try common alternatives
            raise ValueError("No se encontró la columna 'CLIENTE' en la hoja TABLA")

        cliente_col = normalized_map['CLIENTE']
        cliente_index = cols.index(cliente_col)



        # Month columns are the columns after the cliente column
        month_cols = cols[cliente_index + 1:]
        # Select first four month columns (if less than 4, fill with None)
        month_cols = [c for c in month_cols if c is not None and str(c).strip() != '']
        # Keep original header names for MesX
        selected_month_cols = month_cols[:4]

        records = df.to_dict(orient='records')
        processed_count = len(records)

        insert_sql = (
            "INSERT INTO dbo.evidencias_pendientes_tmp (Cliente, Mes1, Total_Mes1, Mes2, Total_Mes2, Mes3, Total_Mes3, Mes4, Total_Mes4, Total_General, Usuario_Creacion) "
            "VALUES (:Cliente, :Mes1, :Total_Mes1, :Mes2, :Total_Mes2, :Mes3, :Total_Mes3, :Mes4, :Total_Mes4, :Total_General, :Usuario_Creacion)"
        )

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

            db.execute(text(insert_sql), params)
            try:
                affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                affected = None
            try:
                total_inserted += int(affected) if affected is not None else 0
            except Exception:
                pass

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

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, header=0)
            logging.getLogger('operations').info(f"Reading pronostico cobranza sheet '{sheet_to_use}' with header=0")
        except Exception as read_err:
            logging.getLogger('operations').error(f"No se pudo leer la hoja TABLA NUEVA: {read_err}")
            raise

        df = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)

        # Normalize header keys
        def nk(s: str) -> str:
            if s is None:
                return ''
            ss = str(s).replace('\xa0', ' ')
            ss = unicodedata.normalize('NFKD', ss)
            ss = ''.join(c for c in ss if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", ss).strip().upper()

        cols = list(df.columns)
        normalized_map = {nk(c): c for c in cols}

        # Prefer explicit CLIENTE header; otherwise use the first physical column as CLIENTE
        if 'CLIENTE' in normalized_map:
            cliente_col = normalized_map['CLIENTE']
        else:
            # map the first physical column to CLIENTE when the header is missing
            cliente_col = cols[0]
            logging.getLogger('operations').info(f"No se encontró columna 'CLIENTE' en TABLA NUEVA; usando primera columna '{cliente_col}' como CLIENTE")

        cliente_index = cols.index(cliente_col)

        # Week columns are the columns after the cliente column
        week_cols = cols[cliente_index + 1:]
        # Keep only non-empty header names
        week_cols = [c for c in week_cols if c is not None and str(c).strip() != '']

        records = df.to_dict(orient='records')
        processed_count = len(records)

        insert_sql = (
            "INSERT INTO dbo.pronostico_cobranza_tmp (Cliente, Semana1, Total_Semana1, Semana2, Total_Semana2, Semana3, Total_Semana3, Semana4, Total_Semana4, Semana5, Total_Semana5, Semana6, Total_Semana6, Semana7, Total_Semana7, Semana8, Total_Semana8, Semana9, Total_Semana9, Semana10, Total_Semana10, Semana11, Total_Semana11, Semana12, Total_Semana12, Semana13, Total_Semana13, Total_General, Usuario_Creacion, Fecha_Creacion) "
            "VALUES (:Cliente, :Semana1, :Total_Semana1, :Semana2, :Total_Semana2, :Semana3, :Total_Semana3, :Semana4, :Total_Semana4, :Semana5, :Total_Semana5, :Semana6, :Total_Semana6, :Semana7, :Total_Semana7, :Semana8, :Total_Semana8, :Semana9, :Total_Semana9, :Semana10, :Total_Semana10, :Semana11, :Total_Semana11, :Semana12, :Total_Semana12, :Semana13, :Total_Semana13, :Total_General, :Usuario_Creacion, :Fecha_Creacion)"
        )

        total_inserted = 0

        # detect explicit Total general column if present
        total_general_col = None
        for k_norm, k in normalized_map.items():
            if 'TOTAL' in k_norm and 'GENERAL' in k_norm:
                total_general_col = k
                break

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

            db.execute(text(insert_sql), params)
            try:
                affected = db.execute(text("SELECT @@ROWCOUNT")).scalar()
            except Exception:
                affected = None
            try:
                total_inserted += int(affected) if affected is not None else 0
            except Exception:
                pass

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