
import logging
import os
import time
from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import SessionLocal, engine
from app import models, schemas, crud, auth
from app.excel_processor import process_excel, process_incidencias, process_pipeline_transporte, process_pipeline_comercial, process_disponibilidad_transporte, process_factoraje, process_relacion_pago, process_evidencias_pendientes, process_venta_perdida, process_pronostico_cobranza
import json
from fastapi.encoders import jsonable_encoder
from jose import JWTError, jwt
import re

# Ensure logs directory exists and configure root logger to write application logs
from app.logging_utils import SizeAndTimedRotatingFileHandler, ensure_logs_dir
logs_dir = ensure_logs_dir()
api_log_path = os.path.join(logs_dir, 'api.log')

# Configure root logger with a handler that rotates daily and also by size (10MB)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
# Remove any existing handlers to avoid duplicate logging when reloading
for h in list(root_logger.handlers):
    try:
        root_logger.removeHandler(h)
    except Exception:
        pass

# Timed (midnight) + size rotation (maxBytes)
api_handler = SizeAndTimedRotatingFileHandler(api_log_path, when='midnight', backupCount=30, encoding='utf-8', maxBytes=10 * 1024 * 1024)
api_formatter = logging.Formatter("%(asctime)s,%(msecs)03d %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
api_handler.setFormatter(api_formatter)
api_handler.setLevel(logging.INFO)
root_logger.addHandler(api_handler)

# Also keep a console handler for convenience in dev
console_h = logging.StreamHandler()
console_h.setLevel(logging.INFO)
console_h.setFormatter(api_formatter)
root_logger.addHandler(console_h)

logging.getLogger().info(f"Root logging initialized, api.log -> {api_log_path}")

# --- operations.log handler: create a dedicated 'operations' logger and attach the operations handler ---
try:
    operations_path = os.path.join(logs_dir, 'operations.log')
    # Only add if not already added to root handlers
    if not any(getattr(h, 'baseFilename', None) and os.path.abspath(getattr(h, 'baseFilename')) == os.path.abspath(operations_path) for h in logging.root.handlers):
        ops_handler = SizeAndTimedRotatingFileHandler(operations_path, when='midnight', backupCount=14, encoding='utf-8', maxBytes=10 * 1024 * 1024)
        ops_handler.setLevel(logging.INFO)
        ops_handler.setFormatter(api_formatter)

        # Create a dedicated logger named 'operations'. Calls to logging.getLogger('operations') will
        # write into operations.log. Keep propagate=False to avoid duplicating into api.log.
        ops_logger = logging.getLogger('operations')
        ops_logger.setLevel(logging.INFO)
        ops_logger.addHandler(ops_handler)
        ops_logger.propagate = False
        logging.getLogger().info(f"Operations logging initialized, operations.log -> {operations_path}")
except Exception:
    logging.getLogger().warning("No se pudo configurar operations.log handler")

# Ensure ops_logger exists even if the above handler creation failed so calls below won't NameError
ops_logger = logging.getLogger('operations')

models.Base.metadata.create_all(bind=engine)


def safe_remove(path: str, attempts: int = 5, delay: float = 0.5):
    """Try to remove a file with retries (Windows can keep files locked briefly).

    Logs a warning if removal ultimately fails.
    """
    for attempt in range(1, attempts + 1):
        try:
            if os.path.exists(path):
                os.remove(path)
            return True
        except PermissionError as pe:
            logging.warning(f"Intento {attempt}/{attempts} - No se pudo eliminar {path}: {pe}")
            if attempt < attempts:
                time.sleep(delay)
            else:
                logging.warning(f"No se pudo eliminar el archivo después de {attempts} intentos: {path}")
                return False
        except Exception as e:
            logging.warning(f"Error eliminando archivo {path}: {e}")
            return False




app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://localhost:3000",
        "http://127.0.0.1:4200",
        "http://127.0.0.1:3000",
        "http://dwh.retornologistico.com",
        "https://dwh.retornologistico.com"
    ],  # Agrega aquí los orígenes necesarios
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    """Dependency to get current user from JWT token. Raises 401 if invalid."""
    try:
        payload = jwt.decode(token, auth.SECRET_KEY, algorithms=[auth.ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials")
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials")
    user = crud.get_usuario_by_nombre(db, username)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user

# --- Endpoint para procesar archivo Excel ---
@app.post("/procesar-excel/")
async def procesar_excel(file: UploadFile = File(...), current_user: models.Usuario = Depends(get_current_user)):
    import os
    # Validar nomenclatura: OnTime_acumulado_AAAA (AAAA = año de 4 dígitos)
    filename = file.filename or ""
    name_only, _ext = os.path.splitext(filename)
    m = re.match(r"^OnTime_acumulado_(\d{4})$", name_only)
    if not m:
        ops_logger.warning(f"Archivo con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'OnTime_acumulado_AAAA'")
    year = int(m.group(1))
    if year < 2000 or year > 2100:
        ops_logger.warning(f"Archivo con año inválido en el nombre: {filename}")
        raise HTTPException(status_code=400, detail="El año en el nombre del archivo no es válido")

    # Antes de procesar, verificar en dbo.mi_bitacora_operaciones que el archivo no haya sido procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:

                # Comprobar tanto el nombre sin extensión como el filename completo
                #sql_check = "SELECT TOP 1 nombre_usuario, fecha FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :n1 OR name_file_load = :n2 ORDER BY fecha DESC"
                row = 0 #_db_check.execute(text(sql_check), {"n1": name_only, "n2": filename}).fetchone()
                if row is not None:
                    proc_user = row[0]
                    proc_fecha = row[1]
                    try:
                        if hasattr(proc_fecha, 'strftime'):
                            proc_fecha_str = proc_fecha.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            proc_fecha_str = str(proc_fecha)
                    except Exception:
                        proc_fecha_str = str(proc_fecha)
                    msg = f"El archivo ya fue procesado por {proc_user} el {proc_fecha_str}"
                    ops_logger.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                # Si la comprobación falla por algún motivo, registrarlo y continuar con el procesamiento
                ops_logger.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        # If the inner check raised an HTTPException (file already processed), re-raise it so the endpoint returns 400.
        if isinstance(e, HTTPException):
            raise
        # Otherwise log and continue (verification couldn't be performed)
        ops_logger.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # If OnTime files should be processed via SP, open a DB session and pass it.
        data = None
        try:
            # Import SessionLocal here to avoid circular imports at module import time
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    # Pass the logged-in user's nombre_usuario so the processor can call dbo.sp_proc_ontime
                    try:
                        ops_logger.info(f"Procesando OnTime archivo={name_only} usuario={current_user.nombre_usuario}")
                    except Exception:
                        # Ensure logging does not break processing if ops_logger misconfigured
                        pass
                    data = process_excel(file_location, db=db, username=current_user.nombre_usuario)
                except Exception as pe_err:
                    # Log full traceback and return a concise error to the caller
                    tb = traceback.format_exc()
                    ops_logger.error(f"Error procesando OnTime y ejecutando SP: {pe_err}\n{tb}")
                    # Clean up temp file before returning
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_read": 0, "error": "Error al procesar archivo OnTime; verificar api.log"})
        except Exception:
            # If DB session cannot be opened or process_excel raised before using DB, try fallback processing without DB
            try:
                data = process_excel(file_location)
            except Exception as fallback_err:
                tb = traceback.format_exc()
                ops_logger.error(f"Error en procesamiento fallback del archivo: {fallback_err}\n{tb}")
                try:
                    safe_remove(file_location)
                except Exception as rm_err:
                    ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                return JSONResponse(status_code=500, content={"rows_read": 0, "error": "Error al procesar archivo; verificar api.log"})

        # Guardar resultados leídos en archivo .txt: acumulado_<AAAA>.txt
        out_filename = f"acumulado_{year}.txt"
        try:
            with open(out_filename, "w", encoding="utf-8") as out_f:
                for row in data:
                    safe_row = jsonable_encoder(row)
                    out_f.write(json.dumps(safe_row, ensure_ascii=False) + "\n")
        except Exception as wf_err:
            ops_logger.error(f"Error escribiendo archivo de salida {out_filename}: {wf_err}")
            # attempt to remove temp file
            try:
                safe_remove(file_location)
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"rows_read": 0, "error": f"Error al guardar el archivo de salida: {wf_err}"})

        rows_count = len(data)

        # Elimina el archivo temporal después de procesar
        try:
            safe_remove(file_location)
        except Exception as del_err:
            ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        # Retornar sólo el número de registros leídos
        return {"rows_read": rows_count}
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error procesando archivo {file.filename}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")

# Dependency
import traceback


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    # Log full stack trace for debugging and return a sanitized 500 response
    tb = traceback.format_exc()
    logging.error(f"Unhandled exception for request {request.url}: {tb}")
    return Response(status_code=500, content="Internal Server Error")


# Endpoint para exponer el archivo de logs `api.log` como JSON
@app.get("/logs/api-log")
def get_api_log(limit: int = 1000, current_user: models.Usuario = Depends(get_current_user)):
    """Return parsed lines from api.log as JSON list.

    Each entry is an object with keys: 'fecha y hora', 'tipo', 'message'.
    - 'limit' limits the number of returned log entries (default 1000).
    """
    try:
        log_path = os.path.join(os.getcwd(), 'logs/operations.log')
        if not os.path.exists(log_path):
            raise HTTPException(status_code=404, detail=f"Log file not found: {log_path}")

        ts_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+([A-Z]+)\s+(.*)$")
        entries = []
        current = None
        with open(log_path, 'r', encoding='utf-8', errors='replace') as fh:
            for line in fh:
                line = line.rstrip('\n')
                m = ts_re.match(line)
                if m:
                    # start of a new log record
                    if current is not None:
                        entries.append(current)
                        if len(entries) >= limit:
                            break
                    current = {
                        'fecha y hora': m.group(1),
                        'tipo': m.group(2),
                        'message': m.group(3)
                    }
                else:
                    # continuation line (traceback or multi-line message)
                    if current is not None:
                        current['message'] = current.get('message', '') + '\n' + line
                    else:
                        # leading garbage lines - ignore
                        continue

        if current is not None and len(entries) < limit:
            entries.append(current)

        return entries
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error leyendo api.log: {e}")
        raise HTTPException(status_code=500, detail="Error leyendo api.log")

# --- Endpoints de autenticación ---
@app.post("/token")
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    logging.info(f"Login attempt for user: {form_data.username}")
    usuario = crud.get_usuario_by_nombre(db, form_data.username)
    if not usuario or not auth.verify_password(form_data.password, usuario.contrasena_hash):
        logging.warning(f"Failed login for user: {form_data.username}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")
    # Generar access token con expiración
    from datetime import timedelta, datetime
    expires_delta = timedelta(minutes=auth.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(data={"sub": usuario.nombre_usuario}, expires_delta=expires_delta)

    # Opcional: desactivar tokens anteriores para este usuario
    try:
        crud.deactivate_tokens_for_user(db, usuario.id)
    except Exception as e:
        logging.warning(f"No se pudieron desactivar tokens anteriores para usuario {usuario.id}: {e}")

    # Guardar el token en la base de datos
    try:
        expires_at = datetime.utcnow() + expires_delta if expires_delta else None
        crud.create_token(db, usuario.id, access_token, fecha_expiracion=expires_at)
    except Exception as e:
        logging.error(f"Error guardando token para usuario {usuario.id}: {e}")

    logging.info(f"Login successful for user: {form_data.username}")
    return {"access_token": access_token, "token_type": "bearer"}

# --- Endpoints de usuarios ---
@app.post("/usuarios/", response_model=schemas.UsuarioOut)
def create_usuario(usuario: schemas.UsuarioCreate, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info(f"Create usuario request: {usuario.nombre_usuario}, {usuario.correo_electronico}")
    try:
        db_usuario = crud.get_usuario_by_nombre(db, nombre_usuario=usuario.nombre_usuario)
        if db_usuario:
            logging.warning(f"Nombre de usuario ya registrado: {usuario.nombre_usuario}")
            raise HTTPException(status_code=400, detail="Nombre de usuario ya registrado")
        try:
            new_usuario = crud.create_usuario(db=db, usuario=usuario)
        except ValueError as ve:
            logging.error(f"Error creando usuario {usuario.nombre_usuario}: {str(ve)}")
            raise HTTPException(status_code=400, detail=str(ve))
        logging.info(f"Usuario creado: {new_usuario.nombre_usuario} (ID: {new_usuario.id})")
        return new_usuario
    except Exception as e:
        logging.error(f"Error creando usuario {usuario.nombre_usuario}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/usuarios/", response_model=list[schemas.UsuarioOut])
def read_usuarios(skip: int = 0, limit: int = 100, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info(f"Read usuarios request: skip={skip}, limit={limit}")
    usuarios = crud.get_usuarios(db, skip=skip, limit=limit)
    return usuarios

@app.get("/usuarios/{usuario_id}", response_model=schemas.UsuarioOut)
def read_usuario(usuario_id: int, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info(f"Read usuario request: usuario_id={usuario_id}")
    db_usuario = crud.get_usuario(db, usuario_id=usuario_id)
    if db_usuario is None:
        logging.warning(f"Usuario no encontrado: usuario_id={usuario_id}")
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return db_usuario

@app.put("/usuarios/{usuario_id}", response_model=schemas.UsuarioOut)
def update_usuario(usuario_id: int, usuario: schemas.UsuarioCreate, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info(f"Update usuario request: usuario_id={usuario_id}")
    db_usuario = crud.update_usuario(db, usuario_id=usuario_id, usuario=usuario)
    if db_usuario is None:
        logging.warning(f"Usuario no encontrado para actualizar: usuario_id={usuario_id}")
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return db_usuario

@app.delete("/usuarios/{usuario_id}", response_model=schemas.UsuarioOut)
def delete_usuario(usuario_id: int, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info(f"Delete usuario request: usuario_id={usuario_id}")
    db_usuario = crud.delete_usuario(db, usuario_id=usuario_id)
    if db_usuario is None:
        logging.warning(f"Usuario no encontrado para eliminar: usuario_id={usuario_id}")
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return db_usuario

# --- Endpoints de roles ---
@app.get("/roles/", response_model=list[schemas.RolOut])
def read_roles(db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info("Read roles request")
    return crud.get_roles(db)

@app.post("/roles/", response_model=schemas.RolOut)
def create_rol(rol: schemas.RolCreate, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info(f"Create rol request: {rol.nombre}")
    return crud.create_rol(db, rol)

# --- Endpoints de bitácora ---
@app.post("/bitacora/", response_model=schemas.BitacoraOut)
def create_bitacora(bitacora: schemas.BitacoraCreate, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info(f"Create bitacora request: usuario_id={bitacora.usuario_id}, accion={bitacora.accion}")
    return crud.create_bitacora(db, bitacora)

@app.get("/bitacora/", response_model=list[schemas.BitacoraOut])
def read_bitacora(usuario_id: int = None, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info(f"Read bitacora request: usuario_id={usuario_id}")
    return crud.get_bitacora(db, usuario_id)


# Endpoint para exponer la view view_cronActivDiarias
@app.api_route("/cronactivdiarias/", methods=["GET", "OPTIONS"], response_model=list[schemas.CronActivDiariaOut])
def get_cron_activ_diarias(request: Request, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    # Manejar preflight OPTIONS explícitamente
    if request.method == "OPTIONS":
        return Response(status_code=200)

    logging.info("Get cronactivdiarias view request")
    # Ejecutar consulta directa a la view
    sql = "SELECT actividad_descripcion, archivo_nombre, archivo_nomenclatura, dias_carga FROM view_cronActivDiarias"
    result = db.execute(text(sql))
    rows = []
    for r in result:
        rows.append({
            "actividad_descripcion": r[0],
            "archivo_nombre": r[1],
            "archivo_nomenclatura": r[2],
            "dias_carga": r[3]
        })
    return rows


# Nuevo endpoint: exponer la tabla dbo.mi_bitacora_operaciones (protegido por token)
@app.api_route("/mi-bitacora-operaciones/", methods=["GET", "OPTIONS"])
def get_mi_bitacora_operaciones(request: Request, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    # Manejar preflight OPTIONS explícitamente
    if request.method == "OPTIONS":
        return Response(status_code=200)

    logging.info(f"Get mi_bitacora_operaciones request by usuario_id={current_user.id}")
    sql = "SELECT * FROM dbo.mi_bitacora_operaciones"
    try:
        result = db.execute(text(sql))
        rows = []
        # Use .mappings() to get dict-like rows with column names
        for r in result.mappings():
            rows.append(dict(r))
        return rows
    except Exception as e:
        logging.error(f"Error consultando dbo.mi_bitacora_operaciones: {e}")
        raise HTTPException(status_code=500, detail="Error consultando bitácora; ver api.log")


@app.post("/procesar-incidencias/")
async def procesar_incidencias(file: UploadFile = File(...), current_user: models.Usuario = Depends(get_current_user)):
    import os
    # Validar nomenclatura: incidencias_MM-AAAA (MM=mes 2 dígitos, AAAA año 4 dígitos)
    filename = file.filename or ""
    name_only, _ext = os.path.splitext(filename)
    m = re.match(r"^incidencias_(\d{2})-(\d{4})$", name_only, re.IGNORECASE)
    if not m:
        ops_logger.warning(f"Archivo de incidencias con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'incidencias_MM-AAAA'")
    month = int(m.group(1))
    year = int(m.group(2))
    if month < 1 or month > 12 or year < 2000 or year > 2100:
        ops_logger.warning(f"Archivo de incidencias con mes/año inválido en el nombre: {filename}")
        raise HTTPException(status_code=400, detail="El mes o año en el nombre del archivo no es válido")

    # Verificar en mi_bitacora_operaciones si ya fue procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:
                sql_check = "SELECT TOP 1 nombre_usuario, fecha FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :n1 OR name_file_load = :n2 ORDER BY fecha DESC"
                row = _db_check.execute(text(sql_check), {"n1": name_only, "n2": filename}).fetchone()
                if row is not None:
                    proc_user = row[0]
                    proc_fecha = row[1]
                    try:
                        if hasattr(proc_fecha, 'strftime'):
                            proc_fecha_str = proc_fecha.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            proc_fecha_str = str(proc_fecha)
                    except Exception:
                        proc_fecha_str = str(proc_fecha)
                    msg = f"El archivo ya fue procesado por {proc_user} el {proc_fecha_str}"
                    ops_logger.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                ops_logger.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        ops_logger.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_incidencias inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    try:
                        ops_logger.info(f"Procesando incidencias archivo={name_only} usuario={current_user.nombre_usuario}")
                    except Exception:
                        pass
                    inserted = process_incidencias(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    ops_logger.error(f"Error procesando incidencias y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "Error al procesar archivo de incidencias; verificar api.log"})
        except Exception:
            # Fallback: try to parse without DB (but we cannot insert), return error
            try:
                # simple parse to count rows
                import pandas as _pd
                df = _pd.read_excel(file_location)
                rows = len(df)
            except Exception:
                rows = 0
            try:
                safe_remove(file_location)
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "No se pudo abrir sesión DB para insertar incidencias; verificar api.log"})

        # cleanup temp
        try:
            safe_remove(file_location)
        except Exception as del_err:
            ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error procesando archivo de incidencias {filename}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")
@app.post("/procesar-pipeline-transporte")
@app.post("/procesar-pipeline-transporte/")
async def procesar_pipeline_transporte(file: UploadFile = File(...), current_user: models.Usuario = Depends(get_current_user)):
    import os
    # Validar nomenclatura: pipelineTransporte_sem_XX_MM-AAAA (XX semana, MM mes, AAAA año)
    filename = file.filename or ""
    name_only, _ext = os.path.splitext(filename)
    m = re.match(r"^pipelineTransporte_sem_(\d{1,2})_(\d{2})-(\d{4})$", name_only, re.IGNORECASE)
    if not m:
        ops_logger.warning(f"Archivo pipeline transporte con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'pipelineTransporte_sem_XX_MM-AAAA'")
    week = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    if week < 1 or week > 53 or month < 1 or month > 12 or year < 2000 or year > 2100:
        ops_logger.warning(f"Archivo pipeline transporte con semana/mes/año inválido en el nombre: {filename}")
        raise HTTPException(status_code=400, detail="La semana, mes o año en el nombre del archivo no es válido")

    # Verificar en mi_bitacora_operaciones si ya fue procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:
                sql_check = "SELECT TOP 1 nombre_usuario, fecha FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :n1 OR name_file_load = :n2 ORDER BY fecha DESC"
                row = _db_check.execute(text(sql_check), {"n1": name_only, "n2": filename}).fetchone()
                if row is not None:
                    proc_user = row[0]
                    proc_fecha = row[1]
                    try:
                        if hasattr(proc_fecha, 'strftime'):
                            proc_fecha_str = proc_fecha.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            proc_fecha_str = str(proc_fecha)
                    except Exception:
                        proc_fecha_str = str(proc_fecha)
                    msg = f"El archivo ya fue procesado por {proc_user} el {proc_fecha_str}"
                    ops_logger.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                ops_logger.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        ops_logger.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_pipeline_transporte inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    try:
                        ops_logger.info(f"Procesando pipeline_transporte archivo={name_only} usuario={current_user.nombre_usuario}")
                    except Exception:
                        pass
                    processed = process_pipeline_transporte(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    ops_logger.error(f"Error procesando pipeline transporte y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "Error al procesar archivo pipeline; verificar api.log"})
        except Exception:
            # Fallback: try to parse without DB (but we cannot insert), return error
            try:
                import pandas as _pd
                df = _pd.read_excel(file_location)
                rows = len(df)
            except Exception:
                rows = 0
            try:
                safe_remove(file_location)
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "No se pudo abrir sesión DB para insertar pipeline; verificar api.log"})

        # cleanup temp
        try:
            safe_remove(file_location)
        except Exception as del_err:
            ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": processed}
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error procesando archivo pipeline {filename}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")


@app.post("/procesar-pipeline-comercial")
@app.post("/procesar-pipeline-comercial/")
async def procesar_pipeline_comercial(file: UploadFile = File(...), current_user: models.Usuario = Depends(get_current_user)):
    import os
    # Validar nomenclatura: pipelineComercial_semXX_DD-MM-AAAA
    filename = file.filename or ""
    name_only, _ext = os.path.splitext(filename)
    m = re.match(r"^pipelineComercial_sem(\d{1,2})_(\d{2})-(\d{2})-(\d{4})$", name_only, re.IGNORECASE)
    if not m:
        ops_logger.warning(f"Archivo pipeline comercial con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'pipelineComercial_semXX_DD-MM-AAAA'")
    week = int(m.group(1))
    day = int(m.group(2))
    month = int(m.group(3))
    year = int(m.group(4))
    if week < 1 or week > 53 or day < 1 or day > 31 or month < 1 or month > 12 or year < 2000 or year > 2100:
        ops_logger.warning(f"Archivo pipeline comercial con semana/dia/mes/año inválido en el nombre: {filename}")
        raise HTTPException(status_code=400, detail="La semana, día, mes o año en el nombre del archivo no es válido")

    # Verificar en mi_bitacora_operaciones si ya fue procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:
                sql_check = "SELECT TOP 1 nombre_usuario, fecha FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :n1 OR name_file_load = :n2 ORDER BY fecha DESC"
                row = _db_check.execute(text(sql_check), {"n1": name_only, "n2": filename}).fetchone()
                if row is not None:
                    proc_user = row[0]
                    proc_fecha = row[1]
                    try:
                        if hasattr(proc_fecha, 'strftime'):
                            proc_fecha_str = proc_fecha.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            proc_fecha_str = str(proc_fecha)
                    except Exception:
                        proc_fecha_str = str(proc_fecha)
                    msg = f"El archivo ya fue procesado por {proc_user} el {proc_fecha_str}"
                    ops_logger.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                ops_logger.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        ops_logger.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_pipeline_comercial inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    try:
                        ops_logger.info(f"Procesando pipeline_comercial archivo={name_only} usuario={current_user.nombre_usuario}")
                    except Exception:
                        pass
                    processed = process_pipeline_comercial(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    ops_logger.error(f"Error procesando pipeline comercial y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "Error al procesar archivo pipeline comercial; verificar api.log"})
        except Exception:
            # Fallback: try to parse without DB (but we cannot insert), return error
            try:
                import pandas as _pd
                df = _pd.read_excel(file_location)
                rows = len(df)
            except Exception:
                rows = 0
            try:
                safe_remove(file_location)
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "No se pudo abrir sesión DB para insertar pipeline comercial; verificar api.log"})

        # cleanup temp
        try:
            safe_remove(file_location)
        except Exception as del_err:
            ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": processed}
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error procesando archivo pipeline comercial {filename}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")


@app.post("/procesar-disponibilidad-transporte")
@app.post("/procesar-disponibilidad-transporte/")
async def procesar_disponibilidad_transporte(file: UploadFile = File(...), current_user: models.Usuario = Depends(get_current_user)):
    import os
    # Validar nomenclatura: disponibilidadTransporte_MM-AAAA (MM=mes 2 dígitos, AAAA año 4 dígitos)
    filename = file.filename or ""
    name_only, _ext = os.path.splitext(filename)

    m = re.match(r"^disponibilidadTransporte_(\d{2})-(\d{4})$", name_only, re.IGNORECASE)
    if not m:
        ops_logger.warning(f"Archivo disponibilidad transporte con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'disponibilidadTransporte_MM-AAAA'")
    month = int(m.group(1))
    year = int(m.group(2))
    if month < 1 or month > 12 or year < 2000 or year > 2100:
        ops_logger.warning(f"Archivo disponibilidad transporte con mes/año inválido en el nombre: {filename}")
        raise HTTPException(status_code=400, detail="El mes o año en el nombre del archivo no es válido")

    # Verificar en mi_bitacora_operaciones si ya fue procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:
                sql_check = "SELECT TOP 1 nombre_usuario, fecha FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :n1 OR name_file_load = :n2 ORDER BY fecha DESC"
                row = _db_check.execute(text(sql_check), {"n1": name_only, "n2": filename}).fetchone()
                if row is not None:
                    proc_user = row[0]
                    proc_fecha = row[1]
                    try:
                        if hasattr(proc_fecha, 'strftime'):
                            proc_fecha_str = proc_fecha.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            proc_fecha_str = str(proc_fecha)
                    except Exception:
                        proc_fecha_str = str(proc_fecha)
                    msg = f"El archivo ya fue procesado por {proc_user} el {proc_fecha_str}"
                    ops_logger.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                ops_logger.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        ops_logger.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call processor inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    try:
                        ops_logger.info(f"Procesando disponibilidad_transporte archivo={name_only} usuario={current_user.nombre_usuario}")
                    except Exception:
                        pass
                    processed = process_disponibilidad_transporte(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    ops_logger.error(f"Error procesando disponibilidad transporte y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "Error al procesar archivo disponibilidad; verificar api.log"})
        except Exception:
            # Fallback: try to parse without DB (but we cannot insert), return error
            try:
                import pandas as _pd
                df = _pd.read_excel(file_location)
                rows = len(df)
            except Exception:
                rows = 0
            try:
                safe_remove(file_location)
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "No se pudo abrir sesión DB para insertar disponibilidad; verificar api.log"})

        # cleanup temp
        try:
            safe_remove(file_location)
        except Exception as del_err:
            ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": processed}
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error procesando archivo disponibilidad {filename}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")


@app.post("/procesar-factoraje/")
@app.post("/procesar-factoraje")
async def procesar_factoraje(file: UploadFile = File(...), current_user: models.Usuario = Depends(get_current_user)):
    import os
    # Validar nomenclatura: factoraje_DD-MM-AAAA (DD día 2 dígitos, MM mes 2 dígitos, AAAA año 4 dígitos)
    filename = file.filename or ""
    name_only, _ext = os.path.splitext(filename)
    m = re.match(r"^factoraje_(\d{2})-(\d{2})-(\d{4})$", name_only, re.IGNORECASE)
    if not m:
        ops_logger.warning(f"Archivo factoraje con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'factoraje_DD-MM-AAAA'")

    day = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    if day < 1 or day > 31 or month < 1 or month > 12 or year < 2000 or year > 2100:
        ops_logger.warning(f"Archivo factoraje con fecha inválida en el nombre: {filename}")
        raise HTTPException(status_code=400, detail="La fecha en el nombre del archivo no es válida")

    # Verificar en mi_bitacora_operaciones si ya fue procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:
                sql_check = "SELECT TOP 1 nombre_usuario, fecha FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :n1 OR name_file_load = :n2 ORDER BY fecha DESC"
                row = _db_check.execute(text(sql_check), {"n1": name_only, "n2": filename}).fetchone()
                if row is not None:
                    proc_user = row[0]
                    proc_fecha = row[1]
                    try:
                        if hasattr(proc_fecha, 'strftime'):
                            proc_fecha_str = proc_fecha.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            proc_fecha_str = str(proc_fecha)
                    except Exception:
                        proc_fecha_str = str(proc_fecha)
                    msg = f"El archivo ya fue procesado por {proc_user} el {proc_fecha_str}"
                    ops_logger.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                ops_logger.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        ops_logger.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_factoraje inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    try:
                        ops_logger.info(f"Procesando factoraje archivo={name_only} usuario={current_user.nombre_usuario}")
                    except Exception:
                        pass
                    inserted = process_factoraje(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    ops_logger.error(f"Error procesando factoraje y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "Error al procesar archivo factoraje; verificar api.log"})
        except Exception:
            # Fallback: try to parse without DB (but we cannot insert), return error
            try:
                import pandas as _pd
                df = _pd.read_excel(file_location)
                rows = len(df)
            except Exception:
                rows = 0
            try:
                safe_remove(file_location)
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "No se pudo abrir sesión DB para insertar factoraje; verificar api.log"})

        # cleanup temp
        try:
            safe_remove(file_location)
        except Exception as del_err:
            ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error procesando archivo factoraje {filename}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")


@app.post("/procesar-relacion-pago/")
@app.post("/procesar-relacion-pago")
async def procesar_relacion_pago(file: UploadFile = File(...), current_user: models.Usuario = Depends(get_current_user)):
    import os
    # Validar nomenclatura: relacionPago_DD-MM-AAAA (DD día 2 dígitos, MM mes 2 dígitos, AAAA año 4 dígitos)
    filename = file.filename or ""
    name_only, _ext = os.path.splitext(filename)
    m = re.match(r"^relacionPago_(\d{2})-(\d{2})-(\d{4})$", name_only, re.IGNORECASE)
    if not m:
        # log the repr so invisible characters (BOM, trailing spaces) are visible in the logs
        ops_logger.warning(f"Archivo relacionPago con nombre inválido recibido: {filename!r}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'relacionPago_DD-MM-AAAA'")

    day = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    if day < 1 or day > 31 or month < 1 or month > 12 or year < 2000 or year > 2100:
        ops_logger.warning(f"Archivo relacionPago con fecha inválida en el nombre: {filename!r}")
        raise HTTPException(status_code=400, detail="La fecha en el nombre del archivo no es válida")

    # Verificar en mi_bitacora_operaciones si ya fue procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:
                sql_check = "SELECT TOP 1 nombre_usuario, fecha FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :n1 OR name_file_load = :n2 ORDER BY fecha DESC"
                row = _db_check.execute(text(sql_check), {"n1": name_only, "n2": filename}).fetchone()
                if row is not None:
                    proc_user = row[0]
                    proc_fecha = row[1]
                    try:
                        if hasattr(proc_fecha, 'strftime'):
                            proc_fecha_str = proc_fecha.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            proc_fecha_str = str(proc_fecha)
                    except Exception:
                        proc_fecha_str = str(proc_fecha)
                    msg = f"El archivo ya fue procesado por {proc_user} el {proc_fecha_str}"
                    ops_logger.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                ops_logger.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        ops_logger.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_relacion_pago inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    try:
                        ops_logger.info(f"Procesando relacion_pago archivo={name_only} usuario={current_user.nombre_usuario}")
                    except Exception:
                        pass
                    inserted = process_relacion_pago(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    ops_logger.error(f"Error procesando relacion_pago y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "Error al procesar archivo factoraje; verificar api.log"})
        except Exception:
            # Fallback: try to parse without DB (but we cannot insert), return error
            try:
                import pandas as _pd
                df = _pd.read_excel(file_location)
                rows = len(df)
            except Exception:
                rows = 0
            try:
                safe_remove(file_location)
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "No se pudo abrir sesión DB para insertar factoraje; verificar api.log"})

        # cleanup temp
        try:
            safe_remove(file_location)
        except Exception as del_err:
            ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error procesando archivo process_relacion_pago {filename}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")


@app.post("/procesar-evidencias-pendientes/")
@app.post("/procesar-evidencias-pendientes")
async def procesar_evidencias_pendientes(file: UploadFile = File(...), current_user: models.Usuario = Depends(get_current_user)):
    import os
    # Validar nomenclatura: evidenciasPendientes_DD_MM-AAAA (DD día 2 dígitos, MM mes 2 dígitos, AAAA año 4 dígitos)
    filename = file.filename or ""
    name_only, _ext = os.path.splitext(filename)
    m = re.match(r"^evidenciasPendientes_(\d{2})_(\d{2})-(\d{4})$", name_only, re.IGNORECASE)
    if not m:
        ops_logger.warning(f"Archivo evidenciasPendientes con nombre inválido recibido: {filename!r}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'evidenciasPendientes_DD_MM-AAAA'")

    day = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    if day < 1 or day > 31 or month < 1 or month > 12 or year < 2000 or year > 2100:
        ops_logger.warning(f"Archivo evidenciasPendientes con fecha inválida en el nombre: {name_only!r}")
        raise HTTPException(status_code=400, detail="La fecha en el nombre del archivo no es válida")

    # Verificar en mi_bitacora_operaciones si ya fue procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:
                sql_check = "SELECT TOP 1 nombre_usuario, fecha FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :n1 OR name_file_load = :n2 ORDER BY fecha DESC"
                row = _db_check.execute(text(sql_check), {"n1": name_only, "n2": filename}).fetchone()
                if row is not None:
                    proc_user = row[0]
                    proc_fecha = row[1]
                    try:
                        if hasattr(proc_fecha, 'strftime'):
                            proc_fecha_str = proc_fecha.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            proc_fecha_str = str(proc_fecha)
                    except Exception:
                        proc_fecha_str = str(proc_fecha)
                    msg = f"El archivo ya fue procesado por {proc_user} el {proc_fecha_str}"
                    ops_logger.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                ops_logger.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        ops_logger.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_evidencias_pendientes inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    try:
                        ops_logger.info(f"Procesando evidencias_pendientes archivo={name_only} usuario={current_user.nombre_usuario}")
                    except Exception:
                        pass
                    inserted = process_evidencias_pendientes(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    ops_logger.error(f"Error procesando evidencias pendientes y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "Error al procesar archivo evidencias pendientes; verificar api.log"})
        except Exception:
            # Fallback: try to parse without DB (but we cannot insert), return error
            try:
                import pandas as _pd
                df = _pd.read_excel(file_location, sheet_name='TABLA')
                rows = len(df)
            except Exception:
                rows = 0
            try:
                safe_remove(file_location)
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "No se pudo abrir sesión DB para insertar evidencias pendientes; verificar api.log"})

        # cleanup temp
        try:
            safe_remove(file_location)
        except Exception as del_err:
            ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error procesando archivo evidencias_pendientes {name_only}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")


@app.post("/procesar-venta-perdida/")
@app.post("/procesar-venta-perdida")
async def procesar_venta_perdida(file: UploadFile = File(...), current_user: models.Usuario = Depends(get_current_user)):
    import os
    # Validar nomenclatura: ventaPerdida_MM-AAAA (MM mes 2 dígitos, AAAA año 4 dígitos)
    filename = file.filename or ""
    name_only, _ext = os.path.splitext(filename)
    m = re.match(r"^ventaPerdida_(\d{2})-(\d{4})$", name_only, re.IGNORECASE)
    if not m:
        ops_logger.warning(f"Archivo ventaPerdida con nombre inválido recibido: {filename!r}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'ventaPerdida_MM-AAAA'")

    month = int(m.group(1))
    year = int(m.group(2))
    if month < 1 or month > 12 or year < 2000 or year > 2100:
        ops_logger.warning(f"Archivo ventaPerdida con mes/año inválido en el nombre: {name_only!r}")
        raise HTTPException(status_code=400, detail="El mes o año en el nombre del archivo no es válido")

    # Verificar en mi_bitacora_operaciones si ya fue procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:
                sql_check = "SELECT TOP 1 nombre_usuario, fecha FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :n1 OR name_file_load = :n2 ORDER BY fecha DESC"
                row = _db_check.execute(text(sql_check), {"n1": name_only, "n2": filename}).fetchone()
                if row is not None:
                    proc_user = row[0]
                    proc_fecha = row[1]
                    try:
                        if hasattr(proc_fecha, 'strftime'):
                            proc_fecha_str = proc_fecha.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            proc_fecha_str = str(proc_fecha)
                    except Exception:
                        proc_fecha_str = str(proc_fecha)
                    msg = f"El archivo ya fue procesado por {proc_user} el {proc_fecha_str}"
                    ops_logger.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                ops_logger.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        ops_logger.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_venta_perdida inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    try:
                        ops_logger.info(f"Procesando venta_perdida archivo={name_only} usuario={current_user.nombre_usuario}")
                    except Exception:
                        pass
                    inserted = process_venta_perdida(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    ops_logger.error(f"Error procesando venta perdida y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "Error al procesar archivo venta perdida; verificar api.log"})
        except Exception:
            # Fallback: try to parse without DB (but we cannot insert), return error
            try:
                import pandas as _pd
                df = _pd.read_excel(file_location)
                rows = len(df)
            except Exception:
                rows = 0
            try:
                safe_remove(file_location)
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "No se pudo abrir sesión DB para insertar venta perdida; verificar api.log"})

        # cleanup temp
        try:
            safe_remove(file_location)
        except Exception as del_err:
            ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error procesando archivo ventaPerdida {name_only}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")


@app.post("/procesar-pronostico-cobranza/")
@app.post("/procesar-pronostico-cobranza")
async def procesar_pronostico_cobranza(file: UploadFile = File(...), current_user: models.Usuario = Depends(get_current_user)):
    import os
    # Validar nomenclatura: pronosticoCobranza_DD-MM-AAAA
    filename = file.filename or ""
    name_only, _ext = os.path.splitext(filename)
    m = re.match(r"^pronosticoCobranza_(\d{2})-(\d{2})-(\d{4})$", name_only, re.IGNORECASE)
    if not m:
        ops_logger.warning(f"Archivo pronosticoCobranza con nombre inválido recibido: {filename!r}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'pronosticoCobranza_DD-MM-AAAA'")

    day = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    if day < 1 or day > 31 or month < 1 or month > 12 or year < 2000 or year > 2100:
        ops_logger.warning(f"Archivo pronosticoCobranza con fecha inválida en el nombre: {name_only!r}")
        raise HTTPException(status_code=400, detail="La fecha en el nombre del archivo no es válida")

    # Verificar en mi_bitacora_operaciones si ya fue procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:
                sql_check = "SELECT TOP 1 nombre_usuario, fecha FROM dbo.mi_bitacora_operaciones WHERE name_file_load = :n1 OR name_file_load = :n2 ORDER BY fecha DESC"
                row = _db_check.execute(text(sql_check), {"n1": name_only, "n2": filename}).fetchone()
                if row is not None:
                    proc_user = row[0]
                    proc_fecha = row[1]
                    try:
                        if hasattr(proc_fecha, 'strftime'):
                            proc_fecha_str = proc_fecha.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            proc_fecha_str = str(proc_fecha)
                    except Exception:
                        proc_fecha_str = str(proc_fecha)
                    msg = f"El archivo ya fue procesado por {proc_user} el {proc_fecha_str}"
                    ops_logger.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                ops_logger.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        ops_logger.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_pronostico_cobranza inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    try:
                        ops_logger.info(f"Procesando pronostico_cobranza archivo={name_only} usuario={current_user.nombre_usuario}")
                    except Exception:
                        pass
                    inserted = process_pronostico_cobranza(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    ops_logger.error(f"Error procesando pronostico cobranza y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "Error al procesar archivo pronostico cobranza; verificar api.log"})
        except Exception:
            # Fallback: try to parse without DB (but we cannot insert), return error
            try:
                import pandas as _pd
                df = _pd.read_excel(file_location)
                rows = len(df)
            except Exception:
                rows = 0
            try:
                safe_remove(file_location)
            except Exception:
                pass
            return JSONResponse(status_code=500, content={"rows_inserted": 0, "error": "No se pudo abrir sesión DB para insertar pronostico cobranza; verificar api.log"})

        # cleanup temp
        try:
            safe_remove(file_location)
        except Exception as del_err:
            ops_logger.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error procesando archivo pronosticoCobranza {name_only}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")


# --- Endpoints para tokens ---
@app.get("/tokens/{usuario_id}")
def list_tokens(usuario_id: int, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info(f"List tokens request for usuario_id={usuario_id}")
    tokens = db.query(models.Token).filter(models.Token.usuario_id == usuario_id).all()
    return [{"id": t.id, "usuario_id": t.usuario_id, "activo": t.activo, "fecha_creacion": t.fecha_creacion, "fecha_expiracion": t.fecha_expiracion} for t in tokens]


@app.post("/tokens/{token_id}/revoke")
def revoke_token(token_id: int, db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    logging.info(f"Revoke token request: token_id={token_id}")
    token = db.query(models.Token).filter(models.Token.id == token_id).first()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    token.activo = False
    db.commit()
    db.refresh(token)
    return {"id": token.id, "activo": token.activo}


# --- Endpoint para sincronización de datos ---
@app.post("/sincronizacion-data")
async def sincronizacion_data(db: Session = Depends(get_db), current_user: models.Usuario = Depends(get_current_user)):
    """
    Endpoint que se autentica con la API externa en localhost:5000 y obtiene datos de clientes.
    
    Proceso:
    1. Llama a /login para obtener el token
    2. Usa el token para llamar a /clientes
    3. Inserta los clientes en dbo.Clientes_tmp
    4. Registra la respuesta en el log
    """
    import httpx
    import json
    
    # Leer la URL base desde el archivo de propiedades
    properties_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.properties')
    BASE_URL = "http://localhost:5000"  # Valor por defecto
    
    try:
        with open(properties_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key.strip() == 'api.external.base_url':
                        BASE_URL = value.strip()
                        break
        ops_logger.info(f"URL base de API externa cargada desde config.properties: {BASE_URL}")
    except Exception as e:
        ops_logger.warning(f"No se pudo leer config.properties, usando valor por defecto: {e}")
    
    LOGIN_ENDPOINT = f"{BASE_URL}/login"
    CLIENTES_ENDPOINT = f"{BASE_URL}/clientes"
    
    try:
        logging.getLogger('operations').info(f"Iniciando sincronización de datos - Usuario: {current_user.nombre_usuario}")
        
        # Paso 1: Autenticación
        login_payload = {
            "username": "admin",
            "password": "admin123"
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Login
            ops_logger.info(f"Conectando login")
            login_response = await client.post(LOGIN_ENDPOINT, json=login_payload)
            
            if login_response.status_code != 200:
                ops_logger.error(f"Error en login: Status {login_response.status_code}, Response: {login_response.text}")
                raise HTTPException(
                    status_code=500, 
                    detail=f"Error al autenticar con API externa: {login_response.status_code}"
                )
            
            login_data = login_response.json()
            ops_logger.info(f"Login exitoso - Usuario: {login_data.get('usuario', {}).get('username')}, Token tipo: {login_data.get('tipo')}")
            
            # Extraer el token
            token = login_data.get("token")
            if not token:
                ops_logger.error("No se obtuvo token en la respuesta de login")
                raise HTTPException(status_code=500, detail="No se obtuvo token de autenticación")
            
            # Paso 2: Llamar al endpoint de clientes con el token
            headers = {
                "Authorization": f"Bearer {token}"
            }
            
            ops_logger.info(f"Conectando clientes")
            clientes_response = await client.get(CLIENTES_ENDPOINT, headers=headers)
            
            if clientes_response.status_code != 200:
                ops_logger.error(f"Error al obtener clientes: Status {clientes_response.status_code}, Response: {clientes_response.text}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Error al obtener datos de clientes: {clientes_response.status_code}"
                )
            
            response_data = clientes_response.json()
            
            # Registrar la respuesta en el log
            ops_logger.info("=" * 80)
            ops_logger.info("RESPUESTA OBTENIDA:")
            ops_logger.info("=" * 80)
            #ops_logger.info(f"Respuesta completa: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
            ops_logger.info("=" * 80)
            
            # Extraer el array de clientes del objeto de respuesta
            clientes_data = response_data.get('clientes', [])
            
        # Paso 3: Insertar clientes en dbo.Clientes_tmp
        ops_logger.info(f"Iniciando inserción de clientes - Total: {len(clientes_data)}")
        
        if not isinstance(clientes_data, list):
            ops_logger.error("Los datos de clientes no son una lista")
            raise HTTPException(status_code=500, detail="Formato de datos de clientes inválido")
        
        try:
            # Primero, obtener los nombres de columnas reales de la tabla
            #ops_logger.info("Consultando estructura de tabla dbo.Clientes_tmp")
            columns_result = db.execute(text("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Clientes_tmp'
                ORDER BY ORDINAL_POSITION
            """))
            column_names = [row[0] for row in columns_result]
            #ops_logger.info(f"Columnas encontradas en dbo.Clientes_tmp: {column_names}")
            
            # Limpiar la tabla temporal antes de insertar
            #ops_logger.info("Limpiando tabla dbo.Clientes_tmp")
            db.execute(text("TRUNCATE TABLE dbo.Clientes_tmp"))
            
            # Construir el SQL de inserción dinámicamente basado en las columnas reales
            if len(column_names) >= 2:
                col1, col2 = column_names[0], column_names[1]
                insert_sql = text(f"INSERT INTO dbo.Clientes_tmp ({col1}, {col2}) VALUES (:id, :razon_social)")
                #ops_logger.info(f"SQL de inserción: INSERT INTO dbo.Clientes_tmp ({col1}, {col2}) VALUES (:id, :razon_social)")
            else:
                ops_logger.error(f"La tabla no tiene suficientes columnas: {column_names}")
                raise HTTPException(status_code=500, detail="Estructura de tabla Clientes_tmp inválida")
            
            clientes_insertados = 0
            
            for cliente in clientes_data:
                # Extraer ID y RazonSocial del cliente
                cliente_id = cliente.get('id') or cliente.get('ID') or cliente.get('cliente_id')
                razon_social = cliente.get('razon_social') or cliente.get('RazonSocial') or cliente.get('nombre') or cliente.get('Nombre')
                
                if cliente_id is None or razon_social is None:
                    ops_logger.warning(f"Cliente sin ID o RazonSocial, saltando: {cliente}")
                    continue
                
                # Insertar el cliente
                db.execute(insert_sql, {"id": cliente_id, "razon_social": razon_social})
                clientes_insertados += 1
            
            # Commit de todas las inserciones
            db.commit()
            
            ops_logger.info(f"Inserción completada: {clientes_insertados} clientes insertados")
            
            # Ejecutar el stored procedure de sincronización
            #ops_logger.info("Ejecutando stored procedure dbo.sp_sincroniza_data")
            try:
                db.execute(text("EXEC dbo.sp_sincroniza_data"))
                db.commit()
                ops_logger.info("Stored procedure ejecutado exitosamente")
            except Exception as sp_err:
                ops_logger.error(f"Error ejecutando stored procedure : {str(sp_err)}")
                try:
                    db.rollback()
                except Exception:
                    pass
                raise HTTPException(
                    status_code=500,
                    detail=f"Error ejecutando stored procedure: {str(sp_err)}"
                )
            
            ops_logger.info("Sincronización completada exitosamente")
            
            return {
                "success": True,
                "mensaje": "Sincronización completada exitosamente",
                "total_clientes": len(clientes_data),
                "clientes_insertados": clientes_insertados,
                "stored_procedure_ejecutado": True,
                "datos_registrados_en_log": True
            }
            
        except Exception as db_err:
            ops_logger.error(f"Error al insertar clientes en base de datos: {str(db_err)}")
            try:
                db.rollback()
            except Exception:
                pass
            raise HTTPException(
                status_code=500,
                detail=f"Error al insertar clientes en base de datos: {str(db_err)}"
            )
            
    except httpx.RequestError as e:
        ops_logger.error(f"Error de conexión con API externa: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=f"No se pudo conectar con la API externa: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        ops_logger.error(f"Error inesperado en sincronización: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error en sincronización: {str(e)}"
        )
