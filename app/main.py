
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
from app.excel_processor import process_excel, process_incidencias, process_pipeline_transporte, process_pipeline_comercial, process_disponibilidad_transporte, process_factoraje, process_relacion_pago, process_evidencias_pendientes
import json
from fastapi.encoders import jsonable_encoder
from jose import JWTError, jwt
import re

logging.basicConfig(
    filename="api.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    encoding="utf-8"
)

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
        logging.warning(f"Archivo con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'OnTime_acumulado_AAAA'")
    year = int(m.group(1))
    if year < 2000 or year > 2100:
        logging.warning(f"Archivo con año inválido en el nombre: {filename}")
        raise HTTPException(status_code=400, detail="El año en el nombre del archivo no es válido")

    # Antes de procesar, verificar en dbo.mi_bitacora_operaciones que el archivo no haya sido procesado
    try:
        from app.database import SessionLocal as _SessionLocal
        with _SessionLocal() as _db_check:
            try:

                # Comprobar tanto el nombre sin extensión como el filename completo
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
                    logging.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                # Si la comprobación falla por algún motivo, registrarlo y continuar con el procesamiento
                logging.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        # If the inner check raised an HTTPException (file already processed), re-raise it so the endpoint returns 400.
        if isinstance(e, HTTPException):
            raise
        # Otherwise log and continue (verification couldn't be performed)
        logging.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

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
                    data = process_excel(file_location, db=db, username=current_user.nombre_usuario)
                except Exception as pe_err:
                    # Log full traceback and return a concise error to the caller
                    tb = traceback.format_exc()
                    logging.error(f"Error procesando OnTime y ejecutando SP: {pe_err}\n{tb}")
                    # Clean up temp file before returning
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                    return JSONResponse(status_code=500, content={"rows_read": 0, "error": "Error al procesar archivo OnTime; verificar api.log"})
        except Exception:
            # If DB session cannot be opened or process_excel raised before using DB, try fallback processing without DB
            try:
                data = process_excel(file_location)
            except Exception as fallback_err:
                tb = traceback.format_exc()
                logging.error(f"Error en procesamiento fallback del archivo: {fallback_err}\n{tb}")
                try:
                    safe_remove(file_location)
                except Exception as rm_err:
                    logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
                return JSONResponse(status_code=500, content={"rows_read": 0, "error": "Error al procesar archivo; verificar api.log"})

        # Guardar resultados leídos en archivo .txt: acumulado_<AAAA>.txt
        out_filename = f"acumulado_{year}.txt"
        try:
            with open(out_filename, "w", encoding="utf-8") as out_f:
                for row in data:
                    safe_row = jsonable_encoder(row)
                    out_f.write(json.dumps(safe_row, ensure_ascii=False) + "\n")
        except Exception as wf_err:
            logging.error(f"Error escribiendo archivo de salida {out_filename}: {wf_err}")
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
            logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        # Retornar sólo el número de registros leídos
        return {"rows_read": rows_count}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error procesando archivo {file.filename}: {e}")
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")

# Dependency
import traceback


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    # Log full stack trace for debugging and return a sanitized 500 response
    tb = traceback.format_exc()
    logging.error(f"Unhandled exception for request {request.url}: {tb}")
    return Response(status_code=500, content="Internal Server Error")

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
        logging.warning(f"Archivo de incidencias con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'incidencias_MM-AAAA'")
    month = int(m.group(1))
    year = int(m.group(2))
    if month < 1 or month > 12 or year < 2000 or year > 2100:
        logging.warning(f"Archivo de incidencias con mes/año inválido en el nombre: {filename}")
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
                    logging.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                logging.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        logging.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_incidencias inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    inserted = process_incidencias(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    logging.error(f"Error procesando incidencias y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
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
            logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error procesando archivo de incidencias {filename}: {e}")
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
        logging.warning(f"Archivo pipeline transporte con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'pipelineTransporte_sem_XX_MM-AAAA'")
    week = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    if week < 1 or week > 53 or month < 1 or month > 12 or year < 2000 or year > 2100:
        logging.warning(f"Archivo pipeline transporte con semana/mes/año inválido en el nombre: {filename}")
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
                    logging.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                logging.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        logging.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_pipeline_transporte inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    processed = process_pipeline_transporte(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    logging.error(f"Error procesando pipeline transporte y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
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
            logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": processed}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error procesando archivo pipeline {filename}: {e}")
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
        logging.warning(f"Archivo pipeline comercial con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'pipelineComercial_semXX_DD-MM-AAAA'")
    week = int(m.group(1))
    day = int(m.group(2))
    month = int(m.group(3))
    year = int(m.group(4))
    if week < 1 or week > 53 or day < 1 or day > 31 or month < 1 or month > 12 or year < 2000 or year > 2100:
        logging.warning(f"Archivo pipeline comercial con semana/dia/mes/año inválido en el nombre: {filename}")
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
                    logging.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                logging.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        logging.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_pipeline_comercial inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    processed = process_pipeline_comercial(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    logging.error(f"Error procesando pipeline comercial y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
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
            logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": processed}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error procesando archivo pipeline comercial {filename}: {e}")
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
        logging.warning(f"Archivo disponibilidad transporte con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'disponibilidadTransporte_MM-AAAA'")
    month = int(m.group(1))
    year = int(m.group(2))
    if month < 1 or month > 12 or year < 2000 or year > 2100:
        logging.warning(f"Archivo disponibilidad transporte con mes/año inválido en el nombre: {filename}")
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
                    logging.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                logging.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        logging.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call processor inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    processed = process_disponibilidad_transporte(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    logging.error(f"Error procesando disponibilidad transporte y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
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
            logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": processed}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error procesando archivo disponibilidad {filename}: {e}")
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
        logging.warning(f"Archivo factoraje con nombre inválido recibido: {filename}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'factoraje_DD-MM-AAAA'")

    day = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    if day < 1 or day > 31 or month < 1 or month > 12 or year < 2000 or year > 2100:
        logging.warning(f"Archivo factoraje con fecha inválida en el nombre: {filename}")
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
                    logging.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                logging.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        logging.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_factoraje inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    inserted = process_factoraje(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    logging.error(f"Error procesando factoraje y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
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
            logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error procesando archivo factoraje {filename}: {e}")
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
        logging.warning(f"Archivo relacionPago con nombre inválido recibido: {filename!r}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'relacionPago_DD-MM-AAAA'")

    day = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    if day < 1 or day > 31 or month < 1 or month > 12 or year < 2000 or year > 2100:
        logging.warning(f"Archivo relacionPago con fecha inválida en el nombre: {filename!r}")
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
                    logging.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                logging.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        logging.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_relacion_pago inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    inserted = process_relacion_pago(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    logging.error(f"Error procesando relacion_pago y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
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
            logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error procesando archivo process_relacion_pago {filename}: {e}")
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
        logging.warning(f"Archivo evidenciasPendientes con nombre inválido recibido: {filename!r}")
        raise HTTPException(status_code=400, detail="El nombre del archivo no cumple con el formato requerido 'evidenciasPendientes_DD_MM-AAAA'")

    day = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    if day < 1 or day > 31 or month < 1 or month > 12 or year < 2000 or year > 2100:
        logging.warning(f"Archivo evidenciasPendientes con fecha inválida en el nombre: {name_only!r}")
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
                    logging.info(f"Archivo {filename} ya procesado: {msg}")
                    raise HTTPException(status_code=400, detail=msg)
            except HTTPException:
                raise
            except Exception as e:
                logging.warning(f"No se pudo verificar si el archivo ya fue procesado (continuando): {e}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        logging.warning(f"No se pudo abrir sesión para verificar bitácora; continuando con el procesamiento: {e}")

    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Open DB session and call process_evidencias_pendientes inside single transaction
        try:
            from app.database import SessionLocal
            with SessionLocal() as db:
                try:
                    inserted = process_evidencias_pendientes(file_location, db=db, username=current_user.nombre_usuario, original_name=name_only)
                    # commit once
                    db.commit()
                except Exception as pi_err:
                    tb = traceback.format_exc()
                    logging.error(f"Error procesando evidencias pendientes y ejecutando SP: {pi_err}\n{tb}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    try:
                        safe_remove(file_location)
                    except Exception as rm_err:
                        logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {rm_err}")
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
            logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")

        return {"rows_inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error procesando archivo evidencias_pendientes {name_only}: {e}")
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
