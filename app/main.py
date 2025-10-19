
import logging
from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File, Request, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import SessionLocal, engine
from app import models, schemas, crud, auth
from app.excel_processor import process_excel
from jose import JWTError, jwt

logging.basicConfig(
    filename="api.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

models.Base.metadata.create_all(bind=engine)




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


def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(lambda: SessionLocal())):
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
    try:
        file_location = f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())
        data = process_excel(file_location)
        response = {"rows": data}
        # Elimina el archivo temporal después de procesar y responder
        try:
            os.remove(file_location)
        except Exception as del_err:
            import logging
            logging.warning(f"No se pudo eliminar el archivo temporal {file_location}: {del_err}")
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error procesando archivo: {str(e)}")

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

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
