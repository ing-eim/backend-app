
import logging
from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
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
        "https://http://dwh.retornologistico.com"
    ],  # Agrega aquí los orígenes necesarios
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")

# --- Endpoint para procesar archivo Excel ---
@app.post("/procesar-excel/")
async def procesar_excel(file: UploadFile = File(...)):
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
    access_token = auth.create_access_token(data={"sub": usuario.nombre_usuario})
    logging.info(f"Login successful for user: {form_data.username}")
    return {"access_token": access_token, "token_type": "bearer"}

# --- Endpoints de usuarios ---
@app.post("/usuarios/", response_model=schemas.UsuarioOut)
def create_usuario(usuario: schemas.UsuarioCreate, db: Session = Depends(get_db)):
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
def read_usuarios(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    logging.info(f"Read usuarios request: skip={skip}, limit={limit}")
    usuarios = crud.get_usuarios(db, skip=skip, limit=limit)
    return usuarios

@app.get("/usuarios/{usuario_id}", response_model=schemas.UsuarioOut)
def read_usuario(usuario_id: int, db: Session = Depends(get_db)):
    logging.info(f"Read usuario request: usuario_id={usuario_id}")
    db_usuario = crud.get_usuario(db, usuario_id=usuario_id)
    if db_usuario is None:
        logging.warning(f"Usuario no encontrado: usuario_id={usuario_id}")
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return db_usuario

@app.put("/usuarios/{usuario_id}", response_model=schemas.UsuarioOut)
def update_usuario(usuario_id: int, usuario: schemas.UsuarioCreate, db: Session = Depends(get_db)):
    logging.info(f"Update usuario request: usuario_id={usuario_id}")
    db_usuario = crud.update_usuario(db, usuario_id=usuario_id, usuario=usuario)
    if db_usuario is None:
        logging.warning(f"Usuario no encontrado para actualizar: usuario_id={usuario_id}")
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return db_usuario

@app.delete("/usuarios/{usuario_id}", response_model=schemas.UsuarioOut)
def delete_usuario(usuario_id: int, db: Session = Depends(get_db)):
    logging.info(f"Delete usuario request: usuario_id={usuario_id}")
    db_usuario = crud.delete_usuario(db, usuario_id=usuario_id)
    if db_usuario is None:
        logging.warning(f"Usuario no encontrado para eliminar: usuario_id={usuario_id}")
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return db_usuario

# --- Endpoints de roles ---
@app.get("/roles/", response_model=list[schemas.RolOut])
def read_roles(db: Session = Depends(get_db)):
    logging.info("Read roles request")
    return crud.get_roles(db)

@app.post("/roles/", response_model=schemas.RolOut)
def create_rol(rol: schemas.RolCreate, db: Session = Depends(get_db)):
    logging.info(f"Create rol request: {rol.nombre}")
    return crud.create_rol(db, rol)

# --- Endpoints de bitácora ---
@app.post("/bitacora/", response_model=schemas.BitacoraOut)
def create_bitacora(bitacora: schemas.BitacoraCreate, db: Session = Depends(get_db)):
    logging.info(f"Create bitacora request: usuario_id={bitacora.usuario_id}, accion={bitacora.accion}")
    return crud.create_bitacora(db, bitacora)

@app.get("/bitacora/", response_model=list[schemas.BitacoraOut])
def read_bitacora(usuario_id: int = None, db: Session = Depends(get_db)):
    logging.info(f"Read bitacora request: usuario_id={usuario_id}")
    return crud.get_bitacora(db, usuario_id)
