
from sqlalchemy.orm import Session
from app import models, schemas
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

# Usuarios
def get_usuario(db: Session, usuario_id: int):
    return db.query(models.Usuario).filter(models.Usuario.id == usuario_id).first()

def get_usuario_by_nombre(db: Session, nombre_usuario: str):
    return db.query(models.Usuario).filter(models.Usuario.nombre_usuario == nombre_usuario).first()

def create_usuario(db: Session, usuario: schemas.UsuarioCreate):
    password = usuario.contrasena
    hashed_password = pwd_context.hash(password)
    db_usuario = models.Usuario(
        nombre_usuario=usuario.nombre_usuario,
        correo_electronico=usuario.correo_electronico,
        contrasena_hash=hashed_password,
        rol_id=usuario.rol_id,
        activo=usuario.activo
    )
    db.add(db_usuario)
    db.commit()
    db.refresh(db_usuario)
    return db_usuario

def get_usuarios(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Usuario).offset(skip).limit(limit).all()

def update_usuario(db: Session, usuario_id: int, usuario: schemas.UsuarioCreate):
    db_usuario = get_usuario(db, usuario_id)
    if db_usuario:
        db_usuario.nombre_usuario = usuario.nombre_usuario
        db_usuario.correo_electronico = usuario.correo_electronico
        db_usuario.contrasena_hash = pwd_context.hash(usuario.contrasena)
        db_usuario.rol_id = usuario.rol_id
        db_usuario.activo = usuario.activo
        db.commit()
        db.refresh(db_usuario)
    return db_usuario

def delete_usuario(db: Session, usuario_id: int):
    db_usuario = get_usuario(db, usuario_id)
    if db_usuario:
        db.delete(db_usuario)
        db.commit()
    return db_usuario

# Roles
def get_rol(db: Session, rol_id: int):
    return db.query(models.Rol).filter(models.Rol.id == rol_id).first()

def get_roles(db: Session):
    return db.query(models.Rol).all()

def create_rol(db: Session, rol: schemas.RolCreate):
    db_rol = models.Rol(nombre=rol.nombre, descripcion=rol.descripcion)
    db.add(db_rol)
    db.commit()
    db.refresh(db_rol)
    return db_rol

# Bitácora
def create_bitacora(db: Session, bitacora: schemas.BitacoraCreate):
    db_bitacora = models.Bitacora(
        usuario_id=bitacora.usuario_id,
        accion=bitacora.accion,
        ip_origen=bitacora.ip_origen
    )
    db.add(db_bitacora)
    db.commit()
    db.refresh(db_bitacora)
    return db_bitacora

def get_bitacora(db: Session, usuario_id: int = None):
    query = db.query(models.Bitacora)
    if usuario_id:
        query = query.filter(models.Bitacora.usuario_id == usuario_id)
    return query.all()
