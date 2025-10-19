
from pydantic import BaseModel, EmailStr
from typing import Optional, List
from datetime import datetime

class RolBase(BaseModel):
    nombre: str
    descripcion: Optional[str] = None

class RolCreate(RolBase):
    pass

class RolOut(RolBase):
    id: int
    class Config:
        orm_mode = True

class UsuarioBase(BaseModel):
    nombre_usuario: str
    correo_electronico: EmailStr
    rol_id: Optional[int] = None
    activo: Optional[bool] = True

class UsuarioCreate(UsuarioBase):
    contrasena: str

class UsuarioOut(UsuarioBase):
    id: int
    fecha_creacion: Optional[datetime] = None
    rol: Optional[RolOut]
    class Config:
        orm_mode = True

class BitacoraBase(BaseModel):
    accion: str
    ip_origen: Optional[str] = None

class BitacoraCreate(BitacoraBase):
    usuario_id: int

class BitacoraOut(BitacoraBase):
    id: int
    usuario_id: int
    fecha: Optional[str]
    class Config:
        orm_mode = True


class CronActivDiariaOut(BaseModel):
    actividad_descripcion: Optional[str]
    archivo_nombre: Optional[str]
    archivo_nomenclatura: Optional[str]
    dias_carga: Optional[str]

    class Config:
        orm_mode = True
