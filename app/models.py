
from sqlalchemy import Column, Integer, String, Text, Boolean, TIMESTAMP, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class Rol(Base):
    __tablename__ = "roles"
    id = Column(Integer, primary_key=True)
    nombre = Column(String(50), nullable=False)
    descripcion = Column(Text)
    usuarios = relationship("Usuario", back_populates="rol")
    permisos = relationship("RolPermiso", back_populates="rol")

class Usuario(Base):
    __tablename__ = "usuarios"
    id = Column(Integer, primary_key=True)
    nombre_usuario = Column(String(50), unique=True, nullable=False)
    correo_electronico = Column(String(100), unique=True, nullable=False)
    contrasena_hash = Column(String(255), nullable=False)
    rol_id = Column(Integer, ForeignKey("roles.id"), nullable=True)
    activo = Column(Boolean, default=True)
    fecha_creacion = Column(TIMESTAMP)
    rol = relationship("Rol", back_populates="usuarios")
    bitacoras = relationship("Bitacora", back_populates="usuario")
    tokens = relationship("Token", back_populates="usuario")
    recuperaciones = relationship("RecuperacionContrasena", back_populates="usuario")

class Bitacora(Base):
    __tablename__ = "bitacora"
    id = Column(Integer, primary_key=True)
    usuario_id = Column(Integer, ForeignKey("usuarios.id"), nullable=False)
    accion = Column(String(255), nullable=False)
    ip_origen = Column(String(45))
    fecha = Column(TIMESTAMP)
    usuario = relationship("Usuario", back_populates="bitacoras")

class Token(Base):
    __tablename__ = "tokens"
    id = Column(Integer, primary_key=True)
    usuario_id = Column(Integer, ForeignKey("usuarios.id"), nullable=False)
    token = Column(Text, nullable=False)
    fecha_creacion = Column(TIMESTAMP)
    fecha_expiracion = Column(TIMESTAMP, nullable=True)
    activo = Column(Boolean, default=True)
    usuario = relationship("Usuario", back_populates="tokens")

class RecuperacionContrasena(Base):
    __tablename__ = "recuperacion_contrasena"
    id = Column(Integer, primary_key=True)
    usuario_id = Column(Integer, ForeignKey("usuarios.id"), nullable=False)
    token = Column(Text, nullable=False)
    fecha_solicitud = Column(TIMESTAMP)
    fecha_expiracion = Column(TIMESTAMP, nullable=True)
    usado = Column(Boolean, default=False)
    usuario = relationship("Usuario", back_populates="recuperaciones")

class Permiso(Base):
    __tablename__ = "permisos"
    id = Column(Integer, primary_key=True)
    nombre = Column(String(100), nullable=False)
    descripcion = Column(Text)
    roles = relationship("RolPermiso", back_populates="permiso")

class RolPermiso(Base):
    __tablename__ = "rol_permisos"
    rol_id = Column(Integer, ForeignKey("roles.id"), primary_key=True)
    permiso_id = Column(Integer, ForeignKey("permisos.id"), primary_key=True)
    rol = relationship("Rol", back_populates="permisos")
    permiso = relationship("Permiso", back_populates="roles")
