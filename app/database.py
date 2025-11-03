from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

# Formato estándar recomendado por SQLAlchemy para SQL Server
SQLALCHEMY_DATABASE_URL = (
    f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
)
# Configure pool parameters to avoid QueuePool timeouts under concurrency.
# pool_size: number of persistent connections to keep
# max_overflow: number of connections allowed above pool_size
# pool_timeout: how long to wait for a connection before raising
# pool_pre_ping: check connection liveness before using it
# pool_recycle: recycle connections after this many seconds to avoid stale connections
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_pre_ping=True,
    pool_recycle=3600,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
