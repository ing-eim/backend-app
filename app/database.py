from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
