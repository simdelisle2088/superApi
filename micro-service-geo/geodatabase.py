from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import os

# Load the .env file
load_dotenv(
    os.path.join(os.path.dirname(__file__), ".env"),
)

# Environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_DATABASE = os.getenv("DB_DATABASE")

MICRO_SERVICE_KEY = os.getenv("MICRO_SERVICE_KEY")

# Create a base class for your models
Base = declarative_base()

# Database URLs
DATABASE_URL = (
    f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"
)

# Create the engine
engine = create_async_engine(
    DATABASE_URL,
    pool_size=2,
    pool_recycle=1200,
)


# Create a session factory function for the primary database
def SessionLocal():
    return AsyncSession(
        bind=engine,
        expire_on_commit=False,
    )


# Dependency to get a database session for the primary database
async def get_db() -> AsyncSession:
    async with SessionLocal() as session:
        yield session
