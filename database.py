import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker

# =========================
# Paths (SAFE for prod)
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "fertility.db")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

DATABASE_URL = f"sqlite:///{DB_PATH}"

# =========================
# Database setup
# =========================

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()

# =========================
# Models
# =========================

class FertilityStats(Base):
    __tablename__ = "fertility_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    region = Column(String, index=True)

    total_number = Column(Integer)
    total_rate = Column(Float)

    total_number_white = Column(Integer)
    total_rate_white = Column(Float)

    total_number_black = Column(Integer)
    total_rate_black = Column(Float)

# =========================
# Init DB
# =========================

def init_db():
    Base.metadata.create_all(engine)

# =========================
# Data seeding (OPTIONAL)
# =========================

def _clean_int(x):
    if pd.isna(x):
        return None
    return int(str(x).replace(",", "").strip())

def seed_data(csv_path: str):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    session = SessionLocal()
    session.query(FertilityStats).delete()

    for _, row in df.iterrows():
        session.add(
            FertilityStats(
                region=str(row["REGION"]).strip(),
                total_number=_clean_int(row["TOTAL NUMBER"]),
                total_rate=float(row["TOTAL RATE"]),
                total_number_white=_clean_int(row["TOTAL NUMBER WHITE"]),
                total_rate_white=float(row["TOTAL RATE WHITE"]),
                total_number_black=_clean_int(row["TOTAL NUMBER BLACK"]),
                total_rate_black=float(row["TOTAL RATE BLACK"]),
            )
        )

    session.commit()
    session.close()
