import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

# ----------------------------
# Paths
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "fertility.db")

os.makedirs(DATA_DIR, exist_ok=True)

# ----------------------------
# Database engine (SQLite)
# ----------------------------
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False},
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

# ----------------------------
# Model
# ----------------------------
class FertilityStats(Base):
    __tablename__ = "fertility_stats"

    id = Column(Integer, primary_key=True, index=True)
    region = Column(String, index=True)

    total_number = Column(Integer)
    total_rate = Column(Float)

    total_number_white = Column(Integer)
    total_rate_white = Column(Float)

    total_number_black = Column(Integer)
    total_rate_black = Column(Float)

# ----------------------------
# Init DB
# ----------------------------
def init_db():
    Base.metadata.create_all(bind=engine)

# ----------------------------
# Seeding helpers
# ----------------------------
DATASET_PATH = os.getenv("DATASET_PATH")

def _clean_int(x):
    if pd.isna(x):
        return None
    return int(str(x).replace(",", "").strip())

def seed_data():
    if not DATASET_PATH or not os.path.exists(DATASET_PATH):
        raise FileNotFoundError(f"Dataset not found at DATASET_PATH={DATASET_PATH}")

    df = pd.read_csv(DATASET_PATH)

    expected = [
        "REGION",
        "TOTAL NUMBER",
        "TOTAL RATE",
        "TOTAL NUMBER WHITE",
        "TOTAL RATE WHITE",
        "TOTAL NUMBER BLACK",
        "TOTAL RATE BLACK",
    ]

    for col in expected:
        if col not in df.columns:
            raise ValueError(f"Missing expected column: {col}")

    df["TOTAL NUMBER"] = df["TOTAL NUMBER"].apply(_clean_int)
    df["TOTAL NUMBER WHITE"] = df["TOTAL NUMBER WHITE"].apply(_clean_int)
    df["TOTAL NUMBER BLACK"] = df["TOTAL NUMBER BLACK"].apply(_clean_int)

    for c in ["TOTAL RATE", "TOTAL RATE WHITE", "TOTAL RATE BLACK"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    session = SessionLocal()
    session.query(FertilityStats).delete()

    for _, row in df.iterrows():
        session.add(
            FertilityStats(
                region=str(row["REGION"]).strip(),
                total_number=row["TOTAL NUMBER"],
                total_rate=row["TOTAL RATE"],
                total_number_white=row["TOTAL NUMBER WHITE"],
                total_rate_white=row["TOTAL RATE WHITE"],
                total_number_black=row["TOTAL NUMBER BLACK"],
                total_rate_black=row["TOTAL RATE BLACK"],
            )
        )

    session.commit()
    session.close()
