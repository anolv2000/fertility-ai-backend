import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
DATASET_PATH = os.getenv("DATASET_PATH")

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


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


def init_db():
    Base.metadata.create_all(engine)


def _clean_int(x):
    # CSV has numbers like "80,242"
    if pd.isna(x):
        return None
    return int(str(x).replace(",", "").strip())


def seed_data():
    if not DATASET_PATH or not os.path.exists(DATASET_PATH):
        raise FileNotFoundError(f"Dataset not found at DATASET_PATH={DATASET_PATH}")

    df = pd.read_csv(DATASET_PATH)

    # Normalize column names we expect
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

    # Clean
    df["TOTAL NUMBER"] = df["TOTAL NUMBER"].apply(_clean_int)
    df["TOTAL NUMBER WHITE"] = df["TOTAL NUMBER WHITE"].apply(_clean_int)
    df["TOTAL NUMBER BLACK"] = df["TOTAL NUMBER BLACK"].apply(_clean_int)

    # Rates should be floats
    rate_cols = ["TOTAL RATE", "TOTAL RATE WHITE", "TOTAL RATE BLACK"]
    for c in rate_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    session = SessionLocal()

    # Optional: clear table to reseed (simple approach for dev)
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
