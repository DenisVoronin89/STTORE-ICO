from sqlalchemy import Column, Float, Date, Integer, String, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from datetime import datetime
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class ExchangeRate(Base):
    __tablename__ = "exchange"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    date = Column(Date, nullable=False, server_default=func.current_date())  # только дата (YYYY-MM-DD)
    buy = Column(Float, nullable=False)
    sell = Column(Float, nullable=False)


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    transaction_hash = Column(String(66), unique=True, nullable=False)
    amount = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class MonthlyStats(Base):
    __tablename__ = "monthly_stats"

    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    total_amount = Column(Float, default=0, nullable=False)
    total_count = Column(Integer, default=0, nullable=False)
    all_time_count = Column(Integer, default=0, nullable=False)

    __table_args__ = (
        UniqueConstraint("year", "month", name="uniq_monthly_stats"),
    )
