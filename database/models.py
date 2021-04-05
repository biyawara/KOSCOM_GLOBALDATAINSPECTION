from sqlalchemy import Column, Integer, String, Float, Index, DateTime, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from database.manager import Base

class eikon_us_equity_hist(Base):
    """미국 종목 히스토리"""

    __tablename__ = "eikon_equity_us_hist"

    code = Column("code", String, primary_key=True)  # 한글명
    timestamp = Column("date", DateTime, primary_key=True)
    open = Column("open", Float)
    high = Column("high", Float)
    low = Column("low", Float)
    close = Column("close", Float)
    volume = Column("volume", Float)
    
    __table_args__ = (
        {"extend_existing": True}
    )


class us_equity_code(Base):
    """미국 종목 정보"""

    __tablename__ = "us_equity_us_code"

    code = Column("code", String, primary_key=True)  # 한글명
    ric = Column("ric", String, primary_key=True)

    __table_args__ = (
        {"extend_existing": True}
    )



