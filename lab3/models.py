from sqlalchemy import Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata

class Weather(Base):
    __tablename__ = "weather"

    id = Column(Integer(), primary_key=True)
    country = Column(String(50)),
    wind_degree = Column(Integer()),
    wind_kph = Column(Float()),
    wind_direction = Column(String(3)),
    last_updated = Column(String(16)),
    sunrise = Column(String(8)),
    precip_mm = Column(Float())
