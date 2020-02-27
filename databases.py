from sqlalchemy import Column, String, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

class Chat_Table(Base):
    __tablename__ = 'Chat_Table'
    id          = Column(Integer, primary_key=True)
    chat_id     = Column(Integer, nullable=False)
    text        = Column(String, nullable=True)
    salary      = Column(Float, nullable=True)
    employment  = Column(String, nullable=True)
    last_update = Column(DateTime)

