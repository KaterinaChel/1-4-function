
from sqlalchemy import create_engine
from connection import connection_string
from sqlalchemy import Column, Integer, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()

class Log(Base):
    __tablename__ = 'logs_new'
    __table_args__ = {'schema': 'logs'}
    log_id = Column(Integer, primary_key=True, autoincrement=True)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    source = Column(Text)
    success = Column(Boolean)
    error_code = Column(Text)
    action_task = Column(Text)

engine = create_engine(connection_string)

def create_logs(param1):
    engine = create_engine(param1)
    Base.metadata.create_all(engine)

def log_action(start_time=None, end_time=None, source=None, success=None, error_code=None, action_task=None):
    Session = sessionmaker(engine)
    with Session() as session:
        log = Log(start_time=start_time, end_time=end_time, source=source, success=success, error_code=error_code, action_task=action_task)
        session.add(log)
        session.commit()


