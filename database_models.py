from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import bcrypt
import os
from sqlalchemy import Text, ForeignKey
from sqlalchemy.orm import relationship
import json

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(80), unique=True, nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    password_hash = Column(String(128), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    
    def set_password(self, password):
        """Hash and set the user's password"""
        salt = bcrypt.gensalt()
        self.password_hash = bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
    
    def check_password(self, password):
        """Check if the provided password matches the stored hash"""
        return bcrypt.checkpw(password.encode('utf-8'), self.password_hash.encode('utf-8'))
    
    def __repr__(self):
        return f'<User {self.username}>'

# Database setup
def create_database_engine(database_url=None):
    """Create database engine - modify the URL as needed"""
    if database_url is None:
        # Default to SQLite for development
        database_url = "sqlite:///streamlit_users.db"
    
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    return engine

def get_session(engine):
    """Get database session"""
    Session = sessionmaker(bind=engine)
    return Session()



class EventLog(Base):
    __tablename__ = "event_logs"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    username = Column(String(80))
    email = Column(String(120))
    level = Column(String(16), default="INFO")
    action = Column(String(100), nullable=False)
    details = Column(Text)

    user = relationship("User", backref="event_logs")


def log_event(engine, *, user_id=None, username=None, email=None,
              action:str, details:dict|str=None, level:str="INFO"):
    Session = sessionmaker(bind=engine)
    s = Session()
    try:
        payload = details if isinstance(details, str) else json.dumps(details or {})
        entry = EventLog(
            user_id=user_id, username=username, email=email,
            action=action, details=payload, level=level
        )
        s.add(entry)
        s.commit()
    except Exception:
        s.rollback()
    finally:
        s.close()
