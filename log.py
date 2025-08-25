import streamlit as st
import pandas as pd
from sqlalchemy.orm import sessionmaker
from database_models import EventLog
from auth_functions import AuthManager
from database_models import User


ADMIN_EMAILS = {"massage2indal@gmail.com"}
AuthManager = AuthManager()
#AuthManager.engine = create_database_engine()

User = User()
email = User.email


def show_user_log(email=ADMIN_EMAILS):
    from database_models import create_database_engine
    ENGINE = create_database_engine()
    if st.session_state.get("username") and st.session_state.get("email") in admin_emails:
        st.sidebar.header("🪵 Event Logs")
        Session = sessionmaker(bind=ENGINE)
        s = Session()
        logs = s.query(EventLog).order_by(EventLog.id.desc()).limit(200).all()
        s.close()
        df = pd.DataFrame([{
            "time": l.created_at, "user": l.username, "email": l.email,
            "action": l.action, "level": l.level, "details": l.details
        } for l in logs])
        st.sidebar.dataframe(df, height=300)
        st.sidebar.download_button("Download Logs CSV", df.to_csv(index=False), "logs.csv")
