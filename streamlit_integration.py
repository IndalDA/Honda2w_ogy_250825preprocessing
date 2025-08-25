import streamlit as st
from auth_functions import AuthManager

class StreamlitAuth:
    def __init__(self, database_url=None):
        self.auth_manager = AuthManager(database_url)
        
        # Initialize session state
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        if 'user_id' not in st.session_state:
            st.session_state.user_id = None
        if 'username' not in st.session_state:
            st.session_state.username = None
    
    def login_form(self):
        """Display login form"""
        st.subheader("Login")
        
        with st.form("login_form"):
            username_or_email = st.text_input("Username or Email")
            password = st.text_input("Password", type="password")
            submit_button = st.form_submit_button("Login")
            
            if submit_button:
                if username_or_email and password:
                    success, user, message = self.auth_manager.login_user(username_or_email, password)
                    
                    if success:
                        st.session_state.authenticated = True
                        st.session_state.user_id = user.id
                        st.session_state.username = user.username
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)
                else:
                    st.error("Please fill in all fields")
    
    def register_form(self):
        """Display registration form"""
        st.subheader("Register")
        
        with st.form("register_form"):
            username = st.text_input("Username")
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            confirm_password = st.text_input("Confirm Password", type="password")
            submit_button = st.form_submit_button("Register")
            
            if submit_button:
                if username and email and password and confirm_password:
                    if password != confirm_password:
                        st.error("Passwords do not match")
                    else:
                        success, message = self.auth_manager.register_user(username, email, password)
                        
                        if success:
                            st.success(message)
                            st.info("You can now login with your credentials")
                        else:
                            st.error(message)
                else:
                    st.error("Please fill in all fields")
    
    def logout(self):
        """Logout user"""
        st.session_state.authenticated = False
        st.session_state.user_id = None
        st.session_state.username = None
        st.rerun()
    
    def require_auth(self):
        """Decorator-like function to require authentication"""
        if not st.session_state.authenticated:
            st.warning("Please login to access this page")
            
            tab1, tab2 = st.tabs(["Login", "Register"])
            
            with tab1:
                self.login_form()
            
            with tab2:
                self.register_form()
            
            return False
        return True
    
    def get_current_user(self):
        """Get current logged-in user"""
        if st.session_state.authenticated and st.session_state.user_id:
            return self.auth_manager.get_user_by_id(st.session_state.user_id)
        return None
