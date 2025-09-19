import streamlit as st
from tbl import user_register, user_login, User_Exist, cursor
import re

def main():

    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.username = ""

    def register_user():
        st.subheader("Register")
        new_username = st.text_input("Choose a Username", key="reg_username")
        new_userEmail = st.text_input("Enter your Email", key="reg_email")
        if new_userEmail:
            if re.match(r"[^@]+@[^@]+\.[^@]+", new_userEmail):
                pass
            else:
                st.error("Please enter a valid email address.")
        new_password = st.text_input("Choose a Password", type="password", key="reg_password")
        confirm_password = st.text_input("Confirm Password", type="password", key="reg_confirm_password")
        register_button = st.button("Register", key="btn_register")
        if register_button:
            if User_Exist(new_userEmail, new_username):
                st.error("Username already exists!")
            elif new_password != confirm_password:
                st.error("Passwords do not match!")
            elif len(new_username.strip()) == 0 or len(new_password.strip()) == 0:
                st.error("Username and password cannot be empty.")
            else:
                user_register(new_username, new_userEmail, new_password)
                st.success("Registration successful! You can now log in.")

    def login_user():
        st.subheader("Login")
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")
        login_button = st.button("Login", key="btn_login")
        # new_ui.py (inside login_user)
        if login_button:
            user_id, name = user_login(username, password)
            if user_id is not None:
                st.session_state.logged_in = True
                st.session_state.user_id = int(user_id) if isinstance(user_id, (int, float)) else user_id
                st.session_state.username = name or username
                st.rerun()
            else:
                st.error("Invalid username or password")



        # if login_button:
        #     login_success = user_login(username, password)
        #     if login_success:
        #         st.session_state.logged_in = True
        #         st.session_state.username = username

        #         # force immediate rerun so sidebar switches to 'logged in' UI
        #         st.rerun()
        #     else:
        #         st.error("Invalid username or password")

    # Show login/register only if NOT logged in
    if not st.session_state.logged_in:
        with st.sidebar:
            st.title("Authentication")
            tab1, tab2 = st.tabs(["Login", "Register"])
            with tab1:
                login_user()
            with tab2:
                register_user()
        # main area hint
        st.info("Please log in to access the app.")
    else:
        # After login: show a compact sidebar with status + logout only
        with st.sidebar:
            st.success(f"✅ Logged in as {st.session_state.username}")
            if st.button("Logout", key="btn_logout"):
                st.session_state.logged_in = False
                st.session_state.username = ""
                # clear any login-related keys (optional but helpful)
                for k in ["login_username", "login_password", "reg_username", "reg_email", "reg_password", "reg_confirm_password"]:
                    if k in st.session_state:
                        del st.session_state[k]
                st.rerun()
