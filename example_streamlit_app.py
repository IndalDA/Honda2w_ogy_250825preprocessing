import streamlit as st
from streamlit_integration import StreamlitAuth

# Initialize authentication
auth = StreamlitAuth()

# App title
st.title("My Streamlit App with Authentication")

# Sidebar for authentication status
with st.sidebar:
    if st.session_state.authenticated:
        st.success(f"Welcome, {st.session_state.username}!")
        if st.button("Logout"):
            auth.logout()
    else:
        st.info("Please login to continue")

# Main app content
if auth.require_auth():
    # This content will only show if user is authenticated
    st.header("Protected Content")
    st.write("This is your main app content that requires authentication.")
    
    # Get current user info
    current_user = auth.get_current_user()
    if current_user:
        st.write(f"User ID: {current_user.id}")
        st.write(f"Email: {current_user.email}")
        st.write(f"Member since: {current_user.created_at}")
    
    # Your existing Streamlit app content goes here
    st.write("Add your existing Streamlit app components below this line...")
