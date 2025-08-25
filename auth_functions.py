from database_models import User, create_database_engine, get_session
from sqlalchemy.exc import IntegrityError
import re

class AuthManager:
    def __init__(self, database_url=None):
        self.engine = create_database_engine(database_url)
    
    def validate_email(self, email):
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None
    
    def validate_password(self, password):
        """Validate password strength"""
        if len(password) < 8:
            return False, "Password must be at least 8 characters long"
        if not re.search(r'[A-Za-z]', password):
            return False, "Password must contain at least one letter"
        if not re.search(r'\d', password):
            return False, "Password must contain at least one number"
        return True, "Password is valid"
    
    def register_user(self, username, email, password):
        """Register a new user"""
        session = get_session(self.engine)
        
        try:
            # Validate inputs
            if not username or len(username) < 3:
                return False, "Username must be at least 3 characters long"
            
            if not self.validate_email(email):
                return False, "Invalid email format"
            
            is_valid, message = self.validate_password(password)
            if not is_valid:
                return False, message
            
            # Check if user already exists
            existing_user = session.query(User).filter(
                (User.username == username) | (User.email == email)
            ).first()
            
            if existing_user:
                if existing_user.username == username:
                    return False, "Username already exists"
                else:
                    return False, "Email already registered"
            
            # Create new user
            new_user = User(username=username, email=email)
            new_user.set_password(password)
            
            session.add(new_user)
            session.commit()
            
            return True, "User registered successfully"
            
        except IntegrityError:
            session.rollback()
            return False, "Username or email already exists"
        except Exception as e:
            session.rollback()
            return False, f"Registration failed: {str(e)}"
        finally:
            session.close()
    
    def login_user(self, username_or_email, password):
        """Authenticate user login"""
        session = get_session(self.engine)
        
        try:
            # Find user by username or email
            user = session.query(User).filter(
                (User.username == username_or_email) | (User.email == username_or_email)
            ).first()
            
            if not user:
                return False, None, "User not found"
            
            if not user.is_active:
                return False, None, "Account is deactivated"
            
            if user.check_password(password):
                return True, user, "Login successful"
            else:
                return False, None, "Invalid password"
                
        except Exception as e:
            return False, None, f"Login failed: {str(e)}"
        finally:
            session.close()
    
    def get_user_by_id(self, user_id):
        """Get user by ID"""
        session = get_session(self.engine)
        try:
            user = session.query(User).filter(User.id == user_id).first()
            return user
        finally:
            session.close()
    
    def update_password(self, user_id, old_password, new_password):
        """Update user password"""
        session = get_session(self.engine)
        
        try:
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                return False, "User not found"
            
            if not user.check_password(old_password):
                return False, "Current password is incorrect"
            
            is_valid, message = self.validate_password(new_password)
            if not is_valid:
                return False, message
            
            user.set_password(new_password)
            session.commit()
            
            return True, "Password updated successfully"
            
        except Exception as e:
            session.rollback()
            return False, f"Password update failed: {str(e)}"
        finally:
            session.close()
