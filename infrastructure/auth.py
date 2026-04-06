"""
ContactIQ — Authentication Middleware
Flask request decoration and session management.
"""
from functools import wraps
from flask import request, redirect, url_for, session, jsonify, g
from infrastructure.database import validate_session, create_session, destroy_session, verify_user


def init_auth(app):
    """Initialize auth on the Flask app."""
    # F-19 fix: Do NOT override SECRET_KEY with a hardcoded fallback.
    # app.py already generates a cryptographically random key if SECRET_KEY env var is unset.
    if not app.config.get('SECRET_KEY'):
        raise RuntimeError("SECRET_KEY must be set before init_auth is called")

    @app.before_request
    def _check_auth():
        """Check authentication on every request except login/static."""
        # Public routes that don't need auth
        public_paths = ['/login', '/api/auth/login', '/api/health', '/static/', '/favicon.ico']
        if any(request.path.startswith(p) for p in public_paths):
            return

        # Check session cookie
        token = session.get('auth_token')
        if token:
            user = validate_session(token)
            if user:
                g.user = user
                return

        # If API request, return 401
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Authentication required', 'redirect': '/login'}), 401

        # Otherwise redirect to login
        return redirect('/login')


def login_user(username, password, ip_address=None):
    """
    Authenticate user and create session.
    Returns (success, user_dict_or_error_message, token).
    """
    user = verify_user(username, password)
    if not user:
        return False, 'Invalid username or password', None

    token = create_session(user['id'], ip_address=ip_address)
    return True, user, token


def logout_user():
    """Destroy current session."""
    token = session.get('auth_token')
    if token:
        destroy_session(token)
    session.clear()


def get_current_user():
    """Get current authenticated user from request context."""
    return getattr(g, 'user', None)


ROLE_HIERARCHY = {'admin': 3, 'supervisor': 2, 'analyst': 1}

def require_role(min_role):
    """Decorator to require a minimum role level. admin > supervisor > analyst."""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            user = get_current_user()
            if not user:
                return jsonify({'error': 'Authentication required'}), 401
            if min_role == 'any':
                return f(*args, **kwargs)
            user_level = ROLE_HIERARCHY.get(user.get('role', 'analyst'), 0)
            required_level = ROLE_HIERARCHY.get(min_role, 0)
            if user_level < required_level:
                return jsonify({'error': f'Insufficient permissions. Requires "{min_role}" role or higher.'}), 403
            return f(*args, **kwargs)
        return wrapper
    return decorator
