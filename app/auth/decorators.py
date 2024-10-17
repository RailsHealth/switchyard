from functools import wraps
from flask import g, redirect, url_for, request, flash, abort
from flask import current_app as app

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not g.user or not g.user_role or g.user_role != 'admin':
            flash('Admin access required for this action.', 'error')
            abort(403)
        return f(*args, **kwargs)
    return decorated_function

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not g.user:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('auth.login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

def org_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not g.organization:
            flash('Please select an organization', 'warning')
            return redirect(url_for('organizations.list_organizations'))
        return f(*args, **kwargs)
    return decorated_function

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not g.user_role or g.user_role not in roles:
                flash(f'Access denied. Required role: {", ".join(roles)}', 'error')
                abort(403)
            return f(*args, **kwargs)
        return decorated_function
    return decorator