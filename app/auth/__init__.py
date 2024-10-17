from flask import Blueprint

bp = Blueprint('auth', __name__)

from . import routes, decorators

# If you want to make the decorators easily importable from app.auth
from .decorators import login_required, admin_required

def init_oauth(app):
    from app.extensions import oauth
    oauth.init_app(app)
    oauth.register(
        name='google',
        client_id=app.config['GOOGLE_CLIENT_ID'],
        client_secret=app.config['GOOGLE_CLIENT_SECRET'],
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email profile'},
    )