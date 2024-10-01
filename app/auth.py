from flask import Blueprint, current_app, redirect, url_for, session, request, render_template, flash, g
from app.models.user import User
from app.models.organization import Organization
from datetime import datetime
from app.extensions import oauth
from flask_wtf import FlaskForm

bp = Blueprint('auth', __name__)

class LoginForm(FlaskForm):
    pass

def init_oauth(app):
    oauth.init_app(app)
    oauth.register(
        name='google',
        client_id=app.config['GOOGLE_CLIENT_ID'],
        client_secret=app.config['GOOGLE_CLIENT_SECRET'],
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email profile'},
    )

@bp.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        return redirect(url_for('auth.google_login'))
    return render_template('login.html', form=form)

@bp.route('/google_login')
def google_login():
    redirect_uri = url_for('auth.authorized', _external=True)
    return oauth.google.authorize_redirect(redirect_uri)

@bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('main.index'))

@bp.route('/authorized')
def authorized():
    try:
        token = oauth.google.authorize_access_token()
        resp = oauth.google.get('https://openidconnect.googleapis.com/v1/userinfo')
        user_info = resp.json()
        google_id = user_info['sub']
        user = User.get_by_google_id(google_id)
        if not user:
            user_id = User.create(
                google_id=google_id,
                name=user_info['name'],
                email=user_info['email'],
                profile_picture_url=user_info.get('picture')
            )
        else:
            user_id = user['uuid']
            User.update(user_id, last_login=datetime.utcnow())
        
        session['user_id'] = user_id
        user = User.get_by_uuid(user_id)
        
        # Set the current organization
        user_orgs = User.get_organizations(user_id)
        if user_orgs:
            session['current_org_id'] = user_orgs[0]['uuid']  # Set the first org as current
        else:
            return redirect(url_for('auth.create_organization'))
        
        return redirect(url_for('main.index'))
    except Exception as e:
        current_app.logger.error(f'Error during Google authentication: {str(e)}')
        return 'Authentication failed', 400

@bp.route('/create_organization', methods=['GET', 'POST'])
def create_organization():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    
    if request.method == 'POST':
        name = request.form.get('name')
        website = request.form.get('website')
        email = request.form.get('email')
        org_type = request.form.get('type')
        
        org_id = Organization.create(
            name=name,
            org_type=org_type,
            website=website,
            email=email
        )
        
        User.add_to_organization(session['user_id'], org_id)
        session['current_org_id'] = org_id
        flash('Organization created successfully!', 'success')
        return redirect(url_for('main.index'))
    
    return render_template('create_organization.html', config=current_app.config)

@bp.route('/switch_organization/<org_id>')
def switch_organization(org_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    
    user = User.get_by_uuid(session['user_id'])
    if org_id in [org['uuid'] for org in user['organizations']]:
        session['current_org_id'] = org_id
        flash('Organization switched successfully', 'success')
    else:
        flash('You do not have access to this organization', 'error')
    
    return redirect(request.referrer or url_for('main.index'))

@bp.before_app_request
def load_logged_in_user():
    user_id = session.get('user_id')
    if user_id is None:
        g.user = None
        g.organization = None
        g.user_role = None
    else:
        g.user = User.get_by_uuid(user_id)
        org_id = session.get('current_org_id')
        if org_id:
            g.organization = Organization.get_by_uuid(org_id)
            g.user_role = User.get_user_role_in_organization(user_id, org_id)
        else:
            g.organization = None
            g.user_role = None

@bp.route('/profile')
def profile():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = User.get_by_uuid(session['user_id'])
    return render_template('profile.html', user=user)