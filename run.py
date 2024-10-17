import logging
import subprocess
import sys
import os
import signal
from app import create_app
from config import Config
from app.extensions import mongo
from flask.cli import FlaskGroup
import redis
import click

# Setup the main logger
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
app_logger = logging.getLogger('app')

print("Config object:", vars(Config))

app = create_app(Config)
cli = FlaskGroup(app)

# Global variables for process management
worker_process = None
beat_process = None

def start_celery_worker():
    global worker_process
    cmd = [
        sys.executable, '-m', 'celery',
        '-A', 'app.celery_app', 'worker',
        '--loglevel=info',
        '-Q', 'hl7v2_conversion,fhir_queue,conversion_queue,file_queue,validation_queue,maintenance_queue,metrics_queue'
    ]
    worker_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return worker_process

def start_celery_beat():
    global beat_process
    cmd = [
        sys.executable, '-m', 'celery',
        '-A', 'app.celery_app', 'beat',
        '--loglevel=info'
    ]
    beat_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return beat_process

def check_redis():
    redis_client = redis.Redis.from_url(app.config['REDIS_URL'])
    try:
        redis_client.ping()
        app_logger.info("Redis is up and running")
        return True
    except redis.ConnectionError:
        app_logger.error("Failed to connect to Redis")
        return False

def signal_handler(signum, frame):
    app_logger.info("Received shutdown signal. Initiating graceful shutdown...")
    cleanup()
    sys.exit(0)

def cleanup():
    app_logger.info("Cleaning up...")
    global worker_process, beat_process
    if worker_process:
        worker_process.terminate()
        app_logger.info("Celery worker terminated")
    if beat_process:
        beat_process.terminate()
        app_logger.info("Celery beat terminated")
    app_logger.info("All processes terminated")

def run_app():
    app_logger.info("Starting the Flask application")

    if not check_redis():
        app_logger.error("Redis is not running. Please start Redis and try again.")
        sys.exit(1)

    try:
        # Start Celery worker
        start_celery_worker()
        app_logger.info("Celery worker started")

        # Start Celery beat
        start_celery_beat()
        app_logger.info("Celery beat started")

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Run the Flask application
        app_logger.info("Starting Flask server...")
        app.run(host='0.0.0.0', port=app.config['PORT'], debug=app.config['DEBUG'], use_reloader=False)

    except Exception as e:
        app_logger.error(f"Error during startup: {str(e)}")
    finally:
        cleanup()

@cli.command("run")
def run_command():
    """Run the Flask application with all components."""
    run_app()

@cli.command("celery-worker")
def celery_worker():
    """Run Celery worker."""
    os.execvp('celery', [
        'celery',
        '-A', 'app.celery_app',
        'worker',
        '--loglevel=info',
        '-Q', 'hl7v2_conversion,fhir_queue,conversion_queue,file_queue,validation_queue,maintenance_queue,metrics_queue'
    ])

@cli.command("celery-beat")
def celery_beat():
    """Run Celery beat."""
    os.execvp('celery', [
        'celery',
        '-A', 'app.celery_app',
        'beat',
        '--loglevel=info'
    ])

@cli.command("init-db")
def init_db():
    """Initialize the database."""
    with app.app_context():
        # Add your database initialization logic here
        app_logger.info("Initializing database...")
        # Example: Create indexes
        mongo.db.messages.create_index([("timestamp", -1)])
        app_logger.info("Database initialized")

@cli.command("create-admin")
@click.argument('email')
@click.argument('password')
def create_admin(email, password):
    """Create an admin user."""
    from app.models.user import User
    with app.app_context():
        if User.get_by_email(email):
            app_logger.error(f"User with email {email} already exists")
            return
        user_id = User.create(email=email, password=password, is_admin=True)
        app_logger.info(f"Admin user created with ID: {user_id}")

@cli.command("list-routes")
def list_routes():
    """List all registered routes."""
    output = []
    for rule in app.url_map.iter_rules():
        options = {}
        for arg in rule.arguments:
            options[arg] = f"[{arg}]"
        methods = ','.join(rule.methods)
        url = rule.rule
        line = f"{rule.endpoint:50s} {methods:20s} {url}"
        output.append(line)
    
    for line in sorted(output):
        print(line)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        run_app()
    else:
        cli()