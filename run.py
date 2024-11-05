import logging
import subprocess
import sys
import os
import signal
import time
from app import create_app
from config import Config
from app.extensions import mongo
from flask.cli import FlaskGroup
import redis
import click
import threading

# Setup the main logger
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
app_logger = logging.getLogger('app')

app = create_app(Config)
cli = FlaskGroup(app)

# Global variables for process management
worker_process = None
beat_process = None
flower_process = None

def start_celery_worker():
    """Start Celery worker process"""
    global worker_process
    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.dirname(os.path.abspath(__file__)) + os.pathsep + env.get('PYTHONPATH', '')
    
    cmd = [
        sys.executable, '-m', 'celery',
        '-A', 'app.extensions.celery_worker:celery',
        'worker',
        '--loglevel=info',
        '-Q', (
            'hl7v2_conversion,'
            'clinical_notes_conversion,'
            'ccda_conversion,'
            'x12_conversion,'
            'pasted_message_parsing,'
            'sftp_file_parsing,'
            'file_parsing,'
            'fhir_queue,'
            'conversion_queue,'
            'validation_queue,'
            'maintenance_queue,'
            'metrics_queue'
        )
    ]
    worker_process = subprocess.Popen(
        cmd, 
        env=env, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1
    )
    return worker_process

def start_celery_beat():
    """Start Celery beat process"""
    global beat_process
    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.dirname(os.path.abspath(__file__)) + os.pathsep + env.get('PYTHONPATH', '')
    
    cmd = [
        sys.executable, '-m', 'celery',
        '-A', 'app.extensions.celery_worker:celery',
        'beat',
        '--loglevel=info'
    ]
    beat_process = subprocess.Popen(
        cmd, 
        env=env, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1
    )
    return beat_process

def start_flower():
    """Start Flower monitoring dashboard"""
    global flower_process
    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.dirname(os.path.abspath(__file__)) + os.pathsep + env.get('PYTHONPATH', '')
    
    cmd = [
        sys.executable, '-m', 'celery',  # Use python -m celery instead of direct flower command
        '-A', 'app.extensions.celery_worker:celery',
        'flower',
        '--port=5555',
        '--address=0.0.0.0',  # Allow external access
        '--broker=' + app.config['REDIS_URL'],
        '--result_backend=' + app.config['REDIS_URL'],
        '--basic_auth=' + os.environ.get('FLOWER_AUTH', 'admin:admin'),
        '--url_prefix=flower',  # Optional: add URL prefix for reverse proxy
        '--persistent=True',  # Enable persistent mode
        '--state_save_interval=60000',  # Save state every minute
        '--logging=info'  # Enable detailed logging
    ]
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Set up log files
    flower_log = open('logs/flower.log', 'a')
    flower_error_log = open('logs/flower_error.log', 'a')
    
    flower_process = subprocess.Popen(
        cmd,
        env=env,
        stdout=flower_log,
        stderr=flower_error_log,
        universal_newlines=True,
        bufsize=1
    )
    return flower_process

def monitor_process_output(process, name):
    """Monitor and log process output"""
    def read_output():
        while True:
            output = process.stdout.readline()
            if output:
                app_logger.info(f"{name} output: {output.strip()}")
            error = process.stderr.readline()
            if error:
                app_logger.error(f"{name} error: {error.strip()}")
            if not output and not error and process.poll() is not None:
                break
    
    thread = threading.Thread(target=read_output, daemon=True)
    thread.start()
    return thread

def check_redis():
    """Check Redis connection"""
    redis_client = redis.Redis.from_url(app.config['REDIS_URL'])
    try:
        redis_client.ping()
        app_logger.info("Redis is up and running")
        return True
    except redis.ConnectionError:
        app_logger.error("Failed to connect to Redis")
        return False

def check_process_health():
    """Check health of all processes"""
    critical_processes = [
        (worker_process, "Celery worker"),
        (beat_process, "Celery beat")
    ]
    
    optional_processes = [
        (flower_process, "Flower")
    ]
    
    # Check critical processes
    for process, name in critical_processes:
        if process and process.poll() is not None:
            return_code = process.poll()
            # Read any error output
            if process.stderr:
                error_output = process.stderr.read()
                app_logger.error(f"{name} process error output: {error_output}")
            app_logger.error(f"{name} process died with return code {return_code}")
            return False
            
    # Check optional processes
    for process, name in optional_processes:
        if process and process.poll() is not None:
            return_code = process.poll()
            app_logger.warning(f"{name} process died with return code {return_code}")
            # Try to restart Flower
            try:
                restart_flower()
            except Exception as e:
                app_logger.error(f"Failed to restart {name}: {str(e)}")
    
    return True

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    app_logger.info("Received shutdown signal. Initiating graceful shutdown...")
    cleanup()
    sys.exit(0)

def cleanup():
    """Clean up all processes"""
    app_logger.info("Cleaning up...")
    global worker_process, beat_process, flower_process
    
    processes = [
        (worker_process, "Celery worker"),
        (beat_process, "Celery beat"),
        (flower_process, "Flower monitoring")
    ]
    
    for process, name in processes:
        if process:
            try:
                # Close file handles if they exist
                if process.stdout:
                    process.stdout.close()
                if process.stderr:
                    process.stderr.close()
                    
                # Terminate process
                process.terminate()
                try:
                    process.wait(timeout=5)  # Wait up to 5 seconds
                    app_logger.info(f"{name} terminated")
                except subprocess.TimeoutExpired:
                    process.kill()  # Force kill if timeout
                    app_logger.warning(f"{name} force killed")
            except Exception as e:
                app_logger.error(f"Error terminating {name}: {str(e)}")
    
    # Close Flower log files
    try:
        os.system('pkill -f "celery flower"')  # Ensure Flower is really stopped
    except Exception as e:
        app_logger.error(f"Error killing Flower process: {str(e)}")
    
    app_logger.info("All processes terminated")

def restart_flower():
    """Restart Flower if it dies"""
    global flower_process
    if flower_process and flower_process.poll() is not None:
        app_logger.warning("Flower process died, attempting restart...")
        try:
            flower_process = start_flower()
            app_logger.info("Flower process restarted successfully")
        except Exception as e:
            app_logger.error(f"Failed to restart Flower: {str(e)}")

def run_app():
    """Run the complete application"""
    app_logger.info("Starting the Flask application")

    if not check_redis():
        app_logger.error("Redis is not running. Please start Redis and try again.")
        sys.exit(1)

    try:
        # Start all processes
        worker = start_celery_worker()
        monitor_process_output(worker, "Celery worker")
        app_logger.info("Celery worker started")

        beat = start_celery_beat()
        monitor_process_output(beat, "Celery beat")
        app_logger.info("Celery beat started")

        # Start Flower after a short delay
        time.sleep(2)  # Wait for worker to initialize
        flower = start_flower()
        monitor_process_output(flower, "Flower")
        app_logger.info("Flower monitoring started at http://localhost:5555")

        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start health check thread
        def health_check_loop():
            while True:
                if not check_process_health():
                    app_logger.error("Critical process died, initiating shutdown")
                    cleanup()
                    sys.exit(1)
                restart_flower()  # Try to restart Flower if it died
                time.sleep(5)

        health_thread = threading.Thread(target=health_check_loop, daemon=True)
        health_thread.start()

        # Run Flask
        app_logger.info("Starting Flask server...")
        app.run(
            host='0.0.0.0',
            port=app.config['PORT'],
            debug=app.config['DEBUG'],
            use_reloader=False
        )

    except Exception as e:
        app_logger.error(f"Error during startup: {str(e)}")
        raise
    finally:
        cleanup()

# CLI Commands
@cli.command("run")
def run_command():
    """Run the Flask application with all components."""
    run_app()

@cli.command("init-db")
def init_db():
    """Initialize the database."""
    with app.app_context():
        app_logger.info("Initializing database...")
        # Create message indexes
        mongo.db.messages.create_index([("timestamp", -1)])
        mongo.db.messages.create_index([("uuid", 1)], unique=True)
        mongo.db.messages.create_index([("conversion_status", 1), ("type", 1)])
        mongo.db.messages.create_index([("organization_uuid", 1)])
        
        # Create FHIR message indexes
        mongo.db.fhir_messages.create_index(
            [("original_message_uuid", 1), ("organization_uuid", 1)],
            unique=True
        )
        
        app_logger.info("Database initialized successfully")

@cli.command("health-check")
def health_check_command():
    """Check the health of all components."""
    with app.app_context():
        checks = {
            "redis": check_redis(),
            "mongodb": mongo.db.command('ping')['ok'] == 1.0,
            "celery_worker": worker_process is not None and worker_process.poll() is None,
            "celery_beat": beat_process is not None and beat_process.poll() is None,
            "flower": flower_process is not None and flower_process.poll() is None
        }
        
        for component, status in checks.items():
            print(f"{component}: {'OK' if status else 'FAILED'}")
        
        return all(checks.values())

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        run_app()
    else:
        cli()