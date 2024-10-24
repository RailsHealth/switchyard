# production.py
import subprocess
import sys
import os
import signal
import logging
from logging.handlers import RotatingFileHandler
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('logs/production.log', maxBytes=10000000, backupCount=5),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ProductionServer:
    def __init__(self):
        self.processes = {}
        self.should_run = True

    def start_gunicorn(self):
        """Start Gunicorn process"""
        cmd = [
            'gunicorn',
            '--config', 'gunicorn.conf.py',
            '--log-level', 'info',
            '--capture-output',
            'run:app'
        ]
        return subprocess.Popen(cmd)

    def start_celery_worker(self):
        """Start Celery worker"""
        cmd = [
            'celery',
            '-A', 'app.celery_app',
            'worker',
            '--loglevel=info',
            '-Q', 'hl7v2_conversion,fhir_queue,conversion_queue,file_queue,'
                  'validation_queue,maintenance_queue,metrics_queue'
        ]
        return subprocess.Popen(cmd)

    def start_celery_beat(self):
        """Start Celery beat"""
        cmd = [
            'celery',
            '-A', 'app.celery_app',
            'beat',
            '--loglevel=info'
        ]
        return subprocess.Popen(cmd)

    def start_all(self):
        """Start all production processes"""
        try:
            logger.info("Starting production services...")
            
            self.processes['gunicorn'] = self.start_gunicorn()
            logger.info("Gunicorn started")
            
            self.processes['celery_worker'] = self.start_celery_worker()
            logger.info("Celery worker started")
            
            self.processes['celery_beat'] = self.start_celery_beat()
            logger.info("Celery beat started")
            
            return True
        except Exception as e:
            logger.error(f"Error starting services: {e}")
            self.cleanup()
            return False

    def cleanup(self):
        """Clean up all processes"""
        logger.info("Cleaning up processes...")
        for name, process in self.processes.items():
            try:
                process.terminate()
                process.wait(timeout=5)
                logger.info(f"{name} terminated")
            except subprocess.TimeoutExpired:
                process.kill()
                logger.warning(f"{name} killed after timeout")
            except Exception as e:
                logger.error(f"Error terminating {name}: {e}")

    def monitor(self):
        """Monitor running processes"""
        while self.should_run:
            for name, process in self.processes.items():
                if process.poll() is not None:
                    logger.error(f"{name} terminated unexpectedly")
                    self.cleanup()
                    sys.exit(1)
            time.sleep(1)

def main():
    server = ProductionServer()
    
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal")
        server.should_run = False
        server.cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if server.start_all():
        try:
            server.monitor()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            server.cleanup()
    else:
        sys.exit(1)

if __name__ == '__main__':
    main()