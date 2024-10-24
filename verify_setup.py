# verify_setup.py
import os
import sys
import subprocess
import logging
import pkg_resources
from pathlib import Path
import socket
import requests
from flask import Flask
import redis
from pymongo import MongoClient
import time
from dotenv import load_dotenv
import json
import site
import platform
from datetime import datetime
import psutil
import shutil
import tempfile

# Setup logging with color support
class ColorFormatter(logging.Formatter):
    """Custom formatter with colors"""
    grey = "\x1b[38;21m"
    blue = "\x1b[38;5;39m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    green = "\x1b[38;5;40m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    def __init__(self, fmt):
        super().__init__()
        self.fmt = fmt
        self.FORMATS = {
            logging.DEBUG: self.grey + self.fmt + self.reset,
            logging.INFO: self.blue + self.fmt + self.reset,
            logging.WARNING: self.yellow + self.fmt + self.reset,
            logging.ERROR: self.red + self.fmt + self.reset,
            logging.CRITICAL: self.bold_red + self.fmt + self.reset
        }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

# Setup logging
def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Console handler with color formatting
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(ColorFormatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)

    # File handler for logging to file
    log_file = os.path.join('logs', 'setup_verification.log')
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    return logger

logger = setup_logging()

class SystemVerifier:
    def __init__(self):
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'environment': {},
            'dependencies': {},
            'connectivity': {},
            'flask': {},
            'directories': {},
            'disk_space': {},
            'system_resources': {},
            'overall': False
        }

    def check_python_environment(self):
        """Check Python environment details"""
        logger.info("\n=== Python Environment ===")
        try:
            env_info = {
                'python_version': sys.version.split()[0],
                'python_full_version': sys.version,
                'python_location': sys.executable,
                'platform': platform.platform(),
                'virtual_env_active': sys.prefix != sys.base_prefix,
                'virtual_env_path': sys.prefix if sys.prefix != sys.base_prefix else 'Not in virtualenv',
                'site_packages': [str(path) for path in site.getsitepackages()],
                'environment': os.environ.get('FLASK_ENV', 'not set'),
                'temp_directory': tempfile.gettempdir(),
                'working_directory': os.getcwd()
            }

            logger.info(f"Python Version: {env_info['python_version']}")
            logger.info(f"Platform: {env_info['platform']}")
            logger.info(f"Virtual Environment: {'Active' if env_info['virtual_env_active'] else 'Inactive'}")
            logger.info(f"Virtual Environment Path: {env_info['virtual_env_path']}")
            logger.info(f"Working Directory: {env_info['working_directory']}")

            self.results['environment'] = env_info
            return True

        except Exception as e:
            logger.error(f"Error checking Python environment: {str(e)}", exc_info=True)
            self.results['environment'] = {'error': str(e)}
            return False

    def load_environment_variables(self):
        """Load and verify environment variables"""
        logger.info("\n=== Environment Variables ===")
        required_vars = {
            'MONGO_URI': 'MongoDB connection string',
            'REDIS_URL': 'Redis connection URL',
            'SECRET_KEY': 'Flask secret key',
            'FLASK_APP': 'Flask application entry point',
            'CELERY_BROKER_URL': 'Celery broker URL',
            'CELERY_RESULT_BACKEND': 'Celery result backend'
        }

        env_path = Path('.') / '.env'
        if env_path.exists():
            try:
                load_dotenv(dotenv_path=env_path)
                logger.info(f"Loaded environment from {env_path}")
            except Exception as e:
                logger.error(f"Error loading .env file: {str(e)}")

        missing = []
        found = []
        env_vars = {}

        for var, description in required_vars.items():
            value = os.environ.get(var)
            if not value:
                missing.append(f"{var} ({description})")
            else:
                # Mask sensitive values
                if var in ['MONGO_URI', 'SECRET_KEY']:
                    display_value = f"{value[:10]}...{value[-10:]}"
                else:
                    display_value = value
                found.append(f"{var}: {display_value}")
                env_vars[var] = display_value

        if missing:
            logger.error(f"Missing variables: {', '.join(missing)}")
        for var in found:
            logger.info(f"✓ {var}")

        self.results['environment_variables'] = {
            'missing': missing,
            'found': env_vars
        }
        return len(missing) == 0

    def check_system_resources(self):
        """Check system resources"""
        logger.info("\n=== System Resources ===")
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            resources = {
                'cpu_usage_percent': cpu_percent,
                'memory_total_gb': memory.total / (1024**3),
                'memory_available_gb': memory.available / (1024**3),
                'memory_used_percent': memory.percent,
                'disk_total_gb': disk.total / (1024**3),
                'disk_free_gb': disk.free / (1024**3),
                'disk_used_percent': disk.percent
            }

            logger.info(f"CPU Usage: {cpu_percent}%")
            logger.info(f"Memory Usage: {memory.percent}%")
            logger.info(f"Disk Usage: {disk.percent}%")

            self.results['system_resources'] = resources
            return True

        except Exception as e:
            logger.error(f"Error checking system resources: {str(e)}")
            return False

    def check_dependencies(self):
        """Check required Python packages"""
        logger.info("\n=== Python Dependencies ===")
        required_packages = {
            'flask': 'Flask web framework',
            'redis': 'Redis client',
            'pymongo': 'MongoDB client',
            'gunicorn': 'WSGI HTTP Server',
            'python-dotenv': 'Environment variable loader',
            'celery': 'Task queue',
            'psutil': 'System and process utilities'
        }

        installed_packages = {pkg.key: pkg.version for pkg in pkg_resources.working_set}
        missing = []
        installed = []

        for package, description in required_packages.items():
            if package.lower() in installed_packages:
                version = installed_packages[package.lower()]
                installed.append(f"{package} v{version} ({description})")
            else:
                missing.append(f"{package} ({description})")

        if missing:
            logger.error(f"Missing packages: {', '.join(missing)}")
        for pkg in installed:
            logger.info(f"✓ {pkg}")

        self.results['dependencies'] = {
            'installed': installed,
            'missing': missing
        }
        return len(missing) == 0

    def check_directories(self):
        """Verify required directories exist and are writable"""
        logger.info("\n=== Directory Structure ===")
        required_dirs = {
            'logs': 'Application logs',
            'static': 'Static files',
            'templates': 'HTML templates',
            'app': 'Application code'
        }

        missing = []
        existing = []
        permissions = {}

        for dir_name, description in required_dirs.items():
            dir_status = {
                'exists': os.path.exists(dir_name),
                'writable': False,
                'readable': False
            }
            
            if not dir_status['exists']:
                missing.append(dir_name)
                try:
                    os.makedirs(dir_name)
                    logger.info(f"Created missing directory: {dir_name} ({description})")
                    dir_status['exists'] = True
                except Exception as e:
                    logger.error(f"Failed to create directory {dir_name}: {str(e)}")
            else:
                existing.append(f"{dir_name} ({description})")

            if dir_status['exists']:
                dir_status['writable'] = os.access(dir_name, os.W_OK)
                dir_status['readable'] = os.access(dir_name, os.R_OK)

            permissions[dir_name] = dir_status

        for dir_name in existing:
            logger.info(f"✓ {dir_name}")

        self.results['directories'] = {
            'existing': existing,
            'missing': missing,
            'permissions': permissions
        }
        return len(missing) == 0

    def check_network_connectivity(self):
        """Check network connectivity and services"""
        logger.info("\n=== Network Connectivity ===")
        services = {
            'MongoDB': self._check_mongodb,
            'Redis': self._check_redis,
            'Web Application Port': self._check_web_port
        }

        results = {}
        all_passed = True

        for service_name, check_func in services.items():
            try:
                success = check_func()
                status = "✓" if success else "✗"
                logger.info(f"{status} {service_name}")
                results[service_name] = success
                all_passed &= success
            except Exception as e:
                logger.error(f"Error checking {service_name}: {str(e)}")
                results[service_name] = False
                all_passed = False

        self.results['connectivity'] = results
        return all_passed

    def _check_mongodb(self):
        """Test MongoDB connection"""
        try:
            uri = os.environ.get('MONGO_URI')
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"MongoDB connection failed: {str(e)}")
            return False

    def _check_redis(self):
        """Test Redis connection"""
        try:
            redis_url = os.environ.get('REDIS_URL')
            client = redis.from_url(redis_url)
            client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis connection failed: {str(e)}")
            return False

    def _check_web_port(self):
        """Check if web application port is available"""
        port = int(os.environ.get('PORT', 5001))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(('0.0.0.0', port))
            result = True
        except socket.error:
            logger.error(f"Port {port} is already in use")
            result = False
        finally:
            sock.close()
        return result

    def test_flask_application(self):
        """Test Flask application configuration"""
        logger.info("\n=== Flask Application ===")
        try:
            app = Flask(__name__)
            app.config['TESTING'] = True
            app.config.from_object('config.Config')
            
            # Test basic configuration
            required_config = ['SECRET_KEY', 'MONGO_URI', 'REDIS_URL']
            missing_config = [key for key in required_config if not app.config.get(key)]
            
            if missing_config:
                logger.error(f"Missing Flask configurations: {', '.join(missing_config)}")
                return False
            
            logger.info("✓ Flask configuration loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Flask application test failed: {str(e)}")
            return False

    def print_diagnostics(self):
        """Print helpful diagnostic information"""
        logger.info("\n=== Diagnostic Information ===")
        
        try:
            # System Information
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            diagnostics = {
                'system': {
                    'cpu_count': psutil.cpu_count(),
                    'memory_total_gb': memory.total / (1024**3),
                    'memory_available_gb': memory.available / (1024**3),
                    'disk_total_gb': disk.total / (1024**3),
                    'disk_free_gb': disk.free / (1024**3)
                },
                'python': {
                    'version': sys.version,
                    'path': sys.path,
                    'executable': sys.executable
                },
                'environment': {
                    var: value for var, value in os.environ.items()
                    if not any(sensitive in var.lower() for sensitive in ['key', 'password', 'secret', 'token'])
                }
            }
            
            logger.info("\nSystem Information:")
            logger.info(f"CPU Cores: {diagnostics['system']['cpu_count']}")
            logger.info(f"Memory: {diagnostics['system']['memory_available_gb']:.2f}GB free of {diagnostics['system']['memory_total_gb']:.2f}GB")
            logger.info(f"Disk: {diagnostics['system']['disk_free_gb']:.2f}GB free of {diagnostics['system']['disk_total_gb']:.2f}GB")
            
            self.results['diagnostics'] = diagnostics
            
        except Exception as e:
            logger.error(f"Error gathering diagnostic information: {str(e)}")

    def verify_all(self):
        """Run all verification checks"""
        logger.info("Starting Comprehensive System Verification\n" + "="*50)

        checks = [
            (self.check_python_environment, "Python Environment"),
            (self.load_environment_variables, "Environment Variables"),
            (self.check_dependencies, "Dependencies"),
            (self.check_directories, "Directories"),
            (self.check_network_connectivity, "Network Connectivity"),
            (self.check_system_resources, "System Resources"),
            (self.test_flask_application, "Flask Application")
        ]

        results = {}
        all_passed = True

        for check_func, check_name in checks:
            try:
                success = check_func()
                results[check_name] = success
                all_passed &= success
            except Exception as e:
                logger.error(f"Error during {check_name} check: {str(e)}")
                results[check_name] = False
                all_passed = False

        logger.info("\n=== Verification Summary ===")
        for check_name, success in results.items():
            status = "✓" if success else "✗"
            logger.info(f"{status} {check_name}")

        self.results['verification_results'] = results
        self.results['overall'] = all_passed
        
        # Add timestamp to results
        self.results['timestamp'] = datetime.now().isoformat()
        
        return all_passed

    def save_results(self):
        """Save verification results to file"""
        try:
            # Create results directory if it doesn't exist
            if not os.path.exists('logs'):
                os.makedirs('logs')
            
            # Save results with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'logs/verification_results_{timestamp}.json'
            
            with open(filename, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
            
            logger.info(f"\nResults saved to: {filename}")
            return True
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")
            return False

def main():
    try:
        # Create verifier instance
        verifier = SystemVerifier()
        
        # Run verification
        success = verifier.verify_all()
        
        # Save results
        verifier.save_results()
        
        if not success:
            logger.error("\nSystem verification failed")
            logger.info("Running diagnostics for additional information...")
            verifier.print_diagnostics()
            sys.exit(1)
        else:
            logger.info("\nSystem verification completed successfully")
            
    except Exception as e:
        logger.error(f"\nCritical error during verification: {str(e)}")
        sys.exit(1)
    finally:
        # Log completion
        logger.info("\nVerification process completed")

if __name__ == "__main__":
    main()