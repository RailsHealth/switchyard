import logging
from app import create_app
from config import Config
from app.extensions import scheduler

# Setup the main logger
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
app_logger = logging.getLogger('app')

app = create_app(Config)

if __name__ == '__main__':
    app_logger.info("Starting the Flask application")
    with app.app_context():
        scheduler.app = app
        if not scheduler.running:
            scheduler.start()
    app.run(host='0.0.0.0', port=Config.PORT, debug=Config.DEBUG)