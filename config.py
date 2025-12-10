import os
from datetime import timedelta

class Config:
    # Use environment variable for secret key in production
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    
    # For production, use /tmp or a persistent volume
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', 'static/uploads')
    
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB max file size
    ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'txt'}
    
    # Cleanup old files after 1 hour
    FILE_LIFETIME = timedelta(hours=1)
    
    # Data cleaning defaults
    DEFAULT_MISSING_THRESHOLD = 0.5
    DEFAULT_DUPLICATE_ACTION = 'drop'
    DEFAULT_DATE_FORMATS = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y%m%d']
    
    @staticmethod
    def init_app(app):
        # Ensure upload folder exists
        if not os.path.exists(app.config['UPLOAD_FOLDER']):
            os.makedirs(app.config['UPLOAD_FOLDER'])