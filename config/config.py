import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
TWEETS_DIR_ID = os.getenv('TWEETS_DIR_ID')
UPLOAD_DIR_ID = os.getenv('UPLOAD_DIR_ID')