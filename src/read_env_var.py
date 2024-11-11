import os
from dotenv import load_dotenv

load_dotenv("../.env")
API_KEY = os.getenv('API_KEY')
ROOT_PATH = os.getenv('ROOT_PATH')
OHLCV_HOUR_PATH = os.getenv('OHLCV_HOUR_PATH')