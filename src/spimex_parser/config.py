import datetime
import os

from dotenv import load_dotenv


load_dotenv()


DB_NAME = os.environ['DB_NAME']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_CONNECTION_URL = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
ASYNC_DB_CONNECTION_URL = f'postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

REDIS_HOST = os.environ['REDIS_HOST']
REDIS_URL = f'redis://{REDIS_HOST}'
