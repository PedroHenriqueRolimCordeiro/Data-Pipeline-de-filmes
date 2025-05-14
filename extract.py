import pandas as pd
import requests 
import os
from dotenv import load_dotenv

load_dotenv()

TMDB_API_KEY = os.getenv('TMDB_API_KEY')
TMBD_READ_TOKEN = os.getenv('TMDB_READ_TOKEN')

