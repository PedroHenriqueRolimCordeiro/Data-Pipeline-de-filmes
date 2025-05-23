import polars as pl
import os
from dotenv import load_dotenv

from data_quality import *
from utils import *
from data_quality import *


load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")

df = pl.read_parquet("/home/pedro/Pipeline ETL movies/filmes_tmdb_completos.parquet")
#print(df.head())
print(df.head)
print(df.schema)



