import polars as pl
import os
from dotenv import load_dotenv
from utils import *
load_dotenv()

TMDB_API_KEY = os.getenv('TMDB_API_KEY')


df = pl.read_parquet("/home/pedro/Pipeline ETL movies/filmes_tmdb_completos.parquet")
#print(df.head())
#print(df.schema)

#Verificar linhas duplicadas
#dup = df.filter(df.is_duplicated())
#print(dup)



#Verificar se há nulos
#null_counts = df.null_count()
#print(null_counts)

genero =  get_genre_mapping(TMDB_API_KEY)

#df = df.with_columns(
#    pl.col("genre_ids").map_elements(
#        lambda ids: [genero.get(id, "Desconhecido") for id in ids]
#    ).alias("gêneros")
#)
#print(df.select(["title", "genre_ids", "gêneros"]).head())