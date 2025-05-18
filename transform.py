import polars as pl
import os
from dotenv import load_dotenv
from utils import *

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")


#df = pl.read_parquet("/home/pedro/Pipeline ETL movies/filmes_tmdb_completos.parquet")
# print(df.head())
#print(df.schema)

# Verificar linhas duplicadas
# dup = df.filter(df.is_duplicated())
# print(dup)

lf = pl.read_parquet("/home/pedro/Pipeline ETL movies/filmes_tmdb_completos.parquet")
#lf_tratado = tratar_erros_dados_lazy()
verificação = verificar_qualidades_dados_lazy(lf)

# Verificação antes de usar a chave
if TMDB_API_KEY is None:
    raise EnvironmentError("A variável de ambiente TMDB_API_KEY não está definida.")


# genero =  get_genre_mapping(TMDB_API_KEY)

# df = df.with_columns(
#    pl.col("genre_ids").map_elements(
#        lambda ids: [genero.get(id, "Desconhecido") for id in ids]
#    ).alias("gêneros")
# )
# print(df.select(["title", "genre_ids", "gêneros"]).head())
