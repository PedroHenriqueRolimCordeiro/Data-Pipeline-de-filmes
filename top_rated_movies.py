import time
import os
from dotenv import load_dotenv
import tmdbsimple as tmdb
import polars as pl
from utils import *


load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
tmdb.API_KEY = TMDB_API_KEY

todos_filmes = []  # lista onde sera armazenadas os filmes
num_paginas = (
    1  # Numero de páginas que vão ser utilizadas. Cada página possui ≈ 20 filmes
)

filmes = (
    tmdb.Movies()
)  # Inicializar o objeto da classe Movies para interagir com a API TMDB

# Loop para percorrer as páginas
for pagina in range(1, num_paginas + 1):  # Loop para percorrer as páginas de filmes

    try:
        resposta = filmes.top_rated(page=pagina, language="pt-BR")
        resultados = resposta.get("results", [])
        todos_filmes.extend(resultados)

        print(f"Pagina {pagina} coletada com sucesso")
        time.sleep(0.25)  # Pausa para evitar erro por excesso de requests

    except Exception as e:
        print(f"Erro na pagina {pagina}: {e}")
        break

df_basico = pl.DataFrame(todos_filmes)

filmes_completos = []

for filme in todos_filmes:
    movie_id = filme["id"]
    print(f"Obtendo detalhes extras para o filme {filme['title']} (ID: {movie_id})")

    # chamada da função para obter os detalhes
    detalhes = obter_detalhes_filmes(movie_id)

    # chamada da função para obter os diretores
    diretores = obter_diretor(movie_id)

    # Criar um dicionário que guarde todos os dados do filme
    filme_completo = {
        "id": movie_id,
        "genre_ids": filme.get("genre_ids", []),
        "title": filme.get("title", "Sem título"),
        "release_date": filme.get("release_date", ""),
        "popularity": filme.get("popularity", 0),
        "vote_average": filme.get("vote_average", 0),
        "vote_count": filme.get("vote_count", 0),
        "overview": filme.get("overview", ""),
        "budget": detalhes["budget"],
        "revenue": detalhes["revenue"],
        "runtime": detalhes["runtime"],
        "original_title": filme.get("original_title", ""),
        "original_language": filme.get("original_language", ""),
        "production_companies": detalhes["production_companies"],
        "status": detalhes["status"],
        "director": diretores,
        "poster_path": filme.get("poster_path", ""),
        "backdrop_path": filme.get("backdrop_path", ""),
    }

    filmes_completos.append(filme_completo)

    # Adicionar pausa para evitar limite de taxa da API

    time.sleep(0.5)

# DataFrame Final com todos os dados
df_final = pl.DataFrame(filmes_completos)

print(df_final.head)

# df_final.write_parquet("filmes_tmdb_completos_.parquet")
