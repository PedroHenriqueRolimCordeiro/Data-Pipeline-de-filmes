import time
import os
from dotenv import load_dotenv
import tmdbsimple as tmdb
import polars as pl
from utils import obter_detalhes_filmes, obter_diretor # Importa funções específicas

def get_popular_movies_data(num_pages: int = 1, language: str = "pt-BR") -> pl.DataFrame:
    """
    Busca dados de filmes populares da API do TMDB, incluindo detalhes estendidos e informações do diretor.

    Args:
        num_pages (int): O número de páginas a serem recuperadas. Cada página contém aproximadamente 20 filmes.
                         O padrão é 1.
        language (str): O idioma para os dados do filme (por exemplo, "en-US", "pt-BR").
                        O padrão é "pt-BR".

    Returns:
        pl.DataFrame: Um Polars DataFrame contendo dados abrangentes de filmes.
    """
    load_dotenv()
    TMDB_API_KEY = os.getenv("TMDB_API_KEY")

    if not TMDB_API_KEY:
        raise ValueError("TMDB_API_KEY não encontrada nas variáveis de ambiente. Por favor, defina-a em um arquivo .env.")

    tmdb.API_KEY = TMDB_API_KEY
    movies_api = tmdb.Movies()

    all_movies_basic_info = []
    print(f"Iniciando a busca de filmes populares do TMDB para {num_pages} página(s) em {language}...")

    # Loop para iterar pelas páginas
    for page in range(1, num_pages + 1):
        try:
            response = movies_api.popular(page=page, language=language)
            results = response.get("results", [])
            all_movies_basic_info.extend(results)
            print(f"Página {page} coletada com sucesso.")
            time.sleep(0.25)  # Pausa para evitar limite de taxa da API
        except Exception as e:
            print(f"Erro na página {page}: {e}")
            break

    if not all_movies_basic_info:
        print("Nenhum dado de filme coletado. Saindo.")
        return pl.DataFrame() # Retorna um DataFrame vazio
        
    complete_movies_data = []
    for movie in all_movies_basic_info:
        movie_id = movie["id"]
        print(f"Obtendo detalhes extras para o filme: {movie.get('title', 'Sem título')} (ID: {movie_id})")

        try:
            detalhes = obter_detalhes_filmes(movie_id)
            diretores = obter_diretor(movie_id)

            full_movie_info = {
                "id": movie_id,
                "genre_ids": movie.get("genre_ids", []),
                "title": movie.get("title", "Sem título"),
                "release_date": movie.get("release_date", ""),
                "popularity": movie.get("popularity", 0),
                "vote_average": movie.get("vote_average", 0),
                "vote_count": movie.get("vote_count", 0),
                "overview": movie.get("overview", ""),
                "budget": detalhes.get("budget", 0),
                "revenue": detalhes.get("revenue", 0),
                "runtime": detalhes.get("runtime", 0),
                "original_title": movie.get("original_title", ""),
                "original_language": movie.get("original_language", ""),
                "production_companies": detalhes.get("production_companies", []),
                "status": detalhes.get("status", ""),
                "director": diretores,
                "poster_path": movie.get("poster_path", ""),
                "backdrop_path": movie.get("backdrop_path", ""),
            }
            complete_movies_data.append(full_movie_info)
            time.sleep(0.5)  # Adiciona pausa para evitar limite de taxa da API
        except Exception as e:
            print(f"Erro ao buscar detalhes para o filme ID {movie_id}: {e}")
            continue # Continua para o próximo filme mesmo que um falhe

    df_final = pl.DataFrame(complete_movies_data)
    return df_final

# Removido a execução direta e a escrita hardcoded do parquet
# df_final = get_popular_movies_data(num_pages=1, language="pt-BR")
# df_final.write_parquet("filmes_tmdb_completos_.parquet")
