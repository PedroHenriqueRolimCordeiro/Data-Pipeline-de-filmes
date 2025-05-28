import time
import os
from dotenv import load_dotenv
import tmdbsimple as tmdb
import polars as pl
from utils import obter_detalhes_completos_filme_unificado
import requests # Importar a biblioteca requests para suas exceções

def get_popular_movies_data(num_pages: int, language: str = "pt-BR") -> pl.DataFrame:
    """
    Busca dados de filmes populares da API do TMDB, incluindo detalhes estendidos e informações do diretor,
    de forma sequencial otimizada.
    """
    load_dotenv()
    TMDB_API_KEY = os.getenv("TMDB_API_KEY")

    if not TMDB_API_KEY:
        raise ValueError("TMDB_API_KEY não encontrada nas variáveis de ambiente. Por favor, defina-a em um arquivo .env.")

    tmdb.API_KEY = TMDB_API_KEY
    movies_api = tmdb.Movies()

    all_movies_basic_info = []
    print(f"Iniciando a busca de filmes populares do TMDB para {num_pages} página(s) em {language}...")

    for page in range(1, num_pages + 1):
        try:
            response = movies_api.popular(page=page, language=language)
            results = response.get("results", [])
            all_movies_basic_info.extend(results)
            print(f"Página {page} coletada com sucesso.")
            time.sleep(0.25)
        except Exception as e:
            print(f"Erro na página {page}: {e}")
            break

    if not all_movies_basic_info:
        print("Nenhum dado de filme coletado (lista básica). Saindo.")
        return pl.DataFrame()
        
    complete_movies_data = []
    for movie_basic in all_movies_basic_info:
        movie_id = movie_basic["id"]
        title_for_log = movie_basic.get('title', 'Sem título')
        print(f"Obtendo detalhes extras para o filme: {title_for_log} (ID: {movie_id})")

        retries = 0
        max_retries = 3
        base_wait_time = 1 
        detalhes_completos = None 

        while retries < max_retries:
            try:
                detalhes_completos = obter_detalhes_completos_filme_unificado(movie_id)
                break 

            except requests.exceptions.HTTPError as e: 
                status_code = e.response.status_code 
                
                if status_code == 429: 
                    retry_after_header = e.response.headers.get("Retry-After")
                    wait_time = int(retry_after_header) if retry_after_header else (base_wait_time * (2 ** retries))
                    print(f"Rate limit para filme {movie_id}. Tentativa {retries + 1}/{max_retries}. Esperando {wait_time}s...")
                    time.sleep(wait_time)
                    retries += 1
                else: 
                    print(f"Erro HTTP {status_code} ao buscar detalhes para o filme ID {movie_id} na tentativa {retries + 1}: {e}")
                    detalhes_completos = { 
                        "budget": 0, "revenue": 0, "runtime": 0, "status": f"Erro HTTP {status_code}",
                        "production_companies": [], "director": [f"Erro HTTP {status_code}"]
                    }
                    break 
            except requests.exceptions.RequestException as e: 
                print(f"Erro de requisição (não HTTP) ao buscar detalhes para o filme ID {movie_id} na tentativa {retries + 1}: {e}")
                if retries < max_retries -1 : 
                    time.sleep(base_wait_time * (2 ** retries))
                    retries += 1
                else:
                    detalhes_completos = {
                        "budget": 0, "revenue": 0, "runtime": 0, "status": "Erro de Requisição",
                        "production_companies": [], "director": ["Erro de Requisição"]
                    }
                    break 
            except Exception as e: 
                print(f"Erro inesperado ao buscar detalhes para o filme ID {movie_id} na tentativa {retries + 1}: {e}")
                detalhes_completos = { 
                    "budget": 0, "revenue": 0, "runtime": 0, "status": "Erro Inesperado",
                    "production_companies": [], "director": ["Erro Inesperado"]
                }
                break 

        if retries == max_retries and detalhes_completos is None: 
            print(f"Falha ao buscar detalhes para o filme ID {movie_id} após {max_retries} tentativas de rate limit.")
            detalhes_completos = { 
                "budget": 0, "revenue": 0, "runtime": 0, "status": "Falha Max Tentativas RL",
                "production_companies": [], "director": ["Falha Max Tentativas RL"]
            }
        
        if detalhes_completos is None: 
             detalhes_completos = {
                "budget": 0, "revenue": 0, "runtime": 0, "status": "Erro Desconhecido Detalhes",
                "production_companies": [], "director": ["Erro Desconhecido Detalhes"]
            }

        full_movie_info = {
            "id": movie_id,
            "genre_ids": movie_basic.get("genre_ids", []),
            "title": title_for_log, 
            "release_date": movie_basic.get("release_date", ""),
            "popularity": movie_basic.get("popularity", 0),
            "vote_average": movie_basic.get("vote_average", 0),
            "vote_count": movie_basic.get("vote_count", 0),
            "overview": movie_basic.get("overview", ""),
            "budget": detalhes_completos.get("budget", 0),
            "revenue": detalhes_completos.get("revenue", 0),
            "runtime": detalhes_completos.get("runtime", 0),
            "original_title": movie_basic.get("original_title", ""),
            "original_language": movie_basic.get("original_language", ""),
            "production_companies": detalhes_completos.get("production_companies", []),
            "status": detalhes_completos.get("status", "Desconhecido"), 
            "director": detalhes_completos.get("director", ["Não Disponível"]), 
            "poster_path": movie_basic.get("poster_path", ""),
            "backdrop_path": movie_basic.get("backdrop_path", ""),
        }
        complete_movies_data.append(full_movie_info)
        time.sleep(0.1)

    df_final = pl.DataFrame(complete_movies_data)
    return df_final

