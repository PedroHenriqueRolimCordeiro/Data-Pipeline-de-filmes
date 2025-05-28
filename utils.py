import polars as pl
import tmdbsimple as tmdb
import datetime


def obter_detalhes_completos_filme_unificado(movie_id):
    """
    Busca detalhes do filme e informações de diretor em uma única chamada API.
    Certifique-se de que tmdb.API_KEY está configurado antes de chamar esta função.
    """
    try:
        movie_obj = tmdb.Movies(movie_id)

        details_with_credits = movie_obj.info(language="pt-BR", append_to_response="credits")

        production_companies = [
            company["name"] for company in details_with_credits.get("production_companies", [])
        ]

        diretores = [
            crew["name"] for crew in details_with_credits.get("credits", {}).get("crew", []) if crew["job"] == "Director"
        ]
        diretores_final = diretores if diretores else ["Não Disponível"]

        return {
            "budget": details_with_credits.get("budget", 0),
            "revenue": details_with_credits.get("revenue", 0),
            "runtime": details_with_credits.get("runtime", 0),
            "status": details_with_credits.get("status", "Desconhecido"),
            "production_companies": production_companies,
            "director": diretores_final,
        }
    except Exception as e:
        print(f"Erro ao obter detalhes completos unificados para o filme {movie_id}: {e}")
        return {
            "budget": 0,
            "revenue": 0,
            "runtime": 0,
            "status": "Erro",
            "production_companies": [],
            "director": ["Não disponível"], # Manter a estrutura
        }

# Requisição para obter o mapeamento de IDS de gêneros para nomes
def mapeamento_genero(api_key: str):
    tmdb.API_KEY = api_key
    genre = tmdb.Genres()
    response = genre.movie_list()

    id_nome = {g["id"]: g["name"] for g in response["genres"]}

    return id_nome

