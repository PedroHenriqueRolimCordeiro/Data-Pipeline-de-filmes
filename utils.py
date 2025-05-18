import polars as pl
import tmdbsimple as tmdb
import datetime


# Função para obter créditos (diretores) de um filme
def obter_diretor(movie_id):
    try:
        movie = tmdb.Movies(movie_id)
        credits = movie.credits(language="pt-BR")

        # Filtra a lista de equipe para encontrar diretores
        diretores = [
            crew["name"] for crew in credits["crew"] if crew["job"] == "Director"
        ]
        return diretores if diretores else ["Não Disponível"]
    except Exception as e:
        print(f"Erro ao obter diretor para o filme {movie_id}: {e}")
        return ["Não disponível"]


# Função para obter detalhes adicionais de um filme
def obter_detalhes_filmes(movie_id):
    try:
        movie = tmdb.Movies(movie_id)
        details = movie.info(language="pt-BR")

        # Extrai informações de companhias de produção
        production_companies = [
            company["name"] for company in details.get("production_companies", [])
        ]

        return {
            "budget": details.get("budget", 0),
            "revenue": details.get("revenue", 0),
            "runtime": details.get("runtime", 0),
            "status": details.get("status", "Desconhecido"),
            "production_companies": production_companies,
        }
    except Exception as e:
        print(f"Erro ao obter detalhes para o filme {movie_id}: {e}")
        return {
            "budget": 0,
            "revenue": 0,
            "runtime": 0,
            "status": "Erro",
            "production_companies": [],
        }


# Requisição para obter o mapeamento de IDS de gêneros para nomes
def mapeamento_genero(api_key: str):
    tmdb.API_KEY = api_key
    genre = tmdb.Genres()
    response = genre.movie_list(language="pt-BR")

    id_nome = {g["id"]: g["name"] for g in response["genres"]}

    return id_nome
