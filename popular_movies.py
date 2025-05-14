import requests
import time
import os
from dotenv import load_dotenv
import tmdbsimple as tmdb
import polars as pl

load_dotenv()

TMDB_API_KEY = os.getenv('TMDB_API_KEY')
tmdb.API_KEY = TMDB_API_KEY

todos_filmes = [] # lista onde sera armazenadas os filmes
num_paginas = 1 #Numero de páginas que vão ser utilizadas. Cada página possui ≈ 20 filmes 

filmes = tmdb.Movies() #Inicializar o objeto da classe Movies para interagir com a API TMDB


for pagina in range(1,num_paginas + 1): #Loop para percorrer as páginas de filmes

    try:
        resposta = filmes.popular(page=pagina, language='pt-BR')
        resultados = resposta.get('results',[])
        todos_filmes.extend(resultados)

        print(f"Pagina {pagina} coletada com sucesso")
        time.sleep(0.25) #Pausa para evitar erro por excesso de requests

    except Exception as e:
        print(f"Erro na pagina {pagina}: {e}") 
        break

#Converter a lista para um DataFrame Polars
df = pl.DataFrame(todos_filmes)
print(df)
