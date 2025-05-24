import polars as pl
import os
from dotenv import load_dotenv
from data_quality import * # Mantido por enquanto, mas considere imports explícitos
from utils import mapeamento_genero # Importa explicitamente mapeamento_genero de utils

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY") #type: ignore
if TMDB_API_KEY is None:
    raise ValueError(
        "A variável de ambiente 'TMDB_API_KEY' não está definida. Por favor, configure-a no seu arquivo .env."
    )
TMDB_API_KEY: str

# Removido o carregamento direto, pois agora é tratado pelo main.py
# df = pl.read_parquet("/home/pedro/Pipeline ETL movies/filmes_tmdb_completos.parquet")


# --- Função de Transformação Principal ---
def transform(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Realiza o pipeline de transformação dos dados do TMDB.

    Args:
        lf (pl.LazyFrame): O LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame transformado.
    """
    # Converte para LazyFrame, se necessário (já tratado em main.py)
    # if not isinstance(lf, pl.LazyFrame):
    #     lf = lf.lazy()

    print("\n--- Iniciando Transformações ---")

    # 1. Tratamento da coluna 'id'
    # Garante que 'id' é um número inteiro e remove linhas onde não é possível converter
    lf = lf.filter(pl.col("id").cast(pl.Int64, strict=False).is_not_null())
    print(" - IDs tratados (garantindo que são números inteiros).")

    # 2. Tratamento da coluna 'title'
    lf = tratar_titulos(lf)
    print(" - Títulos limpos e padronizados.")

    # 3. Tratamento da coluna 'genre_ids' para 'genero'
    # Obtém o mapeamento de gêneros
    genero_mapa = mapeamento_genero(TMDB_API_KEY)
    print(f" - Mapeamento de gêneros obtido (primeiros 5: {list(genero_mapa.items())[:5]}).")

    # Aplica o tratamento de gênero, passando o LazyFrame já com títulos tratados
    # Garante que esta chamada usa o tratar_generos correto de data_quality.py
    lf = tratar_generos(lf, genero_mapa)

    # 4. Tratamento da coluna 'release_date'
    lf = tratar_datas(lf)

    # 5. Tratamento de dados de 'popularity', 'vote_average' e 'vote_count'
    lf = tratar_popularidade(lf)
    lf = tratar_avaliacoes(lf)

    # 6. Aplica as funções tratar_duracao_em_minutos e tratar_status_do_filme
    lf = tratar_duracao_em_minutos(lf)
    lf = tratar_status_do_filme(lf)

    # 7. Aplica as funções tratar_overview
    lf = tratar_overview(lf)

    # 8. Tratamento de dados financeiros
    lf = tratar_financas(lf)

    # 9. Tratamento de linguagem e títulos originais
    lf = tratar_linguagem_e_titulo_originais(lf)

    # 10. Tratar diretores
    lf = tratar_diretores(lf)

    # 11. Tratamento de 'production_companies'
    lf = tratar_empresas_produtoras(lf)

    # 12. Reorganizar colunas
    nova_ordem_colunas = [
        'id',
        'title',
        'original_title',
        'director',
        'genero',
        'release_date',
        'popularity',
        'vote_average',
        'vote_count',
        'overview',
        'production_companies',
        'budget',
        'revenue',
        'runtime',
        'original_language',
        'status',
        'poster_path',
        'backdrop_path'
        ]
    lf_reogarnizado = lf.select(nova_ordem_colunas)

    # 13. Renomear colunas
    # 12. Renomear colunas
    mapa_renomear = {
    'id': 'Id',
    'title': 'Titulo',
    'original_title': 'Titulo_Original',
    'release_date': 'Data_Lancamento',
    'popularity': 'Popularidade',
    'vote_average': 'Media_Votos',
    'vote_count': 'Numero_Votos',
    'overview': 'Sinopse',
    'budget': 'Orcamento',
    'revenue': 'Receita',
    'runtime': 'Duração',
    'original_language': 'Idioma_Original',
    'production_companies': 'Produtoras', # ALTERADO: Removido "(as)"
    'status': 'Status',
    'director': 'Diretores',           # ALTERADO: Removido "(es)"
    'poster_path': 'poster_path',
    'backdrop_path': 'backdrop_path',
    'genero': 'Generos'
    }
    lf_final = lf_reogarnizado.rename(mapa_renomear)
    print("--- Transformações Concluídas ---")
    return lf_final