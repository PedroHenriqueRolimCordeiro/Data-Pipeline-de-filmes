import tmdbsimple as tmdb
from datetime import datetime
import polars as pl
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv() # Carrega as variáveis de ambiente do arquivo .env

# Função para verificar a qualidade dos dados
def verificar_qualidades_dados_lazy(df):
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print("ANALISE DE QUALIDADE DE DADOS")

    schema = lf.collect(streaming=True).schema #type: ignore
    colunas = list(schema.keys())

    resultados = {}
    # Verifica quantos valores nulos existem por coluna
    null_counts = lf.select([
        pl.col(col).null_count().alias(f"{col}_null")
        for col in lf.collect_schema().names()
    ]).collect()
    print(null_counts)
    print("\n")

    # Conta strings vazias nas colunas de texto
    string_cols = [col for col, 
                   dtype in schema.items() 
                   if dtype == pl.Utf8]
    if string_cols:
        print("String Vazias por Coluna:")
        string_vazia = lf.select([
            pl.col(col).filter(pl.col(col) == "").count().alias(f"{col}_vazio")
            for col in string_cols
        ]).collect()
        print(string_vazia)
        print("\n")
    
    # Verifica se há muitos valores zero em colunas numéricas
    num_cols = [col for col,
                dtype in schema.items()
                if dtype in [pl.Int64, pl.Float64]]
    if num_cols:
        print("VALORES ZERO POR COLUNA NUMERICA")
        zeros = lf.select([
            pl.col(col).filter(pl.col(col) == 0).count().alias(f"{col}_zero")
            for col in num_cols
        ]).collect()
        print(zeros)

    # Conta listas vazias (sem elementos)
    list_cols = [col for col,
                 dtype in schema.items()
                 if str(dtype).startswith('List')]
    if list_cols:
        print("LISTAS VAZIAS POR COLUNA:")
        list_vazias = lf.select([
            pl.col(col).filter(pl.col(col).list.len() == 0).count().alias(f"{col}_list_vazias")
            for col in list_cols
        ]).collect()
        print(list_vazias)
        print("\n")

    
    # Mostra estatísticas básicas (média, min, max, etc.)
    if num_cols:
        print("ESTATÍSTICAS RESUMIDAS PARA COLUNAS NUMÉRICAS")
        stats = lf.select(num_cols).collect().describe()
        print(stats)
        print("\n")

    # Tenta identificar datas mal formatadas
    if 'release_date' in lf.collect_schema().names():
        print("ANÁLISE DE DATAS DE LANÇAMENTOS")

        #Contar datas em formato inválido
        data_invalida = lf.filter(
            ~pl.col('release_date').str.contains(r'^\d{4}-\d{2}-\d{2}$') &
            (pl.col('release_date') != "")
        ).select(pl.count()).collect().item()
        print(f"Datas em formato inválido: {data_invalida}")

    #Adicionar verificação para datas futuras


    # Verifica se há valores negativos onde não deveriam existir
    neg_cols = ['vote_count', 'budget', 'revenue', 'runtime']
    neg_cols_presentes = [col for col in neg_cols
                          if col in lf.collect_schema().names()]
    if neg_cols_presentes:
        print("VALORES NEGATIVOS EM COLUNAS QUE DEVERIAM SER POSITIVAS:")
        negativos = lf.select([
            pl.col(col).filter(pl.col(col) < 0).count().alias(f"{col}_negativo")
            for col in neg_cols_presentes
        ]).collect()
        print(negativos)
        print("\n")


    # Garante que as notas estejam dentro da escala 0-10
    if 'vote_average' in lf.collect_schema().names():
        print("VALORES DE AVALIAÇÃO FORA DA ESCALA (0-10):")
        fora_escala = lf.filter(
            (pl.col('vote_average') < 0) | (pl.col('vote_average') > 10)
        ).select(pl.count()).collect().item()
        print(f"Avaliações fora da escala 0-10: {fora_escala}")
        print("\n")

    print("FIM DA VERIFICACAO")    
    return lf

import polars as pl

def tratar_titulos_dos_filmes(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Realiza o tratamento da coluna 'title' em um Dataframe/LazyFrame.

    Args:
        df (pl.DataFrame or pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com a coluna 'title' tratada.
    """
    # Converte para LazyFrame, se necessário, para garantir o fluxo otimizado
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df
    # 1. Limpeza e Padronização da coluna 'title'
    # Garante que 'title' é uma string, remove espaços em branco de início/fim,
    # padroniza strings vazias ("") para nulos.
    lazy_frame = lf.with_columns([
        pl.col('title')
        .cast(pl.Utf8, strict=False)  # Converte para string, falhas resultam em nulos
        .str.strip_chars()            # Remove espaços em branco do início e fim
        .replace("", None)            # Converte strings vazias para nulos
        .alias("title")
    ])

    # 2. Remoção de valores nulos
    # Linhas onde 'title' é nulo são descartadas.
    lazy_frame = lf.filter(
        pl.col("title").is_not_null()
    )

    # 3. Conversão para caixa padrão (Título)
    # A primeira letra de cada palavra em 'title' é capitalizada.
    lazy_frame = lf.with_columns([
        pl.col("title").str.to_titlecase().alias("title")
    ])

    return lf

# Requisição para obter o mapeamento de IDS de gêneros para nomes
def mapeamento_genero(api_key: str):
    tmdb.API_KEY = api_key
    genre = tmdb.Genres()
    response = genre.movie_list()

    id_nome = {g["id"]: g["name"] for g in response["genres"]}

    return id_nome

def tratar_generos(df: pl.DataFrame | pl.LazyFrame, genero_mapa: dict) -> pl.LazyFrame:
    """
    Converte a coluna 'genre_id' (lista de IDs) para 'genero' (lista de nomes em português).

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame/LazyFrame de entrada com a coluna 'genre_id'.
        genero_mapa (dict): O dicionário de mapeamento {ID: Nome do Gênero}.

    Returns:
        pl.LazyFrame: O LazyFrame com a nova coluna 'genero'.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df
    
    #Tratar valores nulos ou vazios antes do mapeamento 
    lazy_frame = lf.with_columns([
        pl.col("genre_ids")
        .cast(pl.List(pl.Int64), strict=False) #Garante que é uma lista de inteiros
        .fill_null(pl.lit([]).cast(pl.List(pl.Int64))) #Preenche nulos com listas vazias para evitar erros
        .alias("genre_ids_cleaned") # Cria uma nova coluna só pra garantir que os dados estejam no formato certo
    ])

    # Mapeia cada id da lista para seu respectivo nome
    lazy_frame = lf.with_columns([
        pl.col("genre_ids_cleaned")
        .list.eval(pl.element().replace(genero_mapa, default=pl.element())) # Troca os IDs dos gêneros pelos nomes
        .alias("genero")
    ])


    # Remove colunas que não precisamos mais depois da conversão
    lazy_frame = lf.drop("genre_ids")
    lazy_frame = lf.drop("genre_ids_cleaned")
    return lf


def tratar_datas_de_lancamento(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Trata a coluna 'release_date', convertendo-a para o tipo Date do Polars.
    Lida com possíveis valores nulos ou formatos inválidos.

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com a coluna 'release_date' tratada.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print(" - Iniciando tratamento da coluna 'release_date'.")

    lazy_frame = lf.with_columns([
        # Converte a coluna 'release_date' para o tipo Date
        # Se a string não pode ser convertida, o valor sera nulo
        pl.col("release_date")
        .str.to_date(format="%Y-%m-%d", strict=False)
        #.dt.strftime("%d-%m-%Y")
        .alias("release_date")
    ])

    #lazy_frame = lf.filter(pl.col("release_date").is_not_null())
    #print(" - Removidas linhas com 'release_date' inválida ou nula.")

    print(" - Coluna 'release_date' tratada e convertida para o tipo Date.")
    return lf


def tratar_popularidade_do_filme(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Trata a coluna 'popularity', garantindo que é um Float64,
    lidando com nulos e opcionalmente arredondando.

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com a coluna 'popularity' tratada.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print(" - Iniciando tratamento da coluna 'popularity'.")

    lazy_frame = lf.with_columns([
        pl.col("popularity")
        .cast(pl.Float64, strict=False) # Garante que é Float64; se não for, vira nulo
        .fill_null(0.0)                 # Preenche valores nulos com 0.0 (ou outro valor padrão)
        .round(2)                       # Arredonda para 2 casas decimais
        .alias("popularity")
    ])

    print(" - Coluna 'popularity' tratada.")
    return lf

def tratar_avaliacoes_do_filme(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Trata as colunas 'vote_average' (Float64) e 'vote_count' (Int64),
    lidando com nulos e garantindo a tipagem correta.
    arredonda 'vote_average'.

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com as colunas de avaliação tratadas.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print(" - Iniciando tratamento das colunas 'vote_average' e 'vote_count'.")

    lazy_frame = lf.with_columns([
        # --- Tratamento de 'vote_average' ---
        pl.col("vote_average")
        .cast(pl.Float64, strict=False) # Garante Float64; falhas viram nulas
        .fill_null(0.0)                 # Preenche nulos com 0.0 (filmes sem voto/média)
        .clip(0.0, 10.0)                # Garante que a média fique entre 0 e 10
        .alias("vote_average"),
    
        # --- Tratamento de 'vote_count' ---
        pl.col("vote_count")
        .cast(pl.Int64, strict=False)
        .fill_null(0)
        .clip(lower_bound=0)
        .alias("vote_count")
    ])

    print(" - Colunas 'vote_average' e 'vote_count' tratadas.")
    return lf

def tratar_overview(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Trata a coluna 'overview', garantindo que é uma String,
    lidando com nulos/vazios e removendo espaços extras.

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com a coluna 'overview' tratada.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print(" - Iniciando tratamento da coluna 'overview'.")

    lazy_frame = lf.with_columns([
        pl.col("overview")
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .replace("",None)
        .fill_null("Sem sinopse")
        .alias("overview")
    ])

    print(" Coluna 'overview' tratada")
    return lf

def tratar_financas_do_filme(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Trata as colunas 'budget' e 'revenue', garantindo a tipagem Int64,
    lidando com nulos e valores zero (considerando-os como ausentes).

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com as colunas financeiras tratadas.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print(" - Iniciando tratamento das colunas 'budget' e 'revenue'.")

    lazy_frame = lf.with_columns([
        pl.col("budget")
        .cast(pl.Int64, strict=False)
        .replace(0, None)
        .fill_null("Valor ausente")
        .clip(lower_bound=0)
        .alias("budget"),

        # --- Tratamento de 'revenue' ---
        pl.col("revenue")
        .cast(pl.Int64, strict=False)
        .replace(0, None)
        .fill_null("Valor ausente")
        .clip(lower_bound=0)
        .alias("revenue")
    ])

    print(" - Colunas 'budget' e 'revenue' tratadas.")
    return lf

def tratar_duracao_em_minutos(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Trata a coluna 'runtime' (duração do filme), garantindo a tipagem Int64,
    lidando com nulos, valores zero e garantindo que não seja negativo.

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com a coluna 'runtime' tratada.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print(" - Iniciando tratamento da coluna 'runtime'.")

    lazy_frame = lf.with_columns([
        pl.col("runtime")
        .cast(pl.Int64, strict=False)
        .replace(0, None)
        .fill_null("Valor Ausente")
        .clip(lower_bound=0)
        .alias("runtime")
    ])
    print(" Coluna 'runtime' tratada")
    return lf

def tratar_linguagem_e_titulo_originais(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Trata as colunas 'original_title' e 'original_language',
    lidando com nulos/vazios e removendo espaços extras.

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com as colunas tratadas.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print(" - Iniciando tratamento das colunas 'original_title' e 'original_language'.")

    lazy_frame = lf.with_columns([
        # --- Tratamento de 'original_title ---
        pl.col("original_title")
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .replace("",None)
        .fill_null("Titulo Original Ausente")
        .alias("original_title"),

        # --- Tratamento de 'original_language ---
        pl.col("original_language")
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .replace("",None)
        .fill_null("Indeterminado")
        .alias("original_language")
    ])

    print(" - Colunas 'original_title' e 'original_language' tratadas.")
    return lf

def tratar_empresas_produtoras(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Trata a coluna 'production_companies' (List(String)),
    garantindo que seja uma lista de strings, lidando com nulos
    e limpando os nomes das empresas.

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com a coluna 'production_companies' tratada.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print(" - Iniciando tratamento da coluna 'production_companies'.")
    
    lazy_frame = lf.with_columns([
        pl.col("production_companies")
        .cast(pl.List(pl.String), strict=False)
        .fill_null([])
        .list.eval(
            pl.element().str.strip_chars().str.replace("").fill_null("Empresa Desconhecida") #type: ignore
        )
        .alias("production_companies")                                                                                                                               
    ])
    print(" - Coluna 'production_companies' tratada.")
    return lf

def tratar_status_do_filme(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Trata a coluna 'status' (String), garantindo a tipagem,
    lidando com nulos/vazios e padronizando o texto.

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com a coluna 'status' tratada.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print(" - Iniciando tratamento da coluna 'status'.")

    lazy_frame = lf.with_columns([
        pl.col("status")
        .cast(pl.String, strict=False)    # Garante que é string; falhas viram nulo
        .str.strip_chars()                # Remove espaços em branco do início e fim
        .replace("", None)                # Converte strings vazias ("") para nulos
        .fill_null("Desconhecido")        # Preenche nulos (e agora vazios) com um valor padrão
        .str.to_titlecase()               # Opcional: Converte para Title Case (Ex: "released" -> "Released")
        .alias("status")
    ])

    print(" - Coluna 'status' tratada.")
    return lf

import polars as pl

def tratar_diretores(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """
    Trata a coluna 'director' (List(String)), garantindo que seja uma lista de strings,
    lidando com nulos e limpando os nomes dos diretores.

    Args:
        df (pl.DataFrame | pl.LazyFrame): O DataFrame ou LazyFrame de entrada.

    Returns:
        pl.LazyFrame: O LazyFrame com a coluna 'director' tratada.
    """
    if not isinstance(df, pl.LazyFrame):
        lazy_frame = data_frame.lazy()
    else:
        lazy_frame = df

    print(" - Iniciando tratamento da coluna 'director'.")

    lazy_frame = lf.with_columns([
        pl.col("director")
        .cast(pl.List(pl.String), strict=False)  # Garante que é List(String); falhas viram nulo
        .fill_null([])                          # Preenche nulos (coluna inteira) com lista vazia
        .list.eval(                               # Itera sobre cada elemento (nome do diretor) na lista
            pl.element().str.strip_chars()      # Remove espaços em branco do início/fim de cada nome
            .replace("", None)                  # Converte strings vazias para nulos
            .fill_null("Diretor Desconhecido")  # Preenche nulos (nomes vazios) com um valor padrão
        )
        .alias("director")
    ])

    print(" - Coluna 'director' tratada.")
    return lf
