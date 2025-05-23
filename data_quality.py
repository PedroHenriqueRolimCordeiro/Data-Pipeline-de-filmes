import tmdbsimple as tmdb
from datetime import datetime
import polars as pl
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv() #carregar as variaveis .env

#Verificação da qualidades dos dados
def verificar_qualidades_dados_lazy(df):
    if not isinstance(df, pl.LazyFrame):
        lf = df.lazy()
    else:
        lf = df

    print("ANALISE DE QUALIDADE DE DADOS")

    schema = lf.collect(streaming=True).schema #type: ignore
    colunas = list(schema.keys())

    resultados = {}
    #1 Contagem de valores nulos por coluna
    null_counts = lf.select([
        pl.col(col).null_count().alias(f"{col}_null")
        for col in lf.collect_schema().names()
    ]).collect()
    print(null_counts)
    print("\n")

    #2 Contagem de strings vazias em colunas de texto
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
    
    #3 Contagem de zeros em colunas numéricas
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

    #4 Listas vazias em colunas de lista
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

    
    #5 Estastísticas resumidas para colunas numéricas
    if num_cols:
        print("ESTATÍSTICAS RESUMIDAS PARA COLUNAS NUMÉRICAS")
        stats = lf.select(num_cols).collect().describe()
        print(stats)
        print("\n")

    #6 Verificar datas inválidas ou futuras
    if 'release_date' in lf.collect_schema().names():
        print("ANÁLISE DE DATAS DE LANÇAMENTOS")

        #Contar datas em formato inválido
        data_invalida = lf.filter(
            ~pl.col('release_date').str.contains(r'^\d{4}-\d{2}-\d{2}$') &
            (pl.col('release_date') != "")
        ).select(pl.count()).collect().item()
        print(f"Datas em formato inválido: {data_invalida}")

    #Adicionar verificação para datas futuras


    #7 Verificar valores negativos onde não faz sentido
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


    #8 Verificar valores de avaliação fora da escala esperada
    if 'vote_average' in lf.collect_schema().names():
        print("VALORES DE AVALIAÇÃO FORA DA ESCALA (0-10):")
        fora_escala = lf.filter(
            (pl.col('vote_average') < 0) | (pl.col('vote_average') > 10)
        ).select(pl.count()).collect().item()
        print(f"Avaliações fora da escala 0-10: {fora_escala}")
        print("\n")

    print("FIM DA VERIFICACAO")    
    return lf

def tratar_e_preencher_titulos(df):
    """
    Realiza o tratamento e preenchimento da coluna 'title' em um Polars DataFrame/LazyFrame.

    Args:
        df (pl.DataFrame or pl.LazyFrame): O DataFrame ou LazyFrame de entrada.
        path_csv_preenchimento (str): O caminho para o arquivo CSV com IDs e títulos para preenchimento.

    Returns:
        pl.LazyFrame: O LazyFrame com a coluna 'title' tratada e preenchida.
    """
    # Converte para LazyFrame, se necessário, para garantir o fluxo otimizado
    if not isinstance(df, pl.LazyFrame):
        lf = df.lazy()
    else:
        lf = df

    path_csv_preenchimento = os.getenv("PATH_CSV_FILL")
    if path_csv_preenchimento is None:
        raise ValueError("O caminho para o CSV não foi encontrado")
    print("Iniciando o tratamento de dados e preenchimento de títulos...")

    # --- 1. Tratamento inicial da coluna 'id' ---
    # Garante que 'id' é um número inteiro e remove linhas onde não é possível converter
    lf = lf.filter(pl.col("id").cast(pl.Int64, strict=False).is_not_null())
    print(" - IDs tratados (garantindo que são números).")

    # --- 2. Preparação para preenchimento de títulos a partir de CSV externo ---
    print(" - Carregando e preparando dados para preenchimento de títulos...")
    df_titulos_extras = pl.read_csv(path_csv_preenchimento)

    # Renomeia a coluna de título do CSV externo para evitar conflito de nomes
    df_titulos_extras = df_titulos_extras.rename({"title": "new_title"})

    # Limpa a lista extra de títulos: remove nulos e strings vazias/apenas com espaços
    df_titulos_extras = df_titulos_extras.filter(
        pl.col("new_title").is_not_null() & (pl.col("new_title").str.strip_chars().str.len_bytes() > 0)
    )
    # Remove títulos duplicados na lista extra, mantendo apenas um por ID
    df_titulos_extras = df_titulos_extras.unique(subset=["id"])
    print(" - Dados externos de títulos preparados.")

    # --- 3. Preenchimento da coluna 'title' com dados externos ---
    # Realiza um LEFT JOIN para trazer os 'new_title' para o LazyFrame principal
    # Mantém todas as linhas do seu LazyFrame original
    lf = lf.join(
        df_titulos_extras.lazy(), # Converte para LazyFrame para o join otimizado
        on="id",                  # A coluna 'id' conecta as duas tabelas
        how="left"                # 'left' join: mantém todas as linhas de 'lf'
    )
    print(" - Join com dados externos realizado.")

    # Usa 'coalesce' para preencher 'title':
    # 1. Tenta usar o valor original de 'title'.
    # 2. Se 'title' for nulo, tenta usar 'new_title' (do CSV externo).
    lf = lf.with_columns(
        pl.coalesce([
            pl.col("title"),    # Prioriza o título existente
            pl.col("new_title") # Se o existente for nulo, usa o do CSV
        ]).alias("title") # O resultado volta para a coluna 'title'
    )
    print(" - Títulos preenchidos usando dados externos.")

    # Remove a coluna temporária 'new_title' que foi usada para o preenchimento
    lf = lf.drop("new_title")
    print(" - Coluna temporária 'new_title' removida.")

    # --- 4. Limpeza final e padronização da coluna 'title' ---
    # Remove linhas onde 'title' é NULO ou uma string vazia (após o preenchimento e strip)
    # Isso é importante para pegar casos onde o ID não foi encontrado no CSV extra
    # OU o CSV extra também tinha um título vazio/nulo para aquele ID.
    lf = lf.filter(
        pl.col("title").is_not_null() & (pl.col("title").str.strip_chars().str.len_bytes() > 0)
    )
    print(" - Linhas com títulos vazios ou nulos removidas após preenchimento.")

    # Remove linhas duplicadas com base no valor da coluna 'title'
    lf = lf.unique(subset=["title"])
    print(" - Títulos duplicados removidos.")

    # Preenche quaisquer NULOS restantes na coluna 'title' (se houver, como fallback)
    lf = lf.with_columns(
        pl.col("title").fill_null("title unknown")
    )
    print(" - Nulos finais em 'title' preenchidos com 'title unknown'.")

    print("Tratamento de dados concluído!")
    return lf
'''   
def tratar_e_preencher_genre_id(df):
    if not isinstance(df, pl.LazyFrame):
        lf = df.lazy()
    else:
        lf = df

    path_csv_preenchimento = os.getenv("PATH_CSV_FILL")
    if path_csv_preenchimento is None:
        raise ValueError("O caminho para o CSV não foi encontrado")
    print("Iniciando o tratamento de dados e preenchimento de generos...")


    df_generos_extras = df_generos_extras.rename({"genre_id"})
'''