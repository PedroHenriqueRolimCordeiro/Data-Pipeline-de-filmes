import tmdbsimple as tmdb
from datetime import datetime
import polars as pl
from pathlib import Path

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

    
    return lf
