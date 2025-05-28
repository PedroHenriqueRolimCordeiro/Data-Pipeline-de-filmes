import os
from dotenv import load_dotenv
import polars as pl
from popular_movies import *
from transform import transform
from load import load_data_to_sqlite

def main():
    """
    Roda o processo todo: pega os dados dos filmes, arruma eles e salva no banco.
    """
    # Carrega as paradas do arquivo .env
    load_dotenv()
    
    # Configurações que a gente pega do .env ou usa um padrão
    # Número de páginas pra buscar lá na API
    NUM_PAGES = int(os.getenv("NUM_PAGES", "1"))
    # Idioma que a gente quer os dados
    LANGUAGE = os.getenv("LANGUAGE", "pt-BR")
    # Nome do arquivo Parquet onde vai o resultado final
    OUTPUT_PARQUET = os.getenv("DF_FINAL", "filmes_tmdb_transformados.parquet")
    # Caminho do arquivo do banco de dados SQLite
    DB_PATH = os.getenv("DB_PATH", "movies.db")
    # Nome da tabela que vai ser criada no banco
    TABLE_NAME = os.getenv("TABLE_NAME", "movies")

    print("Começando o processo todo (ETL)...")

    # 1. Extração: Buscar os dados dos filmes mais populares
    print("\n--- Hora de Extrair os Dados ---")
    df = get_popular_movies_data(num_pages=NUM_PAGES, language=LANGUAGE)
    
    if df.is_empty():
        print("Ih, não veio nada da API. Parando por aqui.")
        return
    
    print(f" - {df.shape[0]} filmes encontrados.")

    # 2. Salvar os dados brutos num arquivo Parquet (tipo um backup)
    df.write_parquet(OUTPUT_PARQUET)
    print(f" - Dados brutos salvos em: {OUTPUT_PARQUET}.")

    # 3. Transformação: Dar um trato nos dados
    print("\n--- Hora de Transformar os Dados ---")
    lf = df.lazy() 
    transformed_lf = transform(lf)
    
    # Pega o resultado final da transformação
    df_transformed = transformed_lf.collect()
    print(f" - Transformação feita. Ficamos com {df_transformed.shape[0]} linhas.")

    # 4. Carregamento: Jogar os dados tratados no banco SQLite
    print("\n--- Hora de Carregar os Dados ---")
    load_data_to_sqlite(df_transformed, DB_PATH, TABLE_NAME)
    print(f" - Dados carregados no banco: {DB_PATH}, tabela: {TABLE_NAME}.")

    print("\nProcesso ETL finalizado com sucesso!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Ocorreu um erro feio no meio do caminho: {e}")