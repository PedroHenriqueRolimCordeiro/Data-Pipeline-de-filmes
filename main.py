import os
from dotenv import load_dotenv
import polars as pl
from popular_movies import get_popular_movies_data
from transform import transform
from load import load_data_to_sqlite

def main():
    """
    Executa o pipeline ETL completo para coletar, transformar e carregar dados de filmes populares do TMDB.
    """
    # Carregar variáveis de ambiente
    load_dotenv()
    
    # Configurações
    NUM_PAGES = int(os.getenv("NUM_PAGES", "1"))  # Número de páginas a coletar
    LANGUAGE = os.getenv("LANGUAGE", "pt-BR")     # Idioma padrão
    # Usa a variável de ambiente para o caminho do parquet de saída
    OUTPUT_PARQUET = os.getenv("DF_FINAL", "filmes_tmdb_transformados.parquet") # Consistente com .env
    DB_PATH = os.getenv("DB_PATH", "movies.db")   # Caminho do banco SQLite
    TABLE_NAME = os.getenv("TABLE_NAME", "movies") # Nome da tabela no SQLite

    print("Iniciando o pipeline ETL...")

    # 1. Extração: Obter dados de filmes populares
    print("\n--- Etapa de Extração ---")
    df = get_popular_movies_data(num_pages=NUM_PAGES, language=LANGUAGE)
    
    if df.is_empty():
        print("Nenhum dado foi coletado. Encerrando o pipeline.")
        return
    
    print(f" - {df.shape[0]} filmes coletados.")

    # 2. Salvando o DataFrame bruto como Parquet (opcional, para backup)
    # Alterado para salvar no caminho de OUTPUT_PARQUET especificado
    df.write_parquet(OUTPUT_PARQUET)
    print(f" - Dados brutos salvos em {OUTPUT_PARQUET}.")

    # 3. Transformação: Aplicar transformações nos dados
    print("\n--- Etapa de Transformação ---")
    lf = df.lazy()  # Converter para LazyFrame para transformações
    transformed_lf = transform(lf)
    
    # Coletar o resultado da transformação
    df_transformed = transformed_lf.collect()
    print(f" - Transformação concluída. {df_transformed.shape[0]} linhas após transformação.")

    # 4. Carregamento: Carregar os dados transformados no SQLite
    print("\n--- Etapa de Carregamento ---")
    load_data_to_sqlite(df_transformed, DB_PATH, TABLE_NAME)
    print(f" - Dados carregados no banco SQLite: {DB_PATH}, tabela: {TABLE_NAME}.")

    print("\nPipeline ETL concluído com sucesso!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Erro durante a execução do pipeline ETL: {e}")