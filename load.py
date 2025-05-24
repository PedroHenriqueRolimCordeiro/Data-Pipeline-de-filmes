import polars as pl
import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()

# Obtém o caminho da variável de ambiente, padrão para um nome sensato
# Garante que isso corresponda à saída da transformação
PARQUET_FILE_PATH = os.getenv("DF_FINAL", "filmes_tmdb_transformados.parquet")

# **Removido o carregamento direto do DataFrame aqui.**
# A função `load_data_to_sqlite` agora aceitará o DataFrame como argumento.
# if not os.path.exists(PARQUET_FILE_PATH):
#     raise FileNotFoundError(f"O arquivo Parquet '{PARQUET_FILE_PATH}' não existe. Garanta que o pipeline ETL tenha sido executado com sucesso.")
# df_movies = pl.read_parquet(PARQUET_FILE_PATH)


def get_sqlite_type(polars_dtype: pl.DataType) -> str:
    """Mapeia tipos de dados do Polars para tipos de dados do SQLite."""
    if polars_dtype == pl.Int64 or polars_dtype == pl.Int32:
        return "INTEGER"
    elif polars_dtype == pl.Float64 or polars_dtype == pl.Float32:
        return "REAL"
    elif polars_dtype == pl.String:
        return "TEXT"
    elif polars_dtype == pl.Date:
        return "TEXT" # SQLite não tem tipo DATE nativo, geralmente usa TEXT (YYYY-MM-DD)
    elif polars_dtype == pl.Boolean:
        return "INTEGER" # 0 para False, 1 para True
    elif isinstance(polars_dtype, pl.List):
        return "TEXT" # Listas serão armazenadas como strings JSON
    else:
        # Para outros tipos, como Unknown, assume TEXT ou levanta um erro
        return "TEXT"


def create_table_schema(df: pl.DataFrame, table_name: str) -> str:
    """
    Gera a instrução SQL CREATE TABLE com base no esquema do Polars DataFrame.
    Adiciona a coluna 'ID' como PRIMARY KEY.
    """
    columns_sql = []
    for col_name, col_dtype in df.schema.items():
        sqlite_type = get_sqlite_type(col_dtype)
        if col_name == "Id": # Alterado para 'Id' para corresponder ao nome da coluna transformada
            columns_sql.append(f"{col_name} {sqlite_type} PRIMARY KEY")
        else:
            columns_sql.append(f"{col_name} {sqlite_type}")
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(columns_sql) + "\n);"

def load_data_to_sqlite(df: pl.DataFrame, db_path: str, table_name: str):
    """
    Carrega um Polars DataFrame para uma tabela SQLite.

    Args:
        df (pl.DataFrame): O Polars DataFrame a ser carregado.
        db_path (str): O caminho para o arquivo de banco de dados SQLite.
        table_name (str): O nome da tabela no banco de dados.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. Gerar e criar a tabela
        create_table_sql = create_table_schema(df, table_name)
        print(f"\n--- Criando ou verificando tabela '{table_name}' ---")
        print(create_table_sql)
        cursor.execute(create_table_sql)
        conn.commit()
        print(f" - Tabela '{table_name}' criada ou verificada com sucesso.")

        # 2. Inserir dados
        # A instrução INSERT OR REPLACE é útil para idempotência (evita duplicatas se 'ID' já existir)
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["?" for _ in df.columns])
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Preparar os dados para inserção
        # Para tipos LIST, é necessário convertê-los para string JSON antes de inserir
        # Para tipos DATE, convertê-los para string (YYYY-MM-DD)
        data_to_insert = []
        for row in df.iter_rows():
            processed_row = []
            for i, item in enumerate(row):
                col_name = df.columns[i]
                col_dtype = df.schema[col_name]
                if isinstance(col_dtype, pl.List):
                    processed_row.append(str(item)) # Converte lista para representação de string
                elif col_dtype == pl.Date:
                    processed_row.append(str(item) if item else None) # Converte data para string YYYY-MM-DD
                else:
                    processed_row.append(item)
            data_to_insert.append(tuple(processed_row))

        print(f"--- Carregando {len(data_to_insert)} linhas para '{table_name}' ---")
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        print(f" - Dados carregados com sucesso para '{table_name}'.")

    except sqlite3.Error as e:
        print(f"Erro ao carregar dados para SQLite: {e}")
        if conn:
            conn.rollback() # Reverte em caso de erro
    finally:
        if conn:
            conn.close()
            print(" - Conexão SQLite fechada.")
