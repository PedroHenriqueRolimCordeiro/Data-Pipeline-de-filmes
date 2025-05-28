import polars as pl
import sqlite3
import os
from dotenv import load_dotenv

load_dotenv() # Puxa as variáveis do .env

PARQUET_FILE_PATH = os.getenv("DF_FINAL", "filmes_tmdb_transformados.parquet")


def get_sqlite_type(polars_dtype: pl.DataType) -> str:
    """Converte o tipo de dado do Polars para o tipo do SQLite."""
    if polars_dtype == pl.Int64 or polars_dtype == pl.Int32:
        return "INTEGER"
    elif polars_dtype == pl.Float64 or polars_dtype == pl.Float32:
        return "REAL"
    elif polars_dtype == pl.String:
        return "TEXT"
    elif polars_dtype == pl.Date:
        return "TEXT" # Data vira texto no SQLite
    elif polars_dtype == pl.Boolean:
        return "INTEGER" # True/False vira 1/0
    elif isinstance(polars_dtype, pl.List):
        return "TEXT" # Listas viram texto (tipo JSON)
    else:
        return "TEXT" # Se não conhecer, tenta TEXT


def create_table_schema(df: pl.DataFrame, table_name: str) -> str:
    """
    Gera o comando SQL para criar a tabela, baseado no DataFrame.
    A coluna 'Id' vira a CHAVE PRIMÁRIA.
    """
    columns_sql = []
    for col_name, col_dtype in df.schema.items():
        sqlite_type = get_sqlite_type(col_dtype)
        if col_name == "Id": 
            columns_sql.append(f"{col_name} {sqlite_type} PRIMARY KEY")
        else:
            columns_sql.append(f"{col_name} {sqlite_type}")
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(columns_sql) + "\n);"

def load_data_to_sqlite(df: pl.DataFrame, db_path: str, table_name: str):
    """Pega um DataFrame do Polars e joga numa tabela do SQLite."""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Cria a tabela (se não existir)
        create_table_sql = create_table_schema(df, table_name)
        print(f"\n--- Criando ou verificando tabela '{table_name}' ---")
        print(create_table_sql)
        cursor.execute(create_table_sql)
        conn.commit()
        print(f" - Tabela '{table_name}' pronta.")

        # Agora, insere os dados
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["?" for _ in df.columns])
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Ajusta os dados pra inserir direitinho
        data_to_insert = []
        for row in df.iter_rows():
            processed_row = []
            for i, item in enumerate(row):
                col_name = df.columns[i]
                col_dtype = df.schema[col_name]
                if isinstance(col_dtype, pl.List):
                    processed_row.append(str(item)) # Lista vira string
                elif col_dtype == pl.Date:
                    processed_row.append(str(item) if item else None) # Data vira string
                else:
                    processed_row.append(item)
            data_to_insert.append(tuple(processed_row))

        print(f"--- Carregando {len(data_to_insert)} linhas para '{table_name}' ---")
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        print(f" - Dados carregados para '{table_name}'.")

    except sqlite3.Error as e:
        print(f"Deu ruim ao carregar os dados: {e}")
        if conn:
            conn.rollback() # Desfaz se deu erro
    finally:
        if conn:
            conn.close()
            print(" - Conexão com SQLite fechada.")