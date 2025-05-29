import psycopg2
from psycopg2 import sql
from contextlib import contextmanager

# Конфигурация подключения (лучше вынести в отдельный config.py)
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '11111',
    'host': '127.0.0.1',
    'port': '5432'
}

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
    except psycopg2.Error as e:
        print(f"Ошибка подключения к БД: {e}")
        raise
    finally:
        if conn:
            conn.close()

def execute_query(query, params=None, fetch=False):

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                print(f"Ошибка выполнения запроса: {e}")
                raise