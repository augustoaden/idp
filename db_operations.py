import os

import psycopg2
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv("production.env")


def connect_db():
    """Conecta a la base de datos y retorna la conexión."""
    try:
        connection = psycopg2.connect(
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"]
        )
        return connection
    except Exception as e:
        raise RuntimeError(f"Error al conectar a la base de datos: {e}")


def execute_query(conn, query, params=None, fetch=False):
    """Ejecuta una consulta SQL."""
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        if fetch:
            return cursor.fetchall()
        conn.commit()
