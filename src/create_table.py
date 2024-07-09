import psycopg2
import logging
from constants import DB_FIELDS, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB

def get_connection():
    return psycopg2.connect(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB
    )

def execute_query(query):
    con = get_connection()
    cur = con.cursor()
    try:
        cur.execute(query)
        con.commit()
        logging.info("Query executed successfully")
    except Exception as e:
        logging.error(f"Failed to execute query: {e}")
        con.rollback()
        raise
    finally:
        cur.close()
        con.close()

def create_table():
    query = f"""
    CREATE TABLE IF NOT EXISTS cycling_table (
        {', '.join([f"{field} VARCHAR(255)" for field in DB_FIELDS])}
    );
    """
    execute_query(query)

if __name__ == "__main__":
    create_table()
