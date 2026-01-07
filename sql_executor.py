from sqlalchemy import text
from database import engine


def is_safe_select(sql: str) -> bool:
    sql_upper = sql.strip().upper()
    return sql_upper.startswith("SELECT") and ";" in sql_upper


def execute_sql(sql: str):
    if not is_safe_select(sql):
        raise ValueError("Only SELECT queries are allowed")

    with engine.connect() as connection:
        result = connection.execute(text(sql))
        columns = result.keys()
        rows = result.fetchall()

    return [dict(zip(columns, row)) for row in rows]
