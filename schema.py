from sqlalchemy import inspect
from database import engine


def get_db_schema():
    inspector = inspect(engine)
    schema = []

    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        column_names = [col["name"] for col in columns]

        schema.append({
            "table": table_name,
            "columns": column_names
        })

    return schema


def schema_to_text(schema):
    text = ""
    for table in schema:
        text += f"Table: {table['table']}\n"
        for col in table["columns"]:
            text += f"- {col}\n"
        text += "\n"
    return text
