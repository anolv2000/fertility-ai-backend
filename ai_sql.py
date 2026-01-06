import os
from dotenv import load_dotenv
from openai import OpenAI

from schema import get_db_schema, schema_to_text

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = """
You are an expert data analyst.

You are given a SQLite database schema.
Your job is to convert the user's natural language question
into a valid SQLite SELECT query.

RULES:
- Only generate SELECT queries
- Do NOT use INSERT, UPDATE, DELETE, DROP
- Use only tables and columns from the schema
- Return ONLY the SQL query
- Do NOT wrap SQL in markdown
"""

def generate_sql(question: str) -> str:
    schema = get_db_schema()
    schema_text = schema_to_text(schema)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"""
Database schema:
{schema_text}

Question:
{question}
"""
            }
        ],
        temperature=0
    )

    return response.choices[0].message.content.strip()
