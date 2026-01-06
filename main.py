from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from backend.ai_sql import generate_sql
from backend.sql_executor import execute_sql
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

app = FastAPI(title="Fertility AI Database Chat")

# ✅ ENABLE CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # or ["http://localhost:5173"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/ask")
def ask(question: str):
    try:
        sql = generate_sql(question)
        results = execute_sql(sql)

        return {
            "question": question,
            "sql": sql,
            "results": results
        }

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
