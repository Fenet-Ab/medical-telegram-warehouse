from fastapi import FastAPI
from database import engine
import pandas as pd

app = FastAPI(title="Medical Telegram API")


@app.get("/api/reports/top-products")
def top_products(limit: int = 10):

    query = """
    SELECT message_text, COUNT(*) 
    FROM fct_messages
    GROUP BY message_text
    ORDER BY count DESC
    LIMIT %s
    """

    df = pd.read_sql(query, engine, params=(limit,))
    return df.to_dict()
