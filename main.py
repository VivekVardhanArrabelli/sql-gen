import os
import logging
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pymysql
import json
import requests

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Set up Jinja2 templates
templates = Jinja2Templates(directory="templates")

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "Mysql_Gen_V2"),
    "cursorclass": pymysql.cursors.DictCursor
}

# xAI API configuration
XAI_API_KEY = os.getenv("XAI_API_KEY", "")
XAI_API_URL = "https://api.x.ai/v1/messages"

def generate_sql(user_query: str) -> dict:
    payload = {
        "model": "grok-2-latest",
        "max_tokens": 500,
        "messages": [
            {"role": "system", "content": "Generate a MySQL query based on the user query."},
            {"role": "user", "content": user_query}
        ]
    }
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {XAI_API_KEY}"}
    
    try:
        response = requests.post(XAI_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        return json.loads(result["content"][0]["text"]) if "content" in result else {"error": "Invalid API response"}
    except Exception as e:
        logger.error(f"API request failed: {e}")
        return {"error": str(e)}

@app.post("/generate_sql")
async def get_sql(query: str = Form(...)):
    result = generate_sql(query)
    return result if "sql" in result else {"error": result.get("error", "Failed to generate SQL.")}

@app.post("/execute_sql")
async def execute_sql(sql: str = Form(...)):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        return {"result": result}
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
