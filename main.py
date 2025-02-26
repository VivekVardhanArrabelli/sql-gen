import os
import logging
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, Response
import pymysql
import csv
from io import StringIO
from anthropic import Anthropic

# Configure logging to show DEBUG messages
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

DB_CONFIG = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", "3306")),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": "employees",  # Pointing to Railway's employees DB
    "cursorclass": pymysql.cursors.DictCursor
}

XAI_API_KEY = "xai-wLjpXGlEqqAjWOqbmXLnNkAGE8REsN3b4B2S2Zhg7QnLiHuiJ4TPYPEAm1sgFYGq9pS93ncHNGNerbmA"

client = Anthropic(
    api_key=XAI_API_KEY,
    base_url="https://api.x.ai",
)

def generate_sql(user_query: str):
    schema = """
    # Employees DB Schema:
    - employees(emp_no, birth_date, first_name, last_name, gender, hire_date)
    - departments(dept_no, dept_name)
    - dept_emp(emp_no, dept_no, from_date, to_date)
    - salaries(emp_no, salary, from_date, to_date)
    """
    try:
        # Simplified messages using only 'user' role
        full_prompt = f"Generate MySQL for:{schema} {user_query}. Return only the SQL query within ```sql and ``` markers, without additional explanation."
        messages = [{"role": "user", "content": full_prompt}]
        
        # Log the messages being sent
        logger.debug(f"Sending messages: {messages}")
        
        completion = client.messages.create(
            model="grok-2-latest",
            max_tokens=1024,
            temperature=0.7,
            messages=messages
        )
        # Handle completion.content as a list of message objects
        if isinstance(completion.content, list):
            for item in completion.content:
                if hasattr(item, 'text') and item.text:
                    # Extract SQL between ```sql and ```
                    text = item.text.strip()
                    sql_start = text.find("```sql\n") + 7  # Skip ```sql\n
                    sql_end = text.find("\n```", sql_start)
                    if sql_start != -1 and sql_end != -1:
                        return text[sql_start:sql_end].strip()
                    return text.strip()  # Fallback if no code block
            return "No SQL generated"  # Fallback if no text found
        return str(completion.content).strip()  # Fallback for unexpected types
    except Exception as e:
        logger.error(f"Error generating SQL: {str(e)}")
        return f"Error generating SQL: {str(e)}"

@app.post("/generate_sql")
def get_sql(query: str = Form(...)):
    sql_query = generate_sql(query)
    if isinstance(sql_query, str) and sql_query.startswith("Error generating SQL:"):
        return {"sql": None, "error": sql_query}
    return {"sql": sql_query, "error": None}

@app.post("/execute_sql")
def execute_sql(sql: str = Form(...)):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        return {"results": results}
    except Exception as e:
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

@app.post("/export")
def export_to_csv(sql: str = Form(...)):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        
        output = StringIO()
        if results:
            # Get headers from the first row
            headers = results[0].keys()
            writer = csv.DictWriter(output, fieldnames=headers)
            
            # Write headers and data
            writer.writeheader()
            writer.writerows(results)
            
        return Response(
            output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment;filename=results.csv"}
        )
    except Exception as e:
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h1>FastAPI SQL Generator</h1>
    <form action="/generate_sql" method="post">
        <input type="text" name="query" placeholder="Enter your query in English">
        <button type="submit">Generate SQL</button>
    </form>
    <form action="/execute_sql" method="post">
        <input type="text" name="sql" placeholder="Enter SQL to execute">
        <button type="submit">Execute SQL</button>
    </form>
    """