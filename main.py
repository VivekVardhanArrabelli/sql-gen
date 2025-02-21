import os
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, Response
import pymysql
import csv
from io import StringIO
from anthropic import Anthropic

app = FastAPI()

DB_CONFIG = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", "3306")),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": "employees",  # Use "employees" since test_db creates this
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
    completion = client.messages.create(
        model="grok-2-latest",
        messages=[
            {"role": "system", "content": f"Generate MySQL for:{schema}"},
            {"role": "user", "content": user_query}
        ]
    )
    return completion.content

@app.post("/generate_sql")
def get_sql(query: str = Form(...)):
    sql_query = generate_sql(query)
    return {"sql": sql_query}

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
            headers = results[0].keys()
            writer = csv.DictWriter(output, fieldnames=headers)
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