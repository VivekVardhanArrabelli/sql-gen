
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
import requests
import pymysql
import pandas as pd
from io import BytesIO
from starlette.responses import Response
from anthropic import anthropic

app = FastAPI()

# XAI API Key (Move to env variables in production)
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
    DB_CONFIG = {
        "host": "localhost",
        "user": "root",
        "password": "colab123",
        "database": "employees"
    }
    try:
        conn = pymysql.connect(host="localhost", user="root", password="colab123", database="employees", cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"results": results}
    except Exception as e:
        return {"error": str(e)}

@app.post("/export")
def export_to_excel(sql: str = Form(...)):
    results, _, _ = execute_sql(sql)
    df = pd.DataFrame(results.get("results", []))
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return Response(
        output.read(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment;filename=results.xlsx"}
    )

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h1>FastAPI SQL Generator</h1>
    <form action="/generate_sql" method="post">
        <input type="text" name="query" placeholder="Enter your query in English">
        <button type="submit">Generate SQL</button>
    </form>
    """
