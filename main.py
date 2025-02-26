import os
import logging
from fastapi import FastAPI, Form, Request
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
        full_prompt = f"Generate MySQL for:{schema} {user_query}. Return only the SQL query within ```sql and ``` markers, without additional explanation."
        messages = [{"role": "user", "content": full_prompt}]
        
        logger.debug(f"Sending messages: {messages}")
        
        completion = client.messages.create(
            model="grok-2-latest",
            max_tokens=1024,
            temperature=0.7,
            messages=messages
        )
        if isinstance(completion.content, list):
            for item in completion.content:
                if hasattr(item, 'text') and item.text:
                    text = item.text.strip()
                    sql_start = text.find("```sql\n") + 7  # Skip ```sql\n
                    sql_end = text.find("\n```", sql_start)
                    if sql_start != -1 and sql_end != -1:
                        return text[sql_start:sql_end].strip()
                    return text.strip()
            return "No SQL generated"
        return str(completion.content).strip()
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
    conn = None
    cursor = None
    try:
        logger.debug(f"Executing SQL: {sql}")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        return {"results": results}
    except Exception as e:
        logger.error(f"SQL execution error: {str(e)}")
        return {"error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.post("/generate_and_execute")
async def generate_and_execute(request: Request, query: str = Form(...)):  # Fixed parameter order
    try:
        # Generate SQL
        sql_query = generate_sql(query)
        if sql_query.startswith("Error generating SQL:"):
            return {"sql": None, "error": sql_query, "results": None, "csv": None}

        # Execute SQL
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()

        # Generate CSV
        output = StringIO()
        if results:
            headers = results[0].keys()
            writer = csv.DictWriter(output, fieldnames=headers)
            writer.writeheader()
            writer.writerows(results)

        # Prepare CSV response
        csv_content = output.getvalue()
        output.close()

        # Trigger CSV download (using JavaScript in HTML)
        return {
            "sql": sql_query,
            "error": None,
            "results": results,
            "csv_content": csv_content  # Will be used by JavaScript to initiate download
        }
    except Exception as e:
        logger.error(f"Error in generate_and_execute: {str(e)}")
        return {"sql": None, "error": str(e), "results": None, "csv": None}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h1>FastAPI SQL Generator & Executor</h1>
    <form action="/generate_and_execute" method="post" onsubmit="handleSubmit(event)">
        <input type="text" name="query" placeholder="Enter your query in English" id="queryInput">
        <button type="submit">Generate, Execute, and Download</button>
    </form>
    <form action="/generate_sql" method="post">
        <input type="text" name="query" placeholder="Enter your query in English">
        <button type="submit">Generate SQL</button>
    </form>
    <form action="/execute_sql" method="post">
        <input type="text" name="sql" placeholder="Enter SQL to execute">
        <button type="submit">Execute SQL</button>
    </form>
    <div id="result"></div>
    <script>
        function handleSubmit(event) {
            event.preventDefault();
            const query = document.getElementById('queryInput').value;
            fetch('/generate_and_execute', {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: 'query=' + encodeURIComponent(query)
            })
            .then(response => response.json())
            .then(data => {
                const resultDiv = document.getElementById('result');
                if (data.error) {
                    resultDiv.innerHTML = `<p>Error: ${data.error}</p>`;
                } else if (data.sql) {
                    resultDiv.innerHTML = `<p>SQL Generated: <code>${data.sql}</code></p>`;
                    if (data.results) {
                        resultDiv.innerHTML += `<p>Results: ${JSON.stringify(data.results)}</p>`;
                    }
                    // Trigger CSV download
                    if (data.csv_content) {
                        const blob = new Blob([data.csv_content], { type: 'text/csv' });
                        const url = window.URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = 'results.csv';
                        document.body.appendChild(a);
                        a.click();
                        document.body.removeChild(a);
                        window.URL.revokeObjectURL(url);
                    }
                }
            })
            .catch(error => {
                document.getElementById('result').innerHTML = `<p>Error: ${error}</p>`;
            });
        }
    </script>
    """