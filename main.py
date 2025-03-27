import os
import logging
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pymysql
import csv
from io import StringIO
from anthropic import Anthropic
import json

# Configure logging to show DEBUG messages
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

# Mount the static directory to serve CSS and JS files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Set up Jinja2 templating
templates = Jinja2Templates(directory="templates")

# Use environment variables for DB configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),  # Default to empty password for local testing
    "database": os.getenv("DB_NAME", "Mysql_Gen_V2"),
    "cursorclass": pymysql.cursors.DictCursor
}

# Use environment variable for API key
XAI_API_KEY = "xai-5z7ha8DF2bWU6LuGyWw5iIMTMiHy23k5gaHDYOHZ23JiMB9d39HyVDcMRzPJzcDZaMhaen8zAvW7Ohma"

client = Anthropic(
    api_key=XAI_API_KEY,
    base_url="https://api.x.ai",
)

# Minimal metadata for two tables
TABLE_METADATA = {
    "pr_site": {
        "columns": {
            "SiteId": "Primary key",
            "SiteName": "Name of the site",
            "StateSiteCode": "State code",
            "StartDate": "Site creation date",
            "CEPStartDate": "CEP program start date",
            "RegionId": "Foreign key to pr_region"
        },
        "default_filter": "StateSiteCode = 'MA'"
    },
    "pr_region": {
        "columns": {
            "RegionId": "Primary key",
            "RegionName": "Name of the region",
            "StateCd": "State code"
        },
        "default_filter": "StateCd = 'MA'"
    }
}

# Function to call xAI API and parse query
def parse_query_with_xai(user_query: str) -> dict:
    schema_info = "\n".join(
        [f"- {table}({', '.join([f'{col} ({purpose})' for col, purpose in schema['columns'].items()])})"
         for table, schema in TABLE_METADATA.items()]
    )
    try:
        completion = client.messages.create(
            model="grok-2-latest",
            max_tokens=500,
            messages=[
                {
                    "role": "system",
                    "content": (
                        f"Generate a MySQL query based on the following schema and query.\n"
                        f"Schema:\n{schema_info}\n\n"
                        f"Instructions:\n"
                        f"- Use the column purpose to determine the correct column (e.g., 'site creation date' for creation).\n"
                        f"- Default to the table's default_filter unless specified.\n"
                        f"- For count queries, return a count.\n"
                        f"- For list queries, return specific columns.\n"
                        f"- For date-based queries, extract the time range from the query (e.g., 'last 6 months', 'since 2021').\n"
                        f"- Return the result as JSON with keys: 'sql' (the SQL query), 'action' (count, list, or date_filter), 'date_field' (e.g., StartDate, CEPStartDate, or none), and 'csv' (true if date_filter, false otherwise).\n"
                        f"Examples:\n"
                        f"- 'How many sites in Massachusetts?' → {{ 'sql': 'SELECT COUNT(*) as count FROM pr_site WHERE StateSiteCode = \'MA\'', 'action': 'count', 'date_field': 'none', 'csv': false }}\n"
                        f"- 'List regions' → {{ 'sql': 'SELECT RegionName, RegionId FROM pr_region WHERE StateCd = \'MA\'', 'action': 'list', 'date_field': 'none', 'csv': false }}\n"
                        f"- 'Sites created in the last 6 months' → {{ 'sql': 'SELECT COUNT(*) as count FROM pr_site WHERE StateSiteCode = \'MA\' AND StartDate >= \'2024-09-10\'', 'action': 'date_filter', 'date_field': 'StartDate', 'csv': true }}\n"
                    )
                },
                {"role": "user", "content": user_query}
            ]
        )
        return json.loads(completion.content[0].text)
    except Exception as e:
        logger.error(f"Error generating SQL: {str(e)}")
        return {"error": f"Failed to parse query with xAI API: {e}"}

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/generate_sql")
async def get_sql(query: str = Form(...)):
    try:
        parsed_result = parse_query_with_xai(query)
        if "error" in parsed_result:
            return {"sql": None, "error": parsed_result["error"]}
        return {"sql": parsed_result["sql"], "action": parsed_result["action"], "date_field": parsed_result["date_field"], "csv": parsed_result["csv"]}
    except Exception as e:
        logger.error(f"Error in generate_sql endpoint: {str(e)}")
        return {"sql": None, "error": str(e)}

@app.post("/execute_sql")
async def execute_sql(sql: str = Form(...), action: str = Form(...), date_field: str = Form("none"), csv: str = Form("false")):
    conn = None
    cursor = None
    try:
        logger.debug(f"Executing SQL: {sql}")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql)
        
        if action in ["count", "date_filter"]:
            result = cursor.fetchone()
            count = result["count"] if result else 0
            if action == "date_filter":
                date_label = "CEP program" if date_field == "CEPStartDate" else "created"
                message = f"Number of {sql.split('FROM')[1].split()[0].replace('pr_', '')} {date_label}: {count}"
            else:
                message = f"Number of {sql.split('FROM')[1].split()[0].replace('pr_', '')}: {count}"
            return {"message": message, "query": sql}
        elif action == "list":
            results = cursor.fetchall()
            items = "\n".join([f"- {', '.join(str(v) for v in row.values())}" for row in results])
            message = f"List of {sql.split('FROM')[1].split()[0].replace('pr_', '')}:\n{items}"
            return {"message": message, "query": sql}
        
        if csv.lower() == "true" and action == "date_filter" and date_field != "none":
            table_name = sql.split('FROM')[1].split()[0]
            csv_query = f"SELECT SiteName, SiteId, {date_field} FROM {table_name} WHERE {sql.split('WHERE')[1] if 'WHERE' in sql else TABLE_METADATA[table_name]['default_filter']}"
            cursor.execute(csv_query)
            csv_data = cursor.fetchall()
            output = StringIO()
            writer = csv.writer(output)
            headers = ["SiteName", "SiteId", date_field]
            writer.writerow(headers)
            writer.writerows([[row[col] for col in headers] for row in csv_data])
            return FileResponse(
                path=None,
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={table_name}_filtered.csv"},
                content=output.getvalue()
            )

    except Exception as e:
        logger.error(f"SQL execution error: {str(e)}")
        return {"error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)