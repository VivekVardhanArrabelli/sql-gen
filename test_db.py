



import pymysql

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "",  # No password
    "database": "Mysql_Gen_V2",
    "cursorclass": pymysql.cursors.DictCursor
}

try:
    conn = pymysql.connect(**DB_CONFIG)
    print("Connection successful!")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM pr_site LIMIT 1")
    result = cursor.fetchone()
    print("Sample data from pr_site:", result)
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Connection failed: {str(e)}")

