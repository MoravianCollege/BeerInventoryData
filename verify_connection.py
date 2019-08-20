import dotenv
import os
import psycopg2

dotenv.load_dotenv()

host = os.getenv('DB_HOST')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
dbname = os.getenv('DB_NAME')

conn = psycopg2.connect(dbname=dbname, host=host, user=user, password=password)
cur = conn.cursor()

cur.execute('SELECT VERSION()')
print(cur.fetchone())

print('\nSUCCESS!')

