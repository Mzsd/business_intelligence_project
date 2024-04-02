from airflow.models import Variable, Connection
from sqlalchemy import create_engine, text
from airflow.settings import Session
import pandas as pd
import requests
import time


def ip_fetcher(**kwargs):
    start = time.time()
    merged_df = pd.read_json(kwargs['data'])
    
    # Get the Postgres connection details from Airflow's Connection table
    postgres_conn_id = "pg_connection"
    session = Session()

    # Query the Connection table to get the Postgres connection
    conn = session.query(Connection).filter(Connection.conn_id == postgres_conn_id).one()

    # Build the URI from the connection details
    uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

    # Create the engine using the URI
    engine = create_engine(uri)
    print("[+] Connection established.")
    
    table_exists = False

    query = """SELECT EXISTS (
    SELECT FROM information_schema.tables 
    WHERE  table_schema = 'public'
    AND    table_name   = 'ip_location'
    );"""

    result = engine.connect().execution_options(isolation_level="AUTOCOMMIT").execute(text(query))
    for row in result:
        table_exists = row[0]

    if not table_exists:
        ip_location = {
            ip: {
                    k: v 
                    for k, v in requests.get(f"https://api.ip2location.io/?key={Variable.get('ip_api_key')}&{ip}&format=json").json().items() 
                    if k in ['country_code', 'country_name', 'region_name', 'city_name']
                }
            for ip in merged_df['c-ip'].unique()
        }

        ip_df = pd.DataFrame(ip_location).T.reset_index()
        ip_df.columns = ['c-ip', 'country_code', 'country_name', 'region_name', 'city_name']
        ip_df.to_sql('ip_location', engine, if_exists='replace', index=False)
        
        print("[+] IP location data fetched.")
    else:
        print("[+] Table already exists.")

    print(f"Time to process: {time.time() - start}")