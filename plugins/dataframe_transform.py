from airflow.models import Variable, Connection
from sqlalchemy import create_engine
from airflow.settings import Session
import pandas as pd
import time


def dataframe_transformer(**kwargs):
    start = time.time()
    merged_df = pd.read_json(kwargs['data'])
    
    engine = create_engine(Variable.get("connection_string"))

    merged_df['file-extension'] = merged_df['cs-uri-stem'].apply(
        lambda x: '.' + x.split("/")[-1].split('.')[-1].split('?')[0] if '.' in x.split("/")[-1] else 'None'
    )
    
    merged_df = merged_df.drop(
        [
            's-ip', 'cs-uri-query', 's-port', 
            'cs-username', 'cs(Cookie)', 'cs(Referer)', 
            'sc-substatus', 'sc-win32-status'
        ], 
        axis=1
    )
    
    merged_df['os'] = merged_df['cs(User-Agent)'].apply(
        lambda x: 'Windows' 
        if 'window' in x.lower() 
        else 'Other' 
        if 'linux' in x.lower() 
        else 'Macintosh' 
        if 'mac' in x.lower() 
        else 'Bots' 
        if 'bot' in x.lower() 
        else 'OS unknown'
    )
    
    merged_df['cs(User-Agent)'] = merged_df['cs(User-Agent)'].apply(
        lambda x: x.split("/")[0].split("+")[0].split('-')[0].replace('ee:', '').replace('http:', '')
    )
    
    user_agents_dict = dict(merged_df['cs(User-Agent)'].value_counts())
    merged_df['cs(User-Agent)'] = merged_df['cs(User-Agent)'].apply(
        lambda x: x if user_agents_dict[x] > 10 and not x == '' else 'Other'
    )
    
    merged_df['c-ip'] = merged_df['c-ip'].apply(
        lambda x: 'None' if x == '' else x
    )
    
    merged_df['c-ip'] = merged_df['c-ip'].apply(
        lambda x: 'None' if x == '' else x
    )
 
    merged_df['cs-bytes'] = merged_df['cs-bytes'].apply(
        lambda x: 'None' if x == None or x == '' else x
    )
    merged_df['sc-bytes'] = merged_df['cs-bytes'].apply(
        lambda x: 'None' if x == None or x == '' else x
    )
    
    merged_df['cs-bytes'] = merged_df['cs-bytes'].fillna('None')
    merged_df['sc-bytes'] = merged_df['sc-bytes'].fillna('None')
    
    merged_df['cs-bytes'] = merged_df['cs-bytes'].apply(
        lambda x: 'None' if x == '' else x
    )
    
    bots_ip = merged_df[merged_df['cs-uri-stem'].str.contains('robots.txt')]['c-ip'].unique()
    merged_df.loc[merged_df['c-ip'].isin(bots_ip), 'os'] = 'Bots'
    
    merged_df['date'] = pd.to_datetime(merged_df['date'])
    
    merged_df = merged_df.dropna(subset=['date'])
    merged_df.reset_index()
    
    merged_df['Month'] = merged_df['date'].dt.strftime('%B')
    merged_df['Day of the Week'] = merged_df['date'].dt.day_name()
    merged_df['Quarter'] = merged_df['date'].dt.quarter

    quarter_mapping = {1: "Quarter One", 2: "Quarter Two", 3: "Quarter Three", 4: "Quarter Four"}
    merged_df['Quarter'] = merged_df['Quarter'].map(quarter_mapping)
    
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
    
    merged_df.to_sql('packets_data', engine, if_exists='replace', index=False)
    
    print(f"Time to process: {time.time() - start}")    

    return merged_df


if __name__ == "__main__":
    dataframe_transformer()