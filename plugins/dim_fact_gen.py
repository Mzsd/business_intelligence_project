from airflow.models import Variable, Connection
from sqlalchemy import create_engine
import pandas as pd
import time


def dim_fact_generator(**kwargs):
    start = time.time()
    merged_df = pd.read_json(kwargs['data'])
    ip_df = pd.read_json(kwargs['ip_data'])
    
    engine = create_engine(Variable.get("connection_string"))
    
    # Datetime dimension table
    datetime_dim = merged_df[['date', 'time', 'Month', 'Day of the Week', 'Quarter']]
    datetime_dim = datetime_dim.drop_duplicates().reset_index(drop=True)
    datetime_dim['datetime_id'] = range(1, len(datetime_dim) + 1)

    # Create dimension tables
    method_dim = merged_df[['cs-method']].drop_duplicates().reset_index(drop=True)
    method_dim['method_id'] = range(1, len(method_dim) + 1)

    uri_dim = merged_df[['cs-uri-stem']].drop_duplicates().reset_index(drop=True)
    uri_dim['uri_id'] = range(1, len(uri_dim) + 1)

    ip_dim = ip_df[['c-ip', 'country_code', 'country_name', 'region_name', 'city_name']].drop_duplicates()
    ip_dim['ip_id'] = range(1, len(ip_dim) + 1)

    user_agent_dim = merged_df[['cs(User-Agent)']].drop_duplicates().reset_index(drop=True)
    user_agent_dim['user_agent_id'] = range(1, len(user_agent_dim) + 1)

    os_dim = merged_df[['os']].drop_duplicates().reset_index(drop=True)
    os_dim['os_id'] = range(1, len(os_dim) + 1)
    
    # Merge the IDs back into the original dataframe
    fact_df = merged_df.merge(datetime_dim, on='time').merge(method_dim, on='cs-method').merge(uri_dim, on='cs-uri-stem').merge(user_agent_dim, on='cs(User-Agent)').merge(os_dim, on='os')
    fact_df = fact_df.merge(ip_dim, on='c-ip')
    
    # Create the fact table
    fact_table = fact_df[
            [
                'datetime_id', 'method_id', 
                'uri_id', 'ip_id', 'user_agent_id', 
                'os_id', 'sc-status', 'sc-bytes', 
                'cs-bytes', 'time-taken'
            ]
        ]

    # Push the dim DataFrame to PostgreSQL as dimension tables
    method_dim.to_sql('method_dimension', engine, if_exists='replace', index=False)
    uri_dim.to_sql('uri_dimension', engine, if_exists='replace', index=False)
    ip_dim.to_sql('ip_dimension', engine, if_exists='replace', index=False)
    user_agent_dim.to_sql('user_agent_dimension', engine, if_exists='replace', index=False)
    os_dim.to_sql('os_dimension', engine, if_exists='replace', index=False)

    # Push the fact DataFrame to PostgreSQL as a fact table
    fact_table.to_sql('packets_data_fact', engine, if_exists='replace', index=False)

    print(f"Time to process: {time.time() - start}")