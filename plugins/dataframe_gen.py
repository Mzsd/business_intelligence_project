from airflow.models import Variable
from hashlib import sha256
import pandas as pd
import boto3
import time


def generate_csv(data, tables):
    list_gather = False
    for d in data:
        if "#Fields" in d:
            csv = [d.split(" ")[1:]]
            list_gather = True
        elif len(d) > 0 and d[0] == "#":
            if list_gather:
                hashed_cols = sha256(csv[0].__str__().encode()).hexdigest()
                tables[hashed_cols] = tables[hashed_cols] + csv[1:] if hashed_cols in tables else csv
            list_gather = False
            continue
        elif list_gather:
            row = d.split(" ")
            csv.append(row if len(csv[0]) == len(row) else [''] * len(csv[0]))
    
    tables[hashed_cols] = tables[hashed_cols] + csv[1:] if hashed_cols in tables else csv

    return tables

def dataframe_generator(**kwargs):
    start = time.time()
    print("[+] Starting...")
    
    session = boto3.Session(
        aws_access_key_id=Variable.get('aws_access_key_id'), 
        aws_secret_access_key=Variable.get('aws_secret_access_key'),
        aws_session_token=Variable.get('aws_session_token')
    )
    
    # Create an S3 client
    s3 = session.resource('s3')

    # Specify the bucket name and folder path
    bucket_name = Variable.get('bucket_name')
    
    bucket = s3.Bucket(bucket_name)
    tables = dict()
    for obj in bucket.objects.all():
        key = obj.key
        if key.split('.')[-1].lower() == 'log':
            raw_log_data =  [r for r in obj.get()['Body'].read().decode('utf-8').split("\n")]
            tables = generate_csv(raw_log_data, tables)

    dfs = [
        pd.DataFrame(v[1:], columns=v[0]) for v in tables.values()    
    ]

    merged_df = pd.concat(dfs, ignore_index=True)

    print(f"Time to process: {time.time() - start}")    

    return merged_df.to_json()


if __name__ == "__main__":
    dataframe_generator()