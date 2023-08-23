import requests
import boto3
import pandas as pd
import io
import psycopg2
import logging

insert_table='jobs_postings'
s3_bucket='toggltest'

def get_secrets():
    with open('secrets.txt') as s_file:
        secrets = s_file.readlines()
        secrets=[s.strip('\n') for s in secrets]
        authKey=secrets[0]
        KEY_ID=secrets[1]
        ACCESS_KEY=secrets[2]
        db_pass=secrets[3]
        secrets_map={}
        secrets_map['authKey']=authKey
        secrets_map['KEY_ID']=KEY_ID
        secrets_map['ACCESS_KEY']=ACCESS_KEY
        secrets_map['db_pass']=db_pass
        return secrets_map                   

def get_postings(secrets_map):
    host = 'data.usajobs.gov'
    userAgent = 'ashwshanker.1990@gmail.com'
    authKey = 'gh3jLR00nD4jPbv2Z4GUEwkWfGtyQvz96Kc3JeHTDa0='
 
    req_url = "https://data.usajobs.gov/api/search?PositionTitle=Data&LocationName=Omaha,%20Nebraska"
 
    api_headers= {          
        "Host": host,          
        "User-Agent": userAgent,          
        "Authorization-Key": secrets_map['authKey']     
    }  
    try:
        resp = requests.get(req_url, headers=api_headers)
    except Excpetion as e:
        logging.error(e)
    data = resp.json()
    return data

def write_db(data,secrets_map):
    s3 = boto3.client(
    's3',
    region_name='us-west-1',
    aws_access_key_id=secrets_map['KEY_ID'],
    aws_secret_access_key=secrets_map['ACCESS_KEY']
)
    content_df=pd.DataFrame(data)
    with io.StringIO() as csv_buffer:
        content_df.to_csv(csv_buffer, index=False)
        s3.put_object(
        Bucket=s3_bucket, Key="jobs_usa.csv", Body=csv_buffer.getvalue()
        )

    logging.info("Written to S3...")
    db_conn = psycopg2.connect(dbname='dev',user='root', host='redshift-cluster-1.c2og61pim4cn.us-west-1.redshift.amazonaws.com', password=secrets_map['db_pass'],port=5439)
    copy_st = '''copy {table_name} from 's3://{s3_bucket}/jobs_usa.csv'
CREDENTIALS 'aws_access_key_id={access};aws_secret_access_key={secret}' delimiter ',' removequotes'''.format(table_name=insert_table,
                                                                                                             s3_bucket=s3_bucket,
                                                                                                             access=secrets_map['KEY_ID'],
                                                                                                             secret=secrets_map['ACCESS_KEY']
                                                                                                             )
    logging.info("Writing to db...")
    with db_conn.cursor() as curs:
         curs.execute(copy_st)
         logging.info("Copy done...")

if __name__=='__main__':
    secrets_map = get_secrets()
    data = get_postings(secrets_map)
    jobs = data['SearchResult']['SearchResultItems']
    jobs = [job['MatchedObjectDescriptor'] for job in jobs]
    jobs_db_insert = []
    for job in jobs:
        jobs_db=[job['PositionTitle'], job['PositionURI'], job['PositionLocationDisplay'],
                 float(job['PositionRemuneration'][0]['MinimumRange']), float(job['PositionRemuneration'][0]['MaximumRange'])
                 ]
        jobs_db_insert.append(jobs_db)
    write_db(jobs_db_insert, secrets_map)
    
