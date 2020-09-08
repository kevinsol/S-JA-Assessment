#!/usr/bin/env python
# coding: utf-8
# load_db_redshift.py
# Author: Kevin Solano


from modules.create_sparkSession import create_spark_session
import configparser
import psycopg2

def load_data_to_redshift():
    """    
    load s3 presentation layer into redshift
    """
  
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')    
    spark = create_spark_session()
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT'].values()))
    cur = conn.cursor()
    S3 = config.get('AWS','S3_BUCKETR')
    ARN = config.get('IAM_ROLE','ARN')
    clean_query = ["TRUNCATE TABLE dim_city;"]

    load_query = [("""  COPY dim_city from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/dim_city",ARN)]
    # Clean data tables
    for query in clean_query:
        cur.execute(query)
        conn.commit()
    
    # load data in redshift    
    for query in load_query:
        cur.execute(query)
        conn.commit()

    conn.close()    