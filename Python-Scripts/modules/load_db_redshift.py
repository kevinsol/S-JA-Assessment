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
    clean_query = ["TRUNCATE TABLE dim_applicant;",
                  "TRUNCATE TABLE dim_benefit;",
                  "TRUNCATE TABLE dim_city;",
                  "TRUNCATE TABLE dim_company;",
                  "TRUNCATE TABLE dim_jobpost;",
                  "TRUNCATE TABLE dim_jobtitle;",
                  "TRUNCATE TABLE dim_skill;",
                  "TRUNCATE TABLE dim_status;",
                  "TRUNCATE TABLE bridge_applicant_skills;",
                  "TRUNCATE TABLE bridge_jobpost_benefits;",
                  "TRUNCATE TABLE fact_job_applications;"]

    load_query = [("""  COPY dim_applicant from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/dim_applicant",ARN),
                 ("""  COPY dim_benefit from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/dim_benefit",ARN),
                 ("""  COPY dim_city from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/dim_city",ARN),
                 ("""  COPY dim_company from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/dim_company",ARN),
                 ("""  COPY dim_jobpost from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/dim_jobpost",ARN),
                 ("""  COPY dim_jobtitle from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/dim_jobtitle",ARN),
                 ("""  COPY dim_skill from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/dim_skill",ARN),
                  ("""  COPY dim_status from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/dim_status",ARN),
                  ("""  COPY bridge_applicant_skills from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/bridge_applicant_skills",ARN),
                 ("""  COPY bridge_jobpost_benefits from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/bridge_jobpost_benefits",ARN),
                 ("""  COPY fact_job_applications from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/jobs/fact_job_applications",ARN)]
    
    # Clean data tables
    for query in clean_query:
        cur.execute(query)
        conn.commit()
    
    # load data in redshift    
    for query in load_query:
        cur.execute(query)
        conn.commit()

    conn.close()    