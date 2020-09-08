#!/usr/bin/env python
# coding: utf-8
# load_fact.py
# Author: Kevin Solano


from modules.create_sparkSession import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql import types as t
import configparser
      

def load_fact_table():
    """    
    load data of applicant and skill bridge table
    """
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()
    # Reading staging Datasets
    df_stg_jobpost = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/jobposts")
    df_stg_applicant = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/applicants")
    # Reading presentation layer Datasets
    df_dim_app = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_applicant")
    df_dim_job = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_jobpost")
    df_dim_jtl = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_jobtitle")
    df_dim_com = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_company")
    df_dim_cit = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_city")
    df_dim_sta = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_status")    
    
    if loadType=="full":
        # Transform applications
        df_stg_apps = df_stg_applicant.select("id","pos","firstName", "lastName", "age", "applicationDate").distinct()\
        .withColumn("appDate", col("applicationDate").cast(t.DoubleType()))
        df_stg_apps = df_stg_apps\
        .withColumn("application_date_key",date_format(df_stg_apps.appDate.cast(dataType=t.TimestampType()), "yyyymmdd"))\
        .withColumn("application_time_key",date_format(df_stg_apps.appDate.cast(dataType=t.TimestampType()), "HHmm"))\
        .withColumn("job_quantity", when(col("pos") == 1, 1).otherwise(0))\
        .withColumn("applicants_quantity", lit(1))\
        .withColumnRenamed("age", "applicant_age")\
        .withColumnRenamed("id", "jobid")
        
        df_stg_applications = df_stg_apps.join(df_dim_app,
                                           [df_stg_apps["firstName"] == df_dim_app["firstname"],
                                            df_stg_apps["lastName"] == df_dim_app["lastname"]], "inner")\
        .select("jobid","applicant_key", "application_date_key", "application_time_key", "applicant_age",
               "job_quantity", "applicants_quantity")
        
        # Get job keys
        df_stg_jobs = df_stg_jobpost.join(df_dim_job,df_stg_jobpost["id"]==df_dim_job["jobpost_id"],"left")\
        .drop("advertId", "applyUrl", "jobpost_id", "advert_id", "advert_applyurl", "created_date")
        
        df_stg_jobs_t = df_stg_jobs.join(df_dim_jtl,df_stg_jobs["title"]==df_dim_jtl["title_name"],"left")\
        .drop("title", "title_name", "created_date")
        
        df_stg_jobs_t_c = df_stg_jobs_t.join(df_dim_com,
                                             [df_stg_jobs_t["company"]==df_dim_com["company_name"],
                                              df_stg_jobs_t["sector"]==df_dim_com["sector_name"]],"left")\
        .drop("company", "company_name", "sector","sector_name","created_date")
        
        df_stg_jobs_t_c_s = df_stg_jobs_t_c.join(df_dim_sta,df_stg_jobs_t_c["status"]==df_dim_sta["status_name"],"left")\
        .drop("status", "status_name", "created_date")
        
        df_stg_jobs_t_c_s_c = df_stg_jobs_t_c_s.join(df_dim_cit,df_stg_jobs_t_c_s["city"]==df_dim_cit["city_name"],"left")\
        .drop("city", "city_name", "created_date")
        
        df_stg_jobs_apps = df_stg_jobs_t_c_s_c.join(df_stg_applications,
                                                    df_stg_jobs_t_c_s_c["id"]==df_stg_applications["jobid"],"left")\
        .drop("id", "jobid")
        
        df_stg_jobs_apps = df_stg_jobs_apps.withColumn("pubDate", col("publicationDateTime").cast(t.DoubleType()))
        df_stg_jobs_apps = df_stg_jobs_apps \
        .withColumn("publication_date_key",date_format(df_stg_jobs_apps.pubDate.cast(dataType=t.TimestampType()), "yyyymmdd")) \
        .withColumn("publication_time_key",date_format(df_stg_jobs_apps.pubDate.cast(dataType=t.TimestampType()), "HHmm")) \
        .withColumnRenamed("activeDays", "activedays") \
        .withColumn("created_date", current_date()) \
        .fillna({'jobpost_key': '-1'}) \
        .fillna({'title_key': '-1'}) \
        .fillna({'applicant_key': '-1'}) \
        .fillna({'company_key': '-1'}) \
        .fillna({'status_key': '-1'}) \
        .fillna({'city_key': '-1'}) \
        .fillna({'publication_date_key': '-1'}) \
        .fillna({'publication_time_key': '-1'}) \
        .fillna({'application_date_key': '-1'}) \
        .fillna({'application_time_key': '-1'}) \
        .fillna({'job_quantity': '1'}) \
        .fillna({'applicants_quantity': '0'}) \
        .select('jobpost_key','applicant_key','title_key','company_key', 'publication_date_key', 'publication_time_key',
                'application_date_key', 'application_time_key', 'status_key','city_key', 'applicant_age', 'applicants_quantity',
                'job_quantity', 'activedays', 'created_date')
        
        df_stg_jobs_apps.show(5)
        
        # load bridge
        df_stg_jobs_apps.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/fact_job_applications",
                                     mode="overwrite")
        
    else:
        # Reading actual fact
        df_act_fact = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/fact_job_applications")
        
        # Transform applications
        df_stg_apps = df_stg_applicant.select("id","pos","firstName", "lastName", "age", "applicationDate").distinct()\
        .withColumn("appDate", col("applicationDate").cast(t.DoubleType()))
        df_stg_apps = df_stg_apps\
        .withColumn("application_date_key",date_format(df_stg_apps.appDate.cast(dataType=t.TimestampType()), "yyyymmdd"))\
        .withColumn("application_time_key",date_format(df_stg_apps.appDate.cast(dataType=t.TimestampType()), "HHmm"))\
        .withColumn("job_quantity", when(col("pos") == 1, 1).otherwise(0))\
        .withColumn("applicants_quantity", lit(1))\
        .withColumnRenamed("age", "applicant_age")\
        .withColumnRenamed("id", "jobid")
        
        df_stg_applications = df_stg_apps.join(df_dim_app,
                                           [df_stg_apps["firstName"] == df_dim_app["firstname"],
                                            df_stg_apps["lastName"] == df_dim_app["lastname"]], "inner")\
        .select("jobid","applicant_key", "application_date_key", "application_time_key", "applicant_age",
               "job_quantity", "applicants_quantity")
        
        # Get job keys
        df_stg_jobs = df_stg_jobpost.join(df_dim_job,df_stg_jobpost["id"]==df_dim_job["jobpost_id"],"left")\
        .drop("advertId", "applyUrl", "jobpost_id", "advert_id", "advert_applyurl", "created_date")
        
        df_stg_jobs_t = df_stg_jobs.join(df_dim_jtl,df_stg_jobs["title"]==df_dim_jtl["title_name"],"left")\
        .drop("title", "title_name", "created_date")
        
        df_stg_jobs_t_c = df_stg_jobs_t.join(df_dim_com,
                                             [df_stg_jobs_t["company"]==df_dim_com["company_name"],
                                              df_stg_jobs_t["sector"]==df_dim_com["company_sector"]],"left")\
        .drop("company", "company_name", "sector","company_sector","created_date")
        
        df_stg_jobs_t_c_s = df_stg_jobs_t_c.join(df_dim_sta,df_stg_jobs_t_c["status"]==df_dim_sta["status_name"],"left")\
        .drop("status", "status_name", "created_date")
        
        df_stg_jobs_t_c_s_c = df_stg_jobs_t_c_s.join(df_dim_cit,df_stg_jobs_t_c_s["city"]==df_dim_cit["city_name"],"left")\
        .drop("city", "city_name", "created_date")
        
        df_stg_jobs_apps = df_stg_jobs_t_c_s_c.join(df_stg_applications,
                                                    df_stg_jobs_t_c_s_c["id"]==df_stg_applications["jobid"],"left")\
        .drop("id", "jobid")
        
        df_stg_jobs_apps = df_stg_jobs_apps.withColumn("pubDate", col("publicationDateTime").cast(t.DoubleType()))
        df_stg_jobs_apps = df_stg_jobs_apps \
        .withColumn("publication_date_key",date_format(df_stg_jobs_apps.pubDate.cast(dataType=t.TimestampType()), "yyyymmdd")) \
        .withColumn("publication_time_key",date_format(df_stg_jobs_apps.pubDate.cast(dataType=t.TimestampType()), "HHmm")) \
        .withColumnRenamed("activeDays", "activedays") \
        .withColumn("created_date", current_date()) \
        .fillna({'jobpost_key': '-1'}) \
        .fillna({'title_key': '-1'}) \
        .fillna({'applicant_key': '-1'}) \
        .fillna({'company_key': '-1'}) \
        .fillna({'status_key': '-1'}) \
        .fillna({'city_key': '-1'}) \
        .fillna({'publication_date_key': '-1'}) \
        .fillna({'publication_time_key': '-1'}) \
        .fillna({'application_date_key': '-1'}) \
        .fillna({'application_time_key': '-1'}) \
        .fillna({'job_quantity': '1'}) \
        .fillna({'applicants_quantity': '0'}) \
        .select('jobpost_key','applicant_key','title_key','company_key', 'publication_date_key', 'publication_time_key',
                'application_date_key', 'application_time_key', 'status_key','city_key', 'applicant_age', 'applicants_quantity',
                'job_quantity', 'activedays', 'created_date')
        
        df_stg_jobs_apps.show(5)
                
        
        # identify delta
        df_delta_fact = df_stg_jobs_apps.join(df_act_fact,
                                             [df_stg_jobs_apps["applicant_key"] == df_act_fact["applicant_key"],
                                              df_stg_jobs_apps["jobpost_key"] == df_act_fact["jobpost_key"]],
                                             "leftanti") \
        .select('jobpost_key','applicant_key','title_key','company_key', 'publication_date_key', 'publication_time_key',
                'application_date_key', 'application_time_key', 'status_key','city_key', 'applicant_age', 'applicants_quantity',
                'job_quantity', 'activedays', 'created_date')

        df_new_fact = df_act_fact.union(df_delta_fact)

        df_new_fact.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/fact_job_applications", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/fact_job_applications") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/fact_job_applications", mode="overwrite")
        