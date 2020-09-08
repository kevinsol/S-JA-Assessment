#!/usr/bin/env python
# coding: utf-8
# load_dims.py
# Author: Kevin Solano


from modules.create_sparkSession import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql import types as t
import configparser

def load_dim_city():
    """    
    load data of city dimension
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Reading staging jobposts Dataset
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/jobposts")
        
    if loadType=="full":
        #DIM CITY        
        # Transform dim
        df_dim = df_stg_jobposts.select("city").distinct() \
            .withColumn("city_key", expr("uuid()")) \
            .withColumnRenamed("city", "city_name") \
            .withColumn("created_date", current_date()) \
            .select("city_key", "city_name", "created_date")
        df_dim.show(5)
        
        # load dim
        df_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/dim_city", mode="overwrite")
                
    else:        
        # Reading Datasets        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_city")

        # Transform dim
        df_dim = df_stg_jobposts.select("city").distinct() \
            .withColumnRenamed("city", "city_name") \
            .withColumn("created_date", current_date()) \
            .select("city_name", "created_date")                
        
        # identify delta
        df_delta_dim = df_dim.join(df_act_dim, df_dim["city_name"] == df_act_dim["city_name"],"leftanti") \
            .withColumn("city_key", expr("uuid()")) \
            .select("city_key", "city_name", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/dim_city", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/dim_city") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_city", mode="overwrite")


def load_dim_jobtitle():
    """    
    load data of job title dimension
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Reading staging jobposts Dataset
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/jobposts")
    
    if loadType=="full":        
        # Transform dim
        df_dim = df_stg_jobposts.select("title").distinct() \
            .withColumn("title_key", expr("uuid()")) \
            .withColumnRenamed("title", "title_name") \
            .withColumn("created_date", current_date()) \
            .select("title_key", "title_name", "created_date")
        df_dim.show(5)
        
        # load dim
        df_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/dim_jobtitle", mode="overwrite")
        
    else:        
        # Reading Datasets        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_jobtitle")

        # Transform dim
        df_dim = df_stg_jobposts.select("title").distinct() \
            .withColumnRenamed("title", "title_name") \
            .withColumn("created_date", current_date()) \
            .select("title_name", "created_date")               
        
        # identify delta
        df_delta_dim = df_dim.join(df_act_dim, df_dim["title_name"] == df_act_dim["title_name"],"leftanti") \
            .withColumn("title_key", expr("uuid()")) \
            .select("title_key", "title_name", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/dim_jobtitle", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/dim_jobtitle") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_jobtitle", mode="overwrite")

def load_dim_company():
    """    
    load data of company dimension
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Reading staging jobposts Dataset
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/jobposts")
        
    if loadType=="full":
        #DIM COMPANY        
        # Transform dim
        df_dim = df_stg_jobposts.select("company", "sector").distinct() \
            .withColumn("company_key", expr("uuid()")) \
            .withColumnRenamed("company", "company_name") \
            .withColumnRenamed("sector", "sector_name") \
            .withColumn("created_date", current_date()) \
            .select("company_key", "company_name", "sector_name","created_date")
        df_dim.show(5)
        
        # load dim
        df_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/dim_company", mode="overwrite")
                
    else:        
        # Reading Datasets        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_company")

        # Transform dim
        df_dim = df_stg_jobposts.select("company", "sector").distinct() \
            .withColumnRenamed("company", "company_name") \
            .withColumnRenamed("sector", "sector_name") \
            .withColumn("created_date", current_date()) \
            .select("company_name", "sector_name","created_date")                
        
        # identify delta
        df_delta_dim = df_dim.join(df_act_dim, df_dim["company_name"] == df_act_dim["company_name"],"leftanti") \
            .withColumn("company_key", expr("uuid()")) \
            .select("company_key", "company_name", "sector_name","created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/dim_company", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/dim_company") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_company", mode="overwrite")
    
def load_dim_status():
    """    
    load data of status dimension
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Reading staging jobposts Dataset
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/jobposts")
        
    if loadType=="full":
        #DIM status        
        # Transform dim
        df_dim = df_stg_jobposts.select("status").distinct() \
            .withColumn("status_key", expr("uuid()")) \
            .withColumnRenamed("status", "status_name") \
            .withColumn("created_date", current_date()) \
            .select("status_key", "status_name", "created_date")
        df_dim.show(5)
        
        # load dim
        df_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/dim_status", mode="overwrite")
                
    else:        
        # Reading Datasets        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_status")

        # Transform dim
        df_dim = df_stg_jobposts.select("status").distinct() \
            .withColumnRenamed("status", "status_name") \
            .withColumn("created_date", current_date()) \
            .select("status_name", "created_date")               
        
        # identify delta
        df_delta_dim = df_dim.join(df_act_dim, df_dim["status_name"] == df_act_dim["status_name"],"leftanti") \
            .withColumn("status_key", expr("uuid()")) \
            .select("status_key", "status_name", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/dim_status", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/dim_status") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_status", mode="overwrite")

def load_dim_jobpost():
    """    
    load data of jobpost dimension
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Reading staging jobposts Dataset
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/jobposts")
        
    if loadType=="full":
        #DIM JOB POST        
        # Transform dim
        df_dim = df_stg_jobposts.select("id", "advertId","applyUrl").distinct() \
            .withColumn("jobpost_key", expr("uuid()")) \
            .withColumnRenamed("id", "jobpost_id") \
            .withColumnRenamed("advertId", "advert_id") \
            .withColumnRenamed("applyUrl", "advert_applyurl") \
            .withColumn("created_date", current_date()) \
            .select("jobpost_key", "jobpost_id", "advert_id", "advert_applyurl", "created_date") 
        df_dim.show(5)
        
        # load dim
        df_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/dim_jobpost", mode="overwrite")
                
    else:        
        # Reading Datasets        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_jobpost")

        # Transform dim
        df_dim = df_stg_jobposts.select("id", "advertId","applyUrl").distinct() \
            .withColumnRenamed("id", "jobpost_id") \
            .withColumnRenamed("advertId", "advert_id") \
            .withColumnRenamed("applyUrl", "advert_applyurl") \
            .withColumn("created_date", current_date()) \
            .select("jobpost_id", "advert_id", "advert_applyurl", "created_date")                
        
        # identify delta
        df_delta_dim = df_dim.join(df_act_dim, df_dim["jobpost_id"] == df_act_dim["jobpost_id"],"leftanti") \
            .withColumn("jobpost_key", expr("uuid()")) \
            .select("jobpost_key", "jobpost_id", "advert_id", "advert_applyurl", "created_date") 

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/dim_jobpost", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/dim_jobpost") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_jobpost", mode="overwrite")
        
def load_dim_benefit():
    """    
    load data of benefit dimension
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Reading staging jobposts Dataset
    df_stg_benefits = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/jobbenefits")
        
    if loadType=="full":
        #DIM BENEFITS        
        # Transform dim
        df_dim = df_stg_benefits.select("benefit").distinct() \
            .withColumn("benefit_key", expr("uuid()")) \
            .withColumnRenamed("benefit", "benefit_name") \
            .withColumn("created_date", current_date()) \
            .select("benefit_key", "benefit_name", "created_date")
        df_dim.show(5)
        
        # load dim
        df_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/dim_benefit", mode="overwrite")
                
    else:        
        # Reading Datasets        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_benefit")

        # Transform dim
        df_dim = df_stg_benefits.select("benefit").distinct() \
            .withColumnRenamed("benefit", "benefit_name") \
            .withColumn("created_date", current_date()) \
            .select("benefit_name", "created_date")
        
        # identify delta
        df_delta_dim = df_dim.join(df_act_dim, df_dim["benefit_name"] == df_act_dim["benefit_name"],"leftanti") \
            .withColumn("benefit_key", expr("uuid()")) \
            .select("benefit_key", "benefit_name", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/dim_benefit", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/dim_benefit") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_benefit", mode="overwrite")

def load_dim_applicant():
    """    
    load data of applicant dimension
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Reading staging jobposts Dataset
    df_stg_applicant = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/applicants")
        
    if loadType=="full":
        #DIM APPLICANT        
        # Transform dim 
        df_dim = df_stg_applicant.select("firstName", "lastName").distinct() \
            .withColumn("applicant_key", expr("uuid()")) \
            .withColumnRenamed("firstName", "firstname") \
            .withColumnRenamed("lastName", "lastname") \
            .withColumn("created_date", current_date()) \
            .select("applicant_key", "firstname", "lastname", "created_date")
        df_dim.show(5)
        
        # load dim
        df_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/dim_applicant", mode="overwrite")
        
    else:        
        # Applicant dim
        # Reading Datasets
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_applicant")

        # Transform dim
        df_dim = df_stg_benefits.select("firstName", "lastName").distinct() \
            .withColumnRenamed("firstName", "firstname") \
            .withColumnRenamed("lastName", "lastname") \
            .withColumn("created_date", current_date()) \
            .select("firstname", "lastname", "created_date")
        
        # identify delta
        df_delta_dim = df_dim.join(df_act_dim, [df_dim["firstname"] == df_act_dim["firstname"],
                                   df_dim["lastname"] == df_act_dim["lastname"]],"leftanti") \
            .withColumn("applicant_key", expr("uuid()")) \
            .select("applicant_key", "firstname", "lastname", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/dim_applicant", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/dim_applicant") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_applicant", mode="overwrite")


def load_dim_skill():
    """    
    load data of skill dimension
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Reading staging jobposts Dataset
    df_stg_applicant = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/applicants")
        
    if loadType=="full":
        #DIM SKILLS        
        # Transform dim 
        df_dim = df_stg_applicant.select("skills").distinct() \
            .withColumn("skill_key", expr("uuid()")) \
            .withColumnRenamed("skills", "skill_name") \
            .withColumn("created_date", current_date()) \
            .select("skill_key", "skill_name", "created_date")
        df_dim.show(5)
        
        # load dim
        df_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/dim_skill", mode="overwrite")
        
    else:                
        # Reading Datasets        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_skill")

        # Transform dim
        df_dim = df_stg_benefits.select("skills").distinct() \
            .withColumnRenamed("skills", "skill_name") \
            .withColumn("created_date", current_date()) \
            .select("skill_name", "created_date")
        
        # identify delta
        df_delta_dim = df_dim.join(df_act_dim, df_dim["skill_name"] == df_act_dim["skill_name"],"leftanti") \
            .withColumn("skill_key", expr("uuid()")) \
            .select("skill_key", "skill_name", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/dim_skill", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/dim_skill") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/dim_skill", mode="overwrite")
                