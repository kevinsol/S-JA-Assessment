#!/usr/bin/env python
# coding: utf-8
# load_bridges.py
# Author: Kevin Solano


from modules.create_sparkSession import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql import types as t
import configparser
      
def load_bridge_jobpost_benefits():
    """    
    load data of jobpost-benefit bridge table
    """
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()
    # Reading staging Dataset
    df_stg_benefits = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/jobbenefits")
    # Reading presentation layer Datasets
    df_dim_benefit = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_benefit")
    df_dim_jobpost = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_jobpost")
        
    if loadType=="full":
        
        # Transform dim
        df_stg_ben = df_stg_benefits.join(df_dim_benefit,df_stg_benefits["benefit"]==df_dim_benefit["benefit_name"],"inner")\
        .drop("benefit","benefit_name","created_date")
        
        df_stg_ben_job = df_stg_ben.join(df_dim_jobpost,df_dim_jobpost["jobpost_id"]==df_stg_ben["id"],"inner")\
        .select("jobpost_key", "benefit_key")
    
        df_stg_ben_job.show(5)
        
        # load bridge
        df_stg_ben_job.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/bridge_jobpost_benefits",
                                     mode="overwrite")
                        
    else:
        # Reading Datasets
        df_act_bridge = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/bridge_jobpost_benefits")

        # Transform dim
        df_stg_ben = df_stg_benefits.join(df_dim_benefit,df_stg_benefits["benefit"]==df_dim_benefit["benefit_name"],"inner")\
        .drop("benefit","benefit_name","created_date")
        
        df_stg_ben_job = df_stg_ben.join(df_dim_jobpost,df_dim_jobpost["jobpost_id"]==df_stg_ben["id"],"inner")\
        .drop("id","jobpost_id", "advert_id", "advert_applyurl", "created_date")
    
        df_stg_ben_job.show(5)
        
        # identify delta
        df_delta_bridge = df_stg_ben_job.join(df_act_bridge,
                                              [df_stg_ben_job["jobpost_key"] == df_act_bridge["jobpost_key"],
                                               df_stg_ben_job["benefit_key"] == df_act_bridge["benefit_key"]],
                                              "leftanti") \
            .select("jobpost_key", "benefit_key")

        df_new_bridge = df_act_bridge.union(df_delta_bridge)

        df_new_bridge.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/bridge_jobpost_benefits", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/bridge_jobpost_benefits") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/bridge_jobpost_benefits", mode="overwrite")

def load_bridge_applicant_skills():
    """    
    load data of applicant and skill bridge table
    """
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()
    # Reading staging aplicants Dataset
    df_stg_applicant = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/applicants")
    # Reading presentation layer Datasets
    df_dim_app = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_applicant")
    df_dim_ski = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_skill")
    df_dim_job = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/jobs/dim_jobpost")
    
    if loadType=="full":
        # Transform dim
        df_stg_app = df_stg_applicant.join(df_dim_app,
                                           [df_stg_applicant["firstName"] == df_dim_app["firstname"],
                                            df_stg_applicant["lastName"] == df_dim_app["lastname"]], "inner")\
        .select("applicant_key", "skills", "id")
        
        df_stg_app_ski = df_stg_app.join(df_dim_ski,df_stg_app["skills"]==df_dim_ski["skill_name"],"inner")\
        .select("applicant_key", "skill_key", "id")
        
        df_stg_bridge = df_stg_app_ski.join(df_dim_job,df_stg_app_ski["id"]==df_dim_job["jobpost_id"],"inner")\
        .select("applicant_key", "skill_key", "jobpost_key")
        
        df_stg_bridge.show(5)
        
        # load bridge
        df_stg_bridge.write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/jobs/bridge_applicant_skills",
                                     mode="overwrite")
        
    else:
        # Reading Datasets
        df_act_bridge = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/bridge_applicant_skills")
        
        # Transform dim
        df_stg_app = df_stg_applicant.join(df_dim_app,
                                           [df_stg_applicant["firstName"] == df_dim_app["firstname"],
                                            df_stg_applicant["lastName"] == df_dim_app["lastname"]], "inner")\
        .select("applicant_key", "skills", "id")
        
        df_stg_app_ski = df_stg_app.join(df_dim_ski,df_stg_app["skills"]==df_dim_ski["skill_name"],"inner")\
        .select("applicant_key", "skill_key", "id")
        
        df_stg_bridge = df_stg_app_ski.join(df_dim_job,df_stg_app_ski["id"]==df_dim_job["jobpost_id"],"inner")\
        .select("applicant_key", "skill_key", "jobpost_key")
        
        # identify delta
        df_delta_bridge = df_stg_bridge.join(df_act_bridge,
                                             [df_stg_bridge["applicant_key"] == df_act_bridge["applicant_key"],
                                              df_stg_bridge["skill_key"] == df_act_bridge["skill_key"],
                                              df_stg_bridge["jobpost_key"] == df_act_bridge["jobpost_key"]],
                                             "leftanti") \
        .select("applicant_key", "skill_key", "jobpost_key")

        df_new_bridge = df_act_bridge.union(df_delta_bridge)

        df_new_bridge.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/jobs/bridge_applicant_skills", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/jobs/bridge_applicant_skills") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/jobs/bridge_applicant_skills", mode="overwrite")
        