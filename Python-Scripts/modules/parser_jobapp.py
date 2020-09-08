#!/usr/bin/env python
# coding: utf-8
# parser_jobapp.py
# Author: Kevin Solano


from modules.create_sparkSession import create_spark_session
from pyspark.sql.functions import *
import configparser

def parser_jobapp():
    """    
    Parse json file and save the entities in the staging layer
    """
  
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')    
    spark = create_spark_session()

    df_raw = spark.read.option("multiline", "true").json(config['AWS']['S3_BUCKET']+"/raw/jobs/*.json")    
    # Comment the next line (only for testing)
    # df_raw = df_raw.limit(10)
    # job posts
    df_jobposts = df_raw.select("id","adverts.id", "adverts.activeDays", "adverts.publicationDateTime",
                               "adverts.applyUrl","adverts.status", "city", "company", "sector", "title")
    df_jobposts = df_jobposts.toDF("id","advertId", "activeDays", "publicationDateTime", "applyUrl", 
                                 "status", "city", "company", "sector", "title")
    df_jobposts.show(5)
    # job benefits
    df_jobbenefits = df_raw.select("id",explode("benefits")).withColumnRenamed("Col", "benefit")
    df_jobbenefits.show(5)
    # job applicants by skill
    df_applicants = df_raw.select("id",posexplode("applicants")).select("id","pos","col.firstName", "col.lastName", "col.age", "col.applicationDate", explode("col.skills"))
    df_applicants = df_applicants.toDF("id","pos", "firstName", "lastName", "age", "applicationDate", "skills")
    df_applicants.show(5)
    
    # save stagings
    df_jobposts.write.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/jobposts", mode="overwrite")
    df_jobbenefits.write.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/jobbenefits", mode="overwrite")
    df_applicants.write.parquet(config['AWS']['S3_BUCKET']+"/staging/jobs/applicants", mode="overwrite")
    
    print("End parser")