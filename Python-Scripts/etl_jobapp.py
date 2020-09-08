#!/usr/bin/env python
# coding: utf-8
# etl_jobapp.py
# Author: Kevin Solano
# Proceso ETL principal

from modules.load_dims import *
from modules.load_bridges import *
from modules.load_fact import *
from modules.load_db_redshift import load_data_to_redshift
from modules.parser_jobapp import parser_jobapp
import configparser

def main():
    """    
    ETL proccess
    """
    # Parameter file
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/KevinSolano/parameters.cfg')

    
    startstep = int(config.get('PARAM','STARTSTEP'))
    endstep = int(config.get('PARAM','ENDSTEP'))
        
    step = 1
    if step >= startstep and step <= endstep:
        print("1. Extract and parse raw job applications data into staging layer")
        parser_jobapp()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("2. Transform and load job-post dimension")
        load_dim_jobpost()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("3. Transform and load job title dimension")
        load_dim_jobtitle()
            
    step = step +1
    if step >= startstep and step <= endstep:
        print("4. Transform and load city dimension")
        load_dim_city()
            
    step = step +1
    if step >= startstep and step <= endstep:
        print("5. Transform and load status dimension")
        load_dim_status()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("6. Transform and load company dimension")
        load_dim_company()    
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("7. Transform and load benefit dimension")
        load_dim_benefit()
            
    step = step +1
    if step >= startstep and step <= endstep:
        print("8. Transform and load applicant dimension")
        load_dim_applicant()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("9. Transform and load skills dimension")
        load_dim_skill()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("10. Transform and load bridge job benefits")
        load_bridge_jobpost_benefits()
            
    step = step +1
    if step >= startstep and step <= endstep:
        print("11. Transform and load bridge applicant skills")
        load_bridge_applicant_skills()
    
    step = step +1
    if step >= startstep and step <= endstep:
        print("12. Transform and load fact table")
        load_fact_table()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("13. Load dimensional model into redshift's presentation layer")
        load_data_to_redshift()
    

if __name__ == "__main__":
    main()