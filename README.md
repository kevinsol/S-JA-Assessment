# S-JA-Assessment

The dataset consists of a sample of 30,000 jobs from 2010 to 2020 that were posted through the Armenian human resource portal CareerCenter

## a.	Data profiling results 
The data profile was made with R, please refer to html file Data-Profiling. Also the rmarkdown code was uploaded, next you have a summary:

![Alt text](https://github.com/kevinsol/S-JA-Assessment/blob/master/a_Data%20profiling%20results/SummaryProfiling.PNG?raw=true "Title")

## b.	Dataflow explanation and diagram
The arquitecture for this project was divided in the next 3 layers:

### Raw
S3, where we assuming that the jobApplications json files are pre-loaded for other processs

### Staging
- **S3** for data storage

### Presentation 
- **S3** for data storage
- **Redshift** for data exposition

![Alt text](https://github.com/kevinsol/S-JA-Assessment/blob/master/b_Dataflow%20diagram/DataflowDiagram.PNG?raw=true "Title")

The solution was implemented with:
- **Spark** to run massive data transformations
- **Python** to write transformations


## c.	Data dictionary for dimension and fact tables
Please refer to the respective folder where you can find

- Data dictionary for each entity: dimension, bridge, fact
- Business rules applied for each field from the json sourcefile


## d.	Dimensional model diagram
The business requirements have been transformed in the next dimensional model:

![Alt text](https://github.com/kevinsol/S-JA-Assessment/blob/master/d_Dimensional%20model/BD_dimensional_model_diagram.PNG?raw=true "Title")

The high level diagram:

![Alt text](https://github.com/kevinsol/S-JA-Assessment/blob/master/d_Dimensional%20model/BD_high_level_diagram.PNG?raw=true "Title")


## e.	Explanation of project structure
The next diagram shows the ETL process flow:

![Alt text](https://github.com/kevinsol/S-JA-Assessment/blob/master/e_Project%20Structure/ETLDiagram.PNG?raw=true "Title")

The project code structure:

![Alt text](https://github.com/kevinsol/S-JA-Assessment/blob/master/e_Project%20Structure/ProjectStructure_1.PNG?raw=true "Title")

![Alt text](https://github.com/kevinsol/S-JA-Assessment/blob/master/e_Project%20Structure/ProjectStructure_2.PNG?raw=true "Title")

Where we have the next files:
- **etl_jobapp.py** Script that orchestrate the ETL process
- **parameters.cfg** Contain all the parameters used in the scripts
- modules/**create_sparkSession.py** Script used to create a spark session
- modules/**parser_jobapp.py** Parse the json file and generate the staging entities
- modules/**load_dims.py** Script than contains all the functions to transform and load the dimensions
- modules/**load_bridges.py** Script than contains all the functions to transform and load the bridges tables
- modules/**load_fact_table.py** Script that transform and load the data to the fact table
- modules/**load_db_redshift.py** Copy from the S3 presentation storage layer to the redshift presentation layer

Thanks!
