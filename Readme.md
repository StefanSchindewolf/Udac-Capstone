# Overview

This Readme gives only a rough overview about the files in this workspace/ folder.
For a detailed description of the project please check the write-up in [notebook no. 05](05_WriteUp.ipynb)

The Udacity Capstone Project is separated into the following files:

| File | Description |
|------|-------------|
| [01_Immigration-Data_ScopeDescribe.ipynb](01_Immigration-Data_ScopeDescribe.ipynb) | Step 1 of the project, describe the purpose and scope of the project |
| [02_Immigration-Data_ExploreAssess.ipynb](02_Immigration-Data_ExploreAssess.ipynb) | Step 1 of the project, describe the purpose and scope of the project |
| [03_DataModel.ipynb](03_DataModel.ipynb) | Step 1 of the project, describe the purpose and scope of the project |
| [04_RunPipeline.ipynb](04_RunPipeline.ipynb) | Step 1 of the project, describe the purpose and scope of the project |
| [05_WriteUp.ipynb](05_WriteUp.ipynb) | Step 1 of the project, describe the purpose and scope of the project |
| etl.py | An ETL script to execute on a Spark instance, is the core part of this project |
| air_schema.json | Running the notebook in Step 2 will create this file - it contains the dataframe schema of the airport import data |
| dem_schema.json | Running the notebook in Step 2 will create this file - it contains the dataframe schema of the demographics import data |
| imm_schema.json | Running the notebook in Step 2 will create this file - it contains the dataframe schema of the immigration import data |
| SAS-Description-Parser.ipynb | A notebook containing the description parser which I developed for reading the ".SAS" description file provided by Udacity |
| nb_helpers.py | A set of functions I am using in the Jupyter notebooks above |
| FlyBySalad.png | Diagram of the target data model
| dl.cfg | Contains configuration variables for etl.py, see Step 4 Notebook for details |
| sas_value_constrains.csv | All the SAS value constrainst (e.g. list of airport codes in use), not used after some checks, but can be used for improving data quality |

** and some files provided by Udacity :-)