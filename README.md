# BCG_Pyspark_CaseStudy
## BCG - Big Data Case Study
---

Provide the useful insights using the sample database of accidents and its consequence

##### Dataset:

Data Set folder has 6 csv files. Please use the data dictionary (attached in the mail) to understand the dataset and then develop your approach to perform below analytics.

#### Project Structure

```
project_directory/
│
├── src/
│   │
│   ├── code/
│   │   ├── config.json
│   │   ├── case_analysis.py
│   │   ├── main.py
│   │   ├── io_utils.py
│   │   ├── logger.py
│   │
│   ├── dataset/
│   │   ├── input_files/
│   │   │   ├── charges_use.csv
│   │   │   ├── damages.csv
│   │   │   ├── Endorse.csv
│   │   │   ├── Primary_person.csv
│   │   │   ├── Restrict.csv
│   │   │   ├── units_use.csv
│   │
│   │   ├── output_files/
│   │   │   ├── analysis1/
│   │   │   │   ├── analysis1_current_time.parquet
│   │   │   │
│   │   │   ├── analysis2/
│   │   │   │   ├── analysis2_current_time.parquet
│   │   │   │
│   │   │   ├── analysis3/
│   │   │   │   ├── analysis3_current_time.parquet
│   │   │   │
│   │   │   ├── analysis4/
│   │   │   │   ├── analysis4_current_time.parquet
│   │   │   │
│   │   │   ├── analysis5/
│   │   │   │   ├── analysis5_current_time.parquet
│   │   │   │
│   │   │   ├── analysis6/
│   │   │   │   ├── analysis6_current_time.parquet
│   │   │   │
│   │   │   ├── analysis7/
│   │   │   │   ├── analysis7_current_time.parquet
│   │   │   │
│   │   │   ├── analysis8/
│   │   │   │   ├── analysis8_current_time.parquet
│   │   │
│   ├── README.md

```


##### Analytics:
##### Application should perform below analysis and store the results for each analysis.

Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?

Analysis 2: How many two wheelers are booked for crashes?

Analysis 3: Which state has highest number of accidents in which females are involved?

Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)



#### Expected Output:

* Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)

* Code should be properly organized in folders as a project.

* Input data sources and output should be config driven

* Code should be strictly developed using Dataframe APIs (Do not use Spark SQL)

#### Steps to run the analysis:

##### Prerequisite

* PySpark
* Python 3.7.x
* 
#### Input file

In this use case, I have used the json, to configure the input files and output file directory.

Update the input file directory in the yaml file and file names.

* One place to change the input directory and if any files names updates

Note: No need to handle in code, if any files name gets renamed


### Execution Steps [Local]:


        spark-submit --master local[*] main.py --config <config_file_path> 
        
