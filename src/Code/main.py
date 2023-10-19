from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from logger import get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import datetime
import read_write_file
import case_analysis
import argparse
logger = get_logger()

class DataProcessor:

    def __init__(self,spark: SparkSession, config_path: str):
        logger.info("Initialising variables")
        self.spark = spark
        self.config_path = config_path
        logger.info("Reading configuration file")
        self.input_config = read_write_file.get_config_details(spark, config_path)


    def load_dataframe(self):
        """
            This methods converts csv file to dataframe
        """
        try:
            logger.info("Passing inputs from configuration dictionary to convert_csv_to+df function")
            for table_name , file_path in self.input_config["input_CSV_Files"].items():
                logger.info(f"Converting to dataframe for the file {table_name}.csv")
                self.df_name = read_write_file.convert_csv_to_df(spark, file_path)
                globals()[f"df_{table_name}"] = self.df_name
                logger.info(f"Successfully converted csv file to dataframe for the file {table_name}.csv")
                logger.info(f"df_{table_name}.show()")

        except Exception as e:
            logger.error("Error occured while converting csv file to dataframe")
            logger.error(f"Error message : {e}")


    def case_analysis(self):
        """
            This methods analyses the dataframe and performs operation
        """
        # Function list with their respective DataFrame parameters
        executor_list = [(case_analysis.case1_count_male_accidents, [df_primary_person,self.input_config]),
                         (case_analysis.case2_count_motorcycle_accidents, [df_units,self.input_config]),
                         (case_analysis.case3_state_with_max_female_accidents, [df_primary_person,self.input_config]),
                         (case_analysis.case4_top5th_15th_vehicle_contributing_to_accidents, [df_units,self.input_config]),
                         (case_analysis.case5_top_ethnic_usr_grp, [df_primary_person, df_units,self.input_config]),
                         (case_analysis.case6_top5_zipcodes_with_alcohols_as_reason, [df_primary_person, df_units,self.input_config]),
                         (case_analysis.case7_count_of_crash_id_with_no_damage, [df_damages, df_units,self.input_config]),
                         (case_analysis.case8_top5_vehicle_makers, [df_units, df_charges, df_primary_person,self.input_config])
                         ]
        try:
            logger.info("Started analysing all the cases")
            # Create a list of functions with their respective DataFrame parameters
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(func, *params) for func, params in executor_list]

            for future in as_completed(futures):
                future.result()
            logger.info("COmpleted executing all the cases")
                
        except Exception as e:
            logger.error(f"Error occurred with error message {e}")
                
if __name__ == "__main__":
    
    config_path = r'C:\Users\Harshitha\PycharmProjects\bcg_case_study\venv\code\config.json'
    parser = argparse.ArgumentParser(description="BCGCaseStudy")
    parser.add_argument('--config', help="provide the config file path", default=config_path, )
    
    args = parser.parse_args()
    config_path = args.config
    
    try:
        logger.info("Creating spark session")
        
        spark = SparkSession \
            .builder \
            .config("spark.app.name", "BCG_VehicleCrash_analysis") \
            .getOrCreate()
        logger.info("STarting execution")
        
        processor = DataProcessor(spark,config_path)
        processor.load_dataframe()
        processor.case_analysis()
        
    except Exception as err:
        logger.error("%s Error : %s", __name__, str(err))

    finally:
        spark.stop()
        logger.info("Successfully completed the execution")
        logger.info("Successfully stopped spark session ")
        


    


