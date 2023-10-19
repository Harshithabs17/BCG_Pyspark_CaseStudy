import json
import logger
import datetime

logger = logger.get_logger()
def convert_csv_to_df(spark, file_path):
    """
        This methods reads the CSV files
        returns the dataframe extracted from csv
    """
    try:
        df_data = spark.read \
            .format("csv") \
            .option("delimiter", ",") \
            .option("header", True) \
            .option("path", file_path) \
            .load()
        logger.info(f"Successfully read the input file {file_path}")
        return df_data
    except Exception as e:
        logger.error(f"Error occured while reading csv file with error message: {e}")



def get_config_details(spark, file_path):
    """
        This methods reads the configuration json file
        return the dictionary
    """
    try:
        with open(file_path,"r") as config_file:
            config = json.load(config_file)
        logger.info("Succesfully read the configuration file and returned the configuration dictionary")
        return config
    except Exception as e:
        logger.info(f"Error occured while reading the config file with error message - {e}")


    


def write_to_file(dataframe, output_path,file_format,file_name):
    """
        This methods write output to file
        return the status string
    """
    try:
        file_name = f"{output_path}/{file_name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.{file_format}"
        logger.info(file_name)
		file_path = str(file_name)
        dataframe.write \
                .format(file_format) \
                .mode("append") \
                .option("path", file_path)
				
        logger.info(f"Written the file in the given path :  {file_name}")
        return "Success"
    except Exception as err:
        logger.error(f" Error occured while writing to a file : {err}")
        return "Failed"