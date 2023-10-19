import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.functions import col, dense_rank
from logger import get_logger
import read_write_file

# Get the logger
logger = get_logger()


def case1_count_male_accidents(df_Primary_Person,input_config):
    """
    Finds the crashes (accidents) in which number of persons killed are male
    """
    try:
        logger.info("Case 1 : Find the number of crashes (accidents) in which number of persons killed are male?")
        df = df_Primary_Person.distinct().where((col("PRSN_GNDR_ID") == 'MALE') & (col("PRSN_INJRY_SEV_ID") == 'KILLED')) \
            .select(countDistinct(col("CRASH_ID")).alias("COUNT_MALE_KILLED"))
        logger.info(f"Result : {df.show(truncate = False)}")
        logger.info(f"SUccessfully executed Case 1 Analysis")
        logger.info("Calling write to file for case 1 analysis")
        case1_status = read_write_file.write_to_file(df,input_config['OutputFiles']['case1']['outputfilepath'],input_config['OutputFiles']['case1']['output_format'],input_config['OutputFiles']['case1']['file_name'])
        logger.info(f"Write to file status: {case1_status}")
    except Exception as e:
        logger.error(f"Error occured while executing Case 1 analysis. The error message - {e}")


def case2_count_motorcycle_accidents(df_units,input_config):
    """
    Finds How many two wheelers are booked for crashes
    """
    try:
        logger.info("Case 2 : How many two wheelers are booked for crashes?")
        df = df_units.filter(upper(col("VEH_BODY_STYL_ID")).contains("MOTORCYCLE")).select(
            countDistinct(col("VIN")).alias("count_motorcycle_accidents"))
        logger.info(f"Result : {df.show(truncate=False)}")
        logger.info(f"SUccessfully executed Case 2 Analysis")
        logger.info("Calling write to file for case 2 analysis")
        case2_status = read_write_file.write_to_file(df,input_config['OutputFiles']['case2']['outputfilepath'],input_config['OutputFiles']['case2']['output_format'],input_config['OutputFiles']['case2']['file_name'])
        logger.info(f"Write to file status: {case2_status}")
    except Exception as e:
        logger.error(f"Error occured while executing Case 2 analysis. The error message - {e}")


def case3_state_with_max_female_accidents(df_primary_person,input_config):
    """
    Finds Which state has highest number of accidents in which females are involved?
    """
    try:
        logger.info("Case 3 : Which state has highest number of accidents in which females are involved? ")
        df = df_primary_person.filter(
            (upper(col("PRSN_GNDR_ID")) == "FEMALE") & (~upper(col("DRVR_LIC_STATE_ID")).isin("NA", "UNKNOWN"))) \
            .groupBy(col("DRVR_LIC_STATE_ID")) \
            .agg(count(col("CRASH_ID")).alias("TotalCrashes")) \
            .orderBy(col("TotalCrashes").desc()) \
            .select(col("DRVR_LIC_STATE_ID").alias("STATE_TOP_FEMALE_CRASH")).limit(1)
        logger.info(f"Result : {df.show(truncate=False)}")

        logger.info("SUccessfully executed Case 3 analysis")
        case3_status = read_write_file.write_to_file(df,input_config['OutputFiles']['case3']['outputfilepath'],input_config['OutputFiles']['case3']['output_format'],input_config['OutputFiles']['case3']['file_name'])
        logger.info(f"Write to file status: {case3_status}")
    except Exception as e:
        logger.error(f"Error occured while executing Case 3 analysis. The error message - {e}")


def case4_top5th_15th_vehicle_contributing_to_accidents(df_units,input_config):
    """
    Finds Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death.

    """
    try:
        logger.info("Case 4 : Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death")
        df = df_units.filter(col("VEH_MAKE_ID") != "NA"). \
            withColumn('TOT_CASUALT_CNT', col("TOT_INJRY_CNT") + col("DEATH_CNT")). \
            groupby("VEH_MAKE_ID").sum("TOT_CASUALT_CNT"). \
            withColumnRenamed("sum(TOT_CASUALT_CNT)", "TOT_CASUALTIES_CNT_AGG"). \
            orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
        

        df_top_5_to_15 = df.limit(15).subtract(df.limit(5))
        
        logger.info(f"Result : {df_top_5_to_15.show(truncate=False)}")

        logger.info("SUccessfully executed Case 4 analysis")
        case4_status = read_write_file.write_to_file(df,input_config['OutputFiles']['case4']['outputfilepath'],input_config['OutputFiles']['case4']['output_format'],input_config['OutputFiles']['case4']['file_name'])
        logger.info(f"Write to file status: {case4_status}")


    except Exception as e:
        logger.error(f"Error occured while executing Case 4 analysis. The error message - {e}")


def case5_top_ethnic_usr_grp(df_units, df_primary_person,input_config):
    """
    Finds For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    """
    try:
        logger.info("Case 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  ")


        df = df_units.join(df_primary_person, on=['CRASH_ID'], how='inner'). \
            groupby(col("VEH_BODY_STYL_ID"), col("PRSN_ETHNICITY_ID")).count() \
            .orderBy(col("count").desc())
        window = Window.partitionBy(col("VEH_BODY_STYL_ID")).orderBy(col("count").desc())

        top_ethnic_user_group_df = df.withColumn("rank", dense_rank().over(window)).filter("rank = 1").drop("rank","count")

        
        logger.info(f"Result : {top_ethnic_user_group_df.show(truncate=False)}")

        logger.info("SUccessfully executed Case 5 analysis")
        case_5status = read_write_file.write_to_file(top_ethnic_user_group_df,input_config['OutputFiles']['case5']['outputfilepath'],input_config['OutputFiles']['case5']['output_format'],input_config['OutputFiles']['case5']['file_name'])
        logger.info(f"Write to file status: {case_5status}")

    except Exception as e:
        logger.error(f"Error occured while executing Case 5 analysis. The error message - {e}")


def case6_top5_zipcodes_with_alcohols_as_reason(df_primary_person, df_units,input_config):
    """
    FInds Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    """
    try:
        logger.info("Case 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)")
        df = df_units.join(df_primary_person, on=['CRASH_ID'], how='inner'). \
            dropna(subset=["DRVR_ZIP"]). \
            filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")). \
            groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
        logger.info(f"Result : {df.show(truncate=False)}")

        logger.info("SUccessfully executed Case 6 analysis")
        case6_status = read_write_file.write_to_file(df,input_config['OutputFiles']['case6']['outputfilepath'],input_config['OutputFiles']['case6']['output_format'],input_config['OutputFiles']['case6']['file_name'])
        logger.info(f"Write to file status: {case6_status}")

    except Exception as e:
        logger.error(f"Error occured while executing Case 6 analysis. The error message - {e}")

def case7_count_of_crash_id_with_no_damage(df_damages, df_units,input_config):
    """
    Finds Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.

    """
    try:
        logger.info("Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance")

        insurance = instr(col("FIN_RESP_TYPE_ID"), "INSURANCE") >= 1

        df = df_units.distinct() \
            .withColumn("VEH_DMG_SCL_1", regexp_extract(col('VEH_DMAG_SCL_1_id'), r'(\d+)', 1).cast('bigint')) \
            .withColumn("VEH_DMG_SCL_2", regexp_extract(col('VEH_DMAG_SCL_2_id'), r'(\d+)', 1).cast('bigint')) \
            .withColumn("INSURANCE_FLG", insurance) \
            .select(col("CRASH_ID"), col('VEH_DMG_SCL_1'), col("VEH_DMG_SCL_2"), col("INSURANCE_FLG"),
                    col("FIN_RESP_PROOF_ID")) \
            .filter(
            (~col("FIN_RESP_PROOF_ID").isin("NA", "NR")) & ((col("VEH_DMG_SCL_1") > 4) | (col("VEH_DMG_SCL_2") > 4)) & (
                col("INSURANCE_FLG")))

        df_damage = df_damages.distinct().filter(~upper(col("DAMAGED_PROPERTY")).isin("NONE", "NONE1"))
        df = df.join(df_damage, "CRASH_ID", "leftanti")

        df = df.select(countDistinct(col('CRASH_ID')).alias('count_of_crash_id_with_no_damage'))

        logger.info(f"Result : {df.show(truncate=False)}")

        logger.info("SUccessfully executed Case 7 analysis")

        case7_status = read_write_file.write_to_file(df,input_config['OutputFiles']['case7']['outputfilepath'],input_config['OutputFiles']['case7']['output_format'],input_config['OutputFiles']['case7']['file_name'])
        logger.info(f"Write to file status: {case7_status}")

    except Exception as e:
        logger.error(f"Error occured while executing Case 7 analysis. The error message - {e}")

def case8_top5_vehicle_makers(df_units, df_charges, df_primary_person,input_config):
    """
    Finds and Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    """
    try:
        logger.info("Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)")

        top_25_state_list = [row[0] for row in df_units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).
        groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
        top_10_used_vehicle_colors = [row[0] for row in df_units.filter(df_units.VEH_COLOR_ID != "NA").
        groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]

        df = df_charges.join(df_primary_person, on=['CRASH_ID'], how='inner'). \
            join(df_units, on=['CRASH_ID'], how='inner'). \
            filter(df_charges.CHARGE.contains("SPEED")). \
            filter(df_primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
            filter(df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors)). \
            filter(df_units.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
            groupby("VEH_MAKE_ID").count(). \
            orderBy(col("count").desc()).limit(5).select("VEH_MAKE_ID")
        
        logger.info(f"Result : {df.show(truncate=False)}")

        logger.info("SUccessfully executed Case 8 analysis")
        case8_status = read_write_file.write_to_file(df,input_config['OutputFiles']['case8']['outputfilepath'],input_config['OutputFiles']['case8']['output_format'],input_config['OutputFiles']['case8']['file_name'])
        logger.info(f"Write to file status: {case8_status}")


    except Exception as e:
        logger.error(f"Error occured while executing Case 8 analysis. The error message - {e}")
