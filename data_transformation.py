import logging.config
from pyspark.sql.functions import *
from pyspark.sql.window import Window

logging.config.fileConfig("properties/configuration/logging.config")
logger=logging.getLogger('Data_transformation')

def data_report1(df1,df2):
    try:
        logging.info("Calculating the number of zips per city.....")
        number_of_cities = df1.withColumn('Zip_counts1', size(split(col('zips'), ' '))).groupBy(col('city'),col('state_id')).agg(
            sum(col('Zip_counts1')).alias('Number of zips'))

        logging.info("Calculating the number of prescribers and total claim count per city......")
        number_of_prescribers = df2.groupBy('presc_state', 'presc_city').agg(countDistinct('presc_id').alias('Number of prescribers'),count('total_claim_count').alias('total_claim_count per city'))

        logging.info("Reporting cities to which prescribers are assigned")
        city_pre=number_of_cities.join(number_of_prescribers,(number_of_cities.city == number_of_prescribers.presc_city) & (number_of_cities.state_id == number_of_prescribers.presc_state),'inner').select('city',col('state_id').alias('state'),'Number of zips','Number of prescribers','total_claim_count per city')

        return city_pre


    except Exception as e:
        logging.info("There has been an error",str(e))

def data_report2(df2):
    try:
        logging.info("Retrieving the top 5 prescribers from each state based on the total_count")
        presc_rank_by_state = df2.select('presc_id', 'presc_name', 'presc_state','presc_city', 'presc_speciality', 'total_claim_count', 'years_of_exp'). \
        filter((col('years_of_exp') >= 20) & (col('years_of_exp') <= 50)).withColumn('presc_rank', dense_rank().over(
        Window.partitionBy(col('presc_state')).orderBy(col('total_claim_count').desc()))). \
        filter(col('presc_rank') <= 5)

        return presc_rank_by_state

    except Exception as e:
        logging.error("There has been an error",str(e))
        raise


