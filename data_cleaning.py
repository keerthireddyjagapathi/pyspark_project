import logging
import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig("properties\configuration\logging.config")

logger = logging.getLogger('Data_cleaning')


def data_cleaning(df1, df2):
    try:
        logger.info("Data Cleaning........")

        df_city = df1.select(upper(df1.city).alias('city'), df1.state_id, upper(df1.state_name).alias('state_name'),
                             upper(df1.county_name).alias('country_name'), df1.population, df1.zips)
        df_presc = df2.select(col('npi').alias('presc_id'), df2.nppes_provider_last_org_name.alias('presc_lname'),
                              df2.nppes_provider_first_name.alias('presc_fname'),
                              df2.nppes_provider_city.alias('presc_city'),
                              df2.nppes_provider_state.alias('presc_state'),
                              df2.specialty_description.alias('presc_speciality'),
                              df2.drug_name, df2.total_claim_count.alias("total_claim_count"), df2.total_day_supply,
                              df2.total_drug_cost,
                              df2.years_of_exp)
        logger.info("Selected particular columns from the data frames")
        country_name = 'USA'
        df_presc = df_presc.withColumn('country_name', lit(country_name)).withColumn('years_of_exp',
                                                                                     regexp_replace('years_of_exp',
                                                                                                    r'^=', "")) \
            .withColumn('years_of_exp', col('years_of_exp').cast('Int'))

        df_presc = df_presc.withColumn('presc_name', concat_ws(" ", col('presc_fname'), col('presc_lname'))).drop(
            'presc_lname', 'presc_fname')


        df_presc = df_presc.dropna(subset='presc_id')
        df_presc = df_presc.dropna(subset='drug_name')



        return df_city, df_presc

    except Exception as e:
        logging.error("There has been error", str(e))


def print_schema(df, df_name):
    try:
        logging.info(f"Printing the schema of the dataframe {df_name}")
        logger.info(f"Printing the schema of the dataframe {df_name}")
        s = df.schema.fields
        for i in s:
            logging.info(f'\t{i}')
            logger.info(f'\t{i}')
    except Exception as e:
        logging.error("There has been an error", str(e))
        raise


def checking_null(df,df_name):
    try:
        logging.info(f"Checking the number of null values in the {df_name} dataframe.....")
        df_presc_nullcnt = df.select([sum(col(c).isNull().cast('Int')).alias(c) for c in df.columns])
        logger.info(f"Checked the number of null values in the dataframe {df_name}")
        return df_presc_nullcnt
    except Exception as e:
        logging.error("There has been an error", str(e))
        raise


def replace_null(df,column):
    try:
        logging.info(f"Imputing the null values in the {column} column with the mean of the column.....")
        mean_tx_cnt = df.select(round(avg(col(column)))).collect()[0][0]
        logger.info(f"Imputed the null values in the {column} column with the mean of the column.....")

        return mean_tx_cnt

    except Exception as e:
        logging.error("There has been an error", str(e))
        raise


