from spark_object import *
import get_env as g
import os
import logging
import logging.config
from ingest import *
from data_cleaning import *
from data_transformation import *
from writing import *
from azure_connection import *


logging.config.fileConfig('properties/configuration/logging.config')


def main():
    try:
        global file_format, header, inferSchema, df_name, df1, df2
        logging.info("Creating the spark object")
        spark = get_spark_object(g.env, g.appName) #calling the get_spark_object function of spark_object file to create the spark object
        logging.info("Validating the spark object")
        get_spark_validation(spark) #Validating the spark object by calling the function get_spark_validation of spark_object file
        logging.info("Scanning the stagging directories to get the file names")

        for f in os.listdir(g.source_olap):
            file_dir = g.source_olap + '\\' + f
            if f.endswith('.csv'):
                header = g.header
                inferSchema = g.inferSchema
                file_format = 'csv'
                df_name = f + "_df"
            elif f.endswith('.parquet'):
                header = 'NA'
                inferSchema = 'NA'
                file_format = 'parquet'
                df_name = f + "_df"
            df2 = ingest_file(spark, file_dir, file_format, header, inferSchema, f, df_name) #calling the ingest_file function of the ingest file to read the data from the parquet and csv files
            display_df(df2, df_name) #calling the display_df function of ingest file to display the dataframes created

        for f in os.listdir(g.source_oltp):
            file_dir = g.source_oltp + '\\' + f
            if f.endswith('.csv'):
                header = g.header
                inferSchema = g.inferSchema
                file_format = 'csv'
                df_name = f + "_df"
            elif f.endswith('.parquet'):
                header = 'NA'
                inferSchema = 'NA'
                file_format = 'parquet'
                df_name = f + "_df"
            df1 = ingest_file(spark, file_dir, file_format, header, inferSchema, f, df_name) #calling the ingest_file function of the ingest file to read the data from the parquet and csv files
            display_df(df1, df_name) #calling the display_df function of ingest file to display the dataframes created

        print_schema(df1, 'df_city') #Printing the schema of the dataframes by calling the function print_schema of data_cleaning file
        print_schema(df2, 'df_presc')

        logging.info("Checking the number of null values before cleaning the data....")
        df1_nullcnt=checking_null(df1,'df_city') #calling function checking_null of data_cleaning file
        display_df(df1_nullcnt,'df_city_nullcnt')
        df2_nullcnt=checking_null(df2,'df_presc')
        display_df(df2_nullcnt, 'df_presc_nullcnt')

        df_city,df_presc=data_cleaning(df1,df2) #calling data_cleaning function of data_cleaning file to clean the data

        logging.info("Checking the number of null values after cleaning the data....")
        df1_nullcnt=checking_null(df_city,'df_city')
        display_df(df1_nullcnt, 'df_city_nullcnt')
        df2_nullcnt=checking_null(df_presc,'df_presc')
        display_df(df2_nullcnt, 'df_presc_nullcnt')

        df_city_columns_with_nulls = [
            column for column in df1_nullcnt.columns if df1_nullcnt.first()[column] > 0
        ] #Fetching the columns from the city dataframe that have the null count greater than 0

        for c in df_city_columns_with_nulls:
            df_city = df_city.withColumn(c, when(col(c).isNull(), replace_null(df_city, c)).otherwise(
                col(c))).withColumn(c, col(c).cast('Int')) #Imputing the null values of a column with the average of that column

        df_presc_columns_with_nulls = [
            column for column in df2_nullcnt.columns if df2_nullcnt.first()[column] > 0
        ] #Fetching the columns from the prescriber dataframe that have the null count greater than 0



        for c in df_presc_columns_with_nulls:
            df_presc = df_presc.withColumn(c,when(col(c).isNull(),replace_null(df_presc,c))\
                        .otherwise(col(c))).withColumn(c,col(c).cast('Int')) #Imputing the null values of a column with the average of that column

        logging.info("Checking the number of null values after imputing the data in the columns that have null count greater than 0 even after cleaning the data....")
        df1_nullcnt = checking_null(df_city, 'df_city')
        display_df(df1_nullcnt, 'df_city_nullcnt')
        df2_nullcnt = checking_null(df_presc, 'df_presc')
        display_df(df2_nullcnt, 'df_presc_nullcnt')

        print_schema(df_city,'df_city')
        print_schema(df_presc,'df_presc')

        display_df(df_city,'df_city')
        display_df(df_presc,'df_presc')

        df_report1=data_report1(df_city,df_presc) #calling the function data_report1 of data_transformation file to make the business required transformations
        display_df(df_report1,'df_report1')

        df_report2=data_report2(df_presc) #calling the function data_report2 of data_transformation file to make the business required transformations on prescriber dataframe
        display_df(df_report2,'df_report2')

        logging.info("Writing the dataframes to disk")

        #calling the writing_to of writing file to write the dataframes to the disk
        writing_to(df_city,'state_name',g.city_path,'overwrite','csv',g.header,'city')

        writing_to(df_presc, 'presc_state', g.presc_path, 'overwrite','csv',g.header,'presc')

        writing_to(df_report1, 'city', g.report1_path, 'overwrite', 'parquet','False','report1')

        writing_to(df_report2, 'presc_state', g.report2_path, 'overwrite', 'parquet','False','report2')

        logging.info("Writing the first report to Azure Blob Storage")

        #calling the connection function of azure_connection file to connect to the azure blob storage and upload the files from disk
        connection("C:\\Users\\Keerthi Jagapathi\\PycharmProjects\\pyspark_project\\output\\data_report1",g.connection_string,"report1")

        logging.info("Writing the second report to Azure Blob Storage")

        connection("C:\\Users\\Keerthi Jagapathi\\PycharmProjects\\pyspark_project\\output\\data_report2",g.connection_string,"report2")

    except Exception as e:
        logging.error("There is an error",str(e))
        sys.exit(1)
    else:
        logging.info("Application has executed successfully!!!!")


if __name__ == '__main__':
    main()

