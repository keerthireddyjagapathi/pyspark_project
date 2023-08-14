import logging
import logging.config

logging.config.fileConfig("properties\configuration\logging.config")

logger = logging.getLogger("Ingest")


def ingest_file(spark, file_dir, file_format, header, inferschema, file_name, df_name):
    df = spark.read.format(file_format).option("header", header).option("inferSchema", inferschema).load(file_dir)
    logger.info(f"Read the file {file_name} into a dataframe {df_name}")
    return df

def display_df(df,df_name):
    logging.info(f"Printing the dataframe {df_name}")
    df.show()
