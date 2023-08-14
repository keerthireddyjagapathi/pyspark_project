from pyspark.sql import SparkSession
import logging.config

logging.config.fileConfig('properties/configuration/logging.config')
loggers = logging.getLogger("Spark_object")


def get_spark_object(env, appName):
    try:
        if env == 'dev':
            master = 'local'
        else:
            master = 'Yarn'
        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
        loggers.info("Spark object is created")
        return spark
    except Exception as e:
        loggers.error("There is an error")
        raise


def get_spark_validation(spark):
    try:
        output = spark.sql("""select current_date as date""")
        output.show()
        loggers.info("Validation is done")
    except Exception as e:
        loggers.error("There is an error", str(e))
