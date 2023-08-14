import logging
import logging.config

logging.config.fileConfig("properties\configuration\logging.config")
logger=logging.getLogger('Writing')
def writing_to(df,partition_by,path,mode,format,header,name):
    try:
        df.write.format(format).option("header",header).option("mode",mode)\
        .bucketBy(4,partition_by).option("path",path).saveAsTable(name)
        logger.info(f"Written the {name} dataframe to the disk in {format} file format")


    except Exception as e:
        logging.info("There has been an error",str(e))
        raise