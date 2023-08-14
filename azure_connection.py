import logging.config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

logging.config.fileConfig("properties\configuration\logging.config")
logger=logging.getLogger("Azure_connection")

def connection(local_folder_path,connection_string,container_name):
    try:
        logging.info("Connection to the azure blob storage.........")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                blob_name = os.path.relpath(local_file_path, local_folder_path)
                blob_client = container_client.get_blob_client(blob_name)
                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(data)

        logging.info(f"Uploaded the file {container_name} into azure blob storage")
        logger.info(f"Uploaded the file {container_name} into azure blob storage")



    except Exception as e:
        logging.info("There was an error",str(e))
        raise