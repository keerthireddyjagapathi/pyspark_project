import os

os.environ['env']='dev'
os.environ['header']='True'
os.environ['inferSchema']='True'
header=os.environ['header']
inferSchema=os.environ['inferSchema']
env=os.environ['env']
appName='pyspark_project'
current_dir=os.getcwd()
source_olap=current_dir + '\stagging\olap'
source_oltp=current_dir + '\stagging\oltp'

city_path='output\cities'
presc_path='output\prescribers'
report1_path='output\data_report1'
report2_path='output\data_report2'

#This is the connection string to Azure blob storage
connection_string="DefaultEndpointsProtocol=https;AccountName=adlspysparkblob;AccountKey=HjzGZKDJ512+zOM1HFljzhb6Lgj0KvVN0CCWp0TRk2G0oCwEQe+FpcKebrLXZnPY2ZJpaLLtM3qd+AStKrXdrA==;EndpointSuffix=core.windows.net"




