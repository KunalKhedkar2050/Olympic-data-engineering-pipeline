# data_ingestion.py - Ingest Olympics data from ADLS Gen2

from pyspark.sql import SparkSession
import config

# Start Spark Session
spark = SparkSession.builder.appName("OlympicsDataIngestion").getOrCreate()

# Mount ADLS Gen2 to Databricks
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": config.CLIENT_ID,
    "fs.azure.account.oauth2.client.secret": config.CLIENT_SECRET,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{config.TENANT_ID}/oauth2/token"
}

dbutils.fs.mount(
    source=f"abfss://{config.CONTAINER_NAME}@{config.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
    mount_point=config.MOUNT_POINT,
    extra_configs=configs
)

print("ADLS Gen2 Mounted Successfully")

# Read raw CSV file from ADLS
df = spark.read.csv(config.RAW_DATA_PATH, header=True, inferSchema=True)
df.show(5)

print("Data Ingestion Completed")
