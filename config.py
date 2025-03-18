# config.py - Configuration settings for Databricks & ADLS

# Azure Data Lake Storage (ADLS) Gen2 Credentials
STORAGE_ACCOUNT_NAME = "<storage-account>"
CONTAINER_NAME = "<container>"
TENANT_ID = "<tenant-id>"
CLIENT_ID = "<client-id>"
CLIENT_SECRET = "<client-secret>"

# Databricks Mount Path
MOUNT_POINT = "/mnt/olympicsdata"

# Raw & Processed Data Paths
RAW_DATA_PATH = f"{MOUNT_POINT}/raw/olympics.csv"
PROCESSED_DATA_PATH = f"{MOUNT_POINT}/processed/olympics.delta"
