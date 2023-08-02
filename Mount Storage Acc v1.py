# Databricks notebook source
#v1
storage_account="storageaccganeshv1"
container_name="data"
source_url="wasbs://{0}@{1}.blob.core.windows.net".format(container_name,storage_account)

access_key= ""
mount_point_url="/mnt/gen1dataset"

extra_configs_key=f"fs.azure.account.key.{storage_account}.blob.core.windows.net"
extra_configs_value = access_key
# extra_configs_dict = {extra_configs_key:extra_configs_value}

# COMMAND ----------

dbutils.fs.mount(source = source_url, 
                 mount_point = mount_point_url,
                 extra_configs= {extra_configs_key:extra_configs_value})

# COMMAND ----------

dbutils.fs.ls(mount_point_url)
