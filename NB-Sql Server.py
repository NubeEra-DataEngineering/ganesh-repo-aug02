# Databricks notebook source
df_remote_table= (
    spark.read.format("sqlserver")
    .option("host", "sqlserver-ganesh1.database.windows.net")
    .option("port", "1433")
    .option("user", "admin123")
    .option("password", "Incedo@1234")
    .option("database", "sqldb-ganesh")
    .option("dbtable","dbo.iris_data")
    .load()
     )
