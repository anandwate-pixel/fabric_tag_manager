# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "28db7020-c7df-4c7f-91a0-bad2f9997486",
# META       "default_lakehouse_name": "monitoring_lakehouse",
# META       "default_lakehouse_workspace_id": "f3c50251-d451-442f-8631-b12b6ad76de2",
# META       "known_lakehouses": [
# META         {
# META           "id": "28db7020-c7df-4c7f-91a0-bad2f9997486"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS monitoring_lakehouse.dbo.tag_notifications (
# MAGIC     key                   STRING NOT NULL, 
# MAGIC     workspace_id          STRING,
# MAGIC     workspace_name        STRING,
# MAGIC     compliant             BOOLEAN,
# MAGIC     notification_sent     BOOLEAN, 
# MAGIC     violation_count       BIGINT,
# MAGIC     violation_signature   STRING, 
# MAGIC     violations_json        STRING,
# MAGIC     first_alert_utc        STRING,
# MAGIC     last_alert_utc          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.columnMapping.mode' = 'none',
# MAGIC     'delta.enableIcebergCompatV2' = 'false'
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql   
# MAGIC DROP TABLE monitoring_lakehouse.dbo.tag_notifications

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE monitoring_lakehouse.dbo.tag_notifications drop COLUMNS status, recipient_email, alerting_method, history_json

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE monitoring_lakehouse.dbo.tag_notifications ADD COLUMN compliant BOOLEAN

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE monitoring_lakehouse.dbo.tag_notifications RENAME COLUMN latest_alert_utc TO last_alert_utc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
