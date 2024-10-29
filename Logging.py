#!/usr/bin/env python
# coding: utf-8

# ## Logging
# 
# New notebook

# # insert_raw_log

# In[ ]:


def insert_raw_log(source: str, 
schema_name: str,
table: str, 
last_loaded_ts: str, 
is_successful_flag: bool, 
expired_records: int = None, 
new_records: int = None, 
updated_records: int = None,
unchanged_records: int = None, 
error: str = None, 
target_destination_workspace: str = None, 
target_destination_lakehouse: str = None) -> None:
    """
    Inserts a new log into the raw_snapshot_log table. 
    This requires that you run the following ahead of time to initialize the temp table reference needed:
    raw_log_df=spark.read.format('delta').load(raw_log_delta_path)
    raw_log_df.createOrReplaceTempView('raw_snapshot_log')

    Args:
        source (str): The source of the data.
        schema_name (str): The schema name.
        table (str): The table name.
        last_loaded_ts (str): The timestamp of the last loaded data.
        is_successful_flag (bool): The flag indicating if the operation was successful.
        expired_records (int, optional): The number of expired records. Defaults to None.
        new_records (int, optional): The number of new records. Defaults to None.
        updated_records (int, optional): The number of updated records. Defaults to None.
        unchanged_records (int, optional): The number of unchanged records. Defaults to None.
        error (str, optional): The error message. Defaults to None.
        target_destination_workspace (str, optional): The target destination workspace. Defaults to None.
        target_destination_lakehouse (str, optional): The target destination lakehouse. Defaults to None.
    """
    # Validate that previous row with the same information is not added.
    validation_df = spark.read.table("raw_snapshot_log").filter((col("source") == source) & (col("schema") == schema_name) & (col("table") == table) & (col("last_loaded_ts") == last_loaded_ts) & (col("is_successful_flag") == is_successful_flag))
    if validation_df.isEmpty():
        # Adds log to the logging table
        df=spark.createDataFrame([[source, schema_name, table, last_loaded_ts, is_successful_flag, expired_records, new_records, updated_records, unchanged_records, error, target_destination_workspace, target_destination_lakehouse]],schema=validation_df.schema)
        df.write.format("delta").mode('append').save(raw_log_delta_path)




# # remove_raw_error

# In[ ]:


def remove_raw_error(source: str, 
table: str, 
last_loaded_ts: str) -> None:
    """
    Removes error logs from the raw_snapshot_log table.

    This requires that you run the following ahead of time to initialize the temp table reference needed:
    raw_log_df=spark.read.format('delta').load(raw_log_delta_path)
    raw_log_df.createOrReplaceTempView('raw_snapshot_log')

    Args:
        source (str): The source of the data.
        table (str): The table name.
        last_loaded_ts (str): The timestamp of the last loaded data.
    """
    spark.sql(f"DELETE FROM raw_snapshot_log WHERE source = '{source}' AND table = '{table}' AND last_loaded_ts = '{last_loaded_ts}' AND is_successful_flag = 0")


# # insert_curated_log

# In[ ]:


def insert_curated_log(source: str, 
schema_name: str,
table: str, 
last_loaded_ts: str, 
is_successful_flag: bool, 
expired_records: int = None, 
new_records: int = None, 
updated_records: int = None,
unchanged_records: int = None, 
error: str = None, 
target_destination_workspace: str = None, 
target_destination_lakehouse: str = None) -> None:
    """
    Inserts a new log into the curated_snapshot_log table.

    This requires that you run the following ahead of time to initialize the temp table reference needed:
    raw_log_df=spark.read.format('delta').load(curated_log_delta_path)
    raw_log_df.createOrReplaceTempView('curated_snapshot_log')

    Args:
        source (str): The source of the data.
        schema_name (str): The schema name.
        table (str): The table name.
        last_loaded_ts (str): The timestamp of the last loaded data.
        is_successful_flag (bool): The flag indicating if the operation was successful.
        expired_records (int, optional): The number of expired records. Defaults to None.
        new_records (int, optional): The number of new records. Defaults to None.
        updated_records (int, optional): The number of updated records. Defaults to None.
        unchanged_records (int, optional): The number of unchanged records. Defaults to None.
        error (str, optional): The error message. Defaults to None.
        target_destination_workspace (str, optional): The target destination workspace. Defaults to None.
        target_destination_lakehouse (str, optional): The target destination lakehouse. Defaults to None.
    """
    # Validate that previous row with the same information is not added.
    validation_df = spark.read.table("curated_snapshot_log").filter((col("source") == source) & (col("schema") == schema_name) & (col("table") == table) & (col("last_loaded_ts") == last_loaded_ts) & (col("is_successful_flag") == is_successful_flag))

    if validation_df.isEmpty():
        # Adds log to the logging table
        df=spark.createDataFrame([[source, schema_name, table, last_loaded_ts, is_successful_flag, expired_records, new_records, updated_records, unchanged_records, error, target_destination_workspace, target_destination_lakehouse]],schema=validation_df.schema)
        df.write.format("delta").mode('append').save(CURATED_LOG_DELTA_PATH)


# # remove_curated_error

# In[ ]:


def remove_curated_error(source: str, 
table: str, 
last_loaded_ts: str) -> None:
    """
    Removes error logs from the curated_snapshot_log table.

    This requires that you run the following ahead of time to initialize the temp table reference needed:
    raw_log_df=spark.read.format('delta').load(curated_log_delta_path)
    raw_log_df.createOrReplaceTempView('curated_snapshot_log')

    Args:
        source (str): The source of the data.
        table (str): The table name.
        last_loaded_ts (str): The timestamp of the last loaded data.
    """
    spark.sql(f"DELETE FROM curated_snapshot_log WHERE source = '{source}' AND table = '{table}' AND last_loaded_ts = '{last_loaded_ts}' AND is_successful_flag = 0")

