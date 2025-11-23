# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "cat_dnamxgro_dv")
dbutils.widgets.text("schema", "gromexdmo_cz")
dbutils.widgets.text("manual_fecha", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
manual_fecha = dbutils.widgets.get("manual_fecha")

# COMMAND ----------

if manual_fecha:
    fecha_value = manual_fecha
    print(f"Using manual fecha: {fecha_value}")
else:
    fecha_value = spark.sql("""
        SELECT date_format(
            date_sub(
                from_utc_timestamp(current_timestamp(), 'America/Mexico_City'),
                1
            ),
            'yyyy-MM-dd'
        )
    """).collect()[0][0]
    print(f"Computed fecha_value: {fecha_value}")

print(f"\nProcessing archive for: {fecha_value}")
print(f"Target: {catalog}.{schema}")

# COMMAND ----------

results = {}
start_time = datetime.now()

stored_procedures = [
    ('sp_insert_employee_voucher', 'a01_employee_voucher_hist', 'Employee Voucher'),
    ('sp_insert_expense', 'a02_expense_hist', 'Expense'),
    ('sp_insert_tray_voucher', 'a03_tray_voucher_hist', 'Tray Voucher')
]

for sp_name, hist_table, display_name in stored_procedures:
    try:
        proc_start = datetime.now()

        spark.sql(f"""
            CALL {catalog}.{schema}.{sp_name}(
                '{fecha_value}',
                '{catalog}',
                '{schema}'
            )
        """)

        proc_end = datetime.now()
        duration = (proc_end - proc_start).total_seconds()

        count = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {catalog}.{schema}.{hist_table}
            WHERE storeday = '{fecha_value}'
        """).collect()[0]['cnt']
        
        results[sp_name] = {
            'status': 'SUCCESS',
            'records': count,
            'duration': duration
        }
        
        print(f"{display_name}: {count} records in {duration:.2f}s")
        
    except Exception as e:
        results[sp_name] = {
            'status': 'FAILED',
            'error': str(e)
        }
        print(f"{display_name}: FAILED - {str(e)}")
        raise