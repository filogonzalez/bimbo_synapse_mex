# Databricks notebook source
dbutils.widgets.text("silver_catalog", "cat_dnamxgro_dv")
dbutils.widgets.text("silver_schema", "gromexdmo_cz")

catalog = dbutils.widgets.get("silver_catalog")
schema = dbutils.widgets.get("silver_schema")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.a01_employee_voucher_hist (
  Org STRING,
  SC_Code STRING,
  SC_Name STRING,
  User_Code STRING,
  Employee_Name STRING,
  Voucher_Date TIMESTAMP,
  Voucher_Number VARCHAR(50),
  Voucher_Type VARCHAR(50),
  Lov_Code VARCHAR(15),
  Lov_Name STRING,
  Voucher_Amount DECIMAL(18,4),
  Paid_Amount DECIMAL(18,4),
  Paid_Date TIMESTAMP,
  Voucher_Creation_User_Code VARCHAR(15),
  Status VARCHAR(1),
  ConceptCuponCode VARCHAR(15),
  ConceptCupon STRING,
  storeday TIMESTAMP
);""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.a02_expense_hist (
  Org STRING,
  SC_Code STRING,
  SC_Name STRING,
  User_Code VARCHAR(15),
  Expense_Date TIMESTAMP,
  Expense_Number STRING,
  Expense_Type VARCHAR(15),
  Expense_Sub_Type STRING,
  Expense_Amount DECIMAL(18,4),
  Paid_Amount DECIMAL(19,4),
  Paid_Date TIMESTAMP,
  Expense_Creation_User_Code VARCHAR(15),
  Status VARCHAR(1),
  Expense_Description STRING,
  storeday TIMESTAMP
);
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.a03_tray_voucher_hist (
  Org STRING,
  SC_Code STRING,
  SC_Name STRING,
  User_Code VARCHAR(15),
  Tray_Voucher_Date TIMESTAMP,
  Tray_Voucher_Number VARCHAR(50),
  Tray_Voucher_Amount DECIMAL(18,4),
  Tray_Voucher_Creation_User_Code VARCHAR(15),
  Status VARCHAR(1),
  storeday TIMESTAMP
);
""")