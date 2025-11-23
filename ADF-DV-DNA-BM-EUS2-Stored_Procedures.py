# Databricks notebook source
# MAGIC %md
# MAGIC # RTM_DMO_L2 Stored Procedures

# COMMAND ----------

dbutils.widgets.text("catalog", "cat_dnamxgro_dv")
dbutils.widgets.text("schema", "gromexdmo_cz")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# -- ============================================================================
# -- Stored Procedure: sp_insert_employee_voucher
# -- Description: Delete-insert pattern to archive employee vouchers with store day
# -- Transpiled from: Synapse RTM_Facts.SP_InsertEmployeeVoucher
# -- ============================================================================
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.sp_insert_employee_voucher(
    p_stday STRING,
    p_catalog STRING,
    p_schema STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Archive employee voucher records for specified store day'
AS
BEGIN
    DELETE FROM IDENTIFIER(CONCAT(p_catalog, '.', p_schema, '.a01_employee_voucher_hist'))
    WHERE storeday = p_stday;

    INSERT INTO IDENTIFIER(CONCAT(p_catalog, '.', p_schema, '.a01_employee_voucher_hist'))
    SELECT *, p_stday AS storeday
    FROM IDENTIFIER(CONCAT(p_catalog, '.', p_schema, '.a01_employee_voucher'));
END;""")

# COMMAND ----------

# -- ============================================================================
# -- Stored Procedure: sp_insert_expense
# -- Description: Delete-insert pattern to archive expenses with store day
# -- Transpiled from: Synapse RTM_Facts.SP_InsertExpense
# -- ============================================================================

spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.sp_insert_expense(
    p_stday STRING,
    p_catalog STRING,
    p_schema STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Archive expense records for specified store day'
AS
BEGIN
    DELETE FROM IDENTIFIER(CONCAT(p_catalog, '.', p_schema, '.a02_expense_hist'))
    WHERE storeday = p_stday;
    
    INSERT INTO IDENTIFIER(CONCAT(p_catalog, '.', p_schema, '.a02_expense_hist'))
    SELECT *, p_stday AS storeday
    FROM IDENTIFIER(CONCAT(p_catalog, '.', p_schema, '.a02_expense'));
END;""")

# COMMAND ----------

# -- ============================================================================
# -- Stored Procedure: sp_insert_tray_voucher
# -- Description: Delete-insert pattern to archive tray vouchers with store day
# -- Transpiled from: Synapse RTM_Facts.SP_InsertTrayVoucher
# -- ============================================================================
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.sp_insert_tray_voucher(
    p_stday STRING,
    p_catalog STRING,
    p_schema STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Archive tray voucher records for specified store day'
AS
BEGIN
    DELETE FROM IDENTIFIER(CONCAT(p_catalog, '.', p_schema, '.a03_tray_voucher_hist'))
    WHERE storeday = p_stday;
    
    INSERT INTO IDENTIFIER(CONCAT(p_catalog, '.', p_schema, '.a03_tray_voucher_hist'))
    SELECT *, p_stday AS storeday
    FROM IDENTIFIER(CONCAT(p_catalog, '.', p_schema, '.a03_tray_voucher'));
END;""")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP PROCEDURE IF EXISTS synapse_pilars_catalog.synapse_rtm_schema.sp_insert_expense