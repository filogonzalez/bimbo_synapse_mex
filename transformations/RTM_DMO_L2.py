# Databricks notebook source
import dlt
from dateutil import tz
import datetime
from pyspark.sql.functions import lit, col, concat_ws

# COMMAND ----------

silver_catalog = spark.conf.get('catalog')
silver_schema = spark.conf.get('schema')

# COMMAND ----------

@dlt.table(
    name="v13_van",
    comment="Ingest van data from raw tables",
    table_properties={"quality": "silver"}
)
# @dlt.expect("valid_sc_code", "sc_code IS NOT NULL")
# @dlt.expect_or_drop("valid_van_code", "Van_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "Active = true")
def ingest_v13_van():
    query = """
        SELECT
            sc.Sales_Center_Code AS sc_code,
            vv.Van_Code,
            vv.Van_Name,
            vv.Registration_Number AS Registration_No,
            vv.Model AS Model_Year,
            vv.Plate_No AS License_Plate,
            '          ' AS Van_Type,
            vv.Active,
            vv.Created_Date,
            vv.Update_date,
            vv.Instance AS Instance
        FROM
            cat_rtmgb_dv.groglortmbm_cz.van_v AS vv
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS sc 
            ON (vv.Sales_Center_Id = sc.Sales_Center_Id
                AND vv.Instance = sc.Instance)
    """
    return spark.sql(query)

@dlt.table(
    name="a01_employee_voucher",
    comment="Ingest employee voucher data from raw tables",
    table_properties={"quality": "silver"}
)
# @dlt.expect("valid_org", "Org IS NOT NULL")
# @dlt.expect_or_drop("valid_voucher_number", "Voucher_Number IS NOT NULL")
# @dlt.expect_or_drop("valid_status", "Status = 'I'")
def ingest_a01_employee_voucher():
    query = """
        SELECT
            le.Legal_Entity_Code AS Org,
            cs.Sales_Center_Code AS SC_Code,
            cs.Sales_Center_Name AS SC_Name,
            COALESCE(em.Employee_Code, uvv.User_Code) AS User_Code,
            CASE
                WHEN em.Employee_Code IS NOT NULL THEN CONCAT(em.First_Name, ' ', em.Last_Name)
                ELSE CONCAT(uvv.First_Name, ' ', uvv.Last_Name)
            END AS Employee_Name,
            ev.Date AS Voucher_Date,
            ev.Voucher_Number AS Voucher_Number,
            et.Lov_Type AS Voucher_Type,
            et.Lov_Code,
            et.Lov_Name,
            ev.Amount AS Voucher_Amount,
            ev.Amount_Paid AS Paid_Amount,
            ev.Modified_Date AS Paid_Date,
            uv.User_Code AS Voucher_Creation_User_Code,
            ev.Status AS Status,
            ec.Lov_Code AS ConceptCuponCode,
            ec.Lov_Name AS ConceptCupon
        FROM
            cat_rtmgb_dv.groglortmbm_cz.employee_voucher_v AS ev
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS cs 
            ON (ev.Sales_Center_Id = cs.Sales_Center_Id
                AND ev.Instance = cs.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.employee_master_v AS em 
            ON (ev.Employee_Id = em.Employee_Id
                AND ev.Instance = em.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.legal_entity_v AS le 
            ON (cs.Legal_Enitity_Id = le.Legal_Entity_Id
                AND cs.Instance = le.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_v AS uv 
            ON (ev.Created_By = uv.User_Id
                AND uv.Instance = cs.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_v AS uvv 
            ON (ev.Employee_Id = uvv.User_Id
                AND ev.Instance = uvv.Instance)
        LEFT JOIN (
            SELECT
                DISTINCT Lov_Id,
                Lov_Code,
                Lov_Name,
                Lov_Type,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'VOUCHER_TYPE'
        ) AS et 
            ON (ev.Purpose_Lov_Id = et.Lov_Id
                AND ev.Instance = et.Instance)
        LEFT JOIN (
            SELECT
                DISTINCT Lov_Id,
                Lov_Code,
                Lov_Name,
                Lov_Type,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'EMPLOYEE_VOUCHER_CONCEPT'
        ) AS ec 
            ON (ev.Voucher_Concept_Id = ec.Lov_Id
                AND ev.Instance = ec.Instance)
        WHERE
            ev.Status = 'I'
            AND ev.Active = 1
            AND ev.Date >= DATEADD(month, -6, current_date())
        ORDER BY
            ev.Date
    """
    return spark.sql(query)

@dlt.table(
    name="a02_expense",
    comment="Ingest expense data from raw tables",
    table_properties={"quality": "silver"}
)
# @dlt.expect("valid_org", "Org IS NOT NULL")
# @dlt.expect_or_drop("valid_expense_number", "Expense_Number IS NOT NULL")
# @dlt.expect_or_drop("valid_status", "Status = 'I'")
def ingest_a02_expense():
    query = """
        SELECT 
            le.Legal_Entity_Code AS Org,
            sc.Sales_Center_Code AS SC_Code,
            sc.Sales_Center_Name AS SC_Name,
            uv.User_Code AS User_Code,
            eh.Date AS Expense_Date,
            eh.Reference_No AS Expense_Number,
            et.Lov_Code AS Expense_Type,
            est.Lov_Name AS Expense_Sub_Type,
            eh.Expense_Amount AS Expense_Amount,
            ed.Expense_Amount AS Paid_Amount,
            eh.Date AS Paid_Date,
            uv1.User_Code AS Expense_Creation_User_Code,
            ed.Status AS Status,
            ed.Expense_Description
        FROM
            cat_rtmgb_dv.groglortmbm_cz.expense_header_v AS eh
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.expense_detail_v AS ed 
            ON (eh.Expense_Header_Id = ed.Expense_Header_Id
                AND eh.Instance = ed.Instance)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS sc 
            ON (eh.Sales_Center_Id = sc.Sales_Center_Id
                AND eh.Instance = sc.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.legal_entity_v AS le 
            ON (sc.Legal_Enitity_Id = le.Legal_Entity_Id
                AND sc.Instance = le.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_v AS uv 
            ON (eh.User_Id = uv.User_Id
                AND eh.Instance = uv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_v AS uv1 
            ON (eh.Created_By = uv1.User_Id
                AND eh.Instance = uv1.Instance)
        LEFT JOIN (
            SELECT
                DISTINCT Lov_Id,
                Lov_Code,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'EXPENSE_TYPE'
        ) AS et 
            ON (ed.Expense_Type_Id = et.Lov_Id
                AND ed.Instance = et.Instance)
        LEFT JOIN (
            SELECT
                DISTINCT Lov_Id,
                Lov_Code,
                Lov_Name,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'EXPENSE_SUB_TYPE'
        ) AS est 
            ON (ed.Reason_Id = est.Lov_Id
                AND ed.Instance = est.Instance)
        WHERE
            ed.Status = 'I'
            AND eh.Is_Active = 1
            AND eh.Date >= DATEADD(month, -6, current_date())
        ORDER BY
            eh.Date
    """
    return spark.sql(query)

@dlt.table(
    name="a03_tray_voucher",
    comment="Ingest tray voucher data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_org", "Org IS NOT NULL")
# @dlt.expect_or_drop("valid_tray_voucher_number", "Tray_Voucher_Number IS NOT NULL")
def ingest_a03_tray_voucher():
    query = """
        SELECT
            le.Legal_Entity_Code AS Org,
            cs.Sales_Center_Code AS SC_Code,
            cs.Sales_Center_Name AS SC_Name,
            uv.User_Code AS User_Code,
            tvh.Date AS Tray_Voucher_Date,
            tvh.Number AS Tray_Voucher_Number,
            tvh.Total_Value AS Tray_Voucher_Amount,
            uv1.User_Code AS Tray_Voucher_Creation_User_Code,
            tvh.Status AS Status
        FROM
            cat_rtmgb_dv.groglortmbm_cz.tray_voucher_detail_v AS tv
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.tray_voucher_header_v AS tvh 
            ON (tv.Tray_Voucher_Header_Id = tvh.Tray_Voucher_Header_Id
                AND tv.Instance = tvh.Instance)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS cs 
            ON (tvh.Sales_Center_Id = cs.Sales_Center_Id
                AND tvh.Instance = cs.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.legal_entity_v AS le 
            ON (cs.Legal_Enitity_Id = le.Legal_Entity_Id
                AND cs.Instance = le.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_v AS uv 
            ON (tvh.User_Id = uv.User_Id 
                AND tvh.Instance = uv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_v AS uv1 
            ON (tv.Created_By = uv1.User_Id 
                AND tvh.Instance = uv1.Instance)
        WHERE
            tvh.Active = 1
            AND CAST(tvh.Date AS DATE) >= DATEADD(month, -6, current_date())
        ORDER BY
            tvh.Date
    """
    return spark.sql(query)

@dlt.table(
    name="m04_distributor_user_nacional",
    comment="Ingest distributor user nacional data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sales_center_code", "Sales_Center_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_user_code", "User_Code IS NOT NULL")
def ingest_m04_distributor_user_nacional():
    query = """
        SELECT
            DISTINCT 
            d.Sales_Center_Code AS Sales_Center_Code,
            b.User_Code AS User_Code,
            a.First_Name AS User_First_Name,
            a.Middle_Name AS User_Middel_Name,
            a.Last_Name AS User_Last_Name,
            e.Role_Code AS User_Role,
            a.Gender AS User_Gender,
            a.Date_of_Birth AS User_DOB,
            et.Lov_Code AS JobPosition_Id,
            a.Instance AS Instancia
        FROM
            cat_rtmgb_dv.groglortmbm_cz.contact_person_detail_v AS a
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_v AS b 
            ON (a.Contact_Person_Id = b.Contact_Person_Id
                AND a.Instance = b.Instance)
        LEFT JOIN (
            SELECT
                DISTINCT Lov_Id,
                Lov_Code,
                Instance,
                Lov_Type,
                Active
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'META4_POSITION'
        ) AS et 
            ON (CAST(b.Flex_2 AS STRING) = CAST(et.Lov_Id AS STRING)
                AND b.Active = et.Active
                AND b.Instance = et.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS d 
            ON (a.Sales_Center_Id = d.Sales_Center_Id
                AND a.Instance = d.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.role_master_v AS e 
            ON (e.Role_Id = a.Role_Id
                AND e.Instance = a.Instance)
        WHERE
            a.Active = 1
            AND b.User_Code IS NOT NULL
            AND et.Active = 1
    """
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="v21_supervisor_master",
    comment="Ingest supervisor data from user and sales center tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_user_code", "User_Code IS NOT NULL")
@dlt.expect("valid_supervisor_name", "Supervisor IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "Active = 1")
def ingest_supervisor_master():
    query = """
        SELECT
            sc.Sales_Center_Code AS sc_code,
            uv.User_Code,
            CONCAT(uv.First_Name, ' ', uv.Last_Name) AS Supervisor,
            uv.Active,
            uv.Created_Date,
            uv.Modified_Date,
            uv.Instance
        FROM
            cat_rtmgb_dv.groglortmbm_cz.user_v AS uv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS sc
            ON (uv.Sales_Center_Id = sc.Sales_Center_Id
                AND uv.Instance = sc.Instance)
        WHERE
            uv.User_Position_Level_Id = 7
            AND uv.Active = 1
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="v20_presell_route_mapping",
    comment="Compare retailer route assignments across Preventa (911) and Entrega (909) user types for different legal entities",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_retailer_code", "Retailer_Code IS NOT NULL")
@dlt.expect("valid_sales_center", "Sales_Center_Code IS NOT NULL")
# @dlt.expect_or_drop("has_preventa_data", "Route_Code IS NOT NULL")
def ingest_retailer_route_comparison():
    query = """
        WITH route_retailer_base AS (
            SELECT
                scv.Sales_Center_Code,
                rv.Route_Code,
                rv.Route_Name,
                rrm.Retailer_Id,
                ret.Retailer_Code,
                uv.User_Type_Id,
                scv.Legal_Enitity_Id
            FROM
                cat_rtmgb_dv.groglortmbm_cz.route_retailer_mapping_v AS rrm
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS scv
                ON (rrm.Sales_Center_Id = scv.Sales_Center_Id
                    AND rrm.Instance = scv.Instance
                    AND rrm.Active = 1)
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.retailer_v AS ret
                ON (rrm.Retailer_Id = ret.Retailer_Id
                    AND rrm.Instance = ret.Instance)
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_route_mapping_v AS urm
                ON (rrm.Route_Id = urm.Route_Id
                    AND rrm.Instance = urm.Instance
                    AND urm.Active = 1)
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_v AS uv
                ON (urm.Seller_Id = uv.User_Id
                    AND urm.Instance = uv.Instance
                    AND uv.Active = 1)
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.route_v AS rv
                ON (rrm.Route_Id = rv.Route_Id
                    AND rrm.Instance = rv.Instance
                    AND rv.Active = 1)
            WHERE
                rrm.Active = 1
        ),
        prev AS (
            SELECT
                Sales_Center_Code,
                Route_Code,
                Route_Name,
                Retailer_Code,
                User_Type_Id,
                Legal_Enitity_Id
            FROM route_retailer_base
            WHERE User_Type_Id = 911
        ),
        entr_bm AS (
            SELECT
                Sales_Center_Code,
                Route_Code,
                Route_Name,
                Retailer_Code,
                User_Type_Id,
                Legal_Enitity_Id
            FROM route_retailer_base
            WHERE User_Type_Id = 909
                AND Legal_Enitity_Id = 1
        ),
        entr_bl AS (
            SELECT
                Sales_Center_Code,
                Route_Code,
                Route_Name,
                Retailer_Code,
                User_Type_Id,
                Legal_Enitity_Id
            FROM route_retailer_base
            WHERE User_Type_Id = 909
                AND Legal_Enitity_Id = 2
        )
        SELECT
            prev.Sales_Center_Code,
            prev.Route_Code,
            prev.Route_Name,
            prev.Retailer_Code,
            prev.User_Type_Id,
            prev.Legal_Enitity_Id,
            entr_bm.Sales_Center_Code AS Sales_Center_Code_BM,
            entr_bm.Route_Code AS Route_Code_BM,
            entr_bm.Route_Name AS Route_Name_BM,
            entr_bm.Retailer_Code AS Retailer_Code_BM,
            entr_bm.User_Type_Id AS User_Type_Id_BM,
            entr_bm.Legal_Enitity_Id AS Legal_Enitity_Id_BM,
            entr_bl.Sales_Center_Code AS Sales_Center_Code_BL,
            entr_bl.Route_Code AS Route_Code_BL,
            entr_bl.Route_Name AS Route_Name_BL,
            entr_bl.Retailer_Code AS Retailer_Code_BL,
            entr_bl.User_Type_Id AS User_Type_Id_BL,
            entr_bl.Legal_Enitity_Id AS Legal_Enitity_Id_BL
        FROM prev
        LEFT JOIN entr_bm
            ON (prev.Retailer_Code = entr_bm.Retailer_Code
                AND entr_bm.Legal_Enitity_Id = 1)
        LEFT JOIN entr_bl
            ON (prev.Retailer_Code = entr_bl.Retailer_Code
                AND entr_bl.Legal_Enitity_Id = 2)
    """
    return spark.sql(query)