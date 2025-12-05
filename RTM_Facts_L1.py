# Databricks notebook source
# Create widgets for date parameters with dynamic defaults matching the logic:
# date_end: Today in CST (Mexico)
# date_begin: Yesterday in CST (Mexico)
# Using SQL expressions to calculate defaults to ensure "SQL friendly" logic.
# Create widgets for date parameters with dynamic defaults matching the logic:
# date_end: Today in CST (Mexico)
# date_begin: Yesterday in CST (Mexico)
# Using SQL expressions to calculate defaults to ensure "SQL friendly" logic.
default_dates_df = spark.sql("""
    SELECT 
        CAST(date_sub(to_date(from_utc_timestamp(current_timestamp(), 'America/Mexico_City')), 2) AS STRING) as two_days_ago,
        CAST(date_sub(to_date(from_utc_timestamp(current_timestamp(), 'America/Mexico_City')), 1) AS STRING) as begin_date,
        CAST(to_date(from_utc_timestamp(current_timestamp(), 'America/Mexico_City')) AS STRING) as end_date
""")
defaults = default_dates_df.first()

dbutils.widgets.text("two_days_ago", defaults.two_days_ago)
dbutils.widgets.text("date_begin", defaults.begin_date)
dbutils.widgets.text("date_end", defaults.end_date)
dbutils.widgets.text("bronze_catalog", "cat_rtmgb_dv")
dbutils.widgets.text("bronze_schema", "groglortmbm_cz")
dbutils.widgets.text("silver_catalog", "cat_dnamxgro_dv")
dbutils.widgets.text("silver_schema", "gromexrtfac_cz") 

bronze_catalog = dbutils.widgets.get("bronze_catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_catalog = dbutils.widgets.get("silver_catalog")
silver_schema = dbutils.widgets.get("silver_schema")
two_days_ago = dbutils.widgets.get("two_days_ago")
date_begin = dbutils.widgets.get("date_begin")
date_end = dbutils.widgets.get("date_end")

print(f"reading from catalog: {bronze_catalog}\nreading from schema: {bronze_schema}\nwriting to catalog: {silver_catalog}\nwriting to schema: {silver_schema}\ntwo_days_ago widget: {two_days_ago}\ndate_begin widget: {date_begin}\ndate_end widget: {date_end}")
# COMMAND ----------
from delta.tables import DeltaTable

def perform_merge(source_df, target_table_name, merge_condition, unique_keys=None):
    """
    Performs a merge operation from source_df to target_table_name.
    Deduplicates source_df based on unique_keys before merging.
    """
    target_table_full_name = f"{silver_catalog}.{silver_schema}.{target_table_name}"
    
    # Deduplicate Source
    if unique_keys:
        deduped_df = source_df.dropDuplicates(unique_keys)
    else:
        deduped_df = source_df
    
    # Create Target if not exists (Schema evolution/First run)
    table_exists = spark.catalog.tableExists(target_table_full_name)
    
    if not table_exists:
        print(f"Table {target_table_full_name} does not exist. Creating it.")
        deduped_df.write.format("delta").saveAsTable(target_table_full_name)
        print(f"Table {target_table_full_name} created and is ready for merge in the next execution.")
    else:
        # Perform Merge
        print(f"Merging into {target_table_full_name}...")
        target_table = DeltaTable.forName(spark, target_table_full_name)
        
        (target_table.alias("tgt")
          .merge(
            deduped_df.alias("src"),
            merge_condition
          )
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute()
        )
        print(f"Merge completed.")

def perform_overwrite_partition(source_df, target_table_name, partition_filter):
    """
    Overwrites data in the target table matching the partition filter.
    Used for 'Delete-Insert' patterns where we replace a specific slice (e.g., storeday).
    """
    target_table_full_name = f"{silver_catalog}.{silver_schema}.{target_table_name}"
    
    print(f"Overwriting {target_table_full_name} with filter: {partition_filter}")
    
    # Check if table exists
    table_exists = spark.catalog.tableExists(target_table_full_name)
    
    if not table_exists:
        print(f"Table {target_table_full_name} does not exist. Creating it.")
        source_df.write.format("delta").saveAsTable(target_table_full_name)
        print(f"Table {target_table_full_name} created.")
    else:
        (source_df.write
          .format("delta")
          .mode("overwrite")
          .option("replaceWhere", partition_filter)
          .saveAsTable(target_table_full_name))
        print(f"Overwrite completed.")

# COMMAND ----------

# Activity: V12_VisitDetails
# Logic: Delete where VisitDate = FECHA -> Insert

print("Processing V12_VisitDetails...")

v12_visit_query = f"""
SELECT
    DISTINCT 
    scv.Sales_Center_Code AS SC_Code,
    rv.Route_Code AS Route,
    emv.Employee_Code AS Seller,
    rv2.Retailer_Code AS Retailer,
    rvhv.Trip_Id,
    to_date(rvhv.Visit_Date) AS VisitDate,
    rvhv.Entry_Time AS TimeIn,
    rvhv.Exit_Time AS TimeOut,
    rvhv.Latitude AS VisitLatitude,
    rvhv.Longitude AS VisitLongitude
FROM
    {bronze_catalog}.{bronze_schema}.Retailer_Visit_Header_V rvhv
INNER JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS scv ON
    (scv.Sales_Center_Id = rvhv.Sales_Center_Id
        AND scv.Instance = rvhv.Instance)
INNER JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
    (rv.Route_Id = rvhv.Route_Id
        AND rv.Instance = rvhv.Instance)
INNER JOIN {bronze_catalog}.{bronze_schema}.Retailer_V AS rv2 ON
    (rv2.Retailer_Id = rvhv.Retailer_Id
        AND rv2.Instance = rvhv.Instance)
INNER JOIN {bronze_catalog}.{bronze_schema}.User_Route_Mapping_V AS urmv ON
    (urmv.Route_Id = rv.Route_Id
        AND urmv.Instance = rv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Employee_Master_V AS emv ON
    (emv.Own_Route_Id = urmv.Seller_Id
        AND emv.Instance = urmv.Instance)
WHERE
    1 = 1
    AND to_date(rvhv.Visit_Date) = '{date_begin}'
"""

df_v12 = spark.sql(v12_visit_query)

perform_overwrite_partition(
    source_df=df_v12,
    target_table_name="FACTS_V12_VisitDetails", # Output dataset name
    partition_filter=f"VisitDate = '{date_begin}'"
)

# COMMAND ----------
# Activity: V07_GRN
# Logic: Delete where GRN_Date = FECHA -> Insert

print("Processing V07_GRN...")

v07_grn_query = f"""
SELECT
    sc.Sales_Center_Code,
    to_date(ahv.ASN_Date) AS GRN_Date,
    ahv.ASN_No AS GRN_Number,
    pmv.Product_code,
    pmv.Product_Full_Desc,
    CASE WHEN pmv.Pack1_Size > 0 THEN COALESCE(adv.Actual_Qty/pmv.Pack1_Size,0) ELSE 0 END AS expected_tray,
    CASE WHEN pmv.Pack1_Size > 0 THEN COALESCE(adv.Actual_Qty%pmv.Pack1_Size,0) ELSE 0 END AS expected_pc,
    CASE WHEN pmv.Pack1_Size > 0 THEN COALESCE(adv.Received_Qty/pmv.Pack1_Size,0) ELSE 0 END AS received_tray,
    CASE WHEN pmv.Pack1_Size > 0 THEN COALESCE(adv.Received_Qty%pmv.Pack1_Size,0) ELSE 0 END AS received_pc,
    CASE WHEN pmv.Pack1_Size > 0 THEN COALESCE(adv.Damaged_Qty/pmv.Pack1_Size,0) ELSE 0 END AS damaged_tray,
    CASE WHEN pmv.Pack1_Size > 0 THEN COALESCE(adv.Damaged_Qty%pmv.Pack1_Size,0) ELSE 0 END AS damaged_pc,
    CASE WHEN pmv.Pack1_Size > 0 THEN COALESCE(adv.Excess_Qty/pmv.Pack1_Size,0) ELSE 0 END AS extra_tray,
    CASE WHEN pmv.Pack1_Size > 0 THEN COALESCE(adv.Excess_Qty%pmv.Pack1_Size,0) ELSE 0 END AS extra_pc,
    CASE WHEN pmv.Pack1_Size > 0 THEN COALESCE(adv.Missed_Qty/pmv.Pack1_Size,0) ELSE 0 END AS missing_tray,
    CASE WHEN pmv.Pack1_Size > 0 THEN COALESCE(adv.Missed_Qty%pmv.Pack1_Size,0) ELSE 0 END AS missing_pc
FROM
    {bronze_catalog}.{bronze_schema}.ASN_Header_V AS ahv
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
    (ahv.Sales_Center_Id = sc.Sales_Center_Id
        AND ahv.Instance = sc.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.ASN_Detail_V AS adv ON
    (ahv.ASN_Header_Id = adv.ASN_Header_Id 
        AND ahv.Instance = adv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.vw_m05_productmaster AS pmv ON
    (adv.Product_Id = pmv.Product_Id
        AND adv.Instance = pmv.Instance)
WHERE
    1 = 1
    AND to_date(ahv.ASN_Date) = '{date_begin}'
    AND pmv.Pack1_Size > 0
"""

df_v07 = spark.sql(v07_grn_query)

perform_overwrite_partition(
    source_df=df_v07,
    target_table_name="FACTS_V07_GRN",
    partition_filter=f"GRN_Date = '{date_begin}'"
)

# COMMAND ----------
# Activity: V08_BlueLabel
# Logic: Delete where Date = FECHA -> Insert

print("Processing V08_BlueLabel...")

v08_bl_query = f"""
SELECT
    sc.Sales_Center_Code AS Branch,
    rv.Route_Code,
    ret.Retailer_Code,
    blpv.Blue_Lable_Reference_No,
    blpv.Amount,
    to_date(blpv.Date) AS Date,
    blpv.Remarks,
    blpv.Tax,
    blpv.Commission
FROM
    {bronze_catalog}.{bronze_schema}.Blue_Lable_Payment_V AS blpv
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
    (blpv.Sales_Center_Id = sc.Sales_Center_Id
        AND blpv.Instance = sc.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
    (blpv.Route_Id = rv.Route_Id
        AND blpv.Instance = rv.Instance
        AND rv.Active = true)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Retailer_V AS ret ON
    (blpv.Retailer_Id = ret.Retailer_Id 
        AND blpv.Instance = ret.Instance
        AND ret.Active = true)
WHERE
    1 = 1
    AND to_date(blpv.Date) = '{date_begin}'
"""

df_v08 = spark.sql(v08_bl_query)

perform_overwrite_partition(
    source_df=df_v08,
    target_table_name="FACTS_V08_BlueLabel",
    partition_filter=f"Date = '{date_begin}'"
)

# COMMAND ----------
# Activity: V19_Surveys
# Logic: Delete where storeday = FECHA -> Insert

print("Processing V19_Surveys...")

v19_surveys_query = f"""
SELECT
    sc.Sales_Center_Code AS SC_Code,
    sc.Sales_Center_Name,
    uv.User_Code AS Route,
    ret.Retailer_Code,
    ret.Retailer_Name,
    to_date(srh.Date) AS `Survey_Date`,
    qv.Description AS Question,
    srd.Answer,
    cast('{date_begin}' AS DATE) AS storeday
FROM
    {bronze_catalog}.{bronze_schema}.Survey_Result_Header_V AS srh
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
    (srh.Sales_Center_Id = sc.Sales_Center_Id
        AND srh.Instance = sc.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Retailer_V AS ret ON
    (srh.Retailer_Id = ret.Retailer_Id
        AND srh.Instance = ret.Instance
        AND ret.Active = true)
LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS uv ON
    (srh.User_Id = uv.User_Id
        AND srh.Sales_Center_Id = uv.Sales_Center_Id
        AND srh.Instance = uv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Survey_Result_Detail_V AS srd ON
    (srh.Reference_No = srd.Survey_Header_Ref_No
        AND srh.Instance = srd.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Question_V AS qv ON
    (srd.Question_Id = qv.Question_Id
        AND srd.Instance = qv.Instance)	
WHERE 
    1 = 1
    AND to_date(srh.Date) = '{date_begin}'
"""

df_v19 = spark.sql(v19_surveys_query)

perform_overwrite_partition(
    source_df=df_v19,
    target_table_name="FACTS_V19_Surveys",
    partition_filter=f"storeday = '{date_begin}'"
)

# COMMAND ----------
# Activity: V04_OrderDump
# Logic: UPSERT Keys: Order_Number, Product_Code, Date
# Filter: (Date >= FECHA OR Delivery_Date >= FECHA_HOY) AND Delivery_Date IS NOT NULL

print("Processing V04_OrderDump...")

v04_orderdump_query = f"""
SELECT
    scv.Sales_Center_Code,
    rv.Route_Code,
    rev.Retailer_Code,
    soh.Order_Number,
    pmv.Product_Code,
    lpu.Lov_Code AS UOM,
    cast(sod.Qty as int) as Qty,
    cast(sod.Replacement_Quantity as int) AS RQty,
    to_date(soh.Date) AS Date,
    to_date(soh.Delivery_Date) AS Delivery_Date,
    m05.Company_Code,
    cast(sod.Unit_Price as float) as Unit_Price,
    cast(sod.Line_Value as float) as Line_Value
FROM
    {bronze_catalog}.{bronze_schema}.Sales_Order_Header_V AS soh
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS scv ON
    (soh.Sales_Center_Id = scv.Sales_Center_Id
        AND soh.Instance = scv.Instance
        AND scv.Active = true)
LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS uv ON
    (soh.User_Id = uv.User_Id
        AND soh.Instance = uv.Instance
        AND uv.Active = true)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
    (soh.Route_Id = rv.Route_Id
        AND soh.Instance = rv.Instance
        AND rv.Active = true)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Retailer_V AS rev ON
    (soh.Retailer_Id = rev.Retailer_Id
        AND soh.Instance = rev.Instance
        AND rev.Active = true)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Order_Detail_V AS sod ON
    (soh.Sales_Order_Header_Id = sod.Sales_Order_Header_Id
        AND soh.Instance = sod.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS pmv ON
    (sod.Product_Id = pmv.Product_Id
        AND sod.Instance = pmv.Instance
        AND pmv.Active = true
        AND pmv.Product_Level_Id = 5)
LEFT JOIN {bronze_catalog}.{bronze_schema}.VW_M05_ProductMaster AS m05 ON
    (sod.Product_Id = m05.Product_Id
        AND sod.Instance = m05.Instance
        AND m05.Active = true)
LEFT JOIN (
    SELECT
        Lov_Id,
        Lov_Code,
        Lov_Name,
        Instance
    FROM
        {bronze_catalog}.{bronze_schema}.List_Value_V
    WHERE
        Lov_Type = 'PRODUCT_UOM'
        AND Active = true) AS lpu ON
        (sod.Uom_Id = lpu.Lov_Id
        AND sod.Instance = lpu.Instance)
WHERE
    1 = 1
    AND uv.User_Type_Id = 911 -- PREVENTA
    AND (to_date(soh.Date) >= '{date_begin}' OR to_date(soh.Delivery_Date) >= '{date_end}')
    AND soh.Delivery_Date IS NOT NULL
ORDER BY
    scv.Sales_Center_Code,
    soh.Order_Number,
    pmv.Product_Code
"""

df_v04 = spark.sql(v04_orderdump_query)

# Perform Merge (Upsert)
perform_merge(
    source_df=df_v04,
    target_table_name="FACTS_OrderDump",
    merge_condition="tgt.Order_Number = src.Order_Number AND tgt.Product_Code = src.Product_Code AND tgt.Date = src.Date",
    unique_keys=["Order_Number", "Product_Code", "Date"]
)

# COMMAND ----------
# Activity: A09_StockAjustment
# Logic: Delete where Date = FECHA -> Insert

print("Processing A09_StockAjustment...")

a09_adjustment_query = f"""
SELECT
    DISTINCT 
    scv.Sales_Center_Code,
    to_date(sahv.Date) as DATE,
    sahv.Reference_No,
    wv.Warehouse_Code,
    pmv.Product_Code,
    pmv.Full_Description,
    cast((sadv.Adjusted_Qty-sadv.Excess_Qty)/ pugmv.Conversion_Qty as int) AS SystemTray,
    cast((sadv.Adjusted_Qty + sadv.Missing_Qty)%pugmv.Conversion_Qty as int) AS SystemPC,
    (sadv.Adjusted_Qty + sadv.Missing_Qty)* sadv.Unit_Price AS SystemValue,
    cast(ROUND(cast((sadv.Adjusted_Qty + sadv.Missing_Qty) as int)/ pugmv.Conversion_Qty, 1) as int) AS PhysicalTray,
    cast(sadv.Adjusted_Qty%pugmv.Conversion_Qty as int) AS PhysicalPC,
    sadv.Adjusted_Qty * sadv.Unit_Price AS PhysicalValue,
    cast(
        (ROUND(cast((sadv.Adjusted_Qty + sadv.Missing_Qty) as int)/ pugmv.Conversion_Qty, 1))
        - ((sadv.Adjusted_Qty-sadv.Excess_Qty)/ pugmv.Conversion_Qty)
        as int
    ) AS VarianceTray,
    cast(sadv.Missing_Qty%pugmv.Conversion_Qty as int) AS VariancePC,
    sadv.Line_Value AS Variance_Value,
    sadv.Adjusted_Reason AS Reason,
    et.lov_code AS Incident_Code,
    sahv.Instance AS Source_IVY
FROM
    {bronze_catalog}.{bronze_schema}.Stock_Adjustment_Header_V sahv
LEFT JOIN {bronze_catalog}.{bronze_schema}.Stock_Adjustment_Detail_V AS sadv ON
    (sadv.Stock_Adjustment_Header_Id = sahv.Stock_Adjustment_Header_Id
        and sadv.Sales_Center_Id = sahv.Sales_Center_Id
        and sadv.Instance = sahv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS scv ON
    (scv.Sales_Center_Id = sahv.Sales_Center_Id
        and scv.Instance = sahv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Warehouse_V AS wv ON
    (wv.Sales_Center_Id = sahv.Sales_Center_Id
        and wv.Instance = sahv.Instance
        and wv.Active = sahv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS pmv ON
    (pmv.Product_Id = sadv.Product_Id
        and pmv.Instance = sadv.Instance
        and pmv.Active = sadv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Uom_Group_Mapping_V AS pugmv ON
    (pugmv.Uom_Group_Id = PMV.Uom_Group_Id
        and pugmv.Instance = pmv.Instance
        and pugmv.Active = pmv.Active)
LEFT JOIN
    (
    SELECT
        DISTINCT Lov_Id,
        Lov_Code,
        Instance,
        Lov_Type,
        Active
    FROM
        {bronze_catalog}.{bronze_schema}.List_Value_V
    WHERE
        Lov_Type = 'REASON') AS et ON
    (sadv.Reason_Id = et.Lov_Id
        AND sadv.Instance = et.Instance
        AND sadv.Active = et.Active)
WHERE
    1 = 1
    AND wv.Is_Default_Warehouse = true
    AND pugmv.Is_Default_Uom = false
    AND to_date(sahv.Date) = '{date_begin}'
    AND pugmv.Product_Uom_Type_ID = '861'
"""

df_a09 = spark.sql(a09_adjustment_query)

perform_overwrite_partition(
    source_df=df_a09,
    target_table_name="A09_StockAjustment",
    partition_filter=f"DATE = '{date_begin}'"
)

# COMMAND ----------
# Activity: L01_Remisiones
# Logic: Delete where GH_Acceptance_Date = FECHA -> Insert

print("Processing L01_Remisiones...")

l01_remisiones_query = f"""
SELECT
DISTINCT 
    scv.Sales_Center_Code AS SC_CODE,
    ahv.ASN_No AS Reference_No,
    to_date(ahv.ASN_Date) AS DATE,
    uv.User_Code AS Created_by,
    CASE 
        WHEN adv.Created_By = 1 THEN 'Ivy_Admin'
        WHEN adv.Created_By = uv.User_Id Then uv.First_Name 
    END AS Creater_Name,
    CASE 
        WHEN ahv.active = true THEN 'Active'
    END AS Status,
    uv.User_Code AS ACCEPTED_BY,
    CASE 
        WHEN adv.Created_By = 1 THEN 'Ivy_Admin'
        WHEN adv.Created_By = uv.User_Id Then uv.First_Name
    END AS Acepter_Name,
    CASE 
        WHEN adv.Active = true THEN 'Active'
    END AS Status3,
    to_timestamp(ahv.Acceptance_Date) AS GH_Acceptance_Date,
    CASE 
        WHEN ahv.Status = 'S' THEN 'ACCEPTED'
    END AS Status2,
    pmv.Product_Code,
    pmv.Full_Description AS PRODUCT_NAME,
    adv.Actual_Qty,
    adv.Received_Qty,
    adv.Damaged_Qty,
    adv.Missed_Qty,
    adv.Excess_Qty
FROM
    {bronze_catalog}.{bronze_schema}.ASN_Header_V ahv
LEFT JOIN {bronze_catalog}.{bronze_schema}.ASN_Detail_V AS adv ON
    (adv.ASN_Header_Id = ahv.ASN_Header_Id
        AND adv.Instance = ahv.Instance
        AND adv.Active = ahv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS scv ON
(scv.Sales_Center_Id = ahv.Sales_Center_Id 
AND scv.Instance = ahv.Instance
AND scv.Active = ahv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS pmv ON
(pmv.Product_Id = adv.Product_Id  
AND pmv.Instance = ahv.Instance
AND pmv.Active = ahv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS uv ON
(uv.User_Id = adv.Created_By 
AND uv.Instance = adv.Instance
AND uv.Active = adv.Active)
WHERE
1 = 1
and to_date(ahv.Acceptance_Date) = '{date_begin}'
"""

df_l01 = spark.sql(l01_remisiones_query)

perform_overwrite_partition(
    source_df=df_l01,
    target_table_name="L01_Remisiones",
    partition_filter=f"cast(GH_Acceptance_Date as DATE) = '{date_begin}'"
)

# COMMAND ----------
# Activity: A05_PesitoCredit
# Logic: Delete where storeday = FECHA -> Insert

print("Processing A05_PesitoCredit...")

a05_pesito_query = f"""
SELECT
    DISTINCT 
    *,
    cast('{date_begin}' AS DATE) AS storeday
FROM
    (
    SELECT
        sc.Sales_Center_Code,
        rv.Route_Code,
        rv.Active,
        rev.Retailer_Code,
        rev.Retailer_Name,
        CONCAT('#', rdp.Invoice_Header_No) AS Invoice_No,
        to_date(rdp.Invoice_Date) AS Invoice_Date,
        rdp.Invoice_Amount,
        rdp.Paid_Amount AS Receipt_Amount,
        rdp.Pesito_Credit_Amount AS Pending_Comissions,
        rdp.Visit_Count AS Visit_Number,
        rdp.Pesito_Commission_Amount AS Paid_Amount_only_commision,
        rdp.Paid_Amount + rdp.Pesito_Commission_Amount AS Paid_Amont_with_Commision,
        rdp.Balance_Amount - rdp.Pesito_Credit_Amount AS Credit_balance,
        rdp.Paid_Amount AS Accumulated_Paid_Amount
    FROM
        (
        SELECT
            *,
            row_number() over(partition by Invoice_Header_No
        order by
            Update_date desc) Fila
        FROM
            {bronze_catalog}.{bronze_schema}.Retailer_Due_Payment_V) AS rdp
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
        (rdp.Sales_Center_Id = sc.Sales_Center_Id
            AND rdp.Instance = sc.Instance)
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
        (rdp.Route_Id = rv.Route_Id
            AND rdp.Sales_Center_Id = rv.Sales_Center_Id
            AND rdp.Instance = rv.Instance)
    LEFT JOIN {bronze_catalog}.{bronze_schema}.User_Route_Mapping_V AS urm ON
        (rv.Route_Id = urm.Route_Id
            AND rv.Sales_Center_Id = urm.Sales_Center_Id
            AND rv.Instance = urm.Instance)
    LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS uv ON
        (urm.Seller_Id = uv.User_Id
            AND urm.Instance = uv.Instance)
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Retailer_V AS rev ON
        (rdp.Retailer_Id = rev.Retailer_Id
            AND rdp.Instance = rev.Instance)
    WHERE
        1 = 1
        AND rdp.Fila = 1
        AND rdp.Active = true
        AND rv.Active = true
        AND sc.Active = true
        AND uv.ACTIVE = true) AS PC
ORDER BY
    Sales_Center_Code,
    Invoice_No
"""

df_a05 = spark.sql(a05_pesito_query)

perform_overwrite_partition(
    source_df=df_a05,
    target_table_name="FACTS_A05_PesitoCredit",
    partition_filter=f"storeday = '{date_begin}'"
)

# COMMAND ----------
# ---------------------------------------------------------
# CHAIN 2: V03_Tmp -> V11 -> V03 -> Semaforo... -> V06
# ---------------------------------------------------------

# Activity: V03_POMS_Tmp
# Logic: Delete where delivery_date = FECHA -> Insert

print("Processing V03_POMS_Tmp...")

v03_poms_tmp_query = f"""
SELECT
    to_date(SPH.date) as Fecha,
    cast(SC.Sales_Center_Code as string) as SC_Code,
    U.User_Code as Seller,
    R.Route_Code,
    P.Product_Code,
    to_date(sph.delivery_date) as delivery_date,
    sph.submitted_date,
    SPH.reference_no,
    cast(SPD.Modified_Qty * SPD.Uom_Conversion_Qty as int) as qty,
    case
        when PL.Level_Code = 'SKU' then 'PIECE'
        else PL.Level_Code
    end as piece,
    round(PM.Price, 2) as BasePrice,
    round((SPD.Modified_Qty * SPD.Uom_Conversion_Qty)* PM.Price, 2) as Order_LineValue,
    R.Cost_Center
FROM
    {bronze_catalog}.{bronze_schema}.Van_Stock_Proposal_Header_V AS SPH
left outer join {bronze_catalog}.{bronze_schema}.Van_Stock_Proposal_Detail_V as SPD 
on
    (SPD.Van_Stock_Proposal_Header_Id = SPH.Van_Stock_Proposal_Header_Id
        and SPD.Instance = SPH.Instance)
left outer join {bronze_catalog}.{bronze_schema}.Product_Master_V as P
on
    (P.Product_Id = SPD.Product_Id
        and P.Instance = SPH.Instance)
left outer join {bronze_catalog}.{bronze_schema}.Product_Level_V as PL
on
    (PL.Product_Level_Id = P.Product_Level_Id
        and PL.Instance = SPH.Instance)
right outer join {bronze_catalog}.{bronze_schema}.Sales_Center_V as SC 
on
    (SC.Sales_Center_Id = SPH.Sales_Center_Id
        and SPH.Instance = SC.Instance)
left outer join {bronze_catalog}.{bronze_schema}.User_V as U 
on
    (U.User_Id = SPH.User_Id
        and u.Instance = SPH.Instance)
left outer join {bronze_catalog}.{bronze_schema}.User_Route_Mapping_V as URM 
on
    (URM.Instance = SPH.Instance
        and URM.Seller_Id = SPH.User_Id
        and URM.Sales_Center_Id = SPH.Sales_Center_Id)
left outer join {bronze_catalog}.{bronze_schema}.Route_V as R
on
    (R.Route_Id = URM.Route_Id
        and R.Sales_Center_Id = SPH.Sales_Center_Id
        and R.Instance = SPH.Instance)
left outer join {bronze_catalog}.{bronze_schema}.Price_Master_V as PM
on
    (PM.Product_ID = P.Product_ID
        and PM.Product_Uom_Id = 860
        and PM.Instance = SPH.Instance
        and PM.Group_Id = 1)
where
    1 = 1
    AND to_date(sph.delivery_date) = '{date_begin}'
    AND R.Route_Code <> 'NULL'
    AND sph.Status = 'S'
"""

df_v03_poms_tmp = spark.sql(v03_poms_tmp_query)

perform_overwrite_partition(
    source_df=df_v03_poms_tmp,
    target_table_name="FACTS_V03_POMS_Tmp",
    partition_filter=f"delivery_date = '{date_begin}'"
)

# COMMAND ----------
# Activity: V11_Dailykpi
# Logic: Delete where TargetDate = FECHA -> Insert

print("Processing V11_Dailykpi...")

v11_dailykpi_query = f"""
SELECT
    cast(tdv.Target_Detail_Id as long) as Mapping_Id,
    scv.Sales_Center_Code as DailyKPI_SalesCenter_DwID,
    rv.Route_Code as DailyKPI_Route_DwID,
    et.lov_code as KPI,
    tdv.Target as TargetValue,
    to_date(tdv.Effective_From) as TargetDate,
    cast(tdv.Active as boolean) as IsActive,
    cast(tdv.Created_By as long) as CreatedBy,
    to_date(thv.Created_Date) as CreatedDate,
    cast(tdv.Modified_By as long) as ModifiedBy,
    tdv.Modified_Date as ModifiedDate
FROM
    {bronze_catalog}.{bronze_schema}.Target_Detail_V tdv
LEFT JOIN {bronze_catalog}.{bronze_schema}.Target_Header_V AS thv ON
    (thv.Target_Header_Id = tdv.Target_Header_Id
    and thv.Instance = tdv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
    (thv.Route_Id = rv.Route_Id
    and thv.Instance = rv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS scv ON
    (scv.Sales_Center_Id = rv.Sales_Center_Id 
    and scv.Instance = rv.Instance)
LEFT JOIN
    (
    SELECT
        DISTINCT Lov_Id,
        lov_parent_id,
        Lov_Code,
        Instance,
        Lov_Type,
        Active 
    FROM
        {bronze_catalog}.{bronze_schema}.List_Value_V
    WHERE
        Lov_Type = 'TARGET_PARAMETERS') AS et ON
    (thv.Target_Type_Id = et.lov_id
        AND thv.Instance = et.Instance
        AND thv.Active = et.Active)
WHERE
    1 = 1
    and to_date(tdv.Effective_From) = '{date_begin}'
"""

df_v11 = spark.sql(v11_dailykpi_query)

perform_overwrite_partition(
    source_df=df_v11,
    target_table_name="FACTS_V11_Dailykpi",
    partition_filter=f"TargetDate = '{date_begin}'"
)

# COMMAND ----------
# Activity: V03_POMS
# Logic: Delete where delivery_date = FECHA -> Insert
# Same structure as V03_POMS_Tmp but Status = 'A'

print("Processing V03_POMS...")

v03_poms_query = v03_poms_tmp_query.replace("sph.Status = 'S'", "sph.Status = 'A'")

df_v03_poms = spark.sql(v03_poms_query)

perform_overwrite_partition(
    source_df=df_v03_poms,
    target_table_name="FACTS_V03_POMS",
    partition_filter=f"delivery_date = '{date_begin}'"
)

# COMMAND ----------
# Activity: SemaforizacionRutas
# Logic: Source >= 1 month ago. Sink: Delete >= 2 months ago -> Insert.
# OPTIMIZATION: Merge last 1 month.

print("Processing SemaforizacionRutas...")

one_month_ago_df = spark.sql(f"SELECT date_sub('{date_end}', 30) as d")
one_month_ago = one_month_ago_df.first().d

semaforizacion_rutas_query = f"""
SELECT
    sc.Sales_Center_Code AS Sales_Center,
    sc.Sales_Center_Name AS Nombre_Sales_Center,
    rv.Route_Code AS Codigo_de_Ruta, 
    tv.Trip_Id AS Trip_Id,
    rv.Route_Name AS Nombre_de_Ruta,
    tv.Created_Date AS Fecha_de_Creacion,
    tv.Status AS Estatus,
    rcshv.Date as Fecha_Liquidacion, 
    rsshv.Total_Variance_Amount AS Variacion,
    rcshv.Amount AS Cantidad_Recibida
FROM
    {bronze_catalog}.{bronze_schema}.Trip_V AS tv
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_Stock_Settlement_Header_V AS rsshv ON
    (tv.Trip_Record_Id = rsshv.Trip_Record_Id 
        AND tv.Instance = rsshv.Instance
        AND tv.Active = rsshv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_Cash_Settlement_Header_V AS rcshv ON
    (rsshv.Route_Stock_Settlement_Header_Id = rcshv.Route_Stock_Settlement_Header_Id   
        AND rcshv.Instance = rsshv.Instance
        AND rcshv.Active = rsshv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
    (sc.Sales_Center_Id = tv.Sales_Center_Id 
        AND sc.Instance = tv.Instance
        AND sc.Active = tv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
    (rv.Route_Id = tv.Route_Id 
        AND rv.Instance = tv.Instance
        AND rv.Active = tv.Active)
where tv.Created_Date >= '{one_month_ago}'
"""

df_semaforo_rutas = spark.sql(semaforizacion_rutas_query)

perform_merge(
    source_df=df_semaforo_rutas,
    target_table_name="DMO_SemaforizacionRutas",
    merge_condition="tgt.Trip_Id = src.Trip_Id AND tgt.Sales_Center = src.Sales_Center",
    unique_keys=["Trip_Id", "Sales_Center"]
)

# COMMAND ----------
# Activity: SemaforizacionMes
# Logic: Truncate -> Insert

print("Processing SemaforizacionMes...")

semaforizacion_mes_query = f"""
SELECT
    Sales_Center,
    Nombre_Sales_Center,
    Codigo_de_Ruta,
    Trip_Id,
    Nombre_de_Ruta,
    Estatus,
    Fecha_de_Creacion,
    Fecha_Liquidacion,
    Variacion, 
    Cantidad_Recibida    
FROM
    {silver_catalog}.{silver_schema}.DMO_SemaforizacionRutas
where Fecha_de_Creacion >= '{one_month_ago}'
"""

df_semaforo_mes = spark.sql(semaforizacion_mes_query)

print(f"Overwriting {silver_catalog}.{silver_schema}.DMO_SemaforizacionMes")
df_semaforo_mes.write.format("delta").mode("overwrite").saveAsTable(f"{silver_catalog}.{silver_schema}.DMO_SemaforizacionMes")

# COMMAND ----------
# Activity: SemaforizacionRutas_Pagos
# Logic: Merge last 1 month.

print("Processing SemaforizacionRutas_Pagos...")

semaforizacion_pagos_query = f"""
SELECT DISTINCT 
    sc.Sales_Center_Code AS Sales_Center,
    sc.Sales_Center_Name AS Nombre_Sales_Center,
    rv.Route_Code AS Codigo_de_Ruta, 
    rv.Route_Name AS Nombre_de_Ruta,
    tv.Created_Date AS Fecha_Y_Hora_de_Creacion,
    to_date(tv.Created_Date) AS Fecha_de_Creacion,
    date_format(tv.Created_Date, 'HH:mm:ss') AS Hora_de_Creacion,
    tv.Status AS Estatus,
    rcshv.Date AS Fecha_Liquidacion, 
    rcshv.Amount AS Cantidad_Recibida_Total,
    lvv.Lov_Name AS Forma_De_Pago,
    rcsdv.amount AS Cantidad_Esperada,
    rcsdv.acutal_amount AS Cantidad_Recibida,
    rcsdv.variance_amount AS `Variación`
FROM
    {bronze_catalog}.{bronze_schema}.Trip_V AS tv
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_Stock_Settlement_Header_V AS rsshv ON
    (tv.Trip_Record_Id = rsshv.Trip_Record_Id
        AND tv.Instance = rsshv.Instance
        AND tv.Active = rsshv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_Cash_Settlement_Header_V AS rcshv ON
    (rsshv.Route_Stock_Settlement_Header_Id = rcshv.Route_Stock_Settlement_Header_Id
        AND rcshv.Instance = rsshv.Instance
        AND rcshv.Active = rsshv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_Cash_Settlement_Detail_V AS rcsdv ON
    (rcsdv.Route_Cash_Settlement_Header_Id = rcshv.Route_Cash_Settlement_Header_Id
        and rcsdv.Instance = rcshv.Instance
        and rcsdv.Active = rcshv.Active)
INNER JOIN {bronze_catalog}.{bronze_schema}.List_Value_V AS lvv ON 
    (rcsdv.Payment_Type_Id = lvv.Lov_Id)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
    (sc.Sales_Center_Id = tv.Sales_Center_Id
        AND sc.Instance = tv.Instance
        AND sc.Active = tv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
    (rv.Route_Id = tv.Route_Id
        AND rv.Instance = tv.Instance
        AND rv.Active = tv.Active)
where
    tv.Created_Date >= '{one_month_ago}'
    and lvv.Lov_Name in (
    'Efectivo',
    'Cheques',
    'Transferencias Electrónicas',
    'Crédito AR',
    'Crédito Pesito',
    'Compra de Producto')
"""

df_semaforo_pagos = spark.sql(semaforizacion_pagos_query)

perform_merge(
    source_df=df_semaforo_pagos,
    target_table_name="DMO_SemaforizacionRutas_Pagos",
    merge_condition="tgt.Sales_Center = src.Sales_Center AND tgt.Codigo_de_Ruta = src.Codigo_de_Ruta AND tgt.Fecha_Y_Hora_de_Creacion = src.Fecha_Y_Hora_de_Creacion AND tgt.Forma_De_Pago = src.Forma_De_Pago",
    unique_keys=["Sales_Center", "Codigo_de_Ruta", "Fecha_Y_Hora_de_Creacion", "Forma_De_Pago"]
)

# COMMAND ----------
# Activity: Semaforizacion_SSCDD
# Logic: Truncate -> Insert

print("Processing Semaforizacion_SSCDD...")

sscd_query = f"""
select
    distinct
    scv.Sales_Center_Code,
    scv.Sales_Center_Name,
    rv.Route_Code,
    rv.Route_Name,
    to_date(art.Visit_Date) as Visit_Date,
    'Sincronización sin cierre de día' as Status,
    art.Instance
from {bronze_catalog}.{bronze_schema}.Retailer_Visit_Header_V art
left join {bronze_catalog}.{bronze_schema}.Trip_V as TT on
    (TT.Sales_Center_Id = art.Sales_Center_Id
    and art.Route_Id  = TT.Route_Id
    and to_date(TT.Trip_End) = to_date(art.Exit_Time)
    and TT.Instance = art.Instance)
INNER JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V scv
    ON art.Sales_Center_Id = scv.Sales_Center_Id
    and art.Instance = scv.Instance
INNER JOIN {bronze_catalog}.{bronze_schema}.Route_V rv
    ON art.Sales_Center_Id = rv.Sales_Center_Id
    and art.Route_Id = rv.Route_Id
    and art.Instance = rv.Instance
where TT.Route_Id is null
    and rv.Active = true
    and to_date(art.Visit_Date) >= '{one_month_ago}'
"""

df_sscd = spark.sql(sscd_query)

print(f"Overwriting {silver_catalog}.{silver_schema}.Semaforizacion_SSCDD")
df_sscd.write.format("delta").mode("overwrite").saveAsTable(f"{silver_catalog}.{silver_schema}.Semaforizacion_SSCDD")

# COMMAND ----------
# Activity: C07_StockAllocation
# Logic: Delete where Date = FECHA -> Insert

print("Processing C07_StockAllocation...")

c07_stock_query = f"""
SELECT
    sc.Sales_Center_Code AS sc_code,
    rv.Route_Code,
    vlh.Reference_No,
    to_date(vlh.Date) AS Date,
    vlh.Submitted_Date AS Transaction,
    pmv.Product_Code,
    pmv.Short_Description,
    SUM(CASE pu.Lov_Name WHEN 'Tray' THEN vld.Load_Qty ELSE 0 END) AS Tray,
    SUM(CASE pu.Lov_Name WHEN 'Bag' THEN vld.Load_Qty ELSE 0 END) AS Bag,
    SUM(CASE pu.Lov_Name WHEN 'Piece' THEN vld.Load_Qty ELSE 0 END) AS Piece,
    SUM(vld.Load_Value) AS load_value
FROM
    {bronze_catalog}.{bronze_schema}.Van_Load_Header_V as vlh
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
    (vlh.Sales_Center_Id = sc.Sales_Center_Id
        AND vlh.Instance = sc.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
    (vlh.Route_Id = rv.Route_Id
        AND vlh.Sales_Center_Id = rv.Sales_Center_Id
        AND vlh.Instance = rv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS uv ON
    (vlh.User_Id = uv.User_Id
        AND vlh.Sales_Center_Id = uv.Sales_Center_Id
        AND vlh.Instance = uv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Van_Load_Detail_V AS vld ON
    vlh.Van_Load_Header_Id = vld.Van_Load_Header_Id 
    AND vlh.Instance = vld.Instance
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS pmv ON
    vld.Product_Id = pmv.Product_Id  
    AND vld.Instance = pmv.Instance
    AND pmv.Product_Level_Id = 5
LEFT JOIN
    (
    SELECT
        DISTINCT Lov_Id,
        Lov_Code,
        Lov_Name,
        Instance,
        Lov_Type
    FROM
        {bronze_catalog}.{bronze_schema}.List_Value_V
    WHERE
        Lov_Type = 'PRODUCT_UOM') AS pu ON
    (vld.Load_Uom_Id = pu.Lov_Id
        AND vld.Instance = pu.Instance)
WHERE
    1 = 1
    AND to_date(vlh.Date) = '{date_begin}'
    AND vlh.Status = 'A'
GROUP BY
    sc.Sales_Center_Code,
    rv.Route_Code,
    vlh.Reference_No,
    to_date(vlh.Date),
    vlh.Date,
    vlh.Submitted_Date,
    pmv.Product_Code,
    pmv.Short_Description
"""

df_c07 = spark.sql(c07_stock_query)

perform_overwrite_partition(
    source_df=df_c07,
    target_table_name="C07_StockAllocation",
    partition_filter=f"Date = '{date_begin}'"
)

# COMMAND ----------
# Activity: V02_VanRejection
# Logic: Delete where Date = FECHA -> Insert

print("Processing V02_VanRejection...")

v02_rejection_query = f"""
SELECT
    sc.Sales_Center_Code AS sc_code,
    rv.Route_Code,
    vlh.Reference_No,
    to_date(vlh.Rejection_Server_Date) AS Date,
    vlh.Rejection_Server_Date AS Transaction,
    pmv.Product_Code,
    vld.Missing_Qty AS MissingQty,
    0 AS ColdQty,
    0 AS DamageQty
FROM
    {bronze_catalog}.{bronze_schema}.Van_Load_Header_V AS vlh
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
    (vlh.Sales_Center_Id = sc.Sales_Center_Id
        AND vlh.Instance = sc.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
    (vlh.Route_Id = rv.Route_Id
        AND vlh.Sales_Center_Id = rv.Sales_Center_Id
        AND vlh.Instance = rv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Van_Rejection_Detail_V AS vld ON
    (vlh.Van_Load_Header_Id = vld.Van_Load_Header_Id 
    AND vlh.Instance = vld.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS pmv ON
    vld.Product_Id = pmv.Product_Id  
    AND vld.Instance = pmv.Instance
    AND pmv.Product_Level_Id = 5
WHERE
    1 = 1
    AND to_date(vlh.Rejection_Server_Date) = '{date_begin}'
    AND vlh.Status = 'A'
"""

df_v02 = spark.sql(v02_rejection_query)

perform_overwrite_partition(
    source_df=df_v02,
    target_table_name="V02_VanRejection",
    partition_filter=f"Date = '{date_begin}'"
)

# COMMAND ----------
# Activity: Z01_Payroll
# Logic: Delete Date BETWEEN fecha_ini AND FECHA -> Insert

# We need a two_days_ago variable
two_days_ago = spark.sql(f"SELECT date_sub('{date_begin}', 1) as d").first().d

print(f"Processing Z01_Payroll for range {two_days_ago} to {date_begin}...")

z01_payroll_query = f"""
SELECT
    epv.Sales_Center_Code,
    to_date(epv.SalesDate) AS Date,
    epv.Seller_Route_Code AS Route_Code,
    epv.Seller_Code,
    epv.Seller_Name,
    epv.BackupSeller_Code,
    epv.BackupSeller_Name,
    CASE
        WHEN epv.Seller_Code = epv.BackupSeller_Code THEN 'NORMAL'
        ELSE 'SUPLENCIA'
    END AS Status,
    lvp.Lov_Name AS Job_Position,
    epv.SalesAmount AS Sales_Amount,
    CASE
        epv.Parent_Id WHEN 1 THEN 'OBM'
        WHEN 2 THEN 'OBL'
        WHEN 3 THEN 'RIC'
        ELSE NULL
    END AS Org
FROM
    {bronze_catalog}.{bronze_schema}.Employee_Payroll_V AS epv
LEFT JOIN (
    SELECT
        Lov_Id,
        Lov_Code,
        Lov_Name,
        Instance
    FROM
        {bronze_catalog}.{bronze_schema}.List_Value_V
    WHERE
        Lov_Type = 'META4_POSITION'
        AND Active = true) AS lvp ON
    (epv.Seller_JobPosition_Id = lvp.Lov_Id
        AND epv.Instance = lvp.Instance)
WHERE
    1 = 1
    AND to_date(SalesDate) BETWEEN '{two_days_ago}' AND '{date_begin}'
"""

df_z01 = spark.sql(z01_payroll_query)

perform_overwrite_partition(
    source_df=df_z01,
    target_table_name="Z01_Payroll",
    partition_filter=f"Date BETWEEN '{two_days_ago}' AND '{date_begin}'"
)

# COMMAND ----------
# Activity: Z07_Dev_SalesCenter
# Logic: Delete where Date = FECHA -> Insert

print("Processing Z07_Dev_SalesCenter...")

z07_dev_query = f"""
SELECT
    sc.Sales_Center_Code AS sc_code,
    scrh.Reference_No,
    scrh.Date,
    scrh.Net_Amount,
    m05.Product_code,
    m05.Product_Full_Desc,
    scrd.Uom_Id,
    scrd.Qty,
    scrd.Price,
    scrd.Line_Value,
    scrd.Return_Reason
FROM
    {bronze_catalog}.{bronze_schema}.Sales_Center_Return_Header_V AS scrh
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
    (scrh.Sales_Center_Id = sc.Sales_Center_Id
        AND scrh.Instance = sc.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_Return_Detail_V AS scrd ON
    (scrh.Sales_Center_Return_Header_Id = scrd.Sales_Center_Return_Header_Id
        AND scrh.Instance = scrd.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.VW_M05_ProductMaster AS m05 ON
    (scrd.Product_Id = m05.Product_Id
        AND scrd.Instance = m05.Instance
        AND m05.Active = true)
WHERE
    1 = 1
    AND scrh.Active = true
    AND to_date(scrh.DATE) = '{date_begin}'
"""

df_z07 = spark.sql(z07_dev_query)

perform_overwrite_partition(
    source_df=df_z07,
    target_table_name="Z07_Dev_SalesCenter",
    partition_filter=f"Date = '{date_begin}'"
)

# COMMAND ----------
# Activity: Z09_Inventory
# Logic: Delete where StockFreezeDate = FECHA -> Insert

print("Processing Z09_Inventory...")

z09_inv_query = f"""
SELECT
    Sales_Center_Code,
    StockFreezeDate,
    Product_Code,
    Price,
    StockQty,
    StockAmount,
    Salable,
    Instance
FROM
    {bronze_catalog}.{bronze_schema}.TBL_DYSCStockFreeze
WHERE 
    to_date(StockFreezeDate) = '{date_begin}'
"""

df_z09 = spark.sql(z09_inv_query)

perform_overwrite_partition(
    source_df=df_z09,
    target_table_name="Z09_Inventory",
    partition_filter=f"StockFreezeDate = '{date_begin}'"
)

# COMMAND ----------
# Activity: V15_ProductBuying
# Logic: Insert where Created_Date = GETDATE() - 1 (Yesterday)

print("Processing V15_ProductBuying...")

v15_prod_buying_query = f"""
SELECT
DISTINCT 
    a.Invoice_Date AS InvoiceDate,
    a.Invoice_No AS InvoiceNumber,
    c.Product_Code AS Product_DwID,
    b.Product_Free_Qty AS ProductBuyingColdQty,
    b.Invoice_Qty AS ProductBuyingFreshQty,
    d.Sales_Center_Code AS SalesCenter_DwID,
    cast(a.Active as boolean) AS IsActive,
    cast(a.Created_By as long) as Created_By,
    a.Created_Date,
    '' AS Modified_By,
    '' AS Modified_Date
FROM
    {bronze_catalog}.{bronze_schema}.Invoice_Header_V a
LEFT JOIN {bronze_catalog}.{bronze_schema}.Invoice_Detail_V AS b ON
(b.Invoice_Header_Id = a.Invoice_Id
and b.Instance = a.Instance 
and b.Active = a.Active)	
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS c ON
(c.Product_Id = b.Product_Id  
and c.Instance = b.Instance 
and c.Active = b.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS d ON
(d.Sales_Center_Id = b.Sales_Center_Id  
and d.Instance = b.Instance 
and d.Active = b.Active)
WHERE 
1=1
AND to_date(a.Created_Date) = '{date_begin}'
"""

df_v15 = spark.sql(v15_prod_buying_query)

perform_overwrite_partition(
    source_df=df_v15,
    target_table_name="V15_ProductBuying",
    partition_filter=f"to_date(Created_Date) = '{date_begin}'"
)

# COMMAND ----------
# Activity: V16_PurchaseReturn
# Logic: Insert where Created_Date = GETDATE() - 1

print("Processing V16_PurchaseReturn...")

v16_return_query = f"""
SELECT
DISTINCT 
    cast(b.Sales_Center_Return_Detail_Id as long) PurchaseReturn_DwID,
    c.Sales_Center_Code SalesCenter_DwID,
    d.User_Code Seller_DwID,
    a.Reference_No PurchaseReturnNo,
    a.Date PurchaseReturnDate,
    e.Product_Code Product_DwID, 
    cast(b.Qty as int) as Qty,
    et.Lov_Code UOM,
    cast(b.price as decimal(18,2)) PRPrice,
    cast(b.line_value as decimal(18,2)) LineValue,
    cast(f.is_salable_warehouse as boolean) IsSaleable,
    b.return_reason Reason,
    a.created_date TransactionDate,
    f.warehouse_code ReturnStockLocation,	
    cast(a.active as boolean) IsActive,
    cast(a.created_by as long) CreatedBy,
    b.created_date CreatedDate,
    cast(e.modified_by as long) ModifiedBy,
    e.Modified_Date ModifiedDate
FROM
    {bronze_catalog}.{bronze_schema}.Sales_Center_Return_Header_V a
INNER JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_Return_Detail_V AS b ON
    (a.Sales_Center_Return_Header_Id = b.Sales_Center_Return_Header_Id
    AND a.Instance = b.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS c ON
    (b.Sales_Center_Id = c.Sales_Center_Id
    AND b.Instance = c.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS d ON
    (a.User_Id = d.User_Id
    AND a.Instance = d.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS e ON
    (b.Product_Id = e.Product_Id
    AND b.Instance = e.Instance)
LEFT JOIN
    (
    SELECT
        DISTINCT Lov_Id,
        Lov_Code,
        Instance,
        Lov_Type,
        Active 
    FROM
        {bronze_catalog}.{bronze_schema}.List_Value_V
    WHERE
        Lov_Type = 'PRODUCT_UOM') AS et ON
    (b.uom_id = et.Lov_Id
        AND b.instance = et.Instance
        AND b.Active = et.active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Warehouse_V AS f ON
    (a.sales_center_id = f.sales_center_id)
WHERE
    1 = 1
    AND f.is_default_warehouse  = true
    AND to_date(a.Created_Date) = '{date_begin}'
"""

df_v16 = spark.sql(v16_return_query)

perform_overwrite_partition(
    source_df=df_v16,
    target_table_name="V16_PurchaseReturn",
    partition_filter=f"to_date(CreatedDate) = '{date_begin}'"
)

# COMMAND ----------
# Activity: V01_VanUnload
# Logic: Insert where Mobile_Date = GETDATE() - 1

print("Processing V01_VanUnload...")

v01_unload_query = f"""
SELECT
    DISTINCT 
    d.Sales_Center_Code SalesCenter_DwID,
    a.Mobile_Date Date_DwID,
    e.Route_Code Route_DwID, 
    f.product_code Product_DwID,
    f.Full_Description ProductName,
    SUM(COALESCE (CASE 
        WHEN c.reason_id = 0 THEN c.unload_qty
    END, 0)) AS Freshqty,
    SUM(COALESCE (CASE 
        WHEN c.reason_id = 1035 THEN c.unload_qty
    END, 0)) AS Coldqty,
    SUM(COALESCE (CASE 
        WHEN c.reason_id = 1003 THEN c.unload_qty
    END, 0)) AS Damageqty,
        SUM(COALESCE (CASE 
        WHEN c.reason_id = 1002 THEN c.unload_qty
    END, 0)) AS NonSalable
FROM
    {bronze_catalog}.{bronze_schema}.Trip_V a
LEFT JOIN {bronze_catalog}.{bronze_schema}.Van_Unload_Header_V AS b ON
    (a.Trip_Id = b.Trip_Id
        AND a.Instance = b.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Van_UnLoad_Detail_V AS c ON
    (b.Van_Unload_Header_Id = c.Van_UnLoad_Header_Id
        AND a.Instance = b.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS d ON
    (a.Sales_Center_Id = d.Sales_Center_Id
        AND a.Instance = d.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS e ON
    (a.Route_Id = e.Route_Id
        AND a.Instance = e.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS f ON
    (c.Product_Id = f.Product_Id
        AND c.Instance = f.Instance)
WHERE
    1 = 1
    AND to_date(a.Mobile_Date) = '{date_begin}'
GROUP BY
    d.Sales_Center_Code,
    a.Mobile_Date,
    e.Route_Code,
    f.product_code,
    f.Full_Description
"""

df_v01 = spark.sql(v01_unload_query)

perform_overwrite_partition(
    source_df=df_v01,
    target_table_name="V01_VanUnload",
    partition_filter=f"to_date(Date_DwID) = '{date_begin}'"
)

# COMMAND ----------
# Activity: Usuarios_Loggeados
# Logic: Truncate -> Insert
# Query: DATE >= DATEADD(DAY ,-7, GETDATE())

print("Processing Usuarios_Loggeados...")

seven_days_ago = spark.sql(f"SELECT date_sub('{date_end}', 7) as d").first().d

usuarios_query = f"""
SELECT
    a.Device_Id,
    a.Activity_Code,
    a.DATE,
    b.User_Code,
    SUBSTRING(b.User_Code, 8,4) AS Ruta,
    c.Sales_Center_Code,
    c.Sales_Center_Name 
FROM
    {bronze_catalog}.{bronze_schema}.Log_User_Activity_V a
LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS b ON
    (a.User_Id = b.user_id)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS c ON
    (b.Sales_Center_Id = c.Sales_Center_Id)
WHERE
    1 = 1
    AND to_date(a.DATE) >= '{seven_days_ago}'
    AND a.Device_Id is not null
GROUP BY
    a.Device_Id,
    a.Activity_Code,
    a.DATE,
    b.User_Code,
    c.Sales_Center_Code,
    c.Sales_Center_Name
"""

df_usuarios = spark.sql(usuarios_query)

print(f"Overwriting {silver_catalog}.{silver_schema}.Usuarios_logeos")
df_usuarios.write.format("delta").mode("overwrite").saveAsTable(f"{silver_catalog}.{silver_schema}.Usuarios_logeos")

# COMMAND ----------
# Activity: V06_VanStockInHand
# Logic: Delete where StockFreezeDate = FECHA -> Insert

print("Processing V06_VanStockInHand...")

v06_stock_query = f"""
SELECT
    StockFreezeDate,
    Sales_Center_Code,
    Route_Code,
    StockInitialDate,
    Price,
    Product_Code,
    Full_Description,
    Qty,
    Amount,
    Instance,
    Insert_date
FROM
    {bronze_catalog}.{bronze_schema}.TBL_DYVanStockFreeze
WHERE
    1 = 1
    AND to_date(StockFreezeDate) = '{date_begin}'
"""

df_v06 = spark.sql(v06_stock_query)

perform_overwrite_partition(
    source_df=df_v06,
    target_table_name="V06_VanStockInHand",
    partition_filter=f"to_date(StockFreezeDate) = '{date_begin}'"
)

# COMMAND ----------
# ---------------------------------------------------------
# CHAIN 3: A08 -> Z13 -> V22 -> Z12
# ---------------------------------------------------------

# Activity: A08_StockTransferWithinWarehouse
# Logic: Delete where CreatedDate = FECHA -> Insert

print("Processing A08_StockTransferWithinWarehouse...")

a08_transfer_query = f"""
SELECT
    DISTINCT
    cast(sthv.Stock_Transfer_Header_Id as long) AS AST_Id,
    stdv.Stock_Transfer_Detail_Id,
    scv.Sales_Center_Code AS SalesCenter_DwID,
    sthv.Reference_No AS TransferReferenceNo,
    CASE
        WHEN Source_Sub_Location_Id < 0 THEN 'Salable Location'
        WHEN Source_Sub_Location_Id > 0 THEN slv.Location_Name  
    END AS `SourceStockLocation`,
    CASE
        WHEN  sthv.Destination_Sub_Location_Id > 0 THEN slv1.Location_Name 
    END AS `DestStockLocation`,
    to_date(stdv.Created_Date) AS TransferedDate,
    sthv.Reason AS TransferReason,
    pmv.Product_Code AS Product_DwID,
    stdv.Transfer_Qty AS TransferredQty,
    et.lov_code AS UOM_DwID,
    stdv.Received_Qty AS ReceivedQty,
    stdv.Unit_Price AS BasePrice,
    stdv.Line_Value AS LineValue,
    sthv.Created_Date AS TransactionDate,
    cast(sthv.Active as boolean) AS IsActive,
    cast(sthv.created_by as long) AS CreatedBy,
    sthv.Created_Date AS CreatedDate,
    cast(sthv.Modified_By as long) AS ModifiedBy,
    sthv.Modified_Date AS ModifiedDate  
FROM
    {bronze_catalog}.{bronze_schema}.Stock_Transfer_Header_V sthv
LEFT JOIN {bronze_catalog}.{bronze_schema}.Stock_Transfer_Detail_V AS stdv ON
    (stdv.Stock_Transfer_Header_Id = sthv.Stock_Transfer_Header_Id
        and stdv.Instance = sthv.Instance
        and stdv.Active = sthv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS pmv ON
    (pmv.Product_Id = stdv.Product_Id
        AND pmv.Instance = stdv.Instance
        AND pmv.Active = stdv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS scv ON
    (scv.Sales_Center_Id = sthv.Sales_Center_Id 
        AND scv.Instance = sthv.Instance
        AND scv.Active = sthv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Uom_Group_Mapping_V AS pugmv ON
    (pugmv.Uom_Group_Id = pmv.Uom_Group_Id 
        and pugmv.Instance = pmv.Instance
        and pugmv.Active = pmv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Stock_Location_V AS slv1 ON
    (slv1.Stock_Location_Id = sthv.Destination_Sub_Location_Id  
        and slv1.Instance = sthv.Instance
        and slv1.Active = sthv.Active)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Stock_Location_V AS slv ON
    (slv.Stock_Location_Id = sthv.Source_Sub_Location_Id  
        and slv.Instance = sthv.Instance
        and slv.Active = sthv.Active)
LEFT JOIN
    (
    SELECT
        DISTINCT Lov_Id,
        Lov_Code,
        Instance,
        Lov_Type,
        Active 
    FROM
        {bronze_catalog}.{bronze_schema}.List_Value_V
    WHERE
        Lov_Type = 'PRODUCT_UOM') AS et ON
    (pugmv.Product_Uom_Type_ID = et.Lov_Id
        AND pugmv.Instance = et.Instance
        AND pugmv.Active = et.Active)
WHERE     
    1 = 1
    AND sthv.Status = 'S'
    AND pugmv.Is_Default_Uom = true
    AND to_date(stdv.Created_Date) = '{date_begin}'
"""

df_a08 = spark.sql(a08_transfer_query)

perform_overwrite_partition(
    source_df=df_a08,
    target_table_name="A08_StockTransferWithinWarehouse",
    partition_filter=f"to_date(CreatedDate) = '{date_begin}'"
)

# COMMAND ----------
# Activity: Z13_MobileVersionCode
# Logic: Delete where [Request Date] = FECHA -> Insert

print("Processing Z13_MobileVersionCode...")

z13_mobile_query = f"""
SELECT
    DISTINCT 
    Su.User_Code,
    Su.First_Name,
    lms.user_id,
    lms.Version_Code as VersionCode,
    -- get_json_object(lms.Request_Header, '$.VersionCode') as VersionCode,
    to_date(lms.Request_Date) as `Request_Date`,
    Su.Active,
    lms.Instance
FROM
    {bronze_catalog}.{bronze_schema}.Log_Mobile_Sync_V AS lms
INNER JOIN {bronze_catalog}.{bronze_schema}.User_V Su on
    Su.User_Id = lms.user_id
    AND Su.Instance = lms.Instance
where
    Request_Type = 'UserMaster'
    AND to_date(lms.Request_Date) = '{date_begin}'
"""

df_z13 = spark.sql(z13_mobile_query)

perform_overwrite_partition(
    source_df=df_z13,
    target_table_name="Z13_MobileVersionCode",
    partition_filter=f"Request_Date = '{date_begin}'"
)

# COMMAND ----------
# Activity: V22_POMS_AS
# Logic: Delete where order_date = FECHA -> Insert

print("Processing V22_POMS_AS...")

v22_poms_as_query = f"""
SELECT
    s.sales_center_code as sc_code,
    r.route_code as route_code,
    to_date(soh.Date) as order_date, 
    to_date(soh.Delivery_Date) as delivery_date,
    soh.order_number as order_number,
    rt.codigo_cliente as retailer_code,
    rt.nombre_cliente as retailer_name,
    p.Product_Code as product_code,
    concat(p.Short_Description, ' ', p.Bar_Code) as producto,
    sod.Line_Value as amount,
    case
        when lv.lov_name = 'TRAY' then sod.qty
        else 0
    end as tray,
    case
        when lv.lov_name = 'BAG' then sod.qty
        else 0
    end as bag,
    case
        when lv.lov_name = 'PIECE' then sod.qty
        else 0
    end as piece
FROM
    {bronze_catalog}.{bronze_schema}.Sales_Order_Header_V soh
left outer join {bronze_catalog}.{bronze_schema}.Sales_Center_V s on
    s.sales_center_id = soh.Sales_Center_Id
    and s.Active = soh.Active
left outer join {bronze_catalog}.{bronze_schema}.Route_V r on
    r.Route_Id = soh.Route_Id
    and r.Active = soh.Active
    and r.Sales_Center_Id = s.Sales_Center_Id
left outer join {bronze_catalog}.{bronze_schema}.Sales_Order_Detail_V sod on
    sod.Sales_Order_Header_Id = soh.Sales_Order_Header_Id
left outer join {bronze_catalog}.{bronze_schema}.Product_Master_V p on
    p.Product_Id = sod.Product_Id
    and p.Active = sod.Active
    and p.Instance = sod.Instance
left outer join {bronze_catalog}.{bronze_schema}.List_Value_V lv on
    lv.lov_id = sod.Uom_Id
    and lv.Active = sod.Active
    and lv.Instance = sod.Instance
left outer join (
    select
        distinct
         r.retailer_id,
        max(r.Retailer_Code) as codigo_cliente
    ,
        max(r.Retailer_Name) as nombre_cliente
    from
        {bronze_catalog}.{bronze_schema}.Retailer_V r
    inner join {bronze_catalog}.{bronze_schema}.List_Value_V lv on
        lv.lov_id = r.Format_Channel_Id
        and lv.Instance = r.Instance
        and lv.Active = r.Active
    where
        r.Active = true
    group by
        r.Retailer_id)rt on
    rt.retailer_id = soh.retailer_id
where
    1 = 1
    and soh.Active = true
    and r.Cost_Center = '561'
    and to_date(soh.Date) = '{date_begin}'
"""

df_v22 = spark.sql(v22_poms_as_query)

perform_overwrite_partition(
    source_df=df_v22,
    target_table_name="V22_POMS_AS",
    partition_filter=f"order_date = '{date_begin}'"
)

# COMMAND ----------
# Activity: Z12_FaltantesAS
# Logic: Delete where Date = FECHA -> Insert

print("Processing Z12_FaltantesAS...")

z12_faltantes_query = f"""
select DISTINCT 
    ADH.Sales_Center_Code as SC_code,
    to_date(DH.Date) as Date,
    SU.User_Code as User_code,
    PH.Product_Code as Product_code,
    PH.Full_Description as Product_name,
    DD.Quantity as Actual_qty_pieces,
    DD.Order_Qty as Order_qty,
    DD.Price as Price,
    DD.Missing_Qty As Rejection_Qty,
    DD.Rejection_Qty As Missing_Qty,
    DH.Trip_Id As Travel_id,
    ADH.Instance
from {bronze_catalog}.{bronze_schema}.Delivery_Header_V DH
Inner Join {bronze_catalog}.{bronze_schema}.Delivery_Detail_V DD on DH.Delivery_Header_Id=DD.Distributor_Id AND DH.Instance=DD.Instance
Inner Join {bronze_catalog}.{bronze_schema}.Sales_Center_V ADH on ADH.Sales_Center_Id=DH.Distributor_Id AND ADH.Instance=DH.Instance
Inner Join {bronze_catalog}.{bronze_schema}.Product_Master_V PH on PH.Product_Id=DD.Product_id AND PH.Instance=DD.Instance
Inner Join {bronze_catalog}.{bronze_schema}.User_V SU on SU.User_Id=DH.User_Id AND SU.Instance=DH.Instance
Where ADH.Sales_Center_Code like '02%' 
and to_date(DH.Date) = '{date_begin}'
"""

df_z12 = spark.sql(z12_faltantes_query)

perform_overwrite_partition(
    source_df=df_z12,
    target_table_name="Z12_FaltantesAS",
    partition_filter=f"Date = '{date_begin}'"
)
# COMMAND ----------
# Activity: C07_StockAllocation_estatus
# Logic: Delete where Date = begin_date -> Insert

print("Processing C07_StockAllocation_estatus...")

c07_stock_allocation_estatus_query = f"""
SELECT
    sc.Sales_Center_Code AS sc_code,
    rv.Route_Code,
    vlh.Reference_No,
    to_date(vlh.Submitted_Date) AS Date,
    CASE
        vlh.Status WHEN 'A' THEN 'Cargo Aceptado'
        WHEN 'I' THEN 'Cargo pendiente'
        ELSE vlh.Status
    END AS Status,
    vlh.Allocated_Status,
    em.Employee_Code AS Seller_Code,
    CONCAT(em.First_Name, ' ', em.Last_Name) AS Seller,
    uvs.User_Code AS Supervisor_Code,
    CONCAT(uvs.First_Name, ' ', uvs.Last_Name) AS Supervisor,
    pmv.Product_Code,
    pmv.Short_Description,
    pu.Lov_Name AS Presentacion,
    vld.Load_Qty,
    vld.Unit_Price,
    vld.Load_Value
FROM
    {bronze_catalog}.{bronze_schema}.Van_Load_Header_V as vlh
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
    (vlh.Sales_Center_Id = sc.Sales_Center_Id
        AND vlh.Instance = sc.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
    (vlh.Route_Id = rv.Route_Id
        AND vlh.Sales_Center_Id = rv.Sales_Center_Id
        AND vlh.Instance = rv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS uv ON
    (vlh.User_Id = uv.User_Id
        AND vlh.Sales_Center_Id = uv.Sales_Center_Id
        AND vlh.Instance = uv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Position_User_Mapping_V AS pum ON
    (vlh.User_Id = pum.User_Id
        AND vlh.Instance = pum.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Position_V AS pv ON
    (pum.Position_Id = pv.Position_Id
        AND pum.Instance = pv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Position_V AS pvs ON
    (pv.Parent_Id = pvs.Position_Id
        AND pv.Instance = pvs.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Position_User_Mapping_V AS pums ON
    (pvs.Position_Id = pums.Position_Id
        AND pvs.Instance = pums.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS uvs ON
    (pums.User_Id = uvs.User_Id
        AND vlh.Sales_Center_Id = uvs.Sales_Center_Id
        AND pums.Instance = uvs.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Employee_Master_V AS em ON
    vlh.User_Id = em.Own_Route_Id
    AND vlh.Instance = em.Instance
    AND em.Active = true
LEFT JOIN {bronze_catalog}.{bronze_schema}.Van_Load_Detail_V AS vld ON
    vlh.Van_Load_Header_Id = vld.Van_Load_Header_Id 
    AND vlh.Instance = vld.Instance
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS pmv ON
    vld.Product_Id = pmv.Product_Id  
    AND vld.Instance = pmv.Instance
    AND pmv.Product_Level_Id = 5
    AND pmv.Active = true
LEFT JOIN
    (
    SELECT
        DISTINCT Lov_Id,
        Lov_Code,
        Lov_Name,
        Instance,
        Lov_Type
    FROM
        {bronze_catalog}.{bronze_schema}.List_Value_V
    WHERE
        Lov_Type = 'PRODUCT_UOM') AS pu ON
    (vld.Load_Uom_Id = pu.Lov_Id
        AND vld.Instance = pu.Instance)
WHERE
    1 = 1
    AND sc.Sales_Center_Code LIKE '02%'
    AND to_date(vlh.Submitted_Date) = '{date_begin}'
"""

df_c07_estatus = spark.sql(c07_stock_allocation_estatus_query)

perform_overwrite_partition(
    source_df=df_c07_estatus,
    target_table_name="C07_StockAllocation_estatus",
    partition_filter=f"Date = '{date_begin}'"
)

# COMMAND ----------
# Activity: C07_StockAllocation_pendientes
# Logic: Truncate -> Insert

print("Processing C07_StockAllocation_pendientes...")

c07_stock_allocation_pendientes_query = f"""
SELECT
    sc.Sales_Center_Code AS sc_code,
    rv.Route_Code,
    vlh.Reference_No,
    to_date(vlh.Submitted_Date) AS Date,
    CASE
        vlh.Status WHEN 'A' THEN 'Cargo Aceptado'
        WHEN 'I' THEN 'Cargo pendiente'
        ELSE vlh.Status
    END AS Status,
    vlh.Allocated_Status,
    em.Employee_Code AS Seller_Code,
    CONCAT(em.First_Name, ' ', em.Last_Name) AS Seller,
    uvs.User_Code AS Supervisor_Code,
    CONCAT(uvs.First_Name, ' ', uvs.Last_Name) AS Supervisor,
    pmv.Product_Code,
    pmv.Short_Description,
    pu.Lov_Name AS Presentacion,
    vld.Load_Qty,
    vld.Unit_Price,
    vld.Load_Value
FROM
    {bronze_catalog}.{bronze_schema}.Van_Load_Header_V as vlh
LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc ON
    (vlh.Sales_Center_Id = sc.Sales_Center_Id
        AND vlh.Instance = sc.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv ON
    (vlh.Route_Id = rv.Route_Id
        AND vlh.Sales_Center_Id = rv.Sales_Center_Id
        AND vlh.Instance = rv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS uv ON
    (vlh.User_Id = uv.User_Id
        AND vlh.Sales_Center_Id = uv.Sales_Center_Id
        AND vlh.Instance = uv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Position_User_Mapping_V AS pum ON
    (vlh.User_Id = pum.User_Id
        AND vlh.Instance = pum.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Position_V AS pv ON
    (pum.Position_Id = pv.Position_Id
        AND pum.Instance = pv.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Position_V AS pvs ON
    (pv.Parent_Id = pvs.Position_Id
        AND pv.Instance = pvs.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Position_User_Mapping_V AS pums ON
    (pvs.Position_Id = pums.Position_Id
        AND pvs.Instance = pums.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.User_V AS uvs ON
    (pums.User_Id = uvs.User_Id
        AND vlh.Sales_Center_Id = uvs.Sales_Center_Id
        AND pums.Instance = uvs.Instance)
LEFT JOIN {bronze_catalog}.{bronze_schema}.Employee_Master_V AS em ON
    vlh.User_Id = em.Own_Route_Id
    AND vlh.Instance = em.Instance
    AND em.Active = true
LEFT JOIN {bronze_catalog}.{bronze_schema}.Van_Load_Detail_V AS vld ON
    vlh.Van_Load_Header_Id = vld.Van_Load_Header_Id 
    AND vlh.Instance = vld.Instance
LEFT JOIN {bronze_catalog}.{bronze_schema}.Product_Master_V AS pmv ON
    vld.Product_Id = pmv.Product_Id  
    AND vld.Instance = pmv.Instance
    AND pmv.Product_Level_Id = 5
    AND pmv.Active = true
LEFT JOIN
    (
    SELECT
        DISTINCT Lov_Id,
        Lov_Code,
        Lov_Name,
        Instance,
        Lov_Type
    FROM
        {bronze_catalog}.{bronze_schema}.List_Value_V
    WHERE
        Lov_Type = 'PRODUCT_UOM') AS pu ON
    (vld.Load_Uom_Id = pu.Lov_Id
        AND vld.Instance = pu.Instance)
WHERE
    1 = 1
    AND (sc.Sales_Center_Code LIKE '02%' OR sc.Sales_Center_Code LIKE '012%')
    AND vlh.Status = 'I'
    AND to_date(vlh.Submitted_Date) BETWEEN '2024-01-01' AND '{date_begin}'
"""

df_c07_pendientes = spark.sql(c07_stock_allocation_pendientes_query)

print(f"Overwriting {silver_catalog}.{silver_schema}.C07_StockAllocation_pendientes")
df_c07_pendientes.write.format("delta").mode("overwrite").saveAsTable(f"{silver_catalog}.{silver_schema}.C07_StockAllocation_pendientes")

print("Pipeline completed successfully.")

