# Databricks notebook source
# Create widgets for date parameters with dynamic defaults matching the logic:
# date_end: Today in CST (Mexico)
# date_begin: Yesterday in CST (Mexico)
# Using SQL expressions to calculate defaults to ensure "SQL friendly" logic.
default_dates_df = spark.sql("""
    SELECT 
        CAST(date_sub(to_date(from_utc_timestamp(current_timestamp(), 'America/Mexico_City')), 1) AS STRING) as begin_date,
        CAST(to_date(from_utc_timestamp(current_timestamp(), 'America/Mexico_City')) AS STRING) as end_date
""")
defaults = default_dates_df.first()

dbutils.widgets.text("date_begin", defaults.begin_date)
dbutils.widgets.text("date_end", defaults.end_date)
dbutils.widgets.text("bronze_catalog", "cat_rtmgb_dv")
dbutils.widgets.text("bronze_schema", "groglortmbm_cz")
dbutils.widgets.text("silver_catalog", "cat_dnamxgro_dv")
dbutils.widgets.text("silver_schema", "gromexdmo_cz")

bronze_catalog = dbutils.widgets.get("bronze_catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_catalog = dbutils.widgets.get("silver_catalog")
silver_schema = dbutils.widgets.get("silver_schema")
date_begin = dbutils.widgets.get("date_begin")
date_end = dbutils.widgets.get("date_end")
print(f"date_begin from widget: {date_begin}\ndate_end from widget: {date_end}")

# COMMAND ----------
# Pipeline: RTM_DMO_RolCatalog
# Bulk insert + upsert into movimientos_catalogo_roll for {date_begin} to {date_end}
# -- Pipeline: RTM_DMO_RolCatalog
# -- Bulk insert + upsert into MovimientosCatalogoRoll for '{date_begin}' to '{date_end}'
rol_catalogo = spark.sql(f"""
CREATE OR REPLACE TEMP VIEW rol_catalogo_sources AS
WITH
product_src AS (
    -- Source of 01_Product
    SELECT
        pmv.Product_Code AS Code,
        pmv.Full_Description AS Description,
        CAST(pmv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        pmv.Created_Date AS Created_Date,
        try_cast(pmv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        pmv.Modified_Date AS Modified_Date,
        try_cast(luav.Log_User_Activity_Id AS BIGINT) AS Log_User_Activity_Id,
        luav.Log_User_Activity_IP AS Log_User_Activity_IP,
        luav.Activity_Code AS Activity_Code,
        luav.Module_Name AS Module_Name,
        '01_Product' AS Module,
        pmv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.product_master_v AS pmv
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON pmv.Created_By = uvc.User_Id
     AND pmv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON pmv.Modified_By = uvm.User_Id
     AND pmv.Instance    = uvm.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.log_user_activity_v AS luav
      ON pmv.Modified_By = luav.User_Id
     AND date_format(pmv.Modified_Date, 'yyyy-MM-dd HH:mm') =
         date_format(luav.Date,          'yyyy-MM-dd HH:mm')
     AND pmv.Instance = luav.Instance
    WHERE CAST(pmv.Modified_Date AS DATE) BETWEEN '{date_begin}' AND '{date_end}'
      AND luav.Module_Name IS NOT NULL
),
m10_retailer_master_src AS (
    -- Source of M10_RetailerMaster
    SELECT DISTINCT
        ret.Retailer_Code AS Code,
        ret.Retailer_Name AS Description,
        CAST(ret.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        ret.Created_Date AS Created_Date,
        try_cast(ret.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        ret.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M10_RetailerMaster' AS Module,
        ret.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.retailer_v AS ret
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON ret.Created_By = uvc.User_Id
     AND ret.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON ret.Modified_By = uvm.User_Id
     AND ret.Instance    = uvm.Instance
    WHERE ret.Active = TRUE
      AND ret.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m11_retailer_attribute_src AS (
    -- Source of M11_RetailerAttribute
    SELECT DISTINCT
        ret.Retailer_Code AS Code,
        ret.Retailer_Name AS Description,
        CAST(ret.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        inv.Created_Date AS Created_Date,
        try_cast(inv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        inv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M11_RetailerAttribute' AS Module,
        ret.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.retailer_distributor_mapping_v AS rdm
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc
      ON rdm.Sales_Center_Id = sc.Sales_Center_Id
     AND rdm.Instance        = sc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.retailer_v AS ret
      ON rdm.Retailer_Id = ret.Retailer_Id
     AND rdm.Instance    = ret.Instance
    LEFT JOIN (
        SELECT
            Customer_Id,
            Address_1 AS inv_street_name,
            Numero_Exterior AS inv_ext_number,
            Numero_Interior AS inv_int_number,
            Address_2 AS inv_neighborhood,
            City AS inv_city,
            Address_3 AS inv_district,
            State AS inv_state,
            `País` AS inv_country,
            Pin_Code AS inv_zip_code,
            Telephone AS inv_phone_no,
            Email AS inv_email_id,
            Entre_Calle_1 AS inv_bt_street1,
            Entre_Calle_2 AS inv_bt_street2,
            Landmark AS inv_reference,
            Address_Type_Id,
            Created_Date,
            Created_By,
            Modified_Date,
            Modified_By,
            Instance
        FROM {bronze_catalog}.{bronze_schema}.address_detail_v
        WHERE Active = TRUE
          AND Address_Type_Id = 741
          AND Customer_Type_Id = 798
    ) AS inv
      ON ret.Retailer_Id = inv.Customer_Id
     AND ret.Instance    = inv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.location_v AS lo
      ON ret.Zip_Code = lo.Location_Id
     AND ret.Instance  = lo.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON inv.Created_By = uvc.User_Id
     AND inv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON inv.Modified_By = uvm.User_Id
     AND inv.Instance    = uvm.Instance
    WHERE rdm.Active = TRUE
      AND ret.Active = TRUE
      AND sc.Active = TRUE
      AND inv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m13_price_master_src AS (
    -- Source of M13_PriceMaster
    SELECT
        gv.group_code AS Code,
        pm.Product_Code AS Description,
        CAST(gv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        pmvv.Created_Date AS Created_Date,
        try_cast(pmvv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        pmvv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M13_PriceMaster' AS Module,
        pmvv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.group_v AS gv
    INNER JOIN (
        SELECT
            Effective_From,
            Effective_To,
            Price,
            Active,
            Product_Id,
            Group_Id,
            Instance,
            Update_date,
            Created_By,
            Created_Date,
            Modified_By,
            Modified_Date
        FROM {bronze_catalog}.{bronze_schema}.price_master_v
        WHERE Effective_To IS NULL
          AND Active = TRUE
          AND Price IS NOT NULL
          AND product_uom_id = 860
    ) AS pmvv
      ON gv.Group_Id = pmvv.Group_Id
     AND gv.Instance = pmvv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.product_master_v AS pm
      ON pm.Product_Id = pmvv.Product_Id
     AND pm.Instance   = pmvv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON pmvv.Created_By = uvc.User_Id
     AND pmvv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON pmvv.Modified_By = uvm.User_Id
     AND pmvv.Instance    = uvm.Instance
    WHERE gv.Group_Type_Id = 820
      AND gv.Active = TRUE
      AND pm.Active = TRUE
      AND gv.Group_Code <> 'Default'
      AND pmvv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m14_price_mapping_src AS (
    -- Source of M14_PriceMapping
    SELECT
        gv.group_code AS Code,
        gv.Group_Name AS Description,
        CAST(gv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        gv.Created_Date AS Created_Date,
        try_cast(gv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        gv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M14_PriceMapping' AS Module,
        gv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.price_apply_mapping_v AS pam
    INNER JOIN (
        SELECT *
        FROM {bronze_catalog}.{bronze_schema}.group_v
        WHERE Group_Type_Id = 820
    ) AS gv
      ON pam.Price_Group_Id = gv.Group_Id
     AND pam.Instance      = gv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.location_v AS lv
      ON pam.Apply_Location_Id = lv.Location_Id
     AND pam.Instance         = lv.Instance
    INNER JOIN (
        SELECT
            Lov_Id,
            Lov_Code,
            Lov_Name,
            Lov_Parent_Id,
            Instance
        FROM {bronze_catalog}.{bronze_schema}.channel_master_v
        WHERE Lov_Type = 'FORMAT_CHANNEL_TYPE'
          AND Active = TRUE
    ) AS fct
      ON pam.Apply_Channel_Id = fct.Lov_Id
     AND pam.Instance         = fct.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON gv.Created_By = uvc.User_Id
     AND gv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON gv.Modified_By = uvm.User_Id
     AND gv.Instance    = uvm.Instance
    WHERE pam.Active = TRUE
      AND gv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m15_tax_group_src AS (
    -- Source of M15_TaxGroup
    SELECT DISTINCT
        pmv.Product_Code AS Code,
        tgh.Tax_Group_Header_Name AS Description,
        CAST(tadv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        tahv.Created_Date AS Created_Date,
        try_cast(tahv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        tahv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M15_TaxGroup' AS Module,
        tahv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.tax_group_header_v tgh
    LEFT JOIN {bronze_catalog}.{bronze_schema}.tax_apply_detail_v AS tadv
      ON tgh.Tax_Group_Header_Id = tadv.Tax_Group_Header_Id
     AND tgh.Instance = tadv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.tax_apply_header_v AS tahv
      ON tadv.Tax_Apply_Header_Id = tahv.Tax_Apply_Header_Id
     AND tadv.Instance           = tahv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.product_master_v AS pmv
      ON tadv.Product_Id = pmv.Product_Id
     AND tadv.Instance   = pmv.Instance
    INNER JOIN {bronze_catalog}.{bronze_schema}.product_level_v AS plv
      ON pmv.Product_Level_Id = plv.Product_Level_Id
     AND pmv.Instance         = plv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON tahv.Created_By = uvc.User_Id
     AND tahv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON tahv.Modified_By = uvm.User_Id
     AND tahv.Instance    = uvm.Instance
    WHERE tadv.Active = TRUE
      AND tahv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m29_plant_master_src AS (
    -- Source of M29_PlantMaster
    SELECT DISTINCT
        plt.Lov_Code AS Code,
        plt.Lov_name AS Description,
        CAST(plt.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        plt.Created_Date AS Created_Date,
        try_cast(plt.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        plt.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M29_PlantMaster' AS Module,
        plt.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.List_Value_V AS plt
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON plt.Created_By = uvc.User_Id
     AND plt.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON plt.Modified_By = uvm.User_Id
     AND plt.Instance    = uvm.Instance
    WHERE plt.Lov_Type = 'PLANT'
      AND plt.Active = TRUE
      AND plt.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m30_plant_mapping_src AS (
    -- Source of M30_PlantMapping
    SELECT DISTINCT
        scv.Sales_Center_Code AS Code,
        scv.Sales_Center_Name AS Description,
        CAST(scv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        scv.Created_Date AS Created_Date,
        try_cast(scv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        scv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M30_PlantMapping' AS Module,
        scv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.business_partner_lov_mapping_v bplmv
    INNER JOIN (
        SELECT DISTINCT
            Lov_Id,
            Lov_name,
            Lov_Code,
            Instance,
            Lov_Type,
            Active
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Type = 'PLANT'
          AND Active = TRUE
    ) AS et
      ON bplmv.Lov_Id = et.Lov_Id
     AND bplmv.Instance = et.Instance
     AND bplmv.Active = et.Active
    INNER JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS scv
      ON scv.Sales_Center_Id = bplmv.Sales_Center_Id
     AND scv.Instance = bplmv.Instance
     AND scv.Active = bplmv.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON scv.Created_By = uvc.User_Id
     AND scv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON scv.Modified_By = uvm.User_Id
     AND scv.Instance    = uvm.Instance
    WHERE scv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
c16_productgroup_order_src AS (
    -- Source of C16_ProductGroup-Order
    SELECT DISTINCT
        gv.Group_Code AS Code,
        gv.Group_Name AS Description,
        CAST(gv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        gv.Created_Date AS Created_Date,
        try_cast(gv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        gv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'C16_ProductGroup' AS Module,
        gv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.group_v AS gv
    LEFT JOIN {bronze_catalog}.{bronze_schema}.group_header_v AS gh
      ON gv.Group_Header_Id = gh.Group_Header_Id
     AND gv.Instance = gh.Instance
     AND gv.Active   = gh.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.group_channel_mapping_v AS gch
      ON gch.Group_Id = gv.Group_Id
     AND gch.Instance = gv.Instance
     AND gch.Active   = gv.Active
    LEFT JOIN (
        SELECT DISTINCT
            Lov_Id,
            Lov_Code,
            Instance,
            Lov_Type,
            Active,
            Lov_Parent_Id
        FROM {bronze_catalog}.{bronze_schema}.channel_master_v
        WHERE Lov_Type = 'FORMAT_CHANNEL_TYPE'
    ) AS et
      ON gch.Channel_Id = et.Lov_Id
     AND gch.Instance = et.Instance
     AND gch.Active   = et.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON gv.Created_By = uvc.User_Id
     AND gv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON gv.Modified_By = uvm.User_Id
     AND gv.Instance    = uvm.Instance
    WHERE gv.Group_Code = et.Lov_Code
      AND gv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
c17_productgroup_invsplit_src AS (
    -- Source of C17_ProductGroup-InvSplit
    SELECT DISTINCT
        gv.Group_Code AS Code,
        gv.Group_Name AS Description,
        CAST(gv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        gv.Created_Date AS Created_Date,
        try_cast(gv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        gv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'C17_ProductGroup-InvSplit' AS Module,
        gv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.group_v AS gv
    INNER JOIN {bronze_catalog}.{bronze_schema}.group_header_v AS gh
      ON gv.Group_Header_Id = gh.Group_Header_Id
     AND gv.Instance = gh.Instance
     AND gv.Active   = gh.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON gv.Created_By = uvc.User_Id
     AND gv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON gv.Modified_By = uvm.User_Id
     AND gv.Instance    = uvm.Instance
    WHERE gv.Active = TRUE
      AND gh.Group_Header_Name = '20190831-INVSPLIT'
      AND gv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
segment_sku_tagging_dump_src AS (
    -- Source of SegmentSKUTaggingDump
    SELECT DISTINCT
        gv.Group_Code AS Code,
        gv.Group_Name AS Description,
        CAST(gv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        gv.Created_Date AS Created_Date,
        try_cast(gv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        gv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'SegmentSKUTaggingDump' AS Module,
        gv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.group_v AS gv
    LEFT JOIN {bronze_catalog}.{bronze_schema}.group_header_v AS ghv
      ON gv.Group_Header_Id = ghv.Group_Header_Id
     AND gv.Instance        = ghv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.product_group_mapping_v AS pgm
      ON gv.Group_Id = pgm.Group_Id
     AND gv.Instance = pgm.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.product_master_v AS pmv
      ON pgm.Product_Id = pmv.Product_Id
     AND pgm.Product_Level_Id = pmv.Product_Level_Id
     AND pgm.Instance = pmv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.group_channel_mapping_v AS gcm_loc
      ON gv.Group_Id = gcm_loc.Group_Id
     AND gcm_loc.Channel_Type_Id IN (6647, 6575)
     AND gv.Instance = gcm_loc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.location_v AS lv
      ON gcm_loc.Channel_Id = lv.Location_Id
     AND gcm_loc.Instance   = lv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.group_channel_mapping_v AS gcm_seg
      ON gv.Group_Id = gcm_seg.Group_Id
     AND gcm_seg.Channel_Type_Id IN (6646, 6574)
     AND gv.Instance = gcm_seg.Instance
    LEFT JOIN (
        SELECT
            Lov_Id,
            Lov_Code,
            Lov_Name,
            Instance
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Type = 'RETAILER_SEGMENT_TYPE'
          AND Active = TRUE
    ) AS lrs
      ON gcm_seg.Channel_Id = lrs.Lov_Id
     AND gcm_seg.Instance   = lrs.Instance
    LEFT JOIN (
        SELECT
            Lov_Id,
            Lov_Code,
            Lov_Name,
            Instance
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Type = 'PRODUCT_TAGGING'
          AND Active = TRUE
    ) AS lt
      ON ghv.Group_Header_Type_Id = lt.Lov_Id
     AND ghv.Instance             = lt.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON gv.Created_By = uvc.User_Id
     AND gv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON gv.Modified_By = uvm.User_Id
     AND gv.Instance    = uvm.Instance
    WHERE gv.Group_Type_Id = 0
      AND gv.Group_Header_Id IN (3, 4)
      AND gv.Active = TRUE
      AND gv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
c20_product_sequence_src AS (
    -- Source of C20_ProductSequence
    SELECT
        pm.Product_Code AS Code,
        CAST(pm.`SEQUENCE` AS STRING) AS Description,
        CAST(pm.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        pm.Created_Date AS Created_Date,
        try_cast(pm.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        pm.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'C20_ProductSequence' AS Module,
        pm.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.product_master_v AS pm
    INNER JOIN {bronze_catalog}.{bronze_schema}.product_level_v AS pl
      ON pm.Product_Level_Id = pl.Product_Level_Id
     AND pm.Active = pl.Active
     AND pm.Instance = pl.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON pm.Created_By = uvc.User_Id
     AND pm.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON pm.Modified_By = uvm.User_Id
     AND pm.Instance    = uvm.Instance
    WHERE pm.Active = TRUE
      AND pl.Level_Code = 'SKU'
      AND pm.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
c21_discount_master_src AS (
    -- Source of C21_DiscountMaster
    SELECT DISTINCT
        dhv.Discount_Code AS Code,
        dhv.Description AS Description,
        CAST(dhv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        dhv.Created_Date AS Created_Date,
        try_cast(dhv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        dhv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'C21_DiscountMaster' AS Module,
        dhv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.discount_header_v dhv
    LEFT JOIN {bronze_catalog}.{bronze_schema}.discount_details_v AS ddv
      ON ddv.Discount_Header_Id = dhv.Record_Id
     AND ddv.Instance = dhv.Instance
     AND ddv.Active   = dhv.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.product_master_v AS pmv
      ON pmv.Product_Id = ddv.Product_Id
     AND pmv.Instance   = ddv.Instance
     AND pmv.Active     = ddv.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.product_level_v AS plv
      ON plv.Product_Level_Id = pmv.Product_Level_Id
     AND plv.Instance         = pmv.Instance
     AND plv.Active           = pmv.Active
    LEFT JOIN (
        SELECT DISTINCT
            Lov_Id,
            Lov_Code,
            Instance,
            Lov_Type,
            Active
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Type = 'DISCOUNT_TYPE'
    ) AS et
      ON et.Lov_id   = dhv.Discount_Type_Id
     AND et.Instance = dhv.Instance
     AND et.Active   = dhv.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON dhv.Created_By = uvc.User_Id
     AND dhv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON dhv.Modified_By = uvm.User_Id
     AND dhv.Instance    = uvm.Instance
    WHERE dhv.Active = TRUE
      AND dhv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
c22_discount_mapping_retailer_src AS (
    -- Source of C22_DiscountMappingRetailer
    SELECT DISTINCT
        dhv.Discount_Code AS Code,
        rv.Retailer_Code AS Description,
        CAST(dhv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        dhv.Created_Date AS Created_Date,
        try_cast(dhv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        dhv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'C22_DiscountMappingRetailer' AS Module,
        dhv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.discount_header_v dhv
    LEFT JOIN {bronze_catalog}.{bronze_schema}.discount_details_v AS ddv
      ON ddv.Discount_Header_Id = dhv.Record_Id
     AND ddv.Instance = dhv.Instance
     AND ddv.Active   = dhv.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.discount_apply_mapping_v AS damv
      ON damv.Discount_Header_Id = ddv.Discount_Header_Id
     AND damv.Instance = ddv.Instance
     AND damv.Active   = ddv.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.retailer_v AS rv
      ON rv.Retailer_Id = damv.Retailer_Id
     AND rv.Instance    = damv.Instance
     AND rv.Active      = damv.Active
    LEFT JOIN (
        SELECT DISTINCT
            Lov_Id,
            Lov_Code,
            Instance,
            Lov_Type,
            Active
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Type = 'DISCOUNT_TYPE'
    ) AS et
      ON et.Lov_id   = dhv.Discount_Type_Id
     AND et.Instance = dhv.Instance
     AND et.Active   = dhv.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON dhv.Created_By = uvc.User_Id
     AND dhv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON dhv.Modified_By = uvm.User_Id
     AND dhv.Instance    = uvm.Instance
    WHERE dhv.Active = TRUE
      AND dhv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
c23_discount_mapping_format_channel_src AS (
    -- Source of C23_DiscountMappingFormat_Channel
    SELECT DISTINCT
        C22.Discount_Code AS Code,
        cmv.Lov_Code AS Description,
        CAST(C22.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        C22.Created_Date AS Created_Date,
        try_cast(C22.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        C22.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'C23_DiscountMappingFormat_Channel' AS Module,
        C22.Instance AS Instance
    FROM (
        SELECT DISTINCT
            dhv.Discount_Code,
            damv.Channel_Id,
            damv.Channel_Hierarchy_Id,
            damv.District_Id,
            rv.Retailer_Code,
            damv.Is_Store_Included AS Store_Mapping,
            damv.Instance AS Instance,
            damv.Active,
            damv.Created_Date,
            damv.Created_By,
            damv.Modified_Date,
            damv.Modified_By
        FROM {bronze_catalog}.{bronze_schema}.discount_apply_mapping_v AS damv
        LEFT JOIN (
            SELECT DISTINCT
                disc_head.Record_Id,
                disc_head.Discount_Code AS Discount_Code,
                disc_head.Description AS Discount_Description,
                prod_list.Level_Name AS Apply_Type,
                prod_list.Level_Code AS Item_Disc_Type,
                prod_list.Product_Code AS Product_Code,
                ddv.Effective_From AS From_Date,
                ddv.Effective_To AS To_Date,
                ddv.Max_Discount_Value AS Discount,
                disc_head.Lov_Code AS Disc_Type,
                ddv.Instance AS Instance,
                ddv.Active AS Active,
                ddv.Modified_Date AS FechadeModificacion
            FROM {bronze_catalog}.{bronze_schema}.discount_details_v AS ddv
            LEFT JOIN (
                SELECT DISTINCT
                    dhv.Record_Id,
                    dhv.Discount_Code,
                    dhv.Description,
                    lvv.Lov_Code,
                    dhv.Active,
                    dhv.Instance,
                    dhv.Update_date
                FROM {bronze_catalog}.{bronze_schema}.discount_header_v AS dhv
                LEFT JOIN {bronze_catalog}.{bronze_schema}.List_Value_V AS lvv
                  ON dhv.Active = lvv.Active
                 AND dhv.Instance = lvv.Instance
                 AND dhv.Level_Id = lvv.Lov_Parent_Id
                WHERE dhv.Active = TRUE
                  AND lvv.Active = TRUE
                  AND lvv.Lov_Code IN ('BILL')
            ) AS disc_head
              ON ddv.Discount_Header_Id = disc_head.Record_Id
             AND ddv.Active = disc_head.Active
             AND ddv.Instance = disc_head.Instance
            LEFT JOIN (
                SELECT DISTINCT
                    plv.Level_Name,
                    plv.Level_Code,
                    pmv.Product_Id,
                    pmv.Product_Code,
                    pmv.Active,
                    pmv.Instance,
                    pmv.Update_date
                FROM {bronze_catalog}.{bronze_schema}.product_master_v AS pmv
                LEFT JOIN {bronze_catalog}.{bronze_schema}.product_level_v AS plv
                  ON pmv.Product_Level_Id = plv.Product_Level_Id
                 AND pmv.Active = plv.Active
                 AND pmv.Instance = plv.Instance
                WHERE pmv.Active = TRUE
                  AND plv.Active = TRUE
                  AND plv.Product_Level_Id IN (5)
                  AND pmv.Product_Level_Id IN (5)
            ) AS prod_list
              ON ddv.Product_Id = prod_list.Product_Id
             AND ddv.Active = prod_list.Active
             AND ddv.Instance = prod_list.Instance
            WHERE ddv.Active = TRUE
              AND prod_list.Active = TRUE
              AND disc_head.Active = TRUE
              AND YEAR(ddv.Effective_From) NOT IN (2021, 2020, 2019, 2018, 2017, 2016)
        ) dhv
          ON damv.Discount_Header_Id = dhv.Record_Id
         AND damv.Instance = dhv.Instance
         AND damv.active = dhv.Active
        LEFT JOIN {bronze_catalog}.{bronze_schema}.retailer_v AS rv
          ON damv.Retailer_Id = rv.Retailer_Id
         AND damv.Instance = rv.Instance
         AND damv.active = rv.active
        WHERE damv.Active = TRUE
          AND dhv.Active = TRUE
          AND rv.Active = TRUE
    ) AS C22
    LEFT JOIN {bronze_catalog}.{bronze_schema}.channel_master_v AS cmv
      ON C22.Channel_Id = cmv.Lov_Parent_Id
     AND C22.Active = cmv.active
     AND C22.Instance = cmv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.location_v AS lv
      ON C22.District_Id = lv.Location_Parent_Id
     AND C22.Active = lv.active
     AND C22.Instance = lv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON C22.Created_By = uvc.User_Id
     AND C22.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON C22.Modified_By = uvm.User_Id
     AND C22.Instance    = uvm.Instance
    WHERE C22.Active = TRUE
      AND lv.Active = TRUE
      AND cmv.Active = TRUE
      AND C22.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
c24_scheme_management_master_src AS (
    -- Source of C24_SchemeManagementMaster
    SELECT DISTINCT
        sv.Scheme_Code AS Code,
        sv.Scheme_Name AS Description,
        CAST(sv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        sv.Created_Date AS Created_Date,
        try_cast(sv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        sv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'C24_SchemeManagementMaster' AS Module,
        sv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.scheme_v AS sv
    LEFT JOIN (
        SELECT DISTINCT
            spv.Scheme_Product_Rocord_Id,
            spv.Scheme_Id,
            spv.Product_Id,
            prod_list.Product_Code,
            prod_list.Level_Name,
            prod_list.Level_Code,
            spv.Active,
            spv.Instance
        FROM {bronze_catalog}.{bronze_schema}.scheme_product_v AS spv
        LEFT JOIN (
            SELECT DISTINCT
                pmv.Product_Id,
                pmv.Product_Code,
                plv.Level_Name,
                plv.Level_Code,
                pmv.Active,
                pmv.Instance,
                pmv.Update_date
            FROM {bronze_catalog}.{bronze_schema}.product_master_v AS pmv
            LEFT JOIN {bronze_catalog}.{bronze_schema}.product_level_v AS plv
              ON pmv.Product_Level_Id = plv.Product_Level_Id
             AND pmv.Active = plv.Active
             AND pmv.Instance = plv.Instance
            WHERE pmv.Active = TRUE
              AND plv.Active = TRUE
              AND plv.Product_Level_Id IN (5)
              AND pmv.Product_Level_Id IN (5)
        ) AS prod_list
          ON spv.Product_Id = prod_list.Product_Id
         AND spv.Active = prod_list.Active
         AND spv.Instance = prod_list.Instance
    ) AS prod
      ON sv.Scheme_Id = prod.Scheme_Id
     AND sv.Active = prod.Active
     AND sv.Instance = prod.Instance
    LEFT JOIN (
        SELECT DISTINCT
            slab.Scheme_Slab_Id,
            slab.Scheme_Id,
            slab.Slab_Description,
            slab.Active,
            slab.Instance,
            buy.From_Qty AS buy,
            get.From_Qty AS get
        FROM {bronze_catalog}.{bronze_schema}.scheme_slab_v AS slab
        LEFT JOIN {bronze_catalog}.{bronze_schema}.scheme_slab_target_v AS buy
          ON slab.Scheme_Slab_Id = buy.Scheme_Slab_Id
         AND slab.Instance = buy.Instance
         AND slab.Active = buy.Active
        LEFT JOIN {bronze_catalog}.{bronze_schema}.scheme_free_product_v AS get
          ON slab.Scheme_Slab_Id = get.Scheme_Slab_Id
         AND slab.Instance = get.Instance
         AND slab.Active = get.Active
        WHERE slab.Active = TRUE
    ) AS bg
      ON bg.Scheme_Id = sv.Scheme_Id
     AND bg.Instance = sv.Instance
     AND bg.Active = sv.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON sv.Created_By = uvc.User_Id
     AND sv.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON sv.Modified_By = uvm.User_Id
     AND sv.Instance    = uvm.Instance
    WHERE sv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
c25_scheme_management_mapping_retailer_src AS (
    -- Source of C25_SchemeManagementMappingRetailer
    SELECT DISTINCT
        c24.Scheme_Code AS Code,
        rv.Retailer_Code AS Description,
        CAST(c24.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        c24.Created_Date AS Created_Date,
        try_cast(c24.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        c24.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'C25_SchemeManagementMappingRetailer' AS Module,
        c24.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.scheme_apply_mapping_v AS samv
    LEFT JOIN (
        SELECT DISTINCT
            sv.Scheme_Id,
            sv.Scheme_Code,
            sv.Scheme_Name,
            sv.Scheme_Type,
            sv.Effective_From,
            sv.Effective_To,
            prod.Level_Name,
            prod.Level_Code,
            prod.Product_Code,
            bg.buy,
            bg.get,
            sv.Instance,
            sv.Active,
            sv.Created_Date,
            sv.Created_By,
            sv.Modified_Date,
            sv.Modified_By
        FROM {bronze_catalog}.{bronze_schema}.scheme_v AS sv
        LEFT JOIN (
            SELECT DISTINCT
                spv.Scheme_Product_Rocord_Id,
                spv.Scheme_Id,
                spv.Product_Id,
                prod_list.Product_Code,
                prod_list.Level_Name,
                prod_list.Level_Code,
                spv.Active,
                spv.Instance
            FROM {bronze_catalog}.{bronze_schema}.scheme_product_v AS spv
            LEFT JOIN (
                SELECT DISTINCT
                    pmv.Product_Id,
                    pmv.Product_Code,
                    plv.Level_Name,
                    plv.Level_Code,
                    pmv.Active,
                    pmv.Instance,
                    pmv.Update_date
                FROM {bronze_catalog}.{bronze_schema}.product_master_v AS pmv
                LEFT JOIN {bronze_catalog}.{bronze_schema}.product_level_v AS plv
                  ON pmv.Product_Level_Id = plv.Product_Level_Id
                 AND pmv.Active = plv.Active
                 AND pmv.Instance = plv.Instance
                WHERE pmv.Active = TRUE
                  AND plv.Active = TRUE
                  AND plv.Product_Level_Id IN (5)
                  AND pmv.Product_Level_Id IN (5)
            ) AS prod_list
              ON spv.Product_Id = prod_list.Product_Id
             AND spv.Active = prod_list.Active
             AND spv.Instance = prod_list.Instance
        ) AS prod
          ON sv.Scheme_Id = prod.Scheme_Id
         AND sv.Active = prod.Active
         AND sv.Instance = prod.Instance
        LEFT JOIN (
            SELECT DISTINCT
                slab.Scheme_Slab_Id,
                slab.Scheme_Id,
                slab.Slab_Description,
                slab.Active,
                slab.Instance,
                buy.From_Qty AS buy,
                get.From_Qty AS get
            FROM {bronze_catalog}.{bronze_schema}.scheme_slab_v AS slab
            LEFT JOIN {bronze_catalog}.{bronze_schema}.scheme_slab_target_v AS buy
              ON slab.Scheme_Slab_Id = buy.Scheme_Slab_Id
             AND slab.Instance = buy.Instance
             AND slab.Active = buy.Active
            LEFT JOIN {bronze_catalog}.{bronze_schema}.scheme_free_product_v AS get
              ON slab.Scheme_Slab_Id = get.Scheme_Slab_Id
             AND slab.Instance = get.Instance
             AND slab.Active = get.Active
            WHERE slab.Active = TRUE
        ) AS bg
          ON bg.Scheme_Id = sv.Scheme_Id
         AND bg.Instance = sv.Instance
         AND bg.Active = sv.Active
        WHERE CAST(Effective_to AS DATE) >= current_date()
    ) AS c24
      ON c24.Scheme_Id = samv.Scheme_Id
     AND c24.Instance = samv.Instance
     AND c24.Active = samv.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.retailer_v AS rv
      ON samv.Apply_Master_Id = rv.Retailer_Id
     AND samv.Instance = rv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON c24.Created_By = uvc.User_Id
     AND c24.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON c24.Modified_By = uvm.User_Id
     AND c24.Instance    = uvm.Instance
    WHERE c24.Active = TRUE
      AND c24.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
c26_scheme_management_mapping_format_channel_src AS (
    -- Source of C26_SchemeManagementMappingFormatChannel
    SELECT DISTINCT
        cod_prom.scheme_code AS Code,
        cod_prom.Format_Channel_Code AS Description,
        CAST(cod_prom.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        cod_prom.Created_Date AS Created_Date,
        try_cast(cod_prom.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        cod_prom.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'C26_SchemeManagementMappingFormatChannel' AS Module,
        cod_prom.Instance AS Instance
    FROM (
        SELECT DISTINCT
            sv.Scheme_Code,
            sv.Scheme_Name,
            CAST(et.lov_code AS STRING) AS Format_Channel_Code,
            et.lov_name AS Format_Channel_Name,
            sv.Instance,
            CAST(sv.Effective_To AS DATE) AS Effective_To,
            samv.Apply_Master_Id,
            sv.Active,
            sv.Created_Date,
            sv.Created_By,
            sv.Modified_Date,
            sv.Modified_By
        FROM {bronze_catalog}.{bronze_schema}.scheme_v sv
        LEFT JOIN {bronze_catalog}.{bronze_schema}.scheme_apply_mapping_v AS samv
          ON samv.Scheme_Id = sv.Scheme_Id
         AND samv.Instance = sv.Instance
         AND samv.Active = sv.Active
        INNER JOIN (
            SELECT DISTINCT
                Lov_Id,
                Lov_Code,
                Lov_name,
                Instance,
                Lov_Type,
                Active
            FROM {bronze_catalog}.{bronze_schema}.channel_master_v
            WHERE Lov_Type = 'FORMAT_CHANNEL_TYPE'
        ) AS et
          ON samv.Apply_Master_Id = et.Lov_Id
         AND samv.Instance = et.Instance
         AND samv.Active = et.Active
    ) AS cod_prom
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc
      ON cod_prom.Created_By = uvc.User_Id
     AND cod_prom.Instance   = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm
      ON cod_prom.Modified_By = uvm.User_Id
     AND cod_prom.Instance    = uvm.Instance
    WHERE cod_prom.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m01_location_master_src AS (
    -- Source of M01_LocationMaster
    SELECT DISTINCT 
        lco4.location_Code AS Code,
        lco4.location_Name AS Description,
        CAST(lco4.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        lco4.Created_Date AS Created_Date,
        try_cast(lco4.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        lco4.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M01_LocationMaster' AS Module,
        lco4.Instance AS Instance
    FROM (
        SELECT
            location_Code,
            location_Name,
            Instance,
            location_Parent_Id,
            location_Id,
            Is_Border,
            active
        FROM {bronze_catalog}.{bronze_schema}.location_v
    ) AS lco
    LEFT JOIN (
        SELECT
            location_Code,
            location_Name,
            Instance,
            location_Parent_Id,
            location_Id,
            Is_Border,
            active
        FROM {bronze_catalog}.{bronze_schema}.location_v
    ) AS lco1 ON lco.location_Id = lco1.Location_Parent_Id
        AND lco.Instance = lco1.Instance
    LEFT JOIN (
        SELECT
            location_Code,
            location_Name,
            Instance,
            location_Parent_Id,
            location_Id,
            Is_Border,
            active
        FROM {bronze_catalog}.{bronze_schema}.location_v
    ) AS lco2 ON lco1.location_Id = lco2.Location_Parent_Id
        AND lco1.Instance = lco2.Instance
    LEFT JOIN (
        SELECT
            location_Code,
            location_Name,
            Instance,
            location_Parent_Id,
            location_Id,
            Is_Border,
            active
        FROM {bronze_catalog}.{bronze_schema}.location_v
    ) AS lco3 ON lco2.location_Id = lco3.Location_Parent_Id
        AND lco2.Instance = lco3.Instance
    LEFT JOIN (
        SELECT
            location_Code,
            location_Name,
            Instance,
            location_Parent_Id,
            location_Id,
            Is_Border,
            active,
            Created_By,
            Created_Date,
            Modified_By,
            Modified_Date
        FROM {bronze_catalog}.{bronze_schema}.location_v
    ) AS lco4 ON lco3.location_Id = lco4.Location_Parent_Id
        AND lco3.Instance = lco4.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON lco4.Created_By = uvc.User_Id
     AND lco4.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON lco4.Modified_By = uvm.User_Id
     AND lco4.Instance = uvm.Instance
    WHERE lco.active = TRUE
      AND lco.Location_Code = 'MX'
      AND lco1.location_Code IN ('SRT', 'CTR', 'MTR', 'NRT', 'ZNF')
      AND lco4.active IS NOT NULL
      AND lco4.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m03_sales_center_src AS (
    -- Source of M03_SalesCenter
    SELECT DISTINCT
        sc.Sales_Center_Code AS Code,
        pv.PV1 AS Description,
        CAST(pv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        pv.Created_Date AS Created_Date,
        try_cast(pv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        pv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M03_SalesCenter' AS Module,
        pv.Instance AS Instance
    FROM (
        SELECT
            Position_LongName AS PV1,
            Position_Id,
            Position_Level_Id,
            active,
            Instance,
            Created_By,
            Created_Date,
            Modified_By,
            Modified_Date
        FROM {bronze_catalog}.{bronze_schema}.position_v
    ) AS pv
    INNER JOIN (
        SELECT
            Position_LongName AS PV2,
            Parent_Id,
            Position_Id,
            active,
            Instance
        FROM {bronze_catalog}.{bronze_schema}.position_v
    ) AS pv2 ON pv.Position_Id = pv2.Parent_Id
        AND pv.Instance = pv2.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.position_user_mapping_v AS pum 
      ON pv2.Position_Id = pum.Position_Id 
     AND pv2.Instance = pum.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc 
      ON sc.Sales_Center_Id = pum.Sales_Center_Id
     AND sc.Instance = pum.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.legal_entity_v AS le 
      ON le.Legal_Entity_Id = sc.Legal_Enitity_Id
     AND le.Instance = sc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.location_v AS lv 
      ON lv.Location_Id = sc.Sales_Center_District_Id 
     AND lv.Instance = sc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON pv.Created_By = uvc.User_Id
     AND pv.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON pv.Modified_By = uvm.User_Id
     AND pv.Instance = uvm.Instance
    WHERE pv.Position_Level_Id = 5
      AND sc.Active = TRUE
      AND pv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m04_distributor_user_src AS (
    -- Source of M04_DistributorUser
    SELECT DISTINCT 
        b.User_Code AS Code,
        a.First_Name AS Description,
        CAST(a.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        a.Created_Date AS Created_Date,
        try_cast(a.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        a.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M04_DistributorUser' AS Module,
        a.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.contact_person_detail_v a
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v b 
      ON a.Contact_Person_Id = b.Contact_Person_Id
     AND a.Instance = b.Instance
    LEFT JOIN (
        SELECT DISTINCT 
            Lov_Id,
            Lov_Code,
            Instance,
            Lov_Type,
            Active
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Type = 'META4_POSITION'
    ) AS et ON CAST(b.Flex_2 AS STRING) = CAST(et.Lov_Id AS STRING)
        AND b.Active = et.Active
        AND b.Instance = et.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS d 
      ON a.Sales_Center_Id = d.Sales_Center_Id
     AND a.Instance = d.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Role_Master_V AS e 
      ON e.Role_Id = a.Role_Id
     AND e.Instance = a.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON a.Created_By = uvc.User_Id
     AND a.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON a.Modified_By = uvm.User_Id
     AND a.Instance = uvm.Instance
    WHERE a.Active = TRUE
      AND b.User_Code IS NOT NULL
      AND et.Active = TRUE
      AND a.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m06_route_type_src AS (
    -- Source of M06_RouteType
    SELECT DISTINCT 
        gv.Group_Code AS Code,
        pm.Product_Code AS Description,
        CAST(rtm.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        gv.Created_Date AS Created_Date,
        try_cast(gv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        gv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M06_RouteType' AS Module,
        gv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.route_type_mapping_v AS rtm
    LEFT JOIN {bronze_catalog}.{bronze_schema}.product_master_v AS pm 
      ON rtm.Product_Id = pm.Product_Id
     AND rtm.Instance = pm.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.product_level_v AS pl 
      ON pm.Product_Level_Id = pl.Product_Level_Id
     AND pm.Instance = pl.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.group_v AS gv 
      ON rtm.Group_Id = gv.Group_Id
     AND rtm.Instance = gv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON gv.Created_By = uvc.User_Id
     AND gv.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON gv.Modified_By = uvm.User_Id
     AND gv.Instance = uvm.Instance
    WHERE gv.Active = TRUE
      AND rtm.Active = TRUE
      AND gv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m07_route_master_src AS (
    -- Source of M07_RouteMaster
    SELECT DISTINCT 
        sc.Sales_Center_Code AS Code,
        rv.Route_Code AS Description,
        CAST(rv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        rv.Created_Date AS Created_Date,
        try_cast(rv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        rv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M07_RouteMaster' AS Module,
        rv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.Route_V AS rv
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc 
      ON rv.Sales_Center_Id = sc.Sales_Center_Id
     AND rv.Instance = sc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.User_Route_Mapping_V AS urm 
      ON rv.Route_Id = urm.Route_Id 
     AND rv.Sales_Center_Id = urm.Sales_Center_Id
     AND rv.Instance = urm.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uv 
      ON urm.Seller_Id = uv.User_Id 
     AND urm.Instance = uv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.group_v AS gv 
      ON rv.Group_Id = gv.Group_Id
     AND rv.Instance = gv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.position_user_mapping_v AS pum 
      ON urm.Seller_Id = pum.User_Id 
     AND urm.Instance = pum.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.position_v AS pv 
      ON pum.Position_Id = pv.Position_Id 
     AND pum.Instance = pv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.position_v AS pvs 
      ON pv.Parent_Id = pvs.Position_Id 
     AND pv.Instance = pvs.Instance
    LEFT JOIN (
        SELECT DISTINCT 
            Lov_Id,
            Lov_Code,
            Instance,
            Lov_Type
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Type = 'SELLER_TYPE'
    ) AS st ON uv.User_Type_Id = st.Lov_Id
        AND uv.Instance = st.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON rv.Created_By = uvc.User_Id
     AND rv.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON rv.Modified_By = uvm.User_Id
     AND rv.Instance = uvm.Instance
    WHERE rv.Active = TRUE
      AND sc.Active = TRUE
      AND uv.Active = TRUE
      AND sc.Sales_Center_Code IS NOT NULL
      AND rv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m08_seller_master_src AS (
    -- Source of M08_SellerMaster
    SELECT
        sc.Sales_Center_Code AS Code,
        emv.Employee_Code AS Description,
        CAST(emv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        emv.Created_Date AS Created_Date,
        try_cast(emv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        emv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M08_SellerMaster' AS Module,
        emv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.Employee_Master_V AS emv
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc 
      ON emv.Sales_Center_Id = sc.Sales_Center_Id
     AND emv.Active = sc.Active
     AND emv.Instance = sc.Instance
    LEFT JOIN (
        SELECT DISTINCT 
            Lov_Id,
            Lov_Code,
            Instance,
            Lov_Type,
            Active 
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Type = 'SELLER_TYPE'
    ) AS et ON emv.Employee_Type_Id = et.Lov_Id
        AND emv.Instance = et.Instance
        AND emv.Active = et.active
    LEFT JOIN (
        SELECT DISTINCT 
            Lov_Id,
            Lov_Code,
            Instance,
            Lov_Type,
            Active 
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Code = 'SR' 
          AND lov_Type = 'TRANSACTION_TYPE'
    ) AS et2 ON emv.Instance = et2.Instance
        AND emv.Active = et2.Active
    LEFT JOIN (
        SELECT DISTINCT 
            Lov_Id,
            Lov_Code,
            Instance,
            Lov_Type,
            Active 
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Type = 'META4_POSITION'
    ) AS et1 ON emv.Job_Type_Id = et1.Lov_Id
        AND emv.Instance = et1.Instance
        AND emv.Active = et1.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.User_Route_Mapping_V AS urmv 
      ON urmv.Seller_Id = emv.Own_Route_Id 
     AND urmv.Active = emv.Active
     AND urmv.Instance = emv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv 
      ON rv.Route_Id = urmv.Route_Id 
     AND rv.Active = urmv.Active
     AND rv.Instance = urmv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON emv.Created_By = uvc.User_Id
     AND emv.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON emv.Modified_By = uvm.User_Id
     AND emv.Instance = uvm.Instance
    WHERE emv.Active = TRUE
      AND sc.Active = TRUE
      AND emv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m09_product_distribution_src AS (
    -- Source of M09_ProductDistribution
    SELECT
        scv.sales_center_code AS Code,
        pmv.product_code AS Description,
        CAST(scpmv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        scpmv.Created_Date AS Created_Date,
        try_cast(scpmv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        scpmv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M09_ProductDistribution' AS Module,
        scpmv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.product_master_v pmv
    INNER JOIN {bronze_catalog}.{bronze_schema}.sales_center_product_mapping_v scpmv 
      ON scpmv.product_id = pmv.product_id
     AND scpmv.Instance = pmv.Instance
    INNER JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V scv 
      ON scv.Sales_Center_Id = scpmv.Distributor_Id
     AND scv.Instance = scpmv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON scpmv.Created_By = uvc.User_Id
     AND scpmv.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON scpmv.Modified_By = uvm.User_Id
     AND scpmv.Instance = uvm.Instance
    WHERE scv.active = TRUE
      AND pmv.Created_Date BETWEEN '{date_begin}' AND '{date_end}'
),
customer_hierarchy_src AS (
    -- Source of CustomerHierarchy
    SELECT DISTINCT 
        nat.Lov_Code AS Code,
        nat.Lov_Name AS Description,
        CAST(nat.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        nat.Created_Date AS Created_Date,
        try_cast(nat.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        nat.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'CustomerHierarchy' AS Module,
        nat.Instance AS Instance
    FROM (
        SELECT
            Lov_Id, 
            Lov_Code,
            Lov_Name,
            Lov_Parent_Id,
            Instance,
            Created_By,
            Created_Date,
            Modified_By,
            Modified_Date,
            Active
        FROM {bronze_catalog}.{bronze_schema}.channel_master_v
        WHERE Lov_Type = 'NATIONAL_CHANNEL_TYPE'
          AND Active = TRUE
    ) AS nat
    LEFT JOIN (
        SELECT
            Lov_Id,
            Lov_Code,
            Lov_Name,
            Lov_Parent_Id,
            Instance
        FROM {bronze_catalog}.{bronze_schema}.channel_master_v
        WHERE Lov_Type = 'ACCOUNT_CHANNEL_TYPE'
          AND Active = TRUE
    ) AS act ON nat.Lov_Id = act.Lov_Parent_Id
        AND nat.Instance = act.Instance
    LEFT JOIN (
        SELECT
            Lov_Id,
            Lov_Code,
            Lov_Name,
            Lov_Parent_Id,
            Instance,
            Lov_Type
        FROM {bronze_catalog}.{bronze_schema}.List_Value_V
        WHERE Lov_Type = 'SUB_CHAIN_TYPE'
          AND Active = TRUE
    ) AS sct ON act.Lov_Id = sct.Lov_Parent_Id
        AND act.Instance = sct.Instance
    LEFT JOIN (
        SELECT
            Lov_Id,
            Lov_Code,
            Lov_Name,
            Lov_Parent_Id,
            Instance
        FROM {bronze_catalog}.{bronze_schema}.channel_master_v
        WHERE Lov_Type = 'FORMAT_CHANNEL_TYPE'
          AND Active = TRUE
    ) AS fct ON sct.Lov_Id = fct.Lov_Parent_Id
        AND sct.Instance = fct.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON nat.Created_By = uvc.User_Id
     AND nat.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON nat.Modified_By = uvm.User_Id
     AND nat.Instance = uvm.Instance
    WHERE nat.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m12_route_plan_src AS (
    -- Source of M12_RoutePlan
    SELECT
        CAST(rum.Retailer_User_Mapping_Id AS STRING) AS Code,
        rv.Route_Code AS Description,
        CAST(rum.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        rum.Created_Date AS Created_Date,
        try_cast(rum.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        rum.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M12_RoutePlan' AS Module,
        rum.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.retailer_user_mapping_v AS rum
    INNER JOIN {bronze_catalog}.{bronze_schema}.position_v AS pv 
      ON rum.User_Position_Id = pv.Position_Id
     AND rum.Instance = pv.Instance
    INNER JOIN {bronze_catalog}.{bronze_schema}.position_user_mapping_v AS pum 
      ON pv.Position_Id = pum.Position_Id
     AND pv.Instance = pum.Instance
    INNER JOIN {bronze_catalog}.{bronze_schema}.user_v AS uv 
      ON pum.User_Id = uv.User_Id
     AND pum.Instance = uv.Instance
    INNER JOIN {bronze_catalog}.{bronze_schema}.User_Route_Mapping_V AS urm 
      ON uv.User_Id = urm.Seller_Id
     AND uv.Instance = urm.Instance
    INNER JOIN {bronze_catalog}.{bronze_schema}.Route_V AS rv 
      ON urm.Sales_Center_Id = rv.Sales_Center_Id
     AND urm.Route_Id = rv.Route_Id
     AND urm.Instance = rv.Instance
    INNER JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc 
      ON rum.Sales_Center_Id = sc.Sales_Center_Id
     AND rum.Instance = sc.Instance
    INNER JOIN {bronze_catalog}.{bronze_schema}.retailer_v AS ret 
      ON rum.Retailer_Id = ret.Retailer_Id
     AND rum.Instance = ret.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON rum.Created_By = uvc.User_Id
     AND rum.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON rum.Modified_By = uvm.User_Id
     AND rum.Instance = uvm.Instance
    WHERE rum.Active = TRUE
      AND pv.Active = TRUE
      AND pum.Active = TRUE
      AND uv.Active = TRUE
      AND urm.Active = TRUE
      AND rv.Active = TRUE
      AND sc.Active = TRUE
      AND ret.Active = TRUE
      AND rum.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m28_route_type_lead_time_src AS (
    -- Source of M28_RouteTypeLeadTime
    SELECT DISTINCT 
        sc.Sales_Center_Code AS Code,
        gv.Group_Name AS Description,
        CAST(rv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        rv.Created_Date AS Created_Date,
        try_cast(rv.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        rv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M28_RouteTypeLeadTime' AS Module,
        rv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.Route_V AS rv
    LEFT JOIN {bronze_catalog}.{bronze_schema}.Sales_Center_V AS sc 
      ON rv.Sales_Center_Id = sc.Sales_Center_Id
     AND rv.Instance = sc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.group_v AS gv 
      ON rv.Group_Id = gv.Group_Id
     AND rv.Instance = gv.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.route_type_lead_time_v AS lt 
      ON rv.Group_Id = lt.Route_Type_Group_Id
     AND rv.Sales_Center_Id = lt.Sales_Center_Id
     AND rv.Instance = lt.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON rv.Created_By = uvc.User_Id
     AND rv.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON rv.Modified_By = uvm.User_Id
     AND rv.Instance = uvm.Instance
    WHERE sc.Sales_Center_Code IS NOT NULL
      AND gv.Active = TRUE
      AND rv.Active = TRUE
      AND sc.Active = TRUE
      AND rv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
product_set_salable_flag_src AS (
    -- Source of ProductSetSalableFlag
    SELECT 
        pmv.Product_Code AS Code,
        CASE 
            WHEN pav.Attributes_Id IS NULL THEN 'Salable'
            WHEN pav.Attributes_Id = 1 THEN 'No Salable'
            ELSE 'No Returnable'
        END AS Description,
        CAST(pav.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        pav.Created_Date AS Created_Date,
        try_cast(pav.Modified_By AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        pav.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'ProductSetSalableFlag' AS Module,
        pav.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.product_master_v AS pmv
    LEFT JOIN {bronze_catalog}.{bronze_schema}.product_attributes_v AS pav 
      ON pmv.Product_Id = pav.Product_Id
     AND pmv.Instance = pav.Instance
     AND pmv.Active = pav.Active
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON pav.Created_By = uvc.User_Id
     AND pav.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON pav.Modified_By = uvm.User_Id
     AND pav.Instance = uvm.Instance
    WHERE pmv.Active = TRUE
      AND pmv.Product_Level_Id = 5
      AND pav.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
m40_political_geography_src AS (
    -- Source of M40_PoliticalGeography
    SELECT
        pgv.Postal_Code AS Code,
        pgv.State_Code AS Description,
        CAST(pgv.Active AS BOOLEAN) AS Active,
        uvc.User_Code AS UsuarioCreador,
        CONCAT(uvc.First_Name, ' ', uvc.Last_Name) AS NombreUsuarioCreador,
        pgv.Created_Date AS Created_Date,
        CAST(NULL AS BIGINT) AS Modified_By,
        uvm.User_Code AS UsuarioEditor,
        CONCAT(uvm.First_Name, ' ', uvm.Last_Name) AS NombreUsuarioEditor,
        pgv.Modified_Date AS Modified_Date,
        CAST(NULL AS BIGINT) AS Log_User_Activity_Id,
        CAST(NULL AS STRING) AS Log_User_Activity_IP,
        CAST(NULL AS STRING) AS Activity_Code,
        CAST(NULL AS STRING) AS Module_Name,
        'M40_PoliticalGeography' AS Module,
        pgv.Instance AS Instance
    FROM {bronze_catalog}.{bronze_schema}.political_geography_v AS pgv
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvc 
      ON try_cast(pgv.Created_By AS BIGINT) = uvc.User_Id
     AND pgv.Instance = uvc.Instance
    LEFT JOIN {bronze_catalog}.{bronze_schema}.user_v AS uvm 
      ON try_cast(pgv.Modified_By AS BIGINT) = uvm.User_Id
     AND pgv.Instance = uvm.Instance
    WHERE pgv.Created_Date >= '2023-12-02'
       OR pgv.Modified_Date BETWEEN '{date_begin}' AND '{date_end}'
),
insert_sources AS (
    SELECT * FROM product_src
),
upsert_sources AS (
    SELECT * FROM m10_retailer_master_src
    UNION ALL
    SELECT * FROM m11_retailer_attribute_src
    UNION ALL
    SELECT * FROM m13_price_master_src
    UNION ALL
    SELECT * FROM m14_price_mapping_src
    UNION ALL
    SELECT * FROM m15_tax_group_src
    UNION ALL
    SELECT * FROM m29_plant_master_src
    UNION ALL
    SELECT * FROM m30_plant_mapping_src
    UNION ALL
    SELECT * FROM c16_productgroup_order_src
    UNION ALL
    SELECT * FROM c17_productgroup_invsplit_src
    UNION ALL
    SELECT * FROM segment_sku_tagging_dump_src
    UNION ALL
    SELECT * FROM c20_product_sequence_src
    UNION ALL
    SELECT * FROM c21_discount_master_src
    UNION ALL
    SELECT * FROM c22_discount_mapping_retailer_src
    UNION ALL
    SELECT * FROM c23_discount_mapping_format_channel_src
    UNION ALL
    SELECT * FROM c24_scheme_management_master_src
    UNION ALL
    SELECT * FROM c25_scheme_management_mapping_retailer_src
    UNION ALL
    SELECT * FROM c26_scheme_management_mapping_format_channel_src
    UNION ALL
    SELECT * FROM m01_location_master_src
    UNION ALL
    SELECT * FROM m03_sales_center_src
    UNION ALL
    SELECT * FROM m04_distributor_user_src
    UNION ALL
    SELECT * FROM m06_route_type_src
    UNION ALL
    SELECT * FROM m07_route_master_src
    UNION ALL
    SELECT * FROM m08_seller_master_src
    UNION ALL
    SELECT * FROM m09_product_distribution_src
    UNION ALL
    SELECT * FROM customer_hierarchy_src
    UNION ALL
    SELECT * FROM m12_route_plan_src
    UNION ALL
    SELECT * FROM m28_route_type_lead_time_src
    UNION ALL
    SELECT * FROM product_set_salable_flag_src
    UNION ALL
    SELECT * FROM m40_political_geography_src
),
all_sources AS (
    SELECT
        'insert' AS load_type,
        Code,
        Description,
        Active,
        UsuarioCreador,
        NombreUsuarioCreador,
        Created_Date,
        Modified_By,
        UsuarioEditor,
        NombreUsuarioEditor,
        Modified_Date,
        Log_User_Activity_Id,
        Log_User_Activity_IP,
        Activity_Code,
        Module_Name,
        Module,
        Instance
    FROM insert_sources
    UNION ALL
    SELECT
        'upsert' AS load_type,
        Code,
        Description,
        Active,
        UsuarioCreador,
        NombreUsuarioCreador,
        Created_Date,
        Modified_By,
        UsuarioEditor,
        NombreUsuarioEditor,
        Modified_Date,
        Log_User_Activity_Id,
        Log_User_Activity_IP,
        Activity_Code,
        Module_Name,
        Module,
        Instance
    FROM upsert_sources
)
SELECT *, current_timestamp() AS Ingestion_Timestamp FROM all_sources;""")
# COMMAND ----------
# Deduplicate the rol_catalogo_sources table
rol_catalogo_deduped = spark.table("rol_catalogo_sources") \
    .dropDuplicates(subset=["Code", "Modified_Date", "Module", "Instance"])
rol_catalogo_deduped.createOrReplaceTempView("rol_catalogo_deduped")
# COMMAND ----------
# Idempotent:CREATE TABLE IF NOT EXISTS target table
spark.sql(f"""CREATE TABLE IF NOT EXISTS {silver_catalog}.{silver_schema}.movimientos_catalogo_roll (
    Code STRING,
    Description STRING,
    Active BOOLEAN,
    UsuarioCreador STRING,
    NombreUsuarioCreador STRING,
    Created_Date TIMESTAMP,
    Modified_By STRING,
    UsuarioEditor STRING,
    NombreUsuarioEditor STRING,
    Modified_Date TIMESTAMP,
    Log_User_Activity_Id STRING,
    Log_User_Activity_IP STRING,
    Activity_Code STRING,
    Module_Name STRING,
    Module STRING,
    Instance STRING,
    Ingestion_Timestamp TIMESTAMP
)""")
# COMMAND ----------
# Idempotent:AUTO OPTIMIZE target table to compact small files and improve merge performance
spark.sql(f"""
        ALTER TABLE {silver_catalog}.{silver_schema}.movimientos_catalogo_roll
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.targetFileSize' = '134217728'
        )
    """)

# COMMAND ----------
# Materialize source data for MERGE to avoid re-computing complex CTEs
spark.sql("""
CREATE OR REPLACE TEMP VIEW materialized_merge_source 
AS SELECT * FROM rol_catalogo_deduped WHERE load_type = 'upsert';""")

# COMMAND ----------
# Bulk insert for pure INSERT branches
spark.sql(f"""
INSERT INTO {silver_catalog}.{silver_schema}.movimientos_catalogo_roll
SELECT
    Code,
    Description,
    Active,
    UsuarioCreador,
    NombreUsuarioCreador,
    Created_Date,
    Modified_By,
    UsuarioEditor,
    NombreUsuarioEditor,
    Modified_Date,
    Log_User_Activity_Id,
    Log_User_Activity_IP,
    Activity_Code,
    Module_Name,
    Module,
    Instance,
    Ingestion_Timestamp
FROM rol_catalogo_sources
WHERE load_type = 'insert';""")

# COMMAND ----------  

# Bulk upsert (MERGE) for UPSERT branches using materialized source
spark.sql(f"""
MERGE INTO {silver_catalog}.{silver_schema}.movimientos_catalogo_roll AS tgt
USING materialized_merge_source AS src
  ON  tgt.Code          = src.Code
  AND tgt.Modified_Date = src.Modified_Date
  AND tgt.Module        = src.Module
  AND tgt.Instance      = src.Instance
WHEN MATCHED THEN UPDATE SET
    tgt.Description          = src.Description,
    tgt.Active               = src.Active,
    tgt.UsuarioCreador       = src.UsuarioCreador,
    tgt.NombreUsuarioCreador = src.NombreUsuarioCreador,
    tgt.Created_Date         = src.Created_Date,
    tgt.Modified_By          = src.Modified_By,
    tgt.UsuarioEditor        = src.UsuarioEditor,
    tgt.NombreUsuarioEditor  = src.NombreUsuarioEditor,
    tgt.Log_User_Activity_Id = src.Log_User_Activity_Id,
    tgt.Log_User_Activity_IP = src.Log_User_Activity_IP,
    tgt.Activity_Code        = src.Activity_Code,
    tgt.Module_Name          = src.Module_Name
WHEN NOT MATCHED THEN INSERT (
    Code,
    Description,
    Active,
    UsuarioCreador,
    NombreUsuarioCreador,
    Created_Date,
    Modified_By,
    UsuarioEditor,
    NombreUsuarioEditor,
    Modified_Date,
    Log_User_Activity_Id,
    Log_User_Activity_IP,
    Activity_Code,
    Module_Name,
    Module,
    Instance,
    Ingestion_Timestamp
) VALUES (
    src.Code,
    src.Description,
    src.Active,
    src.UsuarioCreador,
    src.NombreUsuarioCreador,
    src.Created_Date,
    src.Modified_By,
    src.UsuarioEditor,
    src.NombreUsuarioEditor,
    src.Modified_Date,
    src.Log_User_Activity_Id,
    src.Log_User_Activity_IP,
    src.Activity_Code,
    src.Module_Name,
    src.Module,
    src.Instance,
    src.Ingestion_Timestamp
);""")
