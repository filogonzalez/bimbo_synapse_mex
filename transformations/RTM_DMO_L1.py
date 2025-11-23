# Databricks notebook source
import dlt
from pyspark.sql.functions import col

# COMMAND ----------

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")

# COMMAND ----------

@dlt.table(
    name=f"C26_Scheme_Management_Format_Channel",
    comment="Ingest scheme management format channel data from its RAW table",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_scheme_code", "scheme_code IS NOT NULL")
# @dlt.expect_or_drop("valid_effective_to", "Effective_To IS NOT NULL")
def ingest_scheme_management():

    query = """
        SELECT
            DISTINCT
            cod_prom.scheme_code,
            cod_prom.scheme_name,
            cod_prom.Format_Channel_Code,
            cod_prom.Format_Channel_Name,
            cod_prom.Instancia,
            cod_prom.Effective_To
        FROM
            (SELECT
                DISTINCT
                sv.Scheme_Code,
                sv.Scheme_Name,
                CAST(et.lov_code AS VARCHAR(50)) AS `Format_Channel_Code`,
                et.lov_name AS `Format_Channel_Name`,
                sv.Instance AS `Instancia`,
                CAST(sv.Effective_To AS DATE) AS `Effective_To`,
                samv.Apply_Master_Id 
            FROM
                cat_rtmgb_dv.groglortmbm_cz.scheme_v sv
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.scheme_apply_mapping_v AS samv 
                ON (samv.Scheme_Id = sv.Scheme_Id
                    AND samv.Instance = sv.Instance
                    AND samv.Active = sv.Active)
            INNER JOIN
                (SELECT
                    DISTINCT Lov_Id,
                    Lov_Code,
                    Lov_name,
                    Instance,
                    Lov_Type,
                    Active
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.channel_master_v
                WHERE
                    Lov_Type = 'FORMAT_CHANNEL_TYPE') AS et 
                ON (samv.Apply_Master_Id = et.Lov_Id
                    AND samv.Instance = et.Instance
                    AND samv.Active = et.Active)
            WHERE 
                1=1
                AND sv.Effective_To >= CURRENT_DATE()) AS cod_prom
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="c26_scheme_management_format_location",
    comment="Ingest scheme management format location data from its RAW table",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_scheme_code", "Scheme_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_effective_to", "Effective_To IS NOT NULL")
def ingest_scheme_management_format_location():
    query = """
        SELECT
            DISTINCT
            sv.Scheme_Code,
            sv.Scheme_Name,
            lv.Location_Code,
            lv.Location_Name,
            sv.Instance AS Instancia,
            CAST(sv.Effective_To AS DATE) AS Effective_To
        FROM
            cat_rtmgb_dv.groglortmbm_cz.scheme_v sv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.scheme_apply_mapping_v AS samv 
            ON (samv.Scheme_Id = sv.Scheme_Id
                AND samv.Instance = sv.Instance
                AND samv.Active = sv.Active)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.location_v AS lv 
            ON (samv.Apply_Master_Id = lv.Location_Id
                AND samv.Instance = lv.Instance
                AND samv.Active = lv.Active)
        WHERE
            sv.Effective_To >= CURRENT_DATE()
            AND sv.Scheme_Code IN (
                SELECT
                    DISTINCT cod_prom.scheme_code
                FROM (
                    SELECT
                        DISTINCT sv.Scheme_Code
                    FROM
                        cat_rtmgb_dv.groglortmbm_cz.scheme_v sv
                    LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.scheme_apply_mapping_v AS samv 
                        ON (samv.Scheme_Id = sv.Scheme_Id
                            AND samv.Instance = sv.Instance
                            AND samv.Active = sv.Active)
                    INNER JOIN (
                        SELECT
                            DISTINCT Lov_Id,
                            Lov_name,
                            Instance,
                            Active
                        FROM
                            cat_rtmgb_dv.groglortmbm_cz.channel_master_v
                        WHERE
                            Lov_Type = 'FORMAT_CHANNEL_TYPE'
                    ) et 
                        ON (samv.Apply_Master_Id = et.Lov_Id
                            AND samv.Instance = et.Instance
                            AND samv.Active = et.Active)
                    WHERE
                        sv.Effective_To >= CURRENT_DATE()
                ) cod_prom
            )
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="v14_seller_supervisor_mapping",
    comment="Ingest seller-supervisor mapping data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sc_code", "SC_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_supervisor_code", "Supervisor_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_seller_code", "Seller_Code IS NOT NULL")
def ingest_seller_supervisor_mapping():
    query = """
        SELECT
            DISTINCT 
            k.Sales_Center_Code AS SC_Code,
            a.User_Code AS Supervisor_Code,
            CONCAT(a.First_Name, ' ', a.Middle_Name, ' ', a.Last_Name) AS Supervisor_Name,
            d.Position_Name AS Agencia_Ruta,
            j.Employee_Code AS Seller_Code,
            CONCAT(j.First_Name, ' ', j.Last_Name, ' ', j.Mother_Name) AS Seller_Name,
            g.Route_Name,
            g.Route_Code,
            g.Cost_Center,
            h.User_Type_Id,
            j.Active,
            a.Instance AS Source_IVY,
            c.Created_Date,
            CAST(c.Update_Date AS TIMESTAMP) AS Update_date
        FROM
            cat_rtmgb_dv.groglortmbm_cz.user_v a
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.position_user_mapping_v c 
            ON a.User_Id = c.User_Id
            AND a.Instance = c.Instance
            AND a.User_Position_Level_Id = 7
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.position_v d 
            ON c.Position_Id = d.Parent_Id
            AND c.Instance = d.Instance
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.position_user_mapping_v e 
            ON d.Position_Id = e.Position_Id
            AND d.Instance = e.Instance
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.user_route_mapping_v f 
            ON e.User_Id = f.Seller_Id
            AND e.Instance = f.Instance
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.route_v g 
            ON f.Sales_Center_Id = g.Sales_Center_Id
            AND f.Route_Id = g.Route_Id
            AND f.Instance = g.Instance
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.employee_master_v j 
            ON f.Seller_Id = j.Own_Route_Id
            AND f.Instance = j.Instance
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_v h 
            ON h.User_Id = j.Own_Route_Id
            AND h.Instance = j.Instance
            AND h.User_Position_Level_Id = 8
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.list_value_v i 
            ON h.User_Type_Id = i.Lov_Id
            AND h.Instance = i.Instance
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v k 
            ON a.Sales_Center_Id = k.Sales_Center_Id
            AND a.Instance = k.Instance
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.group_v l 
            ON g.group_id = l.group_id
            AND g.Instance = l.Instance
        WHERE
            a.Active = 1
            AND j.Active = 1
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m08_seller_master_national",
    comment="Ingest seller master national data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sales_center_code", "Sales_Center_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_employee_code", "Employee_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_date_of_joining", "Date_Of_Joining IS NOT NULL")
def ingest_seller_master_national():
    query = """
        SELECT
            sc.Sales_Center_Code,
            emv.Employee_Code,
            emv.First_Name,
            emv.Last_Name,
            et.lov_code AS UserSalesRepType,
            emv.License_Number AS Driver_License_Registration_Number,
            et2.lov_code AS User_Type,
            emv.Gender,
            CAST(emv.Date_Of_Birth AS TIMESTAMP) AS Date_Of_Birth,
            CAST(emv.Date_Of_Joining AS TIMESTAMP) AS Date_Of_Joining,
            emv.Mobile_No,
            emv.Email_Id,
            emv.Mother_Name,
            et1.lov_code AS JobPosition_Id,
            emv.RFC,
            rv.Route_Code,
            emv.Instance AS Source_IVY
        FROM
            cat_rtmgb_dv.groglortmbm_cz.employee_master_v AS emv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS sc
            ON (emv.Sales_Center_Id = sc.Sales_Center_Id
                AND emv.Active = sc.Active
                AND emv.Instance = sc.Instance)
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
                Lov_Type = 'SELLER_TYPE'
        ) AS et
            ON (emv.Employee_Type_Id = et.Lov_Id
                AND emv.Instance = et.Instance
                AND emv.Active = et.Active)
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
                Lov_Code = 'SR' AND Lov_Type = 'TRANSACTION_TYPE'
        ) AS et2
            ON (emv.Instance = et2.Instance
                AND emv.Active = et2.Active)
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
        ) AS et1
            ON (emv.Job_Type_Id = et1.Lov_Id
                AND emv.Instance = et1.Instance
                AND emv.Active = et1.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_route_mapping_v AS urmv
            ON (urmv.Seller_Id = emv.Own_Route_Id
                AND urmv.Active = emv.Active
                AND urmv.Instance = emv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.route_v AS rv
            ON (rv.Route_Id = urmv.Route_Id
                AND rv.Active = urmv.Active
                AND rv.Instance = urmv.Instance)
        WHERE
            emv.Active = 1
            AND sc.Active = 1
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m30_plant_product_mapping",
    comment="Ingest plant product mapping data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sales_center_code", "Sales_Center_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_plant_code", "Plant_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_company_code", "Company_Code IS NOT NULL")
def ingest_plant_product_mapping():
    query = """
        SELECT
            DISTINCT
            scv.Sales_Center_Code,
            scv.Sales_Center_Name,
            et.lov_code AS Plant_Code,
            et.lov_name AS Plant_Name,
            CASE 
                WHEN et.lov_name LIKE '%BIMBO%' THEN 'OBM'
                WHEN et.lov_name LIKE '%BARCEL%' THEN 'OBL'
                WHEN et.lov_name LIKE '%RICOLINO%' THEN 'RIC'
            END AS Company_Code,
            CASE
                WHEN et.lov_name LIKE '%BIMBO%' THEN 'BIMBO'
                WHEN et.lov_name LIKE '%BARCEL%' THEN 'BARCEL'
                WHEN et.lov_name LIKE '%RICOLINO%' THEN 'RICOLINO'
            END AS Company_Name,
            scv.Instance AS Source_IVY
        FROM
            cat_rtmgb_dv.groglortmbm_cz.business_partner_lov_mapping_v bplmv
        INNER JOIN (
            SELECT
                DISTINCT
                Lov_Id,
                Lov_name,
                Lov_Code,
                Instance,
                Lov_Type,
                Active
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'PLANT' AND Active = 1
        ) AS et
            ON (bplmv.Lov_Id = et.lov_id
                AND bplmv.Instance = et.Instance
                AND bplmv.Active = et.Active)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS scv
            ON (scv.Sales_Center_Id = bplmv.Sales_Center_Id
                AND scv.Instance = bplmv.Instance
                AND scv.Active = bplmv.Active)
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m29_plant_master",
    comment="Ingest plant master data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_plant_code", "Plant_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_plant_name", "Plant_Name IS NOT NULL")
def ingest_plant_master():
    query = """
        SELECT
            DISTINCT
            Lov_Code AS Plant_Code,
            Lov_name AS Plant_Name,
            Instance AS Instancia,
            Active AS Activo,
            DATE_FORMAT(Created_Date, 'MM/dd/yy hh:mm:ss a') AS Fecha_Modificacion
        FROM
            cat_rtmgb_dv.groglortmbm_cz.list_value_v
        WHERE
            Lov_Type = 'PLANT' AND Active = 1
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="C23_DiscountMapping_Channel",
    comment="Ingest discount master data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_discount_code", "Discount_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_format_channel_code", "Format_Channel_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_effective_to", "Effective_To IS NOT NULL")
def ingest_discount_master():
    query = """
        SELECT
            DISTINCT
            dhv.Discount_Code,
            dhv.Description AS Discount_Description,
            disc.lov_code AS Format_Channel_Code,
            disc.lov_name AS Description,
            et1.lov_code AS Disc_Type,
            dhv.Instance AS Source_IVY,
            lv.Location_Code,
            lv.Location_Name,
            CAST(ddv.Effective_To AS TIMESTAMP) AS Effective_To
        FROM
            cat_rtmgb_dv.groglortmbm_cz.discount_header_v dhv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.discount_details_v AS ddv
            ON (ddv.Discount_Header_Id = dhv.Record_Id
                AND ddv.Instance = dhv.Instance
                AND ddv.Active = dhv.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.discount_apply_mapping_v AS damv
            ON (damv.Discount_Header_Id = dhv.Record_Id
                AND damv.Instance = dhv.Instance
                AND damv.Active = dhv.Active)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.location_v AS lv
            ON (damv.District_Id = lv.Location_Id
                AND damv.Instance = lv.Instance
                AND damv.Active = lv.Active)
        LEFT JOIN (
            SELECT
                DISTINCT
                dhv.Record_Id,
                et.lov_code,
                et.lov_name
            FROM
                cat_rtmgb_dv.groglortmbm_cz.discount_header_v dhv
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.discount_apply_mapping_v AS damv1
                ON (damv1.Discount_Header_Id = dhv.Record_Id
                    AND damv1.Instance = dhv.Instance)
            INNER JOIN (
                SELECT
                    DISTINCT
                    Lov_Id,
                    Lov_Code,
                    Lov_name,
                    Instance,
                    Lov_Type,
                    Active
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.channel_master_v
                WHERE
                    Lov_Type = 'FORMAT_CHANNEL_TYPE'
            ) AS et
                ON (damv1.Channel_Id = et.Lov_Id
                    AND damv1.Instance = et.Instance
                    AND damv1.Active = et.Active)
        ) AS disc
            ON (disc.Record_Id = dhv.Record_Id)
        LEFT JOIN (
            SELECT
                DISTINCT
                Lov_Id,
                Lov_Code,
                Instance,
                Lov_Type,
                Active
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'DISCOUNT_TYPE'
        ) AS et1 
            ON (et1.Lov_id = dhv.Discount_Type_Id
                AND et1.Instance = dhv.Instance
                AND et1.Active = dhv.Active)
        WHERE
            dhv.Active = 1
            AND ddv.Active = 1
            AND ddv.Effective_To >= CURRENT_DATE()
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m15_tax_group",
    comment="Ingest tax group data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_tax_group_header_name", "Tax_Group_Header_Name IS NOT NULL")
# @dlt.expect_or_drop("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_effective_to", "Effective_To IS NOT NULL")
def ingest_tax_group():
    query = """
        SELECT
            DISTINCT 
            plv.Level_Name,
            CAST(tahv.Effective_From AS TIMESTAMP) AS Effective_From,
            CAST(tahv.Effective_To AS TIMESTAMP) AS Effective_To,
            tgh.Tax_Group_Header_Name,
            lv.Location_Name,
            pmv.Product_Code,
            tgh.Instance AS Source_IVY
        FROM
            cat_rtmgb_dv.groglortmbm_cz.tax_group_header_v tgh
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.tax_apply_detail_v AS tadv 
            ON (tgh.Tax_Group_Header_Id = tadv.Tax_Group_Header_Id
                AND tgh.Instance = tadv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.tax_apply_header_v AS tahv 
            ON (tadv.Tax_Apply_Header_Id = tahv.Tax_Apply_Header_Id
                AND tadv.Instance = tahv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.location_v AS lv 
            ON (tahv.Tax_Apply_Location_Id = lv.Location_Id
                AND tahv.Instance = lv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pmv 
            ON (tadv.Product_Id = pmv.Product_Id
                AND tadv.Instance = pmv.Instance)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.product_level_v AS plv 
            ON (pmv.Product_Level_Id = plv.Product_Level_Id
                AND pmv.Instance = plv.Instance)
        WHERE
            tadv.Active = 1
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="c25_scheme_management_mapping_retailer",
    comment="Ingest scheme management mapping retailer data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_scheme_code", "Scheme_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_retailer_code", "Retailer_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_effective_to", "Effective_To IS NOT NULL")
def ingest_scheme_management_mapping_retailer():
    query = """
        SELECT
            DISTINCT
            c24.Scheme_Code,
            rv.Retailer_Code,
            CASE
                WHEN samv.Is_Retailer_Included = 1 THEN 'True'
                ELSE 'False'
            END AS Store_Mapping,
            samv.Instance AS Instance,
            c24.Active AS Activo,
            CAST(c24.Effective_From AS TIMESTAMP) AS Effective_From,
            CAST(c24.Effective_To AS TIMESTAMP) AS Effective_To
        FROM
            cat_rtmgb_dv.groglortmbm_cz.scheme_apply_mapping_v AS samv
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
                sv.Active,
                sv.Instance,
                sv.Modified_Date
            FROM
                cat_rtmgb_dv.groglortmbm_cz.scheme_v AS sv
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
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.scheme_product_v AS spv
                LEFT JOIN (
                    SELECT DISTINCT
                        pmv.Product_Id,
                        pmv.Product_Code,
                        plv.Level_Name,
                        plv.Level_Code,
                        pmv.Active,
                        pmv.Instance,
                        pmv.Update_date
                    FROM
                        cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pmv
                    LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_level_v AS plv
                        ON (pmv.Product_Level_Id = plv.Product_Level_Id
                            AND pmv.Active = plv.Active
                            AND pmv.Instance = plv.Instance)
                    WHERE
                        pmv.Active = 1
                        AND plv.Active = 1
                        AND plv.Product_Level_Id IN (5)
                        AND pmv.Product_Level_Id IN (5)
                ) AS prod_list 
                    ON (spv.Product_Id = prod_list.Product_Id
                        AND spv.Active = prod_list.Active
                        AND spv.Instance = prod_list.Instance)
            ) AS prod 
                ON (sv.Scheme_Id = prod.Scheme_Id
                    AND sv.Active = prod.Active
                    AND sv.Instance = prod.Instance)
            LEFT JOIN (
                SELECT DISTINCT
                    slab.Scheme_Slab_Id,
                    slab.Scheme_Id,
                    slab.Slab_Description,
                    slab.Active,
                    slab.Instance,
                    buy.From_Qty AS buy,
                    get.From_Qty AS get
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.scheme_slab_v AS slab
                LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.scheme_slab_target_v AS buy
                    ON (slab.Scheme_Slab_Id = buy.Scheme_Slab_Id
                        AND slab.Instance = buy.Instance
                        AND slab.Active = buy.Active)
                LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.scheme_free_product_v AS get
                    ON (slab.Scheme_Slab_Id = get.Scheme_Slab_Id
                        AND slab.Instance = get.Instance
                        AND slab.Active = get.Active)
                WHERE
                    slab.Active = 1
            ) AS bg
                ON (bg.Scheme_Id = sv.Scheme_Id
                    AND bg.Instance = sv.Instance
                    AND bg.Active = sv.Active)
            WHERE
                CAST(Effective_To AS DATE) >= CURRENT_DATE()
        ) AS c24
            ON (c24.Scheme_Id = samv.Scheme_Id
                AND c24.Instance = samv.Instance
                AND c24.Active = samv.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.retailer_v AS rv
            ON (samv.Apply_Master_Id = rv.Retailer_Id
                AND samv.Instance = rv.Instance)
        WHERE
            c24.Active = 1
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="c24_scheme_management_master",
    comment="Ingest scheme management master data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_scheme_code", "Scheme_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_effective_to", "Effective_To IS NOT NULL")
def ingest_scheme_management_master():
    query = """
        SELECT
            DISTINCT
            sv.Scheme_Id,
            sv.Scheme_Code,
            sv.Scheme_Name,
            sv.Scheme_Type,
            CAST(sv.Effective_From AS TIMESTAMP) AS Effective_From,
            CAST(sv.Effective_To AS TIMESTAMP) AS Effective_To,
            prod.Level_Name,
            prod.Level_Code,
            prod.Product_Code,
            bg.buy,
            bg.get,
            sv.Active,
            sv.Instance AS Source_IVY,
            CAST(sv.Modified_Date AS TIMESTAMP) AS Modified_Date
        FROM
            cat_rtmgb_dv.groglortmbm_cz.scheme_v AS sv
        LEFT JOIN (
            SELECT
                DISTINCT
                spv.Scheme_Product_Rocord_Id,
                spv.Scheme_Id,
                spv.Product_Id,
                prod_list.Product_Code,
                prod_list.Level_Name,
                prod_list.Level_Code,
                spv.Active,
                spv.Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.scheme_product_v AS spv
            LEFT JOIN (
                SELECT
                    DISTINCT
                    pmv.Product_Id,
                    pmv.Product_Code,
                    plv.Level_Name,
                    plv.Level_Code,
                    pmv.Active,
                    pmv.Instance,
                    pmv.Update_date
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pmv
                LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_level_v AS plv
                    ON (pmv.Product_Level_Id = plv.Product_Level_Id
                        AND pmv.Active = plv.Active
                        AND pmv.Instance = plv.Instance)
                WHERE
                    pmv.Active = 1
                    AND plv.Active = 1
                    AND plv.Product_Level_Id IN (5)
                    AND pmv.Product_Level_Id IN (5)
            ) AS prod_list 
                ON (spv.Product_Id = prod_list.Product_Id
                    AND spv.Active = prod_list.Active
                    AND spv.Instance = prod_list.Instance)
        ) AS prod 
            ON (sv.Scheme_Id = prod.Scheme_Id
                AND sv.Active = prod.Active
                AND sv.Instance = prod.Instance)
        LEFT JOIN (
            SELECT
                DISTINCT
                slab.Scheme_Slab_Id,
                slab.Scheme_Id,
                slab.Slab_Description,
                slab.Active,
                slab.Instance,
                buy.From_Qty AS buy,
                get.From_Qty AS get
            FROM
                cat_rtmgb_dv.groglortmbm_cz.scheme_slab_v AS slab
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.scheme_slab_target_v AS buy
                ON (slab.Scheme_Slab_Id = buy.Scheme_Slab_Id
                    AND slab.Instance = buy.Instance
                    AND slab.Active = buy.Active)
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.scheme_free_product_v AS get
                ON (slab.Scheme_Slab_Id = get.Scheme_Slab_Id
                    AND slab.Instance = get.Instance
                    AND slab.Active = get.Active)
            WHERE
                slab.Active = 1
        ) AS bg 
            ON (bg.Scheme_Id = sv.Scheme_Id
                AND bg.Instance = sv.Instance
                AND bg.Active = sv.Active)
        WHERE
            CAST(sv.Effective_To AS DATE) >= CURRENT_DATE()
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="v05_route_preseller_mapping",
    comment="Ingest route preseller mapping data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sales_center_code", "Sales_Center_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_route_code_pre", "Route_Code_Pre IS NOT NULL")
# @dlt.expect_or_drop("valid_route_code_sal", "Route_Code_Sal IS NOT NULL")
def ingest_route_preseller_mapping():
    query = """
        SELECT DISTINCT 
            rpre.Sales_Center_Code,
            rpre.Route_Code AS Route_Code_Pre,
            rpre.Route_Name AS Route_Name_Pre,
            rven.Route_Code AS Route_Code_Sal,
            rven.Route_Name AS Route_Name_Sal
        FROM
            (
                SELECT
                    scv.Sales_Center_Code,
                    rv.Route_Code,
                    rv.Route_Name,
                    rrm.Retailer_Id,
                    uv.User_Type_Id
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.route_retailer_mapping_v AS rrm
                LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS scv
                    ON (rrm.Sales_Center_Id = scv.Sales_Center_Id
                        AND rrm.Instance = scv.Instance
                        AND rrm.Active = 1)
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
                    uv.User_Type_Id IN (911)
                    AND rrm.Active = 1
                    AND scv.Legal_Enitity_Id = 1
            ) AS rpre
        LEFT JOIN (
            SELECT
                scv.Sales_Center_Code,
                rv.Route_Code,
                rv.Route_Name,
                rrm.Retailer_Id,
                uv.User_Type_Id
            FROM
                cat_rtmgb_dv.groglortmbm_cz.route_retailer_mapping_v AS rrm
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS scv
                ON (rrm.Sales_Center_Id = scv.Sales_Center_Id
                    AND rrm.Instance = scv.Instance
                    AND rrm.Active = 1)
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
                uv.User_Type_Id IN (909)
                AND rrm.Active = 1
                AND scv.Legal_Enitity_Id = 1
            ) AS rven
                ON (rpre.Sales_Center_Code = rven.Sales_Center_Code
                    AND rpre.Retailer_Id = rven.Retailer_Id)
        WHERE
            rven.Route_Code IS NOT NULL
        ORDER BY
            rpre.Sales_Center_Code,
            Route_Code_Pre
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="segment_sku_tagging_dump",
    comment="Ingest segment SKU tagging data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_group_code", "Group_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_effective_to", "Effective_To IS NOT NULL")
def ingest_segment_sku_tagging_dump():
    query = """
        SELECT
            gv.Group_Code,
            gv.Group_Name,
            pmv.Product_Code,
            pmv.Full_Description AS product_full_desc,
            CAST(pgm.Effective_From AS TIMESTAMP) AS Effective_From,
            CAST(pgm.Effective_To AS TIMESTAMP) AS Effective_To,
            lv.Location_Code AS Location,
            lrs.Lov_Code AS SEGMENT,
            lt.Lov_Code AS Tagging_Type,
            pmv.Instance AS Instance
        FROM
            cat_rtmgb_dv.groglortmbm_cz.group_v AS gv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_header_v AS ghv 
            ON (gv.Group_Header_Id = ghv.Group_Header_Id
                AND gv.Instance = ghv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_group_mapping_v AS pgm 
            ON (gv.Group_Id = pgm.Group_Id
                AND gv.Instance = pgm.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pmv 
            ON (pgm.Product_Id = pmv.Product_Id
                AND pgm.Product_Level_Id = pmv.Product_Level_Id
                AND pgm.Instance = pmv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_channel_mapping_v AS gcm_loc 
            ON (gv.Group_Id = gcm_loc.Group_Id
                AND gcm_loc.Channel_Type_Id IN (6647, 6575)
                AND gv.Instance = gcm_loc.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.location_v AS lv 
            ON (gcm_loc.Channel_Id = lv.Location_Id
                AND gcm_loc.Instance = lv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_channel_mapping_v AS gcm_seg 
            ON (gv.Group_Id = gcm_seg.Group_Id
                AND gcm_seg.Channel_Type_Id IN (6646, 6574)
                AND gv.Instance = gcm_seg.Instance)
        LEFT JOIN (
            SELECT
                Lov_Id,
                Lov_Code,
                Lov_Name,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'RETAILER_SEGMENT_TYPE'
                AND Active = 1
        ) AS lrs 
            ON (gcm_seg.Channel_Id = lrs.Lov_Id
                AND gcm_seg.Instance = lrs.Instance)
        LEFT JOIN (
            SELECT
                Lov_Id,
                Lov_Code,
                Lov_Name,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'PRODUCT_TAGGING'
                AND Active = 1
        ) AS lt 
            ON (ghv.Group_Header_Type_Id = lt.Lov_Id
                AND ghv.Instance = lt.Instance)
        WHERE
            gv.Group_Type_Id = 0
            AND gv.Group_Header_Id IN (3, 4)
            AND gv.Active = 1
        ORDER BY
            gv.Group_Code,
            pmv.Product_Code,
            lv.Location_Code
    """

    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="product_set_salable_flag",
    comment="Ingest product salable flag data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_set_product_salable", "Set_Product_Salable IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "Active = true")
def ingest_product_set_salable_flag():
    query = """
        SELECT 
            pmv.Product_Code,
            pmv.Full_Description,
            CASE 
                WHEN pav.Attributes_Id IS NULL THEN 'Salable'
                WHEN pav.Attributes_Id = 1 THEN 'No Salable'
                ELSE 'No Returnable'
            END AS Set_Product_Salable,
            CAST(pav.Modified_Date AS TIMESTAMP) AS Modified_Date,
            pmv.Active,
            pmv.Instance AS Source_IVY
        FROM
            cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pmv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_attributes_v AS pav 
            ON (pmv.Product_Id = pav.Product_Id
                AND pmv.Instance = pav.Instance
                AND pmv.Active = pav.Active)
        WHERE 
            pmv.Active = 1
            AND pmv.Product_Level_Id = 5
        ORDER BY 
            pmv.Product_Code
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m40_political_geography",
    comment="Ingest political geography data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_postal_code", "Postal_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_state_code", "State_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "Active = true")
def ingest_political_geography():
    query = """
        SELECT
            Postal_Code,
            State_Code,
            State_Desc,
            Municipality_Code,
            Municipality_Desc,
            Colonia_Code,
            Colonia_Desc,
            Active,
            Instance
        FROM
            cat_rtmgb_dv.groglortmbm_cz.political_geography_v
        ORDER BY
            Postal_Code
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m13_price_master",
    comment="Ingest price master data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_price_group", "Price_Group IS NOT NULL")
# @dlt.expect_or_drop("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_piece_price", "Piece_Price IS NOT NULL")
def ingest_price_master():
    query = """
        SELECT
            gv.group_code AS Price_Group,
            pm.Product_Code,
            pm.Full_Description,
            CAST(pmvv.Effective_From AS TIMESTAMP) AS Effective_From,
            CAST(pmvv.Effective_To AS TIMESTAMP) AS Effective_To,
            pmvv.Price AS Piece_Price,
            CAST(pmvv.Update_date AS TIMESTAMP) AS Update_date,
            pm.Instance AS Source_IVY,
            pm.Active
        FROM 
            cat_rtmgb_dv.groglortmbm_cz.group_v AS gv
        INNER JOIN (
            SELECT 
                Effective_From,
                Effective_To,
                Price,
                Active,
                Product_Id,
                Group_Id,
                Instance,
                Update_date
            FROM
                cat_rtmgb_dv.groglortmbm_cz.price_master_v
            WHERE 
                Effective_To IS NULL
                AND Active = 1
                AND Price IS NOT NULL
                AND product_uom_id = 860
        ) AS pmvv 
            ON (gv.Group_Id = pmvv.Group_Id
                AND gv.Instance = pmvv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pm 
            ON (pm.Product_Id = pmvv.Product_Id
                AND pm.Instance = pmvv.Instance)
        WHERE
            gv.Group_Type_Id = 820
            AND gv.Active = 1
            AND pm.Active = 1
            AND gv.Group_Code <> 'Default'
        ORDER BY
            gv.Group_Code,
            pm.Product_Code
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m03_sales_center",
    comment="Ingest sales center data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sc_code", "SC_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_sc_name", "SC_Name IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "active = true")
def ingest_sales_center():
    query = """
        SELECT
            DISTINCT 
            pv.PV1 AS Divisional_Manager,
            sc.Sales_Center_Code AS SC_Code,
            pv2.PV2 AS SC_Name,
            sc.Tax_Number AS RFC,
            le.Legal_Entity_Code AS Sales_Center_Type,
            lv.Location_Code AS Location_Code,
            sc.Transaction_Type AS Transaction_Type,
            sc.Region_Code AS Region,
            sc.Delivery_Number AS Delivery_Number,
            sc.Lead_Time AS Lead_Time,
            sc.Tipo_Proceso AS Insurance_Company,
            sc.Policy_Number,
            pv.active,
            sc.Instance AS Source_IVY,
            sc.Has_Multiple_Legal_Entity AS Multi_Org_to_be_Enabled
        FROM
            (
                SELECT
                    Position_LongName AS PV1,
                    Position_Id,
                    Position_Level_Id,
                    active,
                    Instance
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.position_v
            ) AS pv
        INNER JOIN (
            SELECT
                Position_LongName AS PV2,
                Parent_Id,
                Position_Id,
                active,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.position_v
        ) AS pv2 
            ON (pv.Position_Id = pv2.Parent_Id
                AND pv.Instance = pv2.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.position_user_mapping_v AS pum 
            ON (pv2.Position_Id = pum.Position_Id 
                AND pv2.Instance = pum.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS sc 
            ON (sc.Sales_Center_Id = pum.Sales_Center_Id
                AND sc.Instance = pum.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.legal_entity_v AS le 
            ON (le.Legal_Entity_Id = sc.Legal_Enitity_Id
                AND le.Instance = sc.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.location_v AS lv 
            ON (lv.Location_Id = sc.Sales_Center_District_Id 
                AND lv.Instance = sc.Instance)
        WHERE
            pv.Position_Level_Id = 5
            AND sc.Active = 1
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m01_location_master",
    comment="Ingest location master data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_country_code", "CountryCode IS NOT NULL")
# @dlt.expect_or_drop("valid_location_code", "LocationCode IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "active = true")
def ingest_location_master():
    query = """
        SELECT
            DISTINCT 
            lco.location_Code AS CountryCode,
            lco.location_Name AS CountryName,
            lco1.location_Code AS RegionCode,
            lco1.location_Name AS RegionName,
            lco2.location_Code AS LocationCode,
            lco2.location_Name AS LocationName,
            lco3.location_Code AS DistrictCode,
            lco3.location_Name AS DistrictName,
            lco4.location_Code AS ZipCode,
            lco4.location_Name AS ZipName,
            lco.Instance AS Instance,
            lco.Is_Border,
            lco.active
        FROM
            (
                SELECT
                    location_Code,
                    location_Name,
                    Instance,
                    location_Parent_Id,
                    location_Id,
                    Is_Border,
                    active
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.location_v
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
            FROM
                cat_rtmgb_dv.groglortmbm_cz.location_v
        ) AS lco1 
            ON (lco.location_Id = lco1.Location_Parent_Id
                AND lco.Instance = lco1.Instance)
        LEFT JOIN (
            SELECT
                location_Code,
                location_Name,
                Instance,
                location_Parent_Id,
                location_Id,
                Is_Border,
                active
            FROM
                cat_rtmgb_dv.groglortmbm_cz.location_v
        ) AS lco2 
            ON (lco1.location_Id = lco2.Location_Parent_Id
                AND lco1.Instance = lco2.Instance)
        LEFT JOIN (
            SELECT
                location_Code,
                location_Name,
                Instance,
                location_Parent_Id,
                location_Id,
                Is_Border,
                active
            FROM
                cat_rtmgb_dv.groglortmbm_cz.location_v
        ) AS lco3 
            ON (lco2.location_Id = lco3.Location_Parent_Id
                AND lco2.Instance = lco3.Instance)
        LEFT JOIN (
            SELECT
                location_Code,
                location_Name,
                Instance,
                location_Parent_Id,
                location_Id,
                Is_Border,
                active
            FROM
                cat_rtmgb_dv.groglortmbm_cz.location_v
        ) AS lco4 
            ON (lco3.location_Id = lco4.Location_Parent_Id
                AND lco3.Instance = lco4.Instance)
        WHERE 
            lco.active = 1
            AND lco.Location_Code = 'MX'
            AND lco1.location_Code IN ('SRT', 'CTR', 'MTR', 'NRT', 'ZNF', 'ZND')
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m05_product_master",
    comment="Ingest product master data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_product_id", "Product_Id IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "Active = true")
def ingest_product_master():
    query = """
        SELECT
            pm.Product_Id,
            CAST(pm.Product_Code AS STRING) AS Product_Code,
            pm.Product_Global_Code,
            pm.Full_Description AS Product_Full_Desc,
            pm.Short_Description AS Product_Short_Desc,
            po.Company_Code,
            po.Company_Name,
            pb.Brand_Code,
            pb.Brand_Name,
            pc.Category_Code,
            pc.Category_Name,
            ps.Segment_Code,
            ps.Segment_Name,
            pl.Line_Code,
            pl.Line_Name,
            'PIECE' AS Base_Uom,
            'PIECE' AS PrimarySalesUOM,
            pm.Product_Weight AS Weight,
            CASE 
                WHEN ps.Segment_Name = 'Envase' THEN 0
                WHEN b1.Conversion_Qty IS NULL THEN 0
                ELSE b1.Conversion_Qty
            END AS Pack1_Size,
            CASE 
                WHEN b1.Conversion_Qty = b.pack2 AND b1.Conversion_Qty <> 0 THEN 0
                ELSE b.pack2
            END AS Pack2_Size,
            pp.Price AS Piece_Price,
            pm.Bar_Code AS Product_BarCode,
            pm.Sequence,
            pm.Is_Salable AS IsSalable,
            pm.Is_Returnable AS IsReturnable,
            c.Product_Code AS Tray_Code,
            pm.HSN_Code AS Clave_Prod,
            pm.Unidad_de_Medida AS Clave_Unidad,
            '' AS Shelf_Life,
            pm.`Devolución_Percentage` AS Return_Rate,
            pp.Price AS Base_Price_Sales_Center,
            lv.Lov_Code AS Unit_Measure,
            pp.Price AS MRP,
            pm.Instance AS Instance,
            pm.Active
        FROM
            cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pm
        LEFT JOIN (
            SELECT
                MIN(conversion_qty) AS pack2,
                uom_group_id,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.product_uom_group_mapping_v
            WHERE
                conversion_uom_type_id <> 0
                AND Active = 1
            GROUP BY
                uom_group_id,
                Instance
        ) AS b
            ON (pm.Uom_Group_Id = b.Uom_Group_Id
                AND pm.Instance = b.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_uom_group_mapping_v AS b1
            ON (pm.Uom_Group_Id = b1.Uom_Group_Id
                AND pm.Instance = b1.Instance
                AND b1.Product_Uom_Type_ID = 861
                AND b1.Active = 1)
        LEFT JOIN (
            SELECT
                Product_Id,
                Product_Code AS Line_Code,
                Full_Description AS Line_Name,
                Product_Parent_Id,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.product_master_v
            WHERE
                Product_Level_Id = 4
                AND Active = 1
        ) AS pl 
            ON (pm.Product_Parent_Id = pl.Product_Id
                AND pm.Instance = pl.Instance)
        LEFT JOIN (
            SELECT
                Product_Id,
                Product_Code AS Segment_Code,
                Full_Description AS Segment_Name,
                Product_Parent_Id,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.product_master_v
            WHERE
                Product_Level_Id = 3
                AND Active = 1
        ) AS ps 
            ON (pl.Product_Parent_Id = ps.Product_Id
                AND pl.Instance = ps.Instance)
        LEFT JOIN (
            SELECT
                Product_Id,
                Product_Code AS Category_Code,
                Full_Description AS Category_Name,
                Product_Parent_Id,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.product_master_v
            WHERE
                Product_Level_Id = 2
                AND Active = 1
        ) AS pc 
            ON (ps.Product_Parent_Id = pc.Product_Id
                AND ps.Instance = pc.Instance)
        LEFT JOIN (
            SELECT
                Product_Id,
                Product_Code AS Brand_Code,
                Full_Description AS Brand_Name,
                Product_Parent_Id,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.product_master_v
            WHERE
                Product_Level_Id = 1
                AND Active = 1
        ) AS pb 
            ON (pc.Product_Parent_Id = pb.Product_Id
                AND pc.Instance = pb.Instance)
        LEFT JOIN (
            SELECT
                Product_Id,
                Product_Code AS Company_Code,
                Full_Description AS Company_Name,
                Product_Parent_Id,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.product_master_v
            WHERE
                Product_Level_Id = 6
                AND Active = 1
        ) AS po 
            ON (pb.Product_Parent_Id = po.Product_Id
                AND pb.Instance = po.Instance)
        LEFT JOIN (
            SELECT
                Product_Id,
                Price,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.price_master_v
            WHERE
                Group_Id = 1
                AND Product_Uom_Id = 860
                AND Effective_To IS NULL
                AND Active = 1
        ) AS pp 
            ON (pm.Product_Id = pp.Product_Id
                AND pm.Instance = pp.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.list_value_v AS lv 
            ON (pm.Unit_Measure_Id = lv.Lov_Id
                AND pm.Instance = lv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_master_v AS c 
            ON (pm.Mapped_Tray_Id = c.Product_Id
                AND pm.Instance = c.Instance)
        WHERE
            pm.Product_Level_Id = 5
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="c16_product_group_invsplit",
    comment="Ingest product group inventory split data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_group_code", "Group_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_activo", "Activo = true")
def ingest_product_group_invsplit():
    query = """
        SELECT
            DISTINCT 
            gv.Group_Code AS Group_Code,
            gv.Group_Name AS Group_Name,
            pm.Product_Code AS Product_Code,
            CAST(gv.Effective_From AS TIMESTAMP) AS Effective_From,
            CAST(gv.Effective_To AS TIMESTAMP) AS Effective_To,
            le.Legal_Entity_Code AS Legal_Entity,
            gh.Group_Header_Name AS Tagging_Type,
            chm.Lov_Code AS Format_Channel_Code,
            gv.Instance AS Instance,
            gv.Active AS Activo,
            CAST(gv.Modified_Date AS TIMESTAMP) AS FechadeModificacion
        FROM
            cat_rtmgb_dv.groglortmbm_cz.group_v AS gv
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.group_header_v AS gh 
            ON (gv.Group_Header_Id = gh.Group_Header_Id
                AND gv.Instance = gh.Instance
                AND gv.Active = gh.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_channel_mapping_v AS gch 
            ON (gch.Group_Id = gv.Group_Id
                AND gch.Instance = gv.Instance
                AND gch.Active = gv.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.channel_master_v AS chm 
            ON (chm.Lov_Id = gch.Channel_Id
                AND chm.Instance = gch.Instance
                AND chm.Active = gch.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_group_mapping_v AS pgm 
            ON (pgm.Group_Id = gv.Group_Id
                AND pgm.Instance = gv.Instance
                AND pgm.Active = gv.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pm 
            ON (pm.Product_Id = pgm.Product_Id
                AND pm.Instance = pgm.Instance
                AND pm.Active = pgm.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_legal_entity_mapping_v AS plm 
            ON (plm.Product_Id = pgm.Product_Id
                AND plm.Instance = pgm.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_principal_mapping_v AS ppmv 
            ON (ppmv.Product_Id = plm.Legal_Entity_Id
                AND ppmv.Instance = plm.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.legal_entity_v AS le 
            ON (le.Legal_Entity_Id = ppmv.Legal_Entity_Id
                AND le.Instance = ppmv.Instance
                AND le.Active = ppmv.Active)
        WHERE
            gv.Active = 1
            AND gh.Group_Header_Name = '20190831-INVSPLIT'
            AND chm.Lov_Code IS NOT NULL
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m06_route_type",
    comment="Ingest route type data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_route_type", "Route_Type IS NOT NULL")
# @dlt.expect_or_drop("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_activo", "Activo = true")
def ingest_route_type():
    query = """
        SELECT
            gv.Group_Code AS Route_Type,
            pl.Level_Code,
            pm.Product_Code,
            pm.Full_Description,
            rtm.Instance AS Instance,
            rtm.Active AS Activo,
            MAX(CAST(rtm.Update_date AS TIMESTAMP)) AS FechadeModificacion,
            gmv.Sequence
        FROM
            cat_rtmgb_dv.groglortmbm_cz.route_type_mapping_v AS rtm
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pm 
            ON (rtm.Product_Id = pm.Product_Id
                AND rtm.Instance = pm.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_level_v AS pl 
            ON (pm.Product_Level_Id = pl.Product_Level_Id
                AND pm.Instance = pl.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_v AS gv 
            ON (rtm.Group_Id = gv.Group_Id
                AND rtm.Instance = gv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_mapping_v AS gmv 
            ON (gmv.Group_Id = gv.Group_Id
                AND gmv.Product_Id = pm.Product_Id
                AND gmv.Instance = rtm.Instance)
        WHERE
            gv.Active = 1
            AND rtm.Active = 1 
            AND gmv.Active = 1
        GROUP BY
            gv.Group_Code,
            pl.Level_Code,
            pm.Product_Code,
            pm.Full_Description,
            rtm.Instance,
            rtm.Active,
            gmv.Sequence
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="catalogo_clave_ivy",
    comment="Ingest clave ivy catalog data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_internal_id", "Internal_ID IS NOT NULL")
# @dlt.expect_or_drop("valid_code", "Code IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "Active = true")
def ingest_catalogo_clave_ivy():
    query = """
        SELECT
            Lov_Id AS Internal_ID,
            Lov_Type AS Type,
            Lov_Code AS Code,
            Lov_Name AS Name,
            Instance AS Instance,
            Active,
            CAST(Created_Date AS TIMESTAMP) AS Created_Date,
            CAST(Modified_Date AS TIMESTAMP) AS Modified_Date
        FROM
            cat_rtmgb_dv.groglortmbm_cz.list_value_v
        WHERE
            Lov_Type = 'CLAVE_PROD_TYPE'
            AND Active = 1
        ORDER BY
            Instance,
            Lov_Code
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="c21_discount_master",
    comment="Ingest discount master data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_discount_code", "Discount_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "Active = true")
def ingest_discount_master():
    query = """
        SELECT
            DISTINCT
            dhv.Discount_Code,
            dhv.Description AS Discount_Description,
            plv.Level_Name AS Apply_Type,
            plv.Level_Code AS Item_Disc_Type,
            pmv.Product_Code,
            CAST(ddv.Effective_From AS TIMESTAMP) AS From_Date,
            CAST(ddv.Effective_To AS TIMESTAMP) AS To_Date,
            ddv.Min_Discount_Value AS Discount,
            et.lov_code AS Disc_Type,
            dhv.Instance AS Instance,
            dhv.Active
        FROM
            cat_rtmgb_dv.groglortmbm_cz.discount_header_v AS dhv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.discount_details_v AS ddv 
            ON (ddv.Discount_Header_Id = dhv.Record_Id
                AND ddv.Instance = dhv.Instance
                AND ddv.Active = dhv.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pmv 
            ON (pmv.Product_Id = ddv.Product_Id
                AND pmv.Instance = ddv.Instance
                AND pmv.Active = ddv.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_level_v AS plv 
            ON (plv.Product_Level_Id = pmv.Product_Level_Id
                AND plv.Instance = pmv.Instance
                AND plv.Active = pmv.Active)
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
                Lov_Type = 'DISCOUNT_TYPE'
        ) AS et 
            ON (et.Lov_id = dhv.Discount_Type_Id
                AND et.Instance = dhv.Instance
                AND et.Active = dhv.Active)
        WHERE
            dhv.Active = 1
            AND ddv.Effective_To >= CURRENT_DATE()
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m07_route_master",
    comment="Ingest route master data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_route_id", "route_id IS NOT NULL")
# @dlt.expect_or_drop("valid_sc_code", "sc_code IS NOT NULL")
# @dlt.expect_or_drop("valid_activo", "Activo = true")
def ingest_route_master():
    query = """
        SELECT
            DISTINCT 
            rv.Route_Id AS route_id,
            sc.Sales_Center_Code AS sc_code,
            sc.Sales_Center_Name AS sc_name,
            rv.Route_Code AS route_code,
            rv.Route_Name AS route_name,
            rv.Cost_Center AS cost_center,
            st.Lov_Code AS route_selling_type,
            pvs.Position_Name AS route_supervisor,
            gv.Group_Name AS route_type,
            rv.Has_Legal_Entity AS Multiorg,
            rv.Distance AS Distance,
            rv.Time_Hrs AS Time_in_Hrs,
            rv.Store_Visit_Time AS Store_Visit_Time,
            rv.Instance AS Instance,
            rv.Active AS Activo,
            CAST(rv.Created_Date AS TIMESTAMP) AS FechadeCreacion,
            CAST(rv.Modified_Date AS TIMESTAMP) AS FechadeModificacion
        FROM
            cat_rtmgb_dv.groglortmbm_cz.route_v AS rv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS sc 
            ON (rv.Sales_Center_Id = sc.Sales_Center_Id
                AND rv.Instance = sc.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_route_mapping_v AS urm 
            ON (rv.Route_Id = urm.Route_Id 
                AND rv.Sales_Center_Id = urm.Sales_Center_Id
                AND rv.Instance = urm.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.user_v AS uv 
            ON (urm.Seller_Id = uv.User_Id 
                AND urm.Instance = uv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_v AS gv 
            ON (rv.Group_Id = gv.Group_Id
                AND rv.Instance = gv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.position_user_mapping_v AS pum 
            ON (urm.Seller_Id = pum.User_Id 
                AND urm.Instance = pum.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.position_v AS pv 
            ON (pum.Position_Id = pv.Position_Id 
                AND pum.Instance = pv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.position_v AS pvs 
            ON (pv.Parent_Id = pvs.Position_Id 
                AND pv.Instance = pvs.Instance)
        LEFT JOIN (
            SELECT
                DISTINCT Lov_Id,
                Lov_Code,
                Instance,
                Lov_Type
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'SELLER_TYPE'
        ) AS st 
            ON (uv.User_Type_Id = st.Lov_Id
                AND uv.Instance = st.Instance)
        WHERE
            rv.Active = 1
            AND sc.Active = 1
            AND uv.Active = 1
            AND sc.Sales_Center_Code IS NOT NULL
        ORDER BY
            sc_code,
            route_code
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m03_sales_center_address",
    comment="Ingest sales center address data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sales_center_code", "Sales_Center_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_lov_code", "lov_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "Active = true")
def ingest_sales_center_address():
    query = """
        SELECT
            sc.Sales_Center_Code,
            et.lov_Code,
            ad.Address_1,
            ad.Address_2,
            ad.Address_3,
            ad.City,
            ad.Telephone,
            ad.Landmark,
            ad.Email,
            ad.Pin_Code,
            ad.Geo_Latitude,
            ad.Geo_Longitude,
            ad.Numero_Exterior,
            ad.Numero_Interior,
            ad.Entre_Calle_1,
            ad.Entre_Calle_2,
            ad.`País`,
            ad.Instance AS Source_IVY,
            ad.Active,
            CAST(ad.Modified_Date AS TIMESTAMP) AS Modified_Date
        FROM
            cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS sc
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.address_detail_v AS ad 
            ON (sc.Sales_Center_Id = ad.Customer_Id
                AND sc.Instance = ad.Instance
                AND sc.Active = ad.Active)
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
                Lov_Type = 'ADDRESS_TYPE'
        ) AS et 
            ON (ad.Address_Type_Id = et.Lov_Id
                AND ad.Instance = et.Instance
                AND ad.Active = et.Active)
        WHERE 
            sc.Active = 1
            AND ad.Customer_Type_Id = 823
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="c22_discount_mapping_retailer",
    comment="Ingest discount mapping retailer data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_discount_code", "Discount_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_retailer_code", "Retailer_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "Active = true")
def ingest_discount_mapping_retailer():
    query = """
        SELECT
            DISTINCT 
            dhv.Discount_Code,
            dhv.Description,
            CAST(ddv.Effective_From AS TIMESTAMP) AS From_Date,
            CAST(ddv.Effective_To AS TIMESTAMP) AS To_Date,
            rv.Retailer_Code,
            rv.Retailer_Name,
            et.Lov_Code AS Disc_Type,
            dhv.Instance AS Instance,
            dhv.Active
        FROM
            cat_rtmgb_dv.groglortmbm_cz.discount_header_v AS dhv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.discount_details_v AS ddv 
            ON (ddv.Discount_Header_Id = dhv.Record_Id
                AND ddv.Instance = dhv.Instance
                AND ddv.Active = dhv.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.discount_apply_mapping_v AS damv 
            ON (damv.Discount_Header_Id = ddv.Discount_Header_Id 
                AND damv.Instance = ddv.Instance
                AND damv.Active = ddv.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.retailer_v AS rv 
            ON (rv.Retailer_Id = damv.Retailer_Id 
                AND rv.Instance = damv.Instance
                AND rv.Active = damv.Active)
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
                Lov_Type = 'DISCOUNT_TYPE'
        ) AS et 
            ON (et.Lov_id = dhv.Discount_Type_Id
                AND et.Instance = dhv.Instance
                AND et.Active = dhv.Active)
        WHERE
            dhv.Active = 1
            AND ddv.Effective_To >= CURRENT_DATE()
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m28_route_type_lead_time",
    comment="Ingest route type lead time data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sc_code", "SC_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_route_type", "Route_Type IS NOT NULL")
# @dlt.expect_or_drop("valid_activo", "Activo = true")
def ingest_route_type_lead_time():
    query = """
        SELECT
            sc.Sales_Center_Code AS SC_Code,
            gv.Group_Name AS Route_Type,
            lt.Lead_Time,
            gv.Instance AS Instance,
            gv.Active AS Activo,
            MAX(CAST(gv.Modified_Date AS TIMESTAMP)) AS FechadeModificacion
        FROM
            cat_rtmgb_dv.groglortmbm_cz.route_v AS rv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS sc 
            ON (rv.Sales_Center_Id = sc.Sales_Center_Id
                AND rv.Instance = sc.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_v AS gv 
            ON (rv.Group_Id = gv.Group_Id
                AND rv.Instance = gv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.route_type_lead_time_v AS lt 
            ON (rv.Group_Id = lt.Route_Type_Group_Id
                AND rv.Sales_Center_Id = lt.Sales_Center_Id
                AND rv.Instance = lt.Instance)
        WHERE
            sc.Sales_Center_Code IS NOT NULL
            AND gv.Active = 1
            AND rv.Active = 1
            AND sc.Active = 1
        GROUP BY
            sc.Sales_Center_Code,
            gv.Group_Name,
            lt.Lead_Time,
            gv.Instance,
            gv.Active
        ORDER BY
            sc.Sales_Center_Code,
            gv.Group_Name
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="c16_product_group_order",
    comment="Ingest product group order data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_group_code", "Group_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_activo", "Activo = true")
def ingest_product_group_order():
    query = """
        SELECT
            DISTINCT 
            gv.Group_Code AS Group_Code,
            gv.Group_Name AS Group_Name,
            pm.Product_Code AS Product_Code,
            CAST(gv.Effective_From AS TIMESTAMP) AS Effective_From,
            CAST(gv.Effective_To AS TIMESTAMP) AS Effective_To,
            gh.Group_Header_Name AS Tagging_Type,
            et.Lov_Code AS Format_Channel_Code,
            gv.Instance AS Instance,
            gv.Active AS Activo,
            CAST(gv.Modified_Date AS TIMESTAMP) AS FechadeModificacion
        FROM
            cat_rtmgb_dv.groglortmbm_cz.group_v AS gv
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_header_v AS gh 
            ON (gv.Group_Header_Id = gh.Group_Header_Id
                AND gv.Instance = gh.Instance
                AND gv.Active = gh.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_channel_mapping_v AS gch 
            ON (gch.Group_Id = gv.Group_Id
                AND gch.Instance = gv.Instance
                AND gch.Active = gv.Active)
        LEFT JOIN (
            SELECT
                DISTINCT Lov_Id,
                Lov_Code,
                Instance,
                Lov_Type,
                Active,
                Lov_Parent_Id
            FROM
                cat_rtmgb_dv.groglortmbm_cz.channel_master_v
            WHERE
                Lov_Type = 'FORMAT_CHANNEL_TYPE'
        ) AS et 
            ON (gch.Channel_Id = et.Lov_Id
                AND gch.Instance = et.Instance
                AND gch.Active = et.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_group_mapping_v AS pgm 
            ON (pgm.Group_Id = gv.Group_Id
                AND pgm.Instance = gv.Instance
                AND pgm.Active = gv.Active)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pm 
            ON (pm.Product_Id = pgm.Product_Id
                AND pm.Instance = pgm.Instance
                AND pm.Active = pgm.Active)
        WHERE
            gv.Active = 1
            AND gh.group_header_code = '20190831-MENU_STK_O'
            AND gv.Instance = 'National'
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="c23_discount_mapping_format_channel",
    comment="Ingest discount mapping format channel data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_discount_code", "Discount_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_format_channel_code", "Format_Channel_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_activo", "Activo = true")
def ingest_discount_mapping_format_channel():
    query = """
        SELECT
            DISTINCT
            C22.Discount_Code,
            cmv.Lov_Code AS Format_Channel_Code,
            lv.Location_Code AS Location_Code,
            C22.Instance,
            C22.Activo,
            CAST(C22.FechaModificaion AS TIMESTAMP) AS FechaModificaion
        FROM (
            SELECT
                DISTINCT
                dhv.Discount_Code,
                damv.Channel_Id,
                damv.Channel_Hierarchy_Id,
                damv.District_Id,
                rv.Retailer_Code,
                CASE
                    WHEN damv.Is_Store_Included = 1 THEN 'True'
                    ELSE 'False'
                END AS Store_Mapping,
                damv.Instance AS Instance,
                damv.Active AS Activo,
                damv.Modified_Date AS FechaModificaion
            FROM
                cat_rtmgb_dv.groglortmbm_cz.discount_apply_mapping_v AS damv
            LEFT JOIN (
                SELECT
                    DISTINCT 
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
                    ddv.Active AS Activo,
                    ddv.Modified_Date AS FechadeModificacion
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.discount_details_v AS ddv
                LEFT JOIN (
                    SELECT
                        DISTINCT 
                        dhv.Record_Id,
                        dhv.Discount_Code,
                        dhv.Description,
                        lvv.Lov_Code,
                        dhv.Active,
                        dhv.Instance,
                        dhv.Update_date
                    FROM
                        cat_rtmgb_dv.groglortmbm_cz.discount_header_v AS dhv
                    LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.list_value_v AS lvv 
                        ON (dhv.Active = lvv.Active
                            AND dhv.Instance = lvv.Instance
                            AND dhv.Level_Id = lvv.Lov_Parent_Id)
                    WHERE
                        dhv.Active = 1
                        AND lvv.Active = 1
                        AND lvv.Lov_Code IN ('BILL')
                ) AS disc_head 
                    ON (ddv.Discount_Header_Id = disc_head.Record_Id
                        AND ddv.Active = disc_head.Active
                        AND ddv.Instance = disc_head.Instance)
                LEFT JOIN (
                    SELECT
                        DISTINCT 
                        plv.Level_Name,
                        plv.Level_Code,
                        pmv.Product_Id,
                        pmv.Product_Code,
                        pmv.Active,
                        pmv.Instance,
                        pmv.Update_date
                    FROM
                        cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pmv
                    LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.product_level_v AS plv 
                        ON (pmv.Product_Level_Id = plv.Product_Level_Id
                            AND pmv.Active = plv.Active
                            AND pmv.Instance = plv.Instance)
                    WHERE
                        pmv.Active = 1
                        AND plv.Active = 1
                        AND plv.Product_Level_Id IN (5)
                        AND pmv.Product_Level_Id IN (5)
                ) AS prod_list 
                    ON (ddv.Product_Id = prod_list.Product_Id
                        AND ddv.Active = prod_list.Active
                        AND ddv.Instance = prod_list.Instance)
                WHERE
                    ddv.Active = 1
                    AND prod_list.Active = 1
                    AND disc_head.Active = 1
                    AND YEAR(ddv.Effective_From) NOT IN (2016, 2017, 2018, 2019, 2020, 2021)
            ) AS dhv 
                ON (damv.Discount_Header_Id = dhv.Record_Id
                    AND damv.Instance = dhv.Instance
                    AND damv.Active = dhv.Activo)
            LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.retailer_v AS rv 
                ON (damv.Retailer_Id = rv.Retailer_Id
                    AND damv.Instance = rv.Instance
                    AND damv.Active = rv.Active)
            WHERE
                damv.Active = 1
                AND dhv.Activo = 1
                AND rv.Active = 1
        ) AS C22
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.channel_master_v AS cmv 
            ON (C22.Channel_Id = cmv.Lov_Parent_Id
                AND C22.Activo = cmv.Active
                AND C22.Instance = cmv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.location_v AS lv 
            ON (C22.District_Id = lv.Location_Parent_Id
                AND C22.Activo = lv.Active
                AND C22.Instance = lv.Instance)
        WHERE
            C22.Activo = 1
            AND lv.Active = 1
            AND cmv.Active = 1
    """
    
    return spark.sql(query)

# COMMAND ----------

# import dlt
# from pyspark.sql.functions import col, expr, greatest, to_timestamp

# # Streaming live table for customer hierarchy (sources as streams from LZ with CDF; MERGE-inferred keys/sequence)
# @dlt.table(
#     name="customer_hierarchy",
#     comment="Streaming customer hierarchy from LZ streams (CDF-enabled for RT updates; MERGE-inferred upsert)",
#     table_properties={"quality": "silver", "delta.autoOptimize.optimizeWrite": "true", "delta.autoOptimize.autoCompact": "true", "delta.enableChangeDataFeed": "true"}
# )
# @dlt.expect_or_drop("valid_national_account_code", "National_Account_Code IS NOT NULL")
# @dlt.expect("valid_instance", "Instance IS NOT NULL")
# def ingest_customer_hierarchy():
#     instance_filter = spark.conf.get("pipelines.instance_filter", "National")
    
#     # Read streams from LZ base tables/views (CDF enabled; filter Active=1)
#     nat_stream = dlt.read_stream("cat_rtmgb_dv.groglortmbm_lz.channel_master_v").filter((col("Lov_Type") == "NATIONAL_CHANNEL_TYPE") & (col("Active") == 1) & (col("Instance") == instance_filter))
#     act_stream = dlt.read_stream("cat_rtmgb_dv.groglortmbm_lz.channel_master_v").filter((col("Lov_Type") == "ACCOUNT_CHANNEL_TYPE") & (col("Active") == 1) & (col("Instance") == instance_filter))
#     sct_stream = dlt.read_stream("cat_rtmgb_dv.groglortmbm_lz.list_value_v").filter((col("Lov_Type") == "SUB_CHAIN_TYPE") & (col("Active") == 1) & (col("Instance") == instance_filter))
#     fct_stream = dlt.read_stream("cat_rtmgb_dv.groglortmbm_lz.channel_master_v").filter((col("Lov_Type") == "FORMAT_CHANNEL_TYPE") & (col("Active") == 1) & (col("Instance") == instance_filter))
    
#     # Derive effective_sequence aligned with inferred TimeStamp_Id (cast long to timestamp if needed)
#     nat_stream = nat_stream.withColumn("effective_sequence", to_timestamp(col("TimeStamp_Id").cast("bigint") / 1000))  # Assume TimeStamp_Id as epoch ms
#     act_stream = act_stream.withColumn("effective_sequence", to_timestamp(col("TimeStamp_Id").cast("bigint") / 1000))
#     sct_stream = sct_stream.withColumn("effective_sequence", to_timestamp(col("TimeStamp_Id").cast("bigint") / 1000))
#     fct_stream = fct_stream.withColumn("effective_sequence", to_timestamp(col("TimeStamp_Id").cast("bigint") / 1000))
    
#     # Enrich via stream-stream joins (watermark on effective_sequence; propagate from primary nat_stream)
#     enriched_stream = (nat_stream.alias("nat")
#         .join(act_stream.alias("act"), (col("nat.Lov_Id") == col("act.Lov_Parent_Id")) & (col("nat.Instance") == col("act.Instance")), "left")
#         .join(sct_stream.alias("sct"), (col("act.Lov_Id") == col("sct.Lov_Parent_Id")) & (col("act.Instance") == col("sct.Instance")), "left")
#         .join(fct_stream.alias("fct"), (col("sct.Lov_Id") == col("fct.Lov_Parent_Id")) & (col("sct.Instance") == col("fct.Instance")), "left")
#         .selectExpr(
#             "nat.Lov_Code AS National_Account_Code",
#             "nat.Lov_Name AS National_Account_Name",
#             "act.Lov_Code AS Account_Channel_Code",
#             "act.Lov_Name AS Account_Channel_Name",
#             "sct.Lov_Code AS Sub_Chain_Code",
#             "sct.Lov_Name AS Sub_Chain_Name",
#             "fct.Lov_Code AS Format_Channel_Code",
#             "fct.Lov_Name AS Format_Channel_Name",
#             "nat.Instance AS Instance",
#             "nat.effective_sequence"  # From primary; aligns with TimeStamp_Id
#         )
#         .withWatermark("effective_sequence", "1 hour")  # Tolerate 1h late data (MERGE out-of-order)
#         .orderBy("National_Account_Code", "Account_Channel_Code", "Sub_Chain_Code", "Format_Channel_Code")
#     )
#     return enriched_stream

# # # Target SCD Type 1 table
# # dlt.create_streaming_table(
# #     name="customer_hierarchy_scd",
# #     comment="SCD Type 1 customer hierarchy (MERGE-inferred upsert via auto CDC)"
# # )

# # # Auto CDC flow (inferred from MERGE: keys Lov_Id+Instance, sequence TimeStamp_Id < target, insert not matched; soft-deletes)
# # dlt.create_auto_cdc_flow(
# #     target="customer_hierarchy_scd",
# #     source="customer_hierarchy",
# #     keys=["Instance", "Format_Channel_Code"],  # Inferred composite (leaf Lov_Id + Instance)
# #     sequence_by=col("effective_sequence"),  # Inferred from TimeStamp_Id (cast/derived for ordering)
# #     apply_as_deletes=expr("(_change_type = 'delete') OR (Active = false AND _change_type = 'update')"),  # Soft-delete (no explicit delete clause)
# #     apply_as_truncates=expr("_change_type = 'truncate'"),  # None inferred
# #     except_column_list=["effective_sequence"],  # Exclude derived seq
# #     stored_as_scd_type=1  # Overwrite non-keys on match (aligns with MERGE update condition)
# # )

# COMMAND ----------

@dlt.table(
    name="customer_hierarchy",
    comment="Ingest customer hierarchy data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_national_account_code", "National_Account_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_format_channel_code", "Format_Channel_Code IS NOT NULL")
def ingest_customer_hierarchy():
    query = """
        SELECT
            DISTINCT 
            nat.Lov_Code AS National_Account_Code,
            nat.Lov_Name AS National_Account_Name,
            act.Lov_Code AS Account_Channel_Code,
            act.Lov_Name AS Account_Channel_Name,
            sct.Lov_Code AS Sub_Chain_Code,
            sct.Lov_Name AS Sub_Chain_Name,
            fct.Lov_Code AS Format_Channel_Code,
            fct.Lov_Name AS Format_Channel_Name,
            nat.Instance AS Instance
        FROM
            (
                SELECT
                    Lov_Id, 
                    Lov_Code,
                    Lov_Name,
                    Lov_Parent_Id,
                    Instance
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.channel_master_v
                WHERE
                    Lov_Type = 'NATIONAL_CHANNEL_TYPE'
                    AND Active = 1
            ) AS nat
        LEFT JOIN (
            SELECT
                Lov_Id,
                Lov_Code,
                Lov_Name,
                Lov_Parent_Id,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.channel_master_v
            WHERE
                Lov_Type = 'ACCOUNT_CHANNEL_TYPE'
                AND Active = 1
        ) AS act 
            ON (nat.Lov_Id = act.Lov_Parent_Id
                AND nat.Instance = act.Instance)
        LEFT JOIN (
            SELECT
                Lov_Id,
                Lov_Code,
                Lov_Name,
                Lov_Parent_Id,
                Instance,
                Lov_Type
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'SUB_CHAIN_TYPE'
                AND Active = 1
        ) AS sct 
            ON (act.Lov_Id = sct.Lov_Parent_Id
                AND act.Instance = sct.Instance)
        LEFT JOIN (
            SELECT
                Lov_Id,
                Lov_Code,
                Lov_Name,
                Lov_Parent_Id,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.channel_master_v
            WHERE
                Lov_Type = 'FORMAT_CHANNEL_TYPE'
                AND Active = 1
        ) AS fct 
            ON (sct.Lov_Id = fct.Lov_Parent_Id
                AND sct.Instance = fct.Instance)
        ORDER BY
            National_Account_Code, 
            Account_Channel_Code, 
            Sub_Chain_Code, 
            Format_Channel_Code
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="c20_product_sequence",
    comment="Ingest product sequence data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_product_sequence", "Product_Sequence IS NOT NULL")
# @dlt.expect_or_drop("valid_activo", "Activo = true")
def ingest_product_sequence():
    query = """
        SELECT
            pm.Product_Code AS Product_Code,
            pm.SEQUENCE AS Product_Sequence,
            pm.Instance AS Instance,
            pm.Active AS Activo,
            CAST(pm.Modified_Date AS TIMESTAMP) AS FechadeModificacion
        FROM
            cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pm
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.product_level_v AS pl 
            ON (pm.Product_Level_Id = pl.Product_Level_Id
                AND pm.Active = pl.Active
                AND pm.Instance = pl.Instance)
        WHERE 
            pm.Active = 1
            AND pl.Level_Code = 'SKU'
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m09_product_distribution",
    comment="Ingest product distribution data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sc_code", "SC_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_product_code", "Product_Code IS NOT NULL")
# @dlt.expect_or_drop("valid_activo", "Activo = true")
def ingest_product_distribution():
    query = """
        SELECT
            scv.sales_center_code AS SC_Code,
            pmv.product_code AS Product_Code,
            pmv.Instance AS INSTANCE,
            pmv.active AS Activo,
            CAST(pmv.modified_date AS TIMESTAMP) AS Fecha_de_Modificacion
        FROM
            cat_rtmgb_dv.groglortmbm_cz.product_master_v AS pmv
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_product_mapping_v AS scpmv 
            ON (scpmv.product_id = pmv.product_id
                AND scpmv.Instance = pmv.Instance)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS scv 
            ON (scv.Sales_Center_Id = scpmv.Distributor_Id
                AND scv.Instance = scpmv.Instance)
        WHERE
            scv.active = 1
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="m14_price_mapping",
    comment="Ingest price mapping data from raw tables",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_primaryid", "primaryid IS NOT NULL")
# @dlt.expect_or_drop("valid_format_channel_code", "format_channel_code IS NOT NULL")
# @dlt.expect_or_drop("valid_active", "Active = true")
def ingest_price_mapping():
    query = """
        SELECT
            pam.Price_Apply_Mapping_Id AS primaryid,
            gv.Group_Code AS price_group,
            gv.Group_Name AS price_group_name,
            COALESCE(lv.Location_Code, 'ALL') AS location_code,
            fct.Lov_Code AS format_channel_code,
            fct.Lov_Name AS format_channel_code_name,
            0 AS row_id,
            pam.Instance AS Instance,
            pam.Active,
            CAST(pam.Created_Date AS TIMESTAMP) AS Created_Date,
            CAST(pam.Modified_Date AS TIMESTAMP) AS Modified_Date
        FROM
            cat_rtmgb_dv.groglortmbm_cz.price_apply_mapping_v AS pam
        INNER JOIN (
            SELECT
                *
            FROM
                cat_rtmgb_dv.groglortmbm_cz.group_v
            WHERE
                Group_Type_Id = 820
        ) AS gv 
            ON (pam.Price_Group_Id = gv.Group_Id
                AND pam.Instance = gv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.location_v AS lv 
            ON (pam.Apply_Location_Id = lv.Location_Id
                AND pam.Instance = lv.Instance)
        INNER JOIN (
            SELECT
                Lov_Id,
                Lov_Code,
                Lov_Name,
                Lov_Parent_Id,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.channel_master_v
            WHERE
                Lov_Type = 'FORMAT_CHANNEL_TYPE'
                AND Active = 1
        ) AS fct 
            ON (pam.Apply_Channel_Id = fct.Lov_Id
                AND pam.Instance = fct.Instance)
        WHERE
            pam.Active = 1
        ORDER BY
            pam.Price_Apply_Mapping_Id
    """
    
    return spark.sql(query)

# COMMAND ----------

@dlt.table(
    name="Reporte_de_Accesos_de_Usuarios_a_RTM",
    comment="Ingest user access report from raw tables",
    table_properties={"quality": "silver"}
)
def ingest_price_mapping():
    query = """
            SELECT
                DISTINCT 
                us.First_Name AS User_Name,
                us.User_Code AS User_Code,
                rm.Role_Code AS Role,
                us.Created_Date AS Created_date,
                us.Modified_Date AS Deactivated_date, 
                us.Instance AS Instance,
                us.Active AS Active
            FROM
                cat_rtmgb_dv.groglortmbm_cz.User_V AS us
            INNER JOIN cat_rtmgb_dv.groglortmbm_cz.Role_Master_V AS rm ON
                (us.Role_Id = rm.Role_Id
                    AND us.Instance = rm.Instance)
    """
    
    return spark.sql(query)