# Databricks notebook source
from pyspark import pipelines as dp

# COMMAND ----------

@dp.table(
    name="m10_retailer_master"
)
@dp.expect("valid_retailer_code", "Retailer_Code IS NOT NULL")
@dp.expect("valid_retailer_name", "Retailer_Name IS NOT NULL")
@dp.expect("has_delivery_address", "del_street_name IS NOT NULL OR del_city IS NOT NULL")
# @dp.expect_or_drop("active_retailers_only", "is_active = 1")
def ingest_retailer_master():
    query = """
        WITH delivery_address AS (
            SELECT
                Customer_Id,
                Address_1 AS del_street_name,
                Numero_Exterior AS del_ext_number,
                Numero_Interior AS del_int_number,
                Address_2 AS del_neighborhood,
                City AS del_city,
                Address_3 AS del_district,
                State AS del_state,
                `País` AS del_country,
                Pin_Code AS del_zip_code,
                Telephone AS del_phone_no,
                Email AS del_email_id,
                Entre_Calle_1 AS del_bt_street1,
                Entre_Calle_2 AS del_bt_street2,
                Landmark AS del_reference,
                Geo_Latitude AS lat_position,
                Geo_Longitude AS long_position,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.address_detail_v
            WHERE
                Active = 1
                AND Address_Type_Id = 742
                AND Customer_Type_Id = 798
        ),
        invoice_address AS (
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
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.address_detail_v
            WHERE
                Active = 1
                AND Address_Type_Id = 741
                AND Customer_Type_Id = 798
        ),
        channel_hierarchy AS (
            SELECT DISTINCT
                nat.Lov_Id AS fn,
                fct.Lov_Id AS ff,
                nat.Lov_Code AS National_Account_Code,
                nat.Lov_Name AS National_Account_Name,
                act.Lov_Code AS Account_Channel_Code,
                act.Lov_Name AS Account_Channel_Name,
                sct.Lov_Code AS Sub_Chain_Code,
                sct.Lov_Name AS Sub_Chain_Name,
                fct.Lov_Code AS Format_Channel_Code,
                fct.Lov_Name AS Format_Channel_Name,
                nat.Instance
            FROM
                (SELECT
                    Lov_Id,
                    Lov_Code,
                    Lov_Name,
                    Lov_Parent_Id,
                    Instance
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.channel_master_v
                WHERE
                    Lov_Type = 'NATIONAL_CHANNEL_TYPE'
                    AND Active = 1) AS nat
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
                    AND Active = 1) AS act
                ON (nat.Lov_Id = act.Lov_Parent_Id
                    AND nat.Instance = act.Instance)
            LEFT JOIN (
                SELECT
                    Lov_Id,
                    Lov_Code,
                    Lov_Name,
                    Lov_Parent_Id,
                    Instance
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.list_value_v
                WHERE
                    Lov_Type = 'SUB_CHAIN_TYPE'
                    AND Active = 1) AS sct
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
                    AND Active = 1) AS fct
                ON (sct.Lov_Id = fct.Lov_Parent_Id
                    AND sct.Instance = fct.Instance)
        ),
        primary_contact AS (
            SELECT DISTINCT
                Contact_Person_Id,
                First_Name,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.contact_person_detail_v
            WHERE
                Is_Primary_Contact = 1
                AND Active = 1
        ),
        secondary_contact AS (
            SELECT DISTINCT
                Customer_Id,
                First_Name,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.contact_person_detail_v
            WHERE
                Is_Primary_Contact = 0
                AND Sales_Center_Id = 0
                AND Active = 1
        ),
        tax_regime AS (
            SELECT
                Lov_Id,
                Lov_Code,
                Lov_Name,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'TAX_SYSTEM_TYPE'
                AND Active = 1
        )
        SELECT DISTINCT
            ret.Retailer_Code,
            ret.Retailer_Name,
            fc.National_Account_Code,
            fc.National_Account_Name,
            fc.Account_Channel_Code,
            fc.Account_Channel_Name,
            fc.Sub_Chain_Code,
            fc.Sub_Chain_Name,
            fc.Format_Channel_Code,
            fc.Format_Channel_Name,
            lo.Location_Code AS Retailer_Location_Code,
            ret.Reference_No AS Cust_Store_Code,
            del.del_ext_number,
            del.lat_position,
            del.long_position,
            `ret`.`Consolidación_Factura` AS consolidates,
            del.del_int_number,
            del.del_neighborhood,
            del.del_street_name,
            del.del_city,
            del.del_district,
            del.del_state,
            del.del_country,
            del.del_zip_code,
            del.del_phone_no,
            del.del_email_id,
            del.del_bt_street1,
            del.del_bt_street2,
            del.del_reference,
            cp.First_Name AS Contact_Person1,
            cp2.First_Name AS Contact_Person2,
            `ret`.`Factura_Electrónica` AS electronic_invoice,
            ret.Tax_Number AS taxpayer_id,
            `ret`.`Razón_Social` AS taxpayer_name,
            ret.Folio_Obligatorio AS requires_folio,
            inv.inv_street_name,
            inv.inv_ext_number,
            inv.inv_int_number,
            inv.inv_neighborhood,
            inv.inv_city,
            inv.inv_district,
            inv.inv_state,
            inv.inv_country,
            inv.inv_zip_code,
            inv.inv_phone_no,
            inv.inv_email_id,
            inv.inv_reference,
            ret.Uso_CFDI AS uso_cfdi,
            ret.Onsite_Invoice_Active AS billing_onsite,
            inv.inv_bt_street1,
            inv.inv_bt_street2,
            ret.EAN_Receptor AS receiver_ean,
            CAST(0.0000 AS DECIMAL(10,4)) AS InvoiceLimit,
            ret.Giro_de_Cliente AS Retailer_Channel,
            ret.Credit_Uso_CFDI AS Credit_Note_Use_CFDI,
            ret.Numero_Proveedor AS EAN_Proveedor,
            ret.EAN_Tienda AS EAN_Tienda,
            ret.EAN_Lugar_Expide AS EAN_Lugar_Expide,
            ret.INE_Enabled,
            ret.No_mostrar_IEPS AS Detail_IEPS,
            rf.Lov_Code AS RegimenFiscalCode,
            rf.Lov_Name AS RegimenFiscal,
            ret.Active AS is_active,
            ret.Instance AS instance,
            ret.Created_Date AS createddate,
            ret.Modified_Date AS modifieddate
        FROM
            cat_rtmgb_dv.groglortmbm_cz.retailer_v AS ret
        LEFT JOIN delivery_address AS del
            ON (ret.Retailer_Id = del.Customer_Id
                AND ret.Instance = del.Instance)
        LEFT JOIN invoice_address AS inv
            ON (ret.Retailer_Id = inv.Customer_Id
                AND ret.Instance = inv.Instance)
        LEFT JOIN channel_hierarchy AS fc
            ON (ret.Format_Channel_Id = fc.ff
                AND ret.Instance = fc.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.location_v AS lo
            ON (ret.Zip_Code = lo.Location_Id
                AND ret.Instance = lo.Instance)
        LEFT JOIN primary_contact AS cp
            ON (ret.Contact_Person_Id = cp.Contact_Person_Id
                AND ret.Instance = cp.Instance)
        LEFT JOIN secondary_contact AS cp2
            ON (ret.Retailer_Id = cp2.Customer_Id
                AND ret.Instance = cp2.Instance)
        LEFT JOIN tax_regime AS rf
            ON (ret.Regimen_Fiscal = rf.Lov_Id
                AND ret.Instance = rf.Instance)
        ORDER BY ret.Retailer_Code
    """

    m10 = spark.sql(query)

    return m10

# COMMAND ----------

@dp.table(
    name="m11_retailer_attribute",
    comment="Retailer attributes including distributor mapping, pricing groups, legal entities, and Mexican tax/invoice information",
    table_properties={"quality": "silver"}
)
@dp.expect("valid_retailer_code", "Retailer_Code IS NOT NULL")
@dp.expect("valid_sales_center", "SC_Code IS NOT NULL")
@dp.expect("active_records_only", "is_active = 1 AND rdm_active = 1 AND sc_active = 1")
# @dp.expect_or_drop("has_credit_period", "Credit_Period >= 0")
def ingest_retailer_attribute():
    query = """
        WITH invoice_address AS (
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
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.address_detail_v
            WHERE
                Active = 1
                AND Address_Type_Id = 741
                AND Customer_Type_Id = 798
        ),
        retailer_type_lov AS (
            SELECT DISTINCT
                Lov_Id,
                Lov_Code,
                Lov_Name,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'RETAILER_TYPE'
        ),
        invoicing_provider_lov AS (
            SELECT DISTINCT
                Lov_Id,
                Lov_Code,
                Lov_Name,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'ONSITE_VENDOR'
        ),
        channel_hierarchy AS (
            SELECT DISTINCT
                nat.Lov_Id AS fn,
                fct.Lov_Id AS ff,
                nat.Lov_Code AS National_Account_Code,
                nat.Lov_Name AS National_Account_Name,
                act.Lov_Code AS Account_Channel_Code,
                act.Lov_Name AS Account_Channel_Name,
                sct.Lov_Code AS Sub_Chain_Code,
                sct.Lov_Name AS Sub_Chain_Name,
                fct.Lov_Code AS Format_Channel_Code,
                fct.Lov_Name AS Format_Channel_Name,
                nat.Instance
            FROM
                (SELECT
                    Lov_Id,
                    Lov_Code,
                    Lov_Name,
                    Lov_Parent_Id,
                    Instance
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.channel_master_v
                WHERE
                    Lov_Type = 'NATIONAL_CHANNEL_TYPE'
                    AND Active = 1) AS nat
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
                    AND Active = 1) AS act
                ON (nat.Lov_Id = act.Lov_Parent_Id
                    AND nat.Instance = act.Instance)
            LEFT JOIN (
                SELECT
                    Lov_Id,
                    Lov_Code,
                    Lov_Name,
                    Lov_Parent_Id,
                    Instance
                FROM
                    cat_rtmgb_dv.groglortmbm_cz.list_value_v
                WHERE
                    Lov_Type = 'SUB_CHAIN_TYPE'
                    AND Active = 1) AS sct
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
                    AND Active = 1) AS fct
                ON (sct.Lov_Id = fct.Lov_Parent_Id
                    AND sct.Instance = fct.Instance)
        ),
        segment_type_lov AS (
            SELECT DISTINCT
                Lov_Id,
                Lov_Code,
                Lov_Name,
                Instance
            FROM
                cat_rtmgb_dv.groglortmbm_cz.list_value_v
            WHERE
                Lov_Type = 'RETAILER_SEGMENT_TYPE'
        )
        SELECT DISTINCT
            ret.Retailer_Id,
            sc.Sales_Center_Code AS SC_Code,
            ret.Retailer_Code,
            COALESCE(rdm.Credit_Period, 0) AS Credit_Period,
            rt.Lov_Code AS Retailer_Type,
            rdm.Blue_Label_Active AS Blue_Label,
            `rdm`.`Pesito_Comisión_Active` AS Pesito_Commission,
            ovt.Lov_Code AS Invoicing_Provider,
            rdm.Numero_Proveedor,
            rst.Lov_Code AS Segment_Type,
            ret.Retailer_Name,
            fc.National_Account_Code,
            fc.National_Account_Name,
            fc.Format_Channel_Code,
            fc.Format_Channel_Name,
            lo.Location_Code AS Retailer_Location_Code,
            `ret`.`Consolidación_Factura` AS consolidates,
            `ret`.`Factura_Electrónica` AS electronic_invoice,
            ret.Tax_Number AS taxpayer_id,
            `ret`.`Razón_Social` AS taxpayer_name,
            ret.Folio_Obligatorio AS requires_folio,
            ret.Address_Id,
            inv.inv_street_name,
            inv.inv_ext_number,
            inv.inv_int_number,
            inv.inv_neighborhood,
            inv.inv_city,
            inv.inv_district,
            inv.inv_state,
            inv.inv_country,
            inv.inv_zip_code,
            ret.Uso_CFDI AS uso_cfdi,
            ret.Onsite_Invoice_Active AS billing_onsite,
            ret.EAN_Receptor AS receiver_ean,
            ret.Credit_Uso_CFDI AS Credit_Note_Use_CFDI,
            ret.Numero_Proveedor AS EAN_Proveedor,
            ret.EAN_Tienda,
            ret.EAN_Lugar_Expide,
            gv.Group_Code AS Price_group,
            gv.Group_Name AS Description_Price_group,
            CAST(1 AS INT) AS LINEA_DE_FACTURA,
            le.Legal_Entity_Code AS `ORGANIZACIÓN`,
            ret.Active AS is_active,
            ret.Created_Date AS createddate,
            ret.Instance,
            ret.Modified_Date AS modifieddate,
            rdm.Active AS rdm_active,
            sc.Active AS sc_active
        FROM
            cat_rtmgb_dv.groglortmbm_cz.retailer_distributor_mapping_v AS rdm
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS sc
            ON (rdm.Sales_Center_Id = sc.Sales_Center_Id
                AND rdm.Instance = sc.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.retailer_v AS ret
            ON (rdm.Retailer_Id = ret.Retailer_Id
                AND rdm.Instance = ret.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.route_retailer_mapping_v AS rrm
            ON (rdm.Retailer_Id = rrm.Retailer_Id
                AND rdm.Sales_Center_Id = rrm.Sales_Center_Id
                AND rdm.Instance = ret.Instance)
        LEFT JOIN invoice_address AS inv
            ON (ret.Retailer_Id = inv.Customer_Id
                AND ret.Instance = inv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.price_apply_mapping_v AS pa
            ON (ret.Zip_Code = pa.Price_Apply_Mapping_Id
                AND ret.Instance = pa.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.group_v AS gv
            ON (pa.Price_Group_Id = gv.Group_Id
                AND pa.Instance = gv.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.legal_entity_v AS le
            ON (sc.Legal_Enitity_Id = le.Legal_Entity_Id
                AND sc.Instance = le.Instance)
        LEFT JOIN retailer_type_lov AS rt
            ON (rdm.Retailer_Type_Id = rt.Lov_Id
                AND rdm.Instance = rt.Instance)
        LEFT JOIN invoicing_provider_lov AS ovt
            ON (rdm.Invoicing_Provider_Id = ovt.Lov_Id
                AND rdm.Instance = ovt.Instance)
        LEFT JOIN channel_hierarchy AS fc
            ON (ret.Format_Channel_Id = fc.ff
                AND ret.Instance = fc.Instance)
        LEFT JOIN segment_type_lov AS rst
            ON (rdm.Segment_Type_Id = rst.Lov_Id
                AND rdm.Instance = rst.Instance)
        LEFT JOIN cat_rtmgb_dv.groglortmbm_cz.location_v AS lo
            ON (ret.Zip_Code = lo.Location_Id
                AND ret.Instance = lo.Instance)
        WHERE
            rdm.Active = 1
            AND ret.Active = 1
            AND sc.Active = 1
        ORDER BY
            SC_Code,
            ret.Retailer_Code
    """
    
    return spark.sql(query)

# COMMAND ----------

@dp.table(
    name="m12_route_plan_source",
    comment="Route visit plan schedule mapping retailers to users with day-of-week and week-of-month visit patterns",
    table_properties={"quality": "silver"}
)
@dp.expect("valid_primary_id", "primary_id IS NOT NULL")
@dp.expect("valid_retailer_code", "Retailer_Code IS NOT NULL")
@dp.expect("valid_sales_center", "sc_code IS NOT NULL")
@dp.expect("active_records_only", "Activo = 1")
# @dp.expect_or_drop("has_visit_schedule", 
#     "Monday = 1 OR Tuesday = 1 OR Wednesday = 1 OR Thursday = 1 OR Friday = 1 OR Saturday = 1 OR Sunday = 1")
def ingest_route_plan():
    query = """
        SELECT
            rum.Retailer_User_Mapping_Id AS primary_id,
            sc.Sales_Center_Code AS sc_code,
            rv.Route_Code AS route_code,
            ret.Retailer_Code,
            ret.Retailer_Name,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'MON') > 0 THEN 1
                ELSE 0
            END AS Monday,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'TUE') > 0 THEN 1
                ELSE 0
            END AS Tuesday,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'WED') > 0 THEN 1
                ELSE 0
            END AS Wednesday,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'THU') > 0 THEN 1
                ELSE 0
            END AS Thursday,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'FRI') > 0 THEN 1
                ELSE 0
            END AS Friday,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'SAT') > 0 THEN 1
                ELSE 0
            END AS Saturday,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'SUN') > 0 THEN 1
                ELSE 0
            END AS Sunday,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'wk1') > 0 THEN 1
                ELSE 0
            END AS WK1,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'wk2') > 0 THEN 1
                ELSE 0
            END AS WK2,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'wk3') > 0 THEN 1
                ELSE 0
            END AS WK3,
            CASE
                WHEN INSTR(rum.Visit_Plan, 'wk4') > 0 THEN 1
                ELSE 0
            END AS WK4,
            rum.Sequence,
            rum.Instance,
            rum.Active AS Activo,
            rum.Modified_Date AS FechadeModificacion
        FROM
            cat_rtmgb_dv.groglortmbm_cz.retailer_user_mapping_v AS rum
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.position_v AS pv
            ON (rum.User_Position_Id = pv.Position_Id
                AND rum.Instance = pv.Instance)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.position_user_mapping_v AS pum
            ON (pv.Position_Id = pum.Position_Id
                AND pv.Instance = pum.Instance)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.user_v AS uv
            ON (pum.User_Id = uv.User_Id
                AND pum.Instance = uv.Instance)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.user_route_mapping_v AS urm
            ON (uv.User_Id = urm.Seller_Id
                AND uv.Instance = urm.Instance)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.route_v AS rv
            ON (urm.Sales_Center_Id = rv.Sales_Center_Id
                AND urm.Route_Id = rv.Route_Id
                AND urm.Instance = rv.Instance)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.sales_center_v AS sc
            ON (rum.Sales_Center_Id = sc.Sales_Center_Id
                AND rum.Instance = sc.Instance)
        INNER JOIN cat_rtmgb_dv.groglortmbm_cz.retailer_v AS ret
            ON (rum.Retailer_Id = ret.Retailer_Id
                AND rum.Instance = ret.Instance)
        WHERE
            rum.Active = 1
            AND pv.Active = 1
            AND pum.Active = 1
            AND uv.Active = 1
            AND urm.Active = 1
            AND rv.Active = 1
            AND sc.Active = 1
            AND ret.Active = 1
        ORDER BY
            rum.Retailer_User_Mapping_Id
    """

    return spark.sql(query)