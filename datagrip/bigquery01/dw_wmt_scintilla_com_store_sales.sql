CREATE OR REPLACE TABLE dw.wmt_scintilla_ecom_sales AS
WITH cte_src as (
    SELECT
--         c.yr
--         , c.yr_month
        CAST(SUBSTR(CAST(business_date AS STRING), 1, 4) AS INT64) AS yr
        , SUBSTR(CAST(business_date AS STRING), 1, 7) AS yr_month
        , c.yr_wk
        , DATE (business_date) as sales_date

--         , cl.yr as last_yr
--         , cl.yr_month as last_yr_month
        , EXTRACT(YEAR FROM DATE_SUB(DATE(o.business_date), INTERVAL 1 YEAR)) AS last_yr
        , SUBSTR(FORMAT_DATE('%Y-%m-%d', DATE_SUB(DATE(o.business_date), INTERVAL 1 YEAR)), 1, 7) AS last_yr_month
        , cl.yr_wk as last_yr_wk
        , DATE_SUB(DATE(o.business_date), INTERVAL 1 YEAR) AS last_sales_date

        --     , mdm.sku as zinus_sku
        --     , mdm.zprod_customer_sku as cust_sku
        , o.* EXCEPT (business_date, product_name)
        , catalog.sku
        , catalog.product_name
        , catalog.product_category AS catalog_category
        , catalog.walmart_item_number AS catalog_walmart_item_number
    FROM
        ods.wmt_ecom_sales o

            --         LEFT JOIN tmp1.mdm_sku_mapping mdm
            --             ON o.ecomm_upc_number = mdm.ean11_new
            LEFT JOIN meta.wmt_retail_link_catalog_mdm catalog
                ON o.ecomm_upc_number = catalog.ean11

--             LEFT JOIN meta.wk_calendar_new c
            LEFT JOIN meta.wk_calendar c
                ON DATE(o.business_date) BETWEEN c.start_date AND c.end_date
--             LEFT JOIN meta.wk_calendar_new cl
            LEFT JOIN meta.wk_calendar cl
                ON date_sub(DATE(o.business_date), INTERVAL 1 YEAR) BETWEEN cl.start_date AND cl.end_date
    QUALIFY
        RANK() OVER (PARTITION BY file_date ORDER BY o.load_datetime DESC) = 1
)
SELECT
    yr
    , yr_month
    , yr_wk
    , sales_date
    , *
    EXCEPT (
        yr, yr_month, yr_wk, sales_date, last_yr, last_yr_month, last_yr_wk, last_sales_date
        , shipped_based_net_sales_amount_last_year
        , shipped_based_net_sales_amount_this_year
        , shipped_based_quantity_last_year
        , shipped_based_quantity_this_year
        , auth_based_item_quantity_last_year
        , auth_based_item_quantity_this_year
        , auth_based_net_sales_amount_last_year
        , auth_based_net_sales_amount_this_year
        )
    , shipped_based_net_sales_amount_this_year as shipped_revenue
    , shipped_based_quantity_this_year as shipped_units
FROM
    cte_src

UNION ALL

SELECT
    last_yr as yr
    , last_yr_month as yr_month
    , last_yr_wk as yr_wk
    , last_sales_date as sales_date
    , *
    EXCEPT (
        yr, yr_month, yr_wk, sales_date, last_yr, last_yr_month, last_yr_wk, last_sales_date
        , shipped_based_net_sales_amount_last_year
        , shipped_based_net_sales_amount_this_year
        , shipped_based_quantity_last_year
        , shipped_based_quantity_this_year
        , auth_based_item_quantity_last_year
        , auth_based_item_quantity_this_year
        , auth_based_net_sales_amount_last_year
        , auth_based_net_sales_amount_this_year
        )
    , shipped_based_net_sales_amount_last_year as shipped_revenue
    , shipped_based_quantity_last_year as shipped_units
FROM
    cte_src
;

SELECT
    DISTINCT
    CAST(SUBSTR(CAST(business_date AS STRING), 1, 4) AS INT64) AS yr
    , SUBSTR(CAST(business_date AS STRING), 1, 7) AS yr_month
    , EXTRACT(YEAR FROM DATE_SUB(DATE(o.business_date), INTERVAL 1 YEAR)) AS last_yr
    , SUBSTR(FORMAT_DATE('%Y-%m-%d', DATE_SUB(DATE(o.business_date), INTERVAL 1 YEAR)), 1, 7) AS last_yr_month
FROM
    ods.wmt_store o;

CREATE OR REPLACE TABLE dw.wmt_scintilla_store_sales AS
WITH cte_src as (
    SELECT
--         c.yr
--         , c.yr_month
        CAST(SUBSTR(CAST(business_date AS STRING), 1, 4) AS INT64) AS yr
        , SUBSTR(CAST(business_date AS STRING), 1, 7) AS yr_month
        , c.yr_wk
        , DATE (o.business_date) as sales_date

--         , cl.yr as last_yr
--         , cl.yr_month as last_yr_month
        , EXTRACT(YEAR FROM DATE_SUB(DATE(o.business_date), INTERVAL 1 YEAR)) AS last_yr
        , SUBSTR(FORMAT_DATE('%Y-%m-%d', DATE_SUB(DATE(o.business_date), INTERVAL 1 YEAR)), 1, 7) AS last_yr_month
        , cl.yr_wk as last_yr_wk
        , date_sub(DATE(o.business_date), INTERVAL 1 YEAR) as last_sales_date

        , o.* EXCEPT (business_date, walmart_item_number)
        --     , mdm.sku as zinus_sku
        --     , mdm.zprod_customer_sku as cust_sku
        , catalog.sku
        , catalog.product_name
        , catalog.product_category as catalog_category
        --         , catalog.walmart_item_number as catalog_walmart_item_number

        , o.walmart_item_number

    FROM
        ods.wmt_store o
            --         LEFT JOIN tmp1.mdm_sku_mapping mdm
            --             ON o.walmart_upc_number = mdm.ean11_new
            LEFT JOIN meta.wmt_retail_link_catalog_mdm catalog
                ON o.walmart_upc_number = catalog.ean11

--             LEFT JOIN meta.wk_calendar_new c
            LEFT JOIN meta.wk_calendar c
                ON DATE (o.business_date) BETWEEN c.start_date AND c.end_date
--             LEFT JOIN meta.wk_calendar_new cl
            LEFT JOIN meta.wk_calendar cl
                ON DATE_SUB(DATE(o.business_date), INTERVAL 1 YEAR) BETWEEN cl.start_date AND cl.end_date

    QUALIFY RANK() OVER (PARTITION BY file_date ORDER BY o.load_datetime desc) = 1

)
SELECT
    yr
    , yr_month
    , yr_wk
    , sales_date
    , *
    EXCEPT (yr, yr_month, yr_wk, sales_date, last_yr, last_yr_month, last_yr_wk, last_sales_date
        , store_on_hand_quantity_last_year
        , store_on_hand_quantity_this_year
        , store_on_hand_retail_last_year
        , store_on_hand_retail_this_year
        , store_on_order_quantity_last_year
        , store_on_order_quantity_this_year
        , store_on_order_retail_last_year
        , store_on_order_retail_this_year
        , pos_quantity_last_year
        , pos_quantity_this_year
        , pos_sales_last_year
        , pos_sales_this_year
        , home_office_recommended_retail_price_last_year
        , home_office_recommended_retail_price_this_year

        )
    , pos_sales_this_year as shipped_revenue
    , pos_quantity_this_year as shipped_units
FROM
    cte_src

union all

SELECT
    last_yr as yr
    , last_yr_month as yr_month
    , last_yr_wk as yr_wk
    , last_sales_date as sales_date
    , *
    EXCEPT (yr, yr_month, yr_wk, sales_date, last_yr, last_yr_month, last_yr_wk, last_sales_date
        , store_on_hand_quantity_last_year
        , store_on_hand_quantity_this_year
        , store_on_hand_retail_last_year
        , store_on_hand_retail_this_year
        , store_on_order_quantity_last_year
        , store_on_order_quantity_this_year
        , store_on_order_retail_last_year
        , store_on_order_retail_this_year
        , pos_quantity_last_year
        , pos_quantity_this_year
        , pos_sales_last_year
        , pos_sales_this_year
        , home_office_recommended_retail_price_last_year
        , home_office_recommended_retail_price_this_year
        )
    , pos_sales_last_year as shipped_revenue
    , pos_quantity_last_year as shipped_units
FROM
    cte_src
;