/*
    ZNS-3006 : Walmart 재고, 판매 데이터 월별 집계
 */

--- [ 재고 집계] ------------------------------------------------------
CREATE OR REPLACE TABLE wook.wmt_com_store_inv_mly AS
WITH ecom_month_end AS (
    SELECT *
    FROM dw.wmt_scintilla_ecom_inv
    QUALIFY inv_date = MAX(inv_date) OVER (PARTITION BY yr_month, sku)
)
, store_month_end AS (
  SELECT *
  FROM dw.wmt_scintilla_store_inv
  QUALIFY inv_date = MAX(inv_date) OVER (PARTITION BY yr_month, sku)
)
SELECT 'WMT.COM' AS channel
    , yr_month
    , sku
    , STRING_AGG(DISTINCT CAST(ecomm_upc_number AS STRING), ', ') AS upc_list
    , ANY_VALUE(product_name) as title
 --   , ANY_VALUE(catalog_category) as category
    , SUM(on_hand_unit) AS on_hand_unit
FROM ecom_month_end
GROUP BY 1, 2, 3
UNION ALL
SELECT 'WMT STORE' AS channel
    , yr_month
    , sku
    , STRING_AGG(DISTINCT CAST(walmart_upc_number AS STRING), ', ') AS upc_list
    , ANY_VALUE(product_name) as title
 --   , ANY_VALUE(catalog_category) as category
    , SUM(on_hand_unit) AS on_hand_unit
FROM store_month_end
GROUP BY 1, 2, 3
;


--- [ 판매량 집계] ------------------------------------------------------
CREATE OR REPLACE TABLE wook.wmt_com_store_sales_mly AS
SELECT 'WMT.COM' as channel
    , yr_month
    , sku
    , STRING_AGG(DISTINCT CAST(ecomm_upc_number AS STRING), ', ') AS upc_list
    , ANY_VALUE(product_name) as title
 --   , ANY_VALUE(brand_name) as brand
 --   , ANY_VALUE(catalog_category) as category
    , SUM(shipped_units) AS shipped_units
    , ROUND(SUM(shipped_revenue),1) AS shipped_revenue
FROM
    dw.wmt_scintilla_ecom_sales
GROUP BY
    1, 2, 3
UNION ALL
SELECT 'WMT STORE' as channel
    , yr_month
    , sku
   , STRING_AGG(DISTINCT CAST(walmart_upc_number AS STRING), ', ') AS upc_list
    , ANY_VALUE(product_name) as title
 --  , ANY_VALUE(brand_name) as brand
 --    , ANY_VALUE(catalog_category) as category
    , SUM(shipped_units) AS shipped_units
    , ROUND(SUM(shipped_revenue),1) AS shipped_revenue
FROM
    dw.wmt_scintilla_store_sales
GROUP BY
    1, 2, 3
;


--- test -----

SELECT
    yr_month
  --  , sku
    , 'Walmart.COM' as channel
    , SUM(shipped_revenue) AS shipped_revenue
    , SUM(shipped_units) AS shipped_units
FROM
    dw.wmt_scintilla_ecom_sales
GROUP BY
    1, 2
UNION ALL
SELECT
    yr_month
 --   , sku
    , 'WALMART STORES' as channel
    , SUM(shipped_revenue) AS shipped_revenue
    , SUM(shipped_units) AS shipped_units
FROM
    dw.wmt_scintilla_store_sales
GROUP BY
    1, 2
order by yr_month DESC, channel
;



-----------------------

SELECT *
from wook.wmt_com_store_sales_mly

SELECT sku, count(DISTINCT walmart_upc_number), sum(on_hand_unit)
FROM dw.wmt_scintilla_store_inv
where yr_month >= 202606
group by 1 having count(DISTINCT walmart_upc_number) > 1
order by 3 DESC

SELECT * FROM dw.wmt_scintilla_store_inv
WHERE sku='7783710' and yr_month >= 202606
order BY inv_date DESC ;


SELECT DISTINCT sku, ecomm_upc_number, catalog_walmart_item_number, catalog_category, catalog_item_id FROM dw.wmt_scintilla_ecom_inv;

SELECT DISTINCT sku, ecomm_upc_number FROM dw.wmt_scintilla_ecom_inv;

SELECT DISTINCT sku, ecomm_upc_number FROM dw.wmt_scintilla_ecom_inv;

select channel
    , count(*)
    , count(DISTINCT sku)
FROM wook.wmt_com_store_inv_mly
GROUP BY 1
;

