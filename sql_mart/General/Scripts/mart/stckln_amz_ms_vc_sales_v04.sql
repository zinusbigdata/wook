-------------------------------- ##### MARKET SHARE REPORT #### -------------------------------------------------------------------------
-- Source data (BigQuery):
---- stck.atlas_sales_all: Stackline raw data
---- crwl.amz_pdt_all: amazon product information from PDP, every day / all major asins including competitors
---- vc.amz_vc_sales_daily_all : amazon retail saels data from vendor central
-----------------------------------------------------------------------------------------------------------------------------------------
-- Version updated
---- v05: Stackline Subcategory Update, Brand Family Name applied
---- v04: weekly YoY data
---- v03: Append Amazon VC Sales data shared by Mark
---- v02: Added Ottomans & Sotrage ottomans subcategory for Mellow
-----------------------------------------------------------------------------------------------------------------------------------------

DECLARE MAX_DT DATE;
-- DECLARE TY_YEAR INT64;

--  2024.04.08 / ytd (yoy) mart
DECLARE MAX_WEEK INT64;
SET MAX_WEEK = (SELECT CAST(SUBSTRING(REGEXP_REPLACE(MAX(yr_week), r'\D', ''), 3,2) AS INT64) AS max_week FROM vs1.stckln_amz_ms_trend);

SET MAX_DT = (select max(WeekEnding) from stck.atlas_sales_all);
-- SET TY_YEAR = (SELECT CAST(SUBSTR(CAST(MAX_WK AS STRING),1,4)||'00' AS INT64));



-- 1) Agg Stackline Sales, Add category naming
DROP TABLE IF EXISTS tmp1.stckln_amz_ms_tmp1;
CREATE TABLE tmp1.stckln_amz_ms_tmp1 AS
SELECT
   CASE WHEN SubCategory='Mattresses' THEN '01. Mattresses'
        WHEN SubCategory='Box Springs' THEN '02. Box Springs'
        WHEN SubCategory='Mattress & Box Spring Sets' THEN '03. Mattress & Box Spring Sets'
        WHEN SubCategory='Folding Mattresses & Cots' THEN '04. Folding Mattresses & Cots'
        WHEN SubCategory='Bed Frames' THEN '05. Bed Frames'
        WHEN SubCategory='Beds' THEN '06. Beds'
        WHEN SubCategory IN ('Mattress Toppers','Mattress Pads','Mattress Pads & Toppers') THEN '07. Mattress Pads & Toppers'
        WHEN SubCategory='Living Room Sofas & Couches' THEN '08. Sofas & Couches'
        WHEN SubCategory='Ottomans & Storage Ottomans' THEN '09. Ottomans' 
        WHEN SubCategory='Television Stands & Entertainment Centers' THEN '10. TV Stands & Entertainment Centers' 
        WHEN SubCategory='Patio Furniture Sets' THEN '11. Patio Furniture Sets'

        WHEN SubCategory='Mattress Encasements & Protectors' THEN  '12. Mattress Encasements & Protectors'
        WHEN SubCategory in ('Bed Pillows','Pillow Protectors','Pillowcases') THEN '13. Pillows & Pillow Cases'

        WHEN SubCategory IN ('Cat Beds', 'Dog Beds', 'Pet Bed Mats') THEN '14. Pet Beds & Furniture'
        WHEN SubCategory IN ('Pet Ramps & Stairs') THEN '15. Pet Doors, Gates & Ramps'

        -- WHEN SubCategory='Living Room Table Sets' THEN '8. Living Room Table Sets'
        -- WHEN SubCategory='Ottomans & Storage Ottomans' THEN '9. Ottomans'        
   END AS bsr_ctgry_label,

   COALESCE(B.FAMILY_NAME,UPPER(TRIM(A.Brand))) AS Brand_adj,
   -- CASE
   --   WHEN LOWER(Brand) IN ('linenspa','lucid') THEN 'LINENSPA (LUCID)'
   --   WHEN LOWER(Brand) IN ('classic brands','vibe') THEN 'CLASSIC BRANDS (VIBE)'
   --   WHEN LOWER(Brand) IN ('best price mattress','mellow', 'otto & ben') THEN 'MELLOW (BPM)'
   --   WHEN LOWER(Brand) IN ('olee sleep','primasleep','granrest') THEN 'GRANTEC (OLEE/PRIMA/GRANREST)'
   --   ELSE UPPER(Brand) END AS Brand_adj,

   UPPER(A.Brand) AS Brand_raw,

   RetailerSku,
   Title,
   WeekEnding,
   SUM(RetailSales) AS RetailSales,
   SUM(UnitsSold) AS UnitsSold
FROM stck.atlas_sales_all A
LEFT OUTER JOIN meta.brand_family_mapping B ON UPPER(TRIM(A.brand))=B.BRAND_UPPER
WHERE
    RetailerName = 'Amazon.com'
    AND SubCategory IN (
                        'Mattresses'
                        , 'Box Springs'
                        , 'Bed Frames'
                        , 'Beds'
                        , 'Mattress Toppers'
                        , 'Mattress Pads & Toppers'
                        , 'Mattress Pads'
                        , 'Living Room Sofas & Couches'
                        , 'Patio Furniture Sets'
                        , 'Ottomans & Storage Ottomans'
                        , 'Folding Mattresses & Cots'
                        , 'Mattress & Box Spring Sets'
                        , 'Television Stands & Entertainment Centers'
                        , 'Cat Beds', 'Dog Beds', 'Pet Bed Mats'
                        , 'Pet Ramps & Stairs'
                        , 'Mattress Encasements & Protectors'
                        , 'Bed Pillows'
                        ,'Pillow Protectors'
                        ,'Pillowcases'
                       )
                                       --  'Living Room Table Sets',
GROUP BY 1,2,3,4,5,6
;



/*
select count(*), count(distinct RetailerSku) from stck.atlas_sales_all
select count(*), count(distinct RetailerSku) from tmp1.stckln_amz_ms_tmp1
*/

-- 2) PDT Master
DROP TABLE IF EXISTS tmp1.amz_ms_pdt_tmp1;
CREATE TABLE tmp1.amz_ms_pdt_tmp1 AS
WITH TMP2 AS (
  WITH TMP1 AS (
    SELECT
      A.asin,
      initialTime,

      COALESCE(B.FAMILY_NAME,UPPER(TRIM(A.Brand))) AS Brand_adj,
      --CASE
      --  WHEN LOWER(brand) IN ('linenspa','lucid') THEN 'LINENSPA (LUCID)'
      --  WHEN LOWER(brand) IN ('classic brands','vibe') THEN 'CLASSIC BRANDS (VIBE)'
      --  WHEN LOWER(brand) IN ('best price mattress','mellow', 'otto & ben') THEN 'MELLOW (BPM)'
      --  WHEN LOWER(brand) IN ('olee sleep','primasleep','granrest') THEN 'GRANTEC (OLEE/PRIMA/GRANREST)'
      --  ELSE UPPER(brand) END AS Brand_adj
      -- ,

      UPPER(brand) AS Brand_raw,
      
      title,
      imageUrl
    FROM crwl.amz_pdt_all A
    LEFT OUTER JOIN meta.brand_family_mapping B ON UPPER(TRIM(A.brand))=B.BRAND_UPPER
    WHERE A.brand IS NOT NULL AND UPPER(A.brand)<>'NONE' --AND title IS NOT NULL
    GROUP BY 1,2,3,4,5,6)
  SELECT
     asin
    ,Brand_adj
    ,Brand_raw
    ,title
    ,imageUrl
    ,ROW_NUMBER() OVER (PARTITION BY asin ORDER BY initialTime DESC) as filter
  FROM TMP1)
SELECT
   A.asin
  ,A.Brand_adj
  ,A.Brand_raw
  ,A.title
  ,A.imageUrl
FROM TMP2 A
WHERE A.filter=1
;

/*
select count(*), count(distinct asin) from tmp1.amz_ms_pdt_tmp1
--select * from tmp1.ms_trend_pdt
select count(*), count(distinct asin), min(initialTime), max(initialTime) from crwl.amz_pdt_all
*/

-- 3) Add PDT Master into Stackline sales
-- modified : 2023-12-21 (nshan) / 6개월 기준 Top10 Brand 컬럼 추가
--      BRAND_OTHERS_6MONTH, Brand_ord_6month

-- yr_week 생성 로직 - meta.wk_calendar 값 사용하도록 수정

-- meta.wk_calendar_new

DROP TABLE IF EXISTS vs1.stckln_amz_ms_trend;
CREATE TABLE vs1.stckln_amz_ms_trend CLUSTER BY yr_week AS
with
    -- adjust month
    -- cte_wk_calendar as (
    --     with cte_adj_yr_mon as (
    --             SELECT
    --                 *
    --                 , IF
    --                   (
    --                         IF
    --                         (
    --                                 DATE_TRUNC(start_date, MONTH) != DATE_TRUNC(end_date, MONTH),
    --                                 DATE_DIFF(LAST_DAY(start_date), start_date, DAY) + 1,
    --                                 0
    --                         ) >= 4,
    --                         FORMAT_DATE('%Y%m', start_date),
    --                         FORMAT_DATE('%Y%m', end_date)
    --                   ) as adj_year_month
    --             FROM
    --                 meta.wk_calendar
    --             -- WHERE
    --             --     yr_wk IN ( 202348, 202352, 202401, 202201, 202152 )
    --         ), cte_adj_wk_cal as (
    --             SELECT
    --                 *
    --                 , CAST(SUBSTR(adj_year_month, 3, 2) AS INT64) AS adj_year
    --                 , CAST(SUBSTR(adj_year_month, 5, 2) AS INT64) AS adj_month
    --                 , CASE
    --                       WHEN CAST(SUBSTR(adj_year_month, 5, 2) AS INT64) <= 3  THEN 1
    --                       WHEN CAST(SUBSTR(adj_year_month, 5, 2) AS INT64) <= 6  THEN 2
    --                       WHEN CAST(SUBSTR(adj_year_month, 5, 2) AS INT64) <= 9  THEN 3
    --                       WHEN CAST(SUBSTR(adj_year_month, 5, 2) AS INT64) <= 12 THEN 4
    --                   END AS adj_quarter
    --             FROM
    --                 cte_adj_yr_mon
    --         )
    --     SELECT
    --         start_date
    --         , end_date
    --         , concat('20', cast(adj_year as string)) as year
    --         , concat(cast(adj_year as string), '-', cast(adj_quarter as string))  AS yr_quarter
    --         , concat(adj_year, '-', lpad(cast(adj_month as string), 2, '0')) as yr_month
    --         , CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) AS yr_week
    --     FROM
    --         cte_adj_wk_cal
    --
    -- ),
    TMP3 AS (
        WITH
            TMP2 AS (
                WITH
                    TMP1 AS (

                        SELECT
                            A.bsr_ctgry_label
                            , COALESCE(B.Brand_adj, A.Brand_adj, 'NONE') AS Brand_adj
                            , COALESCE(B.Brand_raw, A.Brand_raw, 'NONE') AS Brand_raw
                            , A.RetailerSku
                            , COALESCE(B.title, A.Title) AS Title
                            , B.imageUrl
                            , A.WeekEnding
                            , A.RetailSales
                            , A.UnitsSold

                            -- , SUBSTR(CAST(A.WeekEnding AS STRING), 3, 5) AS yr_month
                            -- , c.yr_month
                            , SUBSTRING(c.yr_month_fmt, 3) as yr_month
                            -- , FORMAT_DATE('%y-%Q', A.WeekEnding) AS yr_quarter
                            -- , c.yr_quarter
                            , FORMAT_DATE('%y-%Q', C.thursday_date) AS yr_quarter

--                             , CASE
--                                   WHEN EXTRACT(WEEK FROM A.WeekEnding) = 0 THEN
--                                       "Y" || CAST(CAST(SUBSTR(CAST(A.WeekEnding AS STRING), 3, 2) AS INT64) - 1 AS STRING) || " W" || LPAD(CAST(EXTRACT(WEEK FROM CAST(SUBSTR(CAST(A.WeekEnding AS STRING), 1, 4) || '-12-31' AS DATE)) AS STRING), 2, '0')
--                                   ELSE
--                                       "Y" || CAST(SUBSTR(CAST(A.WeekEnding AS STRING), 3, 2) AS INT64) || " W" || LPAD(CAST(EXTRACT(WEEK FROM A.WeekEnding) AS STRING), 2, '0')
--                               END AS yr_week

                            -- , concat('Y', substr(cast(C.yr_wk as string), 3, 2), ' W', substr(cast(C.yr_wk as string), 5, 2)) as yr_week
                            , c.yr_wk_fmt as yr_week

                            , CASE
                                  WHEN A.WeekEnding = MAX_DT THEN
                                      "Latest Week"
                                  ELSE
--                                       CASE
--                                           WHEN EXTRACT(WEEK FROM A.WeekEnding) = 0 THEN
--                                               "Y" || CAST(CAST(SUBSTR(CAST(A.WeekEnding AS STRING), 3, 2) AS INT64) - 1 AS STRING) || " W" || LPAD(CAST(EXTRACT(WEEK FROM CAST(SUBSTR(CAST(A.WeekEnding AS STRING), 1, 4) || '-12-31' AS DATE)) AS STRING), 2, '0')
--                                           ELSE
--                                               "Y" || CAST(SUBSTR(CAST(A.WeekEnding AS STRING), 3, 2) AS INT64) || " W" || LPAD(CAST(EXTRACT(WEEK FROM A.WeekEnding) AS STRING), 2, '0')
--                                       END
--                                       concat('Y', substr(cast(C.yr_wk as string), 3, 2), ' W', substr(cast(C.yr_wk as string), 5, 2))
--                                     c.yr_week
                                      c.yr_wk_fmt
                              END AS week_str

                            -- , c.year
                            , cast(c.yr as string) as year

                            , SUM(A.RetailSales) OVER (PARTITION BY COALESCE(B.Brand_adj, A.Brand_adj,'NONE')) AS RetailSales_BRAND
                            , SUM(A.RetailSales) OVER (PARTITION BY A.bsr_ctgry_label, COALESCE(B.Brand_adj, A.Brand_adj, 'NONE')) AS RetailSales_CTGRY_BRAND
--                             , SUM(if(A.WeekEnding >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH ), A.RetailSales, 0)) OVER (PARTITION BY A.bsr_ctgry_label, COALESCE(B.Brand_adj, A.Brand_adj, 'NONE')) AS retailsales_ctgry_brand_by_3month
                            , SUM(if(A.WeekEnding >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH ), A.RetailSales, 0)) OVER (PARTITION BY A.bsr_ctgry_label, COALESCE(B.Brand_adj, A.Brand_adj, 'NONE')) AS retailsales_ctgry_brand_by_6month
--                             , SUM(if(A.WeekEnding >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR ), A.RetailSales, 0)) OVER (PARTITION BY A.bsr_ctgry_label, COALESCE(B.Brand_adj, A.Brand_adj, 'NONE')) AS retailsales_ctgry_brand_by_1year
                        FROM
                            tmp1.stckln_amz_ms_tmp1 A
                                -- LEFT OUTER JOIN meta.wk_calendar C ON A.WeekEnding BETWEEN C.start_date and end_date
                                -- LEFT OUTER JOIN cte_wk_calendar C ON A.WeekEnding BETWEEN C.start_date and end_date
                                LEFT OUTER JOIN meta.wk_calendar_new C ON A.WeekEnding BETWEEN C.start_date and end_date
                                LEFT OUTER JOIN tmp1.amz_ms_pdt_tmp1 B
                                    ON A.RetailerSku = B.asin

                    )
                SELECT
                    A.bsr_ctgry_label
                    , A.Brand_raw
                    , A.Brand_adj
                    , A.RetailerSku
                    , A.Title
                    , A.imageUrl
                    , A.WeekEnding
                    , A.RetailSales
                    , A.UnitsSold
                    , A.yr_quarter
                    , A.yr_month
                    , A.yr_week
                    , A.week_str
                    , A.year

                    , A.retailsales_ctgry_brand
--                     , A.retailsales_ctgry_brand_by_3month
                    , A.retailsales_ctgry_brand_by_6month
--                     , A.retailsales_ctgry_brand_by_1year

                    -- Brand_ord_ctgry : Category 별 brand RetailSales 순위 (ZINUS 최상위로 고정)
                    , CASE
                          WHEN Brand_adj = 'ZINUS' THEN 0
                        --WHEN Brand_adj='MELLOW (BPM)' THEN 1
                          WHEN Brand_adj = 'NONE'  THEN 999999999
                        --WHEN Brand_adj='OTHERS' THEN 99999999
                          ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry_label ORDER BY retailsales_ctgry_brand DESC)
                      END AS Brand_ord_ctgry

--                     , CASE
--                           WHEN Brand_adj = 'ZINUS' THEN 0
--                         --WHEN Brand_adj='MELLOW (BPM)' THEN 1
--                           WHEN Brand_adj = 'NONE'  THEN 999999999
--                         --WHEN Brand_adj='OTHERS' THEN 99999999
--                           ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry_label ORDER BY retailsales_ctgry_brand_by_3month DESC)
--                       END AS Brand_ord_ctgry_by_3month

                    , CASE
                          WHEN Brand_adj = 'ZINUS' THEN 0
                        --WHEN Brand_adj='MELLOW (BPM)' THEN 1
                          WHEN Brand_adj = 'NONE'  THEN 999999999
                          WHEN Brand_adj='OTHERS' THEN 99999999
                          ELSE DENSE_RANK() OVER (ORDER BY RetailSales_BRAND DESC)
                      END AS Brand_ord_all
                    , CASE
                          WHEN Brand_adj = 'ZINUS' THEN 0
                        --WHEN Brand_adj='MELLOW (BPM)' THEN 1
                          WHEN Brand_adj = 'NONE'  THEN 999999999
                        --WHEN Brand_adj='OTHERS' THEN 99999999
                          ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry_label ORDER BY retailsales_ctgry_brand_by_6month DESC)
                      END AS Brand_ord_ctgry_by_6month

--                     , CASE
--                           WHEN Brand_adj = 'ZINUS' THEN 0
--                         --WHEN Brand_adj='MELLOW (BPM)' THEN 1
--                           WHEN Brand_adj = 'NONE'  THEN 999999999
--                         --WHEN Brand_adj='OTHERS' THEN 99999999
--                           ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry_label ORDER BY retailsales_ctgry_brand_by_1year DESC)
--                       END AS Brand_ord_ctgry_by_1year

                FROM
                    TMP1 A
            )
        SELECT
            A.* EXCEPT (year)
            -- , SUBSTR(CAST(A.WeekEnding AS STRING), 1, 4) AS year
            , a.year

            -- BRAND_OTHERS for (AMZ Market Share and Retail Sales dashboard - Market Size Trend page - MarketShare by Brand (Top 10))
            , CASE
                  WHEN Brand_adj = 'ZINUS' OR A.Brand_ord_ctgry <= 10 THEN Brand_adj
                  ELSE 'OTHERS'
              END AS BRAND_OTHERS

--             , CASE
--                   WHEN Brand_adj = 'ZINUS' OR A.Brand_ord_ctgry_by_3month <= 10 THEN Brand_adj
--                   ELSE 'OTHERS'
--               END AS BRAND_OTHERS_3MONTH

            , CASE
                  WHEN Brand_adj = 'ZINUS' OR A.Brand_ord_ctgry_by_6month <= 10 THEN Brand_adj
                  ELSE 'OTHERS'
              END AS BRAND_OTHERS_6MONTH

--             , CASE
--                   WHEN Brand_adj = 'ZINUS' OR A.Brand_ord_ctgry_by_1year <= 10 THEN Brand_adj
--                   ELSE 'OTHERS'
--               END AS BRAND_OTHERS_1YEAR

            , SUM(A.RetailSales) OVER (PARTITION BY if(Brand_adj = 'ZINUS' OR A.Brand_ord_ctgry <= 10, Brand_adj, 'OTHERS')) AS RetailSales_TTL
--             , SUM(A.RetailSales) OVER (PARTITION BY if(Brand_adj = 'ZINUS' OR A.Brand_ord_ctgry_by_3month <= 10, Brand_adj, 'OTHERS')) AS RetailSales_TTL_3month
            , SUM(A.RetailSales) OVER (PARTITION BY if(Brand_adj = 'ZINUS' OR A.Brand_ord_ctgry_by_6month <= 10, Brand_adj, 'OTHERS')) AS RetailSales_TTL_6month
--             , SUM(A.RetailSales) OVER (PARTITION BY if(Brand_adj = 'ZINUS' OR A.Brand_ord_ctgry_by_1year <= 10, Brand_adj, 'OTHERS')) AS RetailSales_TTL_1year
        FROM
            TMP2 A
    )
SELECT
    * EXCEPT (Brand_ord_all)
    , CASE
          WHEN BRAND_OTHERS = 'ZINUS'  THEN 0
        --WHEN BRAND_OTHERS='MELLOW (BPM)' THEN 1
          WHEN BRAND_OTHERS = 'NONE'   THEN 999999999
          WHEN BRAND_OTHERS = 'OTHERS' THEN 99999999
          ELSE DENSE_RANK() OVER (ORDER BY RetailSales_TTL DESC)
      END AS Brand_ord
--
--     , CASE
--           WHEN BRAND_OTHERS_3MONTH = 'ZINUS'  THEN 0
--         --WHEN BRAND_OTHERS_3MONTH='MELLOW (BPM)' THEN 1
--           WHEN BRAND_OTHERS_3MONTH = 'NONE'   THEN 999999999
--           WHEN BRAND_OTHERS_3MONTH = 'OTHERS' THEN 99999999
--           ELSE DENSE_RANK() OVER (ORDER BY RetailSales_TTL_3month DESC)
--       END AS Brand_ord_3month
--     , Brand_ord_all AS Brand_ord_6month

    , CASE
          WHEN BRAND_OTHERS_6MONTH = 'ZINUS'  THEN 0
        --WHEN BRAND_OTHERS_6MONTH='MELLOW (BPM)' THEN 1
          WHEN BRAND_OTHERS_6MONTH = 'NONE'   THEN 999999999
          WHEN BRAND_OTHERS_6MONTH = 'OTHERS' THEN 99999999
          ELSE DENSE_RANK() OVER (ORDER BY RetailSales_TTL_6month DESC)
      END AS Brand_ord_6month
--     , CASE
--           WHEN BRAND_OTHERS_1YEAR = 'ZINUS'  THEN 0
--         --WHEN BRAND_OTHERS_1YEAR='MELLOW (BPM)' THEN 1
--           WHEN BRAND_OTHERS_1YEAR = 'NONE'   THEN 999999999
--           WHEN BRAND_OTHERS_1YEAR = 'OTHERS' THEN 99999999
--           ELSE DENSE_RANK() OVER (ORDER BY RetailSales_TTL_1year DESC)
--       END AS Brand_ord_1year

    , SUM(RetailSales) OVER (PARTITION BY bsr_ctgry_label, year) AS RetailSales_MS_TTL

    , CASE
          WHEN BRAND_OTHERS = "ZINUS"           THEN "Zinus"
          WHEN Brand_raw = "MELLOW"             THEN "Mellow"
          WHEN Brand_raw = "BEST PRICE MATTRESS"  THEN "BEST PRICE MATTRESS"
          WHEN Brand_raw = "IYEE NATURE"        THEN "IYEE NATURE"
          WHEN Brand_raw = "JINGWEI"            THEN "JINGWEI"
          WHEN Brand_raw = "JINGXUN"            THEN "JINGXUN"
          WHEN Brand_raw = "MOLBLLY"            THEN "MOLBLLY"
          WHEN Brand_raw = "OYT"                THEN "OYT"
          WHEN Brand_raw = "S SECRETLAND"       THEN "S SECRETLAND"
          WHEN Brand_raw = "RIMENSY"            THEN "RIMENSY"
          WHEN Brand_raw = "NAPQUEEN" then "NAPQUEEN"
          WHEN BRAND_OTHERS = "LINENSPA FAMILY" THEN "LINENSPA FAMILY(LUCID, LINENSPA ESSENTIALS, LINENSPA)"
          WHEN BRAND_OTHERS = "GRANTEC FAMILY"  THEN "GRANTEC FAMILY(PRIMASLEEP, OLEE SLEEP)"
          WHEN BRAND_OTHERS = "ASHLEY FAMILY"   THEN "ASHLEY FAMILY(SIERRA SLEEP BY ASHLEY,  ASHLEY, ASHLEY FURNITURE SIGNATURE DESIGN, SIGNATURE DESIGN BY ASHLEY, ASHLEY FURNITURE)"
          WHEN BRAND_OTHERS = "CLASSIC BRANDS"  THEN "CLASSIC BRANDS FAMILY(CLASSIC BRANDS, VIBE)"
          WHEN BRAND_OTHERS = "CASPER FAMILY"   THEN "CASPER FAMILY(CASPER, CASPER SLEEP)"
          WHEN BRAND_OTHERS = "SEALY FAMILY"    THEN "SEALY FAMILY(TEMPUR-PEDIC, SEALY, COCOON BY SEALY)"
          WHEN BRAND_OTHERS = "SIMMONS FAMILY"  THEN "SIMMONS FAMILY(SIMMONS CURV, SIMMONS BEAUTYREST, SERTA, SIMMONS)"
          ELSE 'OTHERS'
      END AS brand_family3
    , CASE
          WHEN BRAND_OTHERS = "ZINUS" then 1
          WHEN Brand_raw = "MELLOW" then 2
          WHEN Brand_raw = "BEST PRICE MATTRESS" then 3
          WHEN Brand_raw = "IYEE NATURE" then 4
          WHEN Brand_raw = "JINGWEI" then 4
          WHEN Brand_raw = "JINGXUN" then 4
          WHEN Brand_raw = "MOLBLLY" then 4
          WHEN Brand_raw = "OYT" then 4
          WHEN Brand_raw = "S SECRETLAND" then 4
          WHEN Brand_raw = "RIMENSY" then 4
          WHEN Brand_raw = "NAPQUEEN" then 5
          WHEN BRAND_OTHERS = "LINENSPA FAMILY" then 6
          WHEN BRAND_OTHERS = "GRANTEC FAMILY" then 7
          WHEN BRAND_OTHERS = "ASHLEY FAMILY" then 8
          WHEN BRAND_OTHERS = "CLASSIC BRANDS" then 9
          WHEN BRAND_OTHERS = "CASPER FAMILY" then 10
          WHEN BRAND_OTHERS = "SEALY FAMILY" then  11
          WHEN BRAND_OTHERS = "SIMMONS FAMILY" then 12
          ELSE 13
      END AS brand_family_order

--      brand_family4 / for (zinus molblly dashboard - Market Size Trend page - brand family button)
    , CASE
          WHEN BRAND_OTHERS = "ZINUS"           THEN "ZINUS"
          WHEN Brand_raw = "MELLOW"             THEN "ZINUS"
          WHEN Brand_raw = "BEST PRICE MATTRESS"  THEN "ZINUS"
          WHEN Brand_raw in ("IYEE NATURE", "JINGWEI", "JINGXUN", "MOLBLLY", "OYT", "S SECRETLAND", "RIMENSY") THEN "MOLBLLY"
          WHEN Brand_raw = "NAPQUEEN" then "NAPQUEEN"
          WHEN BRAND_OTHERS = "LINENSPA FAMILY" THEN "LINENSPA FAMILY(LUCID, LINENSPA ESSENTIALS, LINENSPA)"
          WHEN BRAND_OTHERS = "GRANTEC FAMILY"  THEN "GRANTEC FAMILY(PRIMASLEEP, OLEE SLEEP)"
          WHEN BRAND_OTHERS = "ASHLEY FAMILY"   THEN "ASHLEY FAMILY(SIERRA SLEEP BY ASHLEY,  ASHLEY, ASHLEY FURNITURE SIGNATURE DESIGN, SIGNATURE DESIGN BY ASHLEY, ASHLEY FURNITURE)"
          WHEN BRAND_OTHERS = "CLASSIC BRANDS"  THEN "CLASSIC BRANDS FAMILY(CLASSIC BRANDS, VIBE)"
          WHEN BRAND_OTHERS = "CASPER FAMILY"   THEN "CASPER FAMILY(CASPER, CASPER SLEEP)"
          WHEN BRAND_OTHERS = "SEALY FAMILY"    THEN "SEALY FAMILY(TEMPUR-PEDIC, SEALY, COCOON BY SEALY)"
          WHEN BRAND_OTHERS = "SIMMONS FAMILY"  THEN "SIMMONS FAMILY(SIMMONS CURV, SIMMONS BEAUTYREST, SERTA, SIMMONS)"
          ELSE 'OTHERS'
      END AS brand_family4
    , CASE
          WHEN BRAND_OTHERS = "ZINUS" then 1
          WHEN Brand_raw = "MELLOW" then 1
          WHEN Brand_raw = "BEST PRICE MATTRESS" then 1
          WHEN Brand_raw in ("IYEE NATURE", "JINGWEI", "JINGXUN", "MOLBLLY", "OYT", "S SECRETLAND", "RIMENSY") then 2
          WHEN BRAND_raw = "NAPQUEEN"  THEN 3
          WHEN BRAND_OTHERS = "LINENSPA FAMILY" then 4
          WHEN BRAND_OTHERS = "GRANTEC FAMILY" then 5
          WHEN BRAND_OTHERS = "ASHLEY FAMILY" then 6
          WHEN BRAND_OTHERS = "CLASSIC BRANDS" then 7
          WHEN BRAND_OTHERS = "CASPER FAMILY" then 8
          WHEN BRAND_OTHERS = "SEALY FAMILY" then 9
          WHEN BRAND_OTHERS = "SIMMONS FAMILY" then 10
          ELSE 11
      END AS brand_family4_order

FROM
    TMP3
;

--  2024.04.08 / ytd (yoy) mart

CREATE OR REPLACE TABLE vs1.stckln_amz_ms_trend_with_yoy AS
with cte_yoy as (
    SELECT
        *
    FROM
        vs1.stckln_amz_ms_trend
    WHERE
        CAST(SUBSTRING(REGEXP_REPLACE(yr_week, r'\D', ''), 3, 2) AS INT64) <= MAX_WEEK
)
SELECT
    bsr_ctgry_label
    , Brand_raw
    , Brand_adj
    , RetailerSku
    , Title
    , imageUrl
    , WeekEnding
    , RetailSales
    , UnitsSold
    , yr_quarter
    , yr_month
    , yr_week
    , week_str
    , retailsales_ctgry_brand
    , retailsales_ctgry_brand_by_6month
    , Brand_ord_ctgry
    , Brand_ord_ctgry_by_6month
    , year
    , BRAND_OTHERS
    , BRAND_OTHERS_6MONTH
    , RetailSales_TTL
    , RetailSales_TTL_6month
    , Brand_ord
    , Brand_ord_6month
    , RetailSales_MS_TTL
    , brand_family3
    , brand_family_order
    , brand_family4
    , brand_family4_order
FROM
    cte_yoy

union all

--     yoy label for bi
select
    concat (bsr_ctgry_label, ' - YOY') as bsr_ctgry_label
    , null as Brand_raw
    , Brand_adj
    , null as RetailerSku
    , null as Title
    , null as imageUrl
    , null as WeekEnding
    , 0 as RetailSales
    , 0 as UnitsSold
    , null as yr_quarter
    , null as yr_month
    , null as yr_week
    , null as week_str
    , null as retailsales_ctgry_brand
    , null as retailsales_ctgry_brand_by_6month
    , null as Brand_ord_ctgry
    , null as Brand_ord_ctgry_by_6month
    , year
    , null as BRAND_OTHERS
    , null as BRAND_OTHERS_6MONTH
    , null as RetailSales_TTL
    , null as RetailSales_TTL_6month
    , Brand_ord
    , null as Brand_ord_6month
    , null as RetailSales_MS_TTL
    , null as brand_family3
    , null as brand_family_order
    , null as brand_family4
    , null as brand_family4_order
from cte_yoy group by 1,Brand_adj, year, Brand_ord;



-- 4) Brand List
DROP TABLE IF EXISTS vs1.stckln_amz_ms_brand_list;
CREATE TABLE vs1.stckln_amz_ms_brand_list AS
SELECT
     Brand_adj
    ,MAX(Brand_ord) AS Brand_ord
FROM vs1.stckln_amz_ms_trend
GROUP BY 1 
;



/****************************************************************************
-- Wook / 2023-4-5
-- vc1.amz_vc_sales_daily_all (based on the sharepoint uploaded by Mark )
-- updated at 8am every Tuesday
-- using Kyungjin's SQL 
*****************************************************************************/

--DECLARE MAX_DATE_AMZ DATE;
--DECLARE MIN_DATE_AMZ DATE;

--SET MAX_DATE_AMZ = (SELECT MAX(date) FROM vc.amz_vc_sales_daily_all);
--SET MIN_DATE_AMZ = (SELECT DATE_ADD(MIN(date), INTERVAL 1 YEAR) FROM vc.amz_vc_sales_daily_all);

-- AMAZON
-- 1) ASIN-ZINUS SKU Mapping TABLE
DROP TABLE IF EXISTS tmp1.amz_sku_mapping;
CREATE TABLE tmp1.amz_sku_mapping AS
WITH TMP2 AS (
  WITH TMP1 AS (
  SELECT
    Zinus_SKU,
    OMSID
  FROM  meta.erp_material_mapping -- meta.erp_material_mapping
  WHERE LOWER(Customer_Name) like 'amazon%' AND OMSID IS NOT NULL AND Zinus_SKU IS NOT NULL
  GROUP BY 1,2)
  SELECT
    Zinus_SKU,
    OMSID,
    ROW_NUMBER() OVER (PARTITION BY OMSID ORDER BY Zinus_SKU DESC) AS RNK
  FROM TMP1
  )
SELECT
  Zinus_SKU,
  OMSID,
FROM TMP2
WHERE RNK=1
;

-- 2) ZINUS SKU PRODUT MST FROM ERP
DROP TABLE IF EXISTS tmp1.zinus_erp_sku_info;
CREATE TABLE tmp1.zinus_erp_sku_info AS
SELECT
  zinus_sku_cd,
  max(zinus_sku_nm) as zinus_sku_nm,
  prdct_h_lv1,
  prdct_h_lv2,
  min(collection) as collection,
  size,
  max(profile) as profile,
  max(prdct_material) as prdct_material,
  --abc_in,
  --pr00
FROM tmp.erp_do_di_pdt_mst
GROUP BY 1,3,4,6
;

-- 3) aggreate daily data
--3.1) this year data
DROP TABLE IF EXISTS tmp1.vc_sales_daily_ty;
CREATE TABLE tmp1.vc_sales_daily_ty AS
SELECT
  date,
  asin,
  SUM(ifnull(ordered_revenue,0)) AS ord_rev,
  SUM(ifnull(ordered_units,0)) AS ord_qty
FROM vc.amz_vc_sales_daily_all
GROUP BY 1,2
;

-- 3.2) last year data
DROP TABLE IF EXISTS tmp1.vc_sales_daily_ly;
CREATE TABLE tmp1.vc_sales_daily_ly AS
SELECT
  DATE_ADD(date, INTERVAL 1 YEAR) AS date,
  asin,
  ord_rev,
  ord_qty
FROM tmp1.vc_sales_daily_ty
;

-- 3.3) Agg. TY & LY daily data
DROP TABLE IF EXISTS tmp1.vc_sales_daily_agg;
CREATE TABLE tmp1.vc_sales_daily_agg AS
WITH TMP1 AS (
  (SELECT
    date,
    asin,
    ord_rev AS ord_rev_ty,
    ord_qty AS ord_qty_ty,
    CAST(NULL AS NUMERIC) AS ord_rev_ly,
    CAST(NULL AS NUMERIC) AS ord_qty_ly
  FROM tmp1.vc_sales_daily_ty)
    UNION ALL
  (SELECT
    date,
    asin,
    CAST(NULL AS NUMERIC) AS ord_rev_ty,
    CAST(NULL AS NUMERIC) AS ord_qty_ty,
    ord_rev AS ord_rev_ly,
    ord_qty AS ord_qty_ly
  FROM tmp1.vc_sales_daily_ly)
  )
SELECT
  date,
  asin,
  SUM(ord_rev_ty) AS ord_rev_ty,
  SUM(ord_qty_ty) AS ord_qty_ty,
  SUM(ord_rev_ly) AS ord_rev_ly,
  SUM(ord_qty_ly) AS ord_qty_ly
FROM TMP1
GROUP BY 1,2
;

-- 3) append sku info to daily vc data
DROP TABLE IF EXISTS vs1.amz_vc_sales_daily;
CREATE TABLE vs1.amz_vc_sales_daily  AS
WITH TMP1 AS (
  SELECT
     CAST(SUBSTR(CAST(A.date AS STRING),1,4) AS INT64) AS year

    ,SUBSTR(CAST(A.date AS STRING),1,7) AS yr_month

    ,CASE WHEN EXTRACT(WEEK FROM A.date)=0 THEN "Y"||CAST(CAST(SUBSTR(CAST(A.date AS STRING),3,2) AS INT64)-1 AS STRING)||
                " W"||LPAD(CAST(EXTRACT(WEEK FROM CAST(SUBSTR(CAST(A.date AS STRING),1,4)||'-12-31' AS DATE)) AS STRING),2,'0')
     ELSE "Y"||CAST(SUBSTR(CAST(A.date AS STRING),3,2) AS INT64)||" W"|| LPAD(CAST(EXTRACT(WEEK FROM A.date) AS STRING),2,'0') END AS yr_week

    ,A.date
    ,A.asin
    ,B.Zinus_SKU AS sku_code
    ,ord_rev_ty
    ,ord_qty_ty
    ,ord_rev_ly
    ,ord_qty_ly
  FROM tmp1.vc_sales_daily_agg A
  LEFT OUTER JOIN tmp.amz_sku_mapping B ON A.asin=B.OMSID
  --WHERE A.date <= MAX_DATE_AMZ AND A.date >= MIN_DATE_AMZ
  WHERE A.date <= (SELECT MAX(date) FROM vc.amz_vc_sales_daily_all) 
)
SELECT
  A.year,
  A.yr_month,
  A.yr_week,
  A.date,
  A.asin,
  C.zinus_sku_cd AS zinus_sku_cd,
  C.zinus_sku_nm AS zinus_sku_nm,
  --C.prdct_h_lv1,
  ifnull(C.prdct_h_lv1, 'NULL') as prdct_h_lv1,
  C.prdct_h_lv2,
  C.collection,
  C.size,
  C.profile,
  --C.prdct_material,
  --C.abc_in,
  --C.pr00,
  A.ord_rev_ty,
  A.ord_qty_ty,
  A.ord_rev_ly,
  A.ord_qty_ly
FROM TMP1 A
LEFT OUTER JOIN tmp1.zinus_erp_sku_info C on A.sku_code=C.zinus_sku_cd
;

-- select distinct prdct_h_lv1 from vs1.amz_vc_sales_daily

--4) Weekly YoY data
--4.1) This Year Weekly Data
CREATE OR REPLACE TABLE vs1.amz_vc_sales_weekly AS
WITH cte_join_wk_cal AS (
		SELECT a.*
      , b.yr_wk
      , b.start_date
      , b.end_date
		FROM tmp1.vc_sales_daily_ty a
		JOIN meta.wk_calendar b ON a.date BETWEEN b.start_date AND b.end_date
		--order by a.date desc
	)
  , cte_category AS (
	  SELECT a.yr_wk
	    , yr_wk + 100 as next_yr_wk
			--, c.prdct_h_lv1
	    , ifnull(c.prdct_h_lv1, 'NULL') as prdct_h_lv1
	    , a.ord_rev
	    , a.ord_qty
	  FROM cte_join_wk_cal a
		  left outer join tmp.amz_sku_mapping b on a.asin = b.OMSID
		  left outer join tmp1.zinus_erp_sku_info c on b.Zinus_SKU = c.zinus_sku_cd
  )
  , cte_base AS (
    SELECT yr_wk
      , next_yr_wk
      , prdct_h_lv1
      , sum(ord_rev) as ord_rev
      , sum(ord_qty) as ord_qty
    FROM cte_category
    GROUP BY 1,2,3
)
SELECT
	cast(substr(cast(t1.yr_wk AS string),1,4) AS numeric) as year
	, t1.yr_wk as yr_wk
	, t1.prdct_h_lv1
  , cast(t1.ord_rev as numeric) as ord_rev_ty
  , cast(t1.ord_qty as numeric) as ord_qty_ty
  , cast(t2.ord_rev as numeric) as ord_rev_ly
  , cast(t2.ord_qty as numeric) as ord_qty_ly
FROM cte_base t1
LEFT JOIN cte_base t2 ON t1.yr_wk = t2.next_yr_wk AND t1.prdct_h_lv1 = t2.prdct_h_lv1
WHERE substr(cast(t1.yr_wk AS string),1,4) >= '2021'
;

/*
select distinct prdct_h_lv1 from vs1.amz_vc_sales_weekly
select max(date) from vs1.amz_vc_sales_daily

select * from vs1.vc_sales_weekly limit 100
select yr_wk, sum(ord_rev_ty), sum(ord_rev_ly) from vs1.amz_vc_sales_weekly group by 1 order by 1 desc
*/
---------------------------------------------------------------------
-- End of Document : last updated on 2023/7/6 12:00 PM 
---------------------------------------------------------------------