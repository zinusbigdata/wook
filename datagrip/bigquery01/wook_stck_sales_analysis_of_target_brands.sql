-- 마스터 중복 제거 ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp.kp_mst_for_stck_sales_analysis_of_target_brands AS
SELECT
    *
FROM
    tmp.kp_mst_for_stck_sales_analysis_of_target_brands
QUALIFY ROW_NUMBER() OVER (PARTITION BY asin ORDER BY load_datetime desc) = 1;

select asin, count(1) from tmp.kp_mst_for_stck_sales_analysis_of_target_brands GROUP BY 1 HAVING COUNT(1) > 1;
select asin, count(1) from tmp.kp_mst_for_stck_sales_analysis_of_target_brands GROUP BY 1;
select * from tmp.kp_mst_for_stck_sales_analysis_of_target_brands TABLESAMPLE SYSTEM ( 1 PERCENT );


-- 분석용 마트 ---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE wook.stck_sales_analysis_of_target_brands
    OPTIONS (
        DESCRIPTION = "Stackline mattress 대상 브랜드 (egohome, novilla, fdw, zinus, mlily)의 2023년 이후 sales, price, first_date 기준 분석용 테이블"
        )
AS
WITH cte_meta AS (
    SELECT
        *
    FROM
        (
            -- pi Master
            SELECT DISTINCT
                TRIM(a.asin) AS asin
                , TRIM(a.zinus_sku) AS zinus_sku
                , IF(a.asin = 'B0B6FQZMJ4', '5', REGEXP_EXTRACT(LOWER(TRIM(inch_color)), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)')) AS inch
                , TRIM(size) AS size
                , if(a.financial_category = 'Foam Mattresses', 'Foam Mattress', a.financial_category) as category
                , if(CONCAT(LOWER(collection), LOWER(product_description), LOWER(abbre)) LIKE '%wonder%', 'SmallBox', cast(null as string)) as box_type
                , 1 AS ord
            FROM
                meta.amz_zinus_master_pdt_pi_enriched a
            WHERE
                a.financial_category in ('Foam Mattresses', 'Spring Mattresses')

            UNION ALL

            -- GPT Mattress Master
            SELECT DISTINCT
                a.asin
                , cast(null as string) AS zinus_sku
                , profile as inch
                , size_adj as size
                , a.category
                , cast(null as string) AS box_type
                , 2 AS ord
            FROM
                -- 26.04.06 gpt 마스터 추가 (tmp.amz_stck_cate_matt_by_gpt_260406)
                meta.amazon_mattress_master a

        )

    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY asin ORDER BY category is null, ord) = 1
)
-- , cte_event_day AS (
--         SELECT
--             UPPER(TRIM(asin)) AS asin
--             , metric
--             , DATE(event_ts_utc) AS event_date
--             , value_num
--         FROM tmp.stck_zns_comp_sales_anal_hist_events
--         WHERE
--             metric = 'list_price'
--         --             ( metric = 'list_price' AND value_num IS NOT NULL )
--         --             OR metric != 'list_price'
--         QUALIFY ROW_NUMBER() OVER (
--             PARTITION BY UPPER(TRIM(asin)), metric, DATE(event_ts_utc)
--             ORDER BY event_ts_utc DESC
--         ) = 1
--     )
-- , cte_list_price AS (
--     SELECT
--         asin
--         , metric
--         , event_date
--         , LEAD(event_date) OVER (
--             PARTITION BY asin, metric
--             ORDER BY event_date
--             ) AS next_event_date
--         , value_num
--     FROM cte_event_day
-- )
    , cte_seller_src as (
        SELECT
            UPPER(TRIM(asin)) AS asin
            , DATE(event_ts_utc) AS event_date
            , nullif(buyBoxSellerId, 'None') as buyBoxSellerId
            , nullif(buyBoxIsAmazon, 'None') as buyBoxIsAmazon
--             , CASE
--                   WHEN buyBoxSellerId = 'ATVPDKIKX0DER' OR buyBoxIsAmazon = 'True' THEN '1p'
--                   WHEN nullif(buyBoxSellerId, 'None') IS NULL THEN 'Unknown'
--                   ELSE '3p'
--               END AS seller_type
        FROM
            tmp.kp_seller_hist_for_stck_sales_analysis_of_target_brands
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(asin)), DATE(event_ts_utc)
            ORDER BY event_ts_utc DESC
        ) = 1
    )
    , cte_seller AS (
        SELECT
            asin
            , event_date
            , LEAD(event_date) OVER (
                PARTITION BY asin
                ORDER BY event_date
            ) AS next_event_date
            , buyBoxSellerId
            , buyBoxIsAmazon
--             , if(buyBoxSellerId = 'ATVPDKIKX0DER' or buyBoxIsAmazon = 'True', '1p', '3p') as seller_type
            , CASE
                  WHEN buyBoxSellerId = 'ATVPDKIKX0DER' OR buyBoxIsAmazon = 'True' THEN '1p'
                  WHEN buyBoxSellerId IS NULL AND (buyBoxIsAmazon is NULL OR buyBoxIsAmazon NOT IN ('True', 'False')) THEN 'Unknown'
                  ELSE '3p'
              END AS seller_type
        FROM cte_seller_src
    )
, cte_with_meta as (
        SELECT
            a.RetailerSku AS asin
            , seller.seller_type
            , FORMAT_DATE('%Y', a.WeekEnding) as year
            , FORMAT_DATE('%Y-%m', a.WeekEnding) as yr_month
            , FORMAT_DATE('%Y-%Q', a.WeekEnding) as yr_quarter
            , ANY_VALUE(b.zinus_sku) AS zinus_sku
            --     , (ARRAY_AGG(a.Brand)) as brand
            , MAX(REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','')) as brand

            , SUM(a.RetailSales) AS sales
            , SUM(a.UnitsSold) AS units

--             , AVG(RetailPrice) AS avg_retail_price
--             , AVG(listprice.value_num) AS avg_list_price

            , ANY_VALUE(b.category) AS category
            , ANY_VALUE(b.size) AS size
            , ANY_VALUE(b.inch) AS inch
            --     , ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)] AS title
            , LOWER(
                    REGEXP_REPLACE(
                            NORMALIZE(
                                    ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)]
                                    , NFKC
                            ), -- NBSP 등 정규화
                            r'[^[:print:]]',
                            ' '
                    )
              ) AS title

            , ANY_VALUE(b.box_type) AS box_type

        FROM
            stck.atlas_sales_all a
                --         join meta.amazon_mattress_master b on a.RetailerSku = b.asin
                JOIN cte_meta b
                    ON a.RetailerSku = b.asin

--                 left join cte_list_price listprice
--                     ON a.RetailerSku = listprice.asin
--                         AND a.WeekEnding BETWEEN listprice.event_date AND COALESCE(DATE_SUB(listprice.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')

                LEFT join cte_seller seller
                    ON a.RetailerSku = seller.asin
                        AND a.WeekEnding BETWEEN seller.event_date AND COALESCE(DATE_SUB(seller.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
        WHERE
            REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','') IN ( 'FDW', 'EGOHOME', 'NOVILLA', 'ZINUS' ,'MLILY' )
            AND b.category IS NOT NULL
            AND a.WeekEnding >= '2023-01-01'
        GROUP BY 1, 2, 3, 4, 5
    )
SELECT DISTINCT
    a.asin
    , a.seller_type
    , COALESCE(a.zinus_sku, b.model) as sku
    , a.brand
    , year
    , yr_month
    , yr_quarter
    , a.title
--     , ROW_NUMBER() OVER (PARTITION BY brand, category, yr_month ORDER BY sales DESC) as yr_month_sales_rank
    , ROW_NUMBER() OVER (PARTITION BY category, year, yr_month ORDER BY sales DESC) as yr_month_sales_rank
    , sales
    , units

--     , avg_retail_price
--     , avg_list_price

    , category
    , COALESCE(nullif(inch, 'OTHERS'), REGEXP_EXTRACT(LOWER(a.title), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)'), 'OTHERS' ) as inch
--     , size
    , CASE
          WHEN a.asin IN ( 'B00X6L6DCO', 'B00X6LCL3O' )                             THEN 'Twin'
          WHEN a.asin = 'B0765C7YPX'                                                THEN 'King'
          WHEN LOWER(a.size) IN ( 'ck', 'california king' )                         THEN 'Cal King'
          WHEN LOWER(a.size) = '12 inch queen medium firm'                          THEN 'Queen'
          WHEN LOWER(a.size) IN ( 'k', 'king (u.s. standard)', 'king' )             THEN 'King'
          WHEN LOWER(a.size) IN ( 't', 'twin', 'twin (75*38)' )                     THEN 'Twin'
          WHEN LOWER(a.size) IN ( 'f', 'full', 'full (75*54)' )                     THEN 'Full'
          WHEN LOWER(a.size) IN ( 's', 'single' )                                   THEN 'Single'
          WHEN LOWER(a.size) IN ( 'txl', 'twin-xl', 'twin xl' )                     THEN 'Twin XL'
          WHEN LOWER(a.size) = 'sq'                                                 THEN 'Short Queen'
          WHEN LOWER(a.size) IN ( 'q', 'queen (u.s. standard)', 'queen' )           THEN 'Queen'
          WHEN LOWER(a.size) = 'nt'                                                 THEN 'Narrow Twin'
          WHEN REGEXP_CONTAINS(a.title, r'\b(cal(?:ifornia)?\s*king|cal\s*king)\b') THEN 'Cal King'
          WHEN REGEXP_CONTAINS(a.title, r'\bshort\s*queen\b')                       THEN 'Short Queen'
          WHEN REGEXP_CONTAINS(a.title, r'\bnarrow\s*twin\b')                       THEN 'Narrow Twin'
          WHEN REGEXP_CONTAINS(a.title, r'\btwin[\s-]*xl\b')                        THEN 'Twin XL'
          WHEN REGEXP_CONTAINS(a.title, r'\bqueen\b|\bqeen\b')                      THEN 'Queen'
          WHEN REGEXP_CONTAINS(a.title, r'\bking\b')                                THEN 'King'
          WHEN REGEXP_CONTAINS(a.title, r'\bfull\b')                                THEN 'Full'
          WHEN REGEXP_CONTAINS(a.title, r'\btwin\b')                                THEN 'Twin'
          WHEN REGEXP_CONTAINS(a.title, r'\bsingle\b')                              THEN 'Single'
          ELSE a.size
      END AS size

    , box_type

    , DATE((
        SELECT MIN(d)
        FROM UNNEST([listed_since_utc, release_date_utc, tracking_since_utc]) AS d
    )) AS first_date -- least_release_date
    , if(
            FORMAT_DATE('%Y', DATE((SELECT MIN(d) FROM UNNEST([listed_since_utc, release_date_utc, tracking_since_utc]) AS d))) < '2023'
            , '2022b'
            , FORMAT_DATE('%Y', DATE((SELECT MIN(d) FROM UNNEST([listed_since_utc, release_date_utc, tracking_since_utc]) AS d)))
      )  as first_year -- least_release_year

--     , FORMAT_DATE('%Y', date(COALESCE(b.listed_since_utc, b.release_date_utc, b.tracking_since_utc))) as main_release_year
--     , FORMAT_DATE('%Y%m', date(COALESCE(b.listed_since_utc, b.release_date_utc, b.tracking_since_utc))) as main_release_year_month
--     , date(COALESCE(b.listed_since_utc, b.release_date_utc, b.tracking_since_utc)) as main_release_date

--     , b.listed_since_utc
--     , b.release_date_utc
--     , b.tracking_since_utc

FROM
    cte_with_meta a
         left join tmp.kp_mst_for_stck_sales_analysis_of_target_brands b
            on a.asin = b.asin
;



-- check ---------------------------------------------------------------------------------------------------------------

-- check distinct values (cardinality / distribution)
select DISTINCT inch from wook.stck_sales_analysis_of_target_brands;
select DISTINCT size from wook.stck_sales_analysis_of_target_brands;
select DISTINCT brand from wook.stck_sales_analysis_of_target_brands;
-- brand
-- ZINUS
-- NOVILLA
-- FDW
-- EGOHOME
select DISTINCT asin, title from wook.stck_sales_analysis_of_target_brands where size='OTHERS';
select DISTINCT asin, title, inch from wook.stck_sales_analysis_of_target_brands where size='OTHERS';
select DISTINCT asin, title, size from wook.stck_sales_analysis_of_target_brands where inch='OTHERS';
select * from wook.stck_sales_analysis_of_target_brands where asin='B0FJYJRJMN';


select DISTINCT inch from wook.stck_sales_analysis_of_target_brands;
select DISTINCT size from wook.stck_sales_analysis_of_target_brands;


-- sales check (mart)
SELECT
    brand
    , year
    , if(grouping(first_year) = 1, 'Total', first_year) as release_year
--     , sales_yr_month
    , sum(sales) as sales
    , sum(units) as unints
FROM
    wook.stck_sales_analysis_of_target_brands
GROUP BY
--     1,2,3
    GROUPING SETS ((1,2,first_year), (1,2))
ORDER BY
    1, 2, first_year
;
-- sales,unints
-- 23899892.18999995,193949

-- sales check (stackline - EGOHOME)
SELECT
    sum(RetailSales)
    , sum(UnitsSold)
FROM
    stck.atlas_sales_all a
        LEFT JOIN meta.amazon_mattress_master b
            ON a.RetailerSku = b.asin
WHERE
    a.WeekEnding >= '2026-01-01'
    AND b.category IS NOT NULL
    AND REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','') = 'EGOHOME'
--             IN ( 'FDW', 'EGOHOME', 'NOVILLA', 'ZINUS' )
;
-- f0_,f1_
-- 23899892.19000001,193949

-- sales check (mart - EGOHOME, NOVILLA)
SELECT
    brand
    , yr_month
    --     , sales_yr_month
    , cast(sum(sales) as numeric) as sales
    , cast(sum(units) as numeric) as unints
FROM
    wook.stck_sales_analysis_of_target_brands
WHERE
    brand IN ( 'NOVILLA', 'EGOHOME' )
GROUP BY
        1,2
ORDER BY
    1, 2
;

-- sales check (stackline - EGOHOME, NOVILLA)
SELECT
    REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','')
    , FORMAT_DATE('%Y-%m', WeekEnding)
    , cast(sum(RetailSales) as NUMERIC) as stck_sales
    , cast(sum(UnitsSold) as numeric) as stck_units
FROM
    stck.atlas_sales_all a
        INNER JOIN meta.amazon_mattress_master b
            ON a.RetailerSku = b.asin
WHERE
    a.WeekEnding >= '2023-01-01'
    AND b.category IS NOT NULL
    AND REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','') IN ( 'NOVILLA', 'EGOHOME' )
GROUP BY
    1, 2
ORDER BY
    1, 2
;

-- created mart table check
select * from wook.stck_sales_analysis_of_target_brands TABLESAMPLE SYSTEM ( 1 PERCENT  );

-- release date check
with cte as (
    SELECT
        asin

        , release_date_utc
        , listed_since_utc
        , tracking_since_utc
        , date(COALESCE(listed_since_utc, release_date_utc)) as main_release_date
--         , date(LEAST(listed_since_utc, release_date_utc, tracking_since_utc)) as least_release_date
        , DATE((
            SELECT MIN(d)
            FROM UNNEST([listed_since_utc, release_date_utc, tracking_since_utc]) AS d
        )) AS least_release_date
    FROM
        tmp.kp_mst_for_stck_sales_analysis_of_target_brands
    GROUP BY
        1, 2, 3, 4
)
SELECT
    *
FROM
    cte
WHERE
    release_date_utc IS NOT NULL
    AND listed_since_utc IS NULL
--     and release_date_utc < listed_since_utc
--     and date(release_date_utc) <= date(listed_since_utc)
;
-- select asin from cte GROUP BY 1 having count(1) > 1;

select count(1) from tmp.kp_mst_for_stck_sales_analysis_of_target_brands where listed_since_utc is not null;
select count(1) from tmp.kp_mst_for_stck_sales_analysis_of_target_brands where release_date_utc is not null;

-- mattress master check
select * from (
    -- pi Master
    SELECT DISTINCT
        TRIM(a.asin) AS asin
        , a.financial_category as category
        , 1 as ord
    FROM
        meta.amz_zinus_master_pdt_pi_enriched a
    WHERE
        a.financial_category in ('Foam Mattresses', 'Spring Mattresses')

    UNION DISTINCT

    -- GPT Mattress Master
    SELECT DISTINCT
        a.asin
        , a.category
        , 2 as ord
    FROM
        meta.amazon_mattress_master a
)
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY asin ORDER BY category is null, ord) = 1;

select count(DISTINCT asin) from wook.stck_sales_analysis_of_target_brands;
-- 1845

-- category duplicated check
SELECT
    asin
FROM
    meta.amazon_mattress_master
GROUP BY
    asin
HAVING
    COUNT(DISTINCT category) > 1
;


select * from wook.stck_sales_analysis_of_target_brands where asin='B08L3QLW8G';

-- seller type check (1p, 3p)
select DISTINCT asin, buyboxsellerid, buyboxisamazon from tmp.kp_seller_hist_for_stck_sales_analysis_of_target_brands order by 1;
select DISTINCT asin from tmp.kp_seller_hist_for_stck_sales_analysis_of_target_brands order by 1;
select asin, count(DISTINCT buyboxsellerid), count(DISTINCT buyBoxIsAmazon) from tmp.kp_seller_hist_for_stck_sales_analysis_of_target_brands GROUP BY 1 HAVING COUNT(asin) > 1;
select DISTINCT buyBoxIsAmazon from tmp.kp_seller_hist_for_stck_sales_analysis_of_target_brands where asin='B01NBOWH4W';

-- data volume check ---------------------------------------------------------------------------------------------------
WITH
    cte_stck AS (
        SELECT
            REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') as brand
            , FORMAT_DATE('%Y-%m', WeekEnding) as yr_month
            , FORMAT_DATE('%Y', WeekEnding) as yr
            , RetailerSku
            , sum(RetailSales) as sales
        FROM
            stck.atlas_sales_all
        WHERE
            WeekEnding >= '2023-01-01'
            AND SubCategory = 'Mattresses'
            AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN ( 'FDW', 'EGOHOME', 'NOVILLA', 'ZINUS', 'MLILY' )
        GROUP BY 1,2,3,4
    )
SELECT
    brand
    , yr
    , count(DISTINCT RetailerSku) as asin_count
    , count(1) as row_count
    , cast(sum(sales) as NUMERIC) as sales
FROM
    cte_stck
GROUP BY 1, 2
ORDER BY 2, 1;
