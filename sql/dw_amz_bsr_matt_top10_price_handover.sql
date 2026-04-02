-- SELECT
--     *
-- FROM
--     ods.keepa_amz_bsr_top10_price_monitoring
-- QUALIFY
--     RANK() OVER (PARTITION BY init_date ORDER BY load_datetime DESC) = 1
-- --     ROW_NUMBER() OVER (PARTITION BY asin, bsr_date ORDER BY load_datetime DESC) = 1
-- ;

-- drop table ods.keepa_amz_bsr_top10_price_monitoring;
CREATE OR REPLACE TABLE dw.amz_bsr_matt_top10_price AS
WITH
    cte_src AS (
        SELECT
            *
        FROM
            (
                SELECT
                    bsr_date, asin, parent_asin, meta_brand, brand, type, size, style, model, material, title, item_height, item_length, item_type_keyword, item_weight, item_width, package_height, package_length, package_quantity, package_weight, package_width, COALESCE(lower(is_bsr_asin), 'UNKNOWN') as is_bsr_asin, buybox_price, list_price, buybox_shipping_price, sales_rank, sales_rank_category_id, sales_rank_category_name, load_datetime
                FROM ods.keepa_amz_bsr_top10_price_monitoring

                UNION ALL

                SELECT
                    bsr_date
                    , asin
                    , parent_asin
                    , meta_brand
                    , brand
                    , type
                    , size
                    , style
                    , model
                    , material
                    , title
                    , item_height
                    , item_length
                    , item_type_keyword
                    , item_weight
                    , item_width
                    , package_height
                    , package_length
                    , package_quantity
                    , package_weight
                    , package_width
                    , COALESCE(lower(CAST(is_bsr_asin AS STRING)), 'UNKNOWN') AS is_bsr_asin
                    , CAST(buybox_price AS STRING)
                    , CAST(list_price AS STRING)
                    , CAST(buybox_shipping_price AS STRING)
                    , sales_rank
                    , sales_rank_category_id
                    , sales_rank_category_name
                    , load_datetime
                FROM dw.amz_bsr_matt_top10_price_2025
            )
             o
        WHERE
            (
                REGEXP_REPLACE(COALESCE(UPPER(brand), 'UNKNOWN'), r'[^[:print:]]','') != 'ZINUS'
                OR EXISTS (
                    SELECT 1
                    FROM meta.amz_zinus_master_pdt_pi_add_new_col m
                    WHERE TRIM(o.asin) = m.asin AND UPPER(m.collection) LIKE '%WONDER%'
                )
           )
          AND lower(style) not like '%topper%'
        QUALIFY
            RANK() OVER (PARTITION BY bsr_date ORDER BY load_datetime DESC) = 1
    )
-- select * from cte_src where asin='B09HZHS4BY';
SELECT
    COALESCE(
            SAFE_CAST(REGEXP_EXTRACT(LOWER(style), r'(\d+(?:\.\d+)?)\s*(?:"|”|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)') AS FLOAT64)
            , SAFE_CAST(REGEXP_EXTRACT(LOWER(title), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)') AS FLOAT64)
            , SAFE_CAST(REGEXP_EXTRACT(LOWER(size), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)') AS FLOAT64)
            , -1
    ) as final_inch
    --     , size as final_size
    --     , style
    --     , title
    ,
    CASE
        WHEN LOWER(size) IN ('ck', 'california king', 'cal king') THEN 'Cal King'
        WHEN LOWER(size) = '12 inch queen medium firm' THEN 'Queen'
        WHEN LOWER(size) IN ('k', 'king (u.s. standard)', 'king') or lower(size) like '%king%' THEN 'King'
        WHEN LOWER(size) IN ('t', 'twin', 'twin (75*38)', 'twin(small box)', 'twin size', 'tw"', 'twin(new)', 'twin (small box)', 'twin (u.s. standard)') THEN 'Twin'
        WHEN LOWER(size) IN ('f', 'full', 'full (75*54)') or lower(size) like '%full%'  THEN 'Full'
        WHEN LOWER(size) IN ('s', 'single') THEN 'Single'
        WHEN LOWER(size) IN ('small single') THEN 'Small Single'
        WHEN LOWER(size) IN ('txl', 'twin-xl', 'twin xl', 'twin xl (small box)') THEN 'Twin XL'
        WHEN LOWER(size) in ('sq', 'short queen') THEN 'Short Queen'
        WHEN LOWER(size) IN ('q', 'queen (u.s. standard)', 'queen', 'queen (small box)') or lower(size) like 'queen%' THEN 'Queen'
        WHEN LOWER(size) IN ('nt', '75" x 30"') THEN 'Narrow Twin'
        ELSE
            CASE
                WHEN LOWER(style) IN ('ck', 'california king', 'cal king') THEN 'Cal King'
                WHEN LOWER(style) = '12 inch queen medium firm' THEN 'Queen'
                WHEN LOWER(style) IN ('k', 'king (u.s. standard)', 'king') THEN 'King'
                WHEN LOWER(style) IN ('t', 'twin', 'twin (75*38)', 'twin(small box)') THEN 'Twin'
                WHEN LOWER(style) IN ('f', 'full', 'full (75*54)') THEN 'Full'
                WHEN LOWER(style) IN ('s', 'single') THEN 'Single'
                WHEN LOWER(style) IN ('txl', 'twin-xl', 'twin xl', 'twin xl (small box)') THEN 'Twin XL'
                WHEN LOWER(style) in ('sq', 'short queen') THEN 'Short Queen'
                WHEN LOWER(style) IN ('q', 'queen (u.s. standard)', 'queen') THEN 'Queen'
                WHEN LOWER(style) IN ('nt', '75" x 30"') THEN 'Narrow Twin'
                ELSE 'UNKNOWN'
            END
          END AS final_size
    , REGEXP_REPLACE(COALESCE(UPPER(brand), 'UNKNOWN'), r'[^[:print:]]','') as brand
    , * EXCEPT (brand)
FROM
    cte_src
;

-- select is_bsr_asin, count(1) from ods.keepa_amz_bsr_top10_price_monitoring GROUP BY is_bsr_asin;
-- select is_bsr_asin, count(1) from dw.amz_bsr_matt_top10_price GROUP BY is_bsr_asin;

-- SELECT
--     *
-- FROM
--     ods.keepa_amz_bsr_top10_price_monitoring
-- WHERE
--     is_bsr_asin IS NULL
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY asin, init_date ORDER BY load_datetime DESC) = 1
-- ;

-- select * from ods.keepa_amz_bsr_top10_price_monitoring where bsr_date = '2026-03-04' and asin='B00Q7EPSHI';

--     and asin='B00Q7EPSHI'
-- qualify row_number() over (partition by asin order by is_bsr_asin desc) = 1

-- ods count check
-- SELECT
--     bsr_date
--     , COUNT(DISTINCT asin)
-- FROM
--     dw.amz_bsr_matt_top10_price
-- GROUP BY
--     1
-- ORDER BY
--     1 DESC
-- ;

-- inch, size check
-- WITH
--     cte_src AS (
--         SELECT
--             *
--         FROM
--             ods.keepa_amz_bsr_top10_price_monitoring
--         QUALIFY
--             RANK() OVER (PARTITION BY init_date ORDER BY load_datetime DESC) = 1
--     )
-- SELECT
--     DISTINCT
--     COALESCE(
--             SAFE_CAST(REGEXP_EXTRACT(LOWER(style), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)') AS FLOAT64)
--         , SAFE_CAST(REGEXP_EXTRACT(LOWER(title), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)') AS FLOAT64)
--         , -1
--     ) as final_inch
--         , style
--         , size
--         , asin
--     --     , title
--     ,
--     CASE
--         WHEN LOWER(size) IN ('ck', 'california king', 'cal king') THEN 'Cal King'
--         WHEN LOWER(size) = '12 inch queen medium firm' THEN 'Queen'
--         WHEN LOWER(size) IN ('k', 'king (u.s. standard)', 'king') or lower(size) like 'king%' THEN 'King'
--         WHEN LOWER(size) IN ('t', 'twin', 'twin (75*38)', 'twin(small box)') THEN 'Twin'
--         WHEN LOWER(size) IN ('f', 'full', 'full (75*54)') or lower(size) like 'full%'  THEN 'Full'
--         WHEN LOWER(size) IN ('s', 'single') THEN 'Single'
--         WHEN LOWER(size) IN ('txl', 'twin-xl', 'twin xl', 'twin xl (small box)') THEN 'Twin XL'
--         WHEN LOWER(size) in ('sq', 'short queen') THEN 'Short Queen'
--         WHEN LOWER(size) IN ('q', 'queen (u.s. standard)', 'queen') THEN 'Queen'
--         WHEN LOWER(size) IN ('nt', '75" x 30"') THEN 'Narrow Twin'
--         ELSE
--             CASE
--                 WHEN LOWER(style) IN ('ck', 'california king', 'cal king') THEN 'Cal King'
--                 WHEN LOWER(style) = '12 inch queen medium firm' THEN 'Queen'
--                 WHEN LOWER(style) IN ('k', 'king (u.s. standard)', 'king') THEN 'King'
--                 WHEN LOWER(style) IN ('t', 'twin', 'twin (75*38)', 'twin(small box)') THEN 'Twin'
--                 WHEN LOWER(style) IN ('f', 'full', 'full (75*54)') THEN 'Full'
--                 WHEN LOWER(style) IN ('s', 'single') THEN 'Single'
--                 WHEN LOWER(style) IN ('txl', 'twin-xl', 'twin xl', 'twin xl (small box)') THEN 'Twin XL'
--                 WHEN LOWER(style) in ('sq', 'short queen') THEN 'Short Queen'
--                 WHEN LOWER(style) IN ('q', 'queen (u.s. standard)', 'queen') THEN 'Queen'
--                 WHEN LOWER(style) IN ('nt', '75" x 30"') THEN 'Narrow Twin'
--                 ELSE 'UNKNOWN'
--             END
--     END AS final_size
--     , REGEXP_REPLACE(COALESCE(UPPER(brand), 'UNKNOWN'), r'[^[:print:]]','') as brand
-- from cte_src;

-- -- mart
-- SELECT
--     t.*
-- FROM
--     dw.amz_bsr_matt_top10_price t
-- WHERE
--     bsr_date = '2026-03-04'
-- ORDER BY
--     final_inch, CASE final_size
--                     WHEN 'Twin'        THEN 1
--                     WHEN 'Twin XL'     THEN 2
--                     WHEN 'Narrow Twin' THEN 2.5
--                     WHEN 'Full'        THEN 3
--                     WHEN 'Queen'       THEN 4
--                     WHEN 'Short Queen' THEN 4.5
--                     WHEN 'King'        THEN 5
--                     WHEN 'Cal King'    THEN 6
--                 END
--     , CASE brand
--           WHEN 'ZINUS'   THEN '1'
--           WHEN 'EGOHOME' THEN '2'
--           WHEN 'NOVILLA' THEN '3'
--           WHEN 'FDW'     THEN '4'
--           WHEN 'MLILY'   THEN '5'
--           ELSE brand
--       END
--    , sales_rank
--    , buybox_price
-- ;