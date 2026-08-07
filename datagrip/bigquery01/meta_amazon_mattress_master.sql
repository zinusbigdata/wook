/*
 * Stackline 메트리스 - 2024년 이후 asin에 대해서 인치/사이즈, 카테고리, first_date 붙이기
 */

-- stackline Mattresses 카테고리 기준으로 정리한 마스터
CREATE OR REPLACE TABLE meta.amazon_mattress_master_by_stackline AS
WITH
    cte_mattress_pi_mst  AS (
        SELECT
            asin
            , category
            -- , if(category = 'Spring Mattress', IF(LOWER(TRIM(subcategory)) LIKE '%bonnell%', 'Bonnell', TRIM(subcategory)), null) AS spring_type
            , subcategory

            , profile
            , size
        FROM
            meta.amz_comp_mst_bubble_pi
        WHERE
            category in ('Spring Mattress', 'Foam Mattress')
    )
    , cte_mattress_gpt_mst as (
        SELECT
            asin
            , nullif(category, 'null') as category
            , nullif(subcategory, 'null') as subcategory
            , nullif(profile, 'null') as profile
            , nullif(size, 'null') as size
        -- EXCEPT (ord)
        FROM (
            -- ZNS-2183 / tmp.amz_stck_cate_by_gpt_250715 / 2024-01-01 ~ 2025-07-15
            SELECT asin, category, subcategory, profile, size, 1 ord from tmp.amz_stck_cate_by_gpt_250715 WHERE
                category IN ( 'Spring Mattress', 'Foam Mattress' )
            UNION ALL
            SELECT asin, category, subcategory, profile, size, 2 from tmp.amz_stck_cate_by_gpt_250716 WHERE
                category IN ( 'Spring Mattress', 'Foam Mattress' )
            UNION ALL
            SELECT asin, category, subcategory, profile, size, 3 from tmp.amz_stck_cate_by_gpt_250717 WHERE
                category IN ( 'Spring Mattress', 'Foam Mattress' )

            UNION ALL
            SELECT asin, category, subcategory, profile, size, 5 from tmp.amz_stck_cate_matt_by_gpt_250801 WHERE
                category IN ( 'Spring Mattress', 'Foam Mattress' )

            UNION ALL

            SELECT
                asin
                , gpt_category
                , gpt_subcategory
                , CAST(profile AS STRING)
                , size
                , 6
            FROM
                tmp.amz_stck_cate_matt_by_gpt_260312
            WHERE
                gpt_category IN ( 'Spring Mattress', 'Foam Mattress' )

            -- ZNS-2721 / 2023년 mattress asins 추가
            UNION ALL

            SELECT
                asin
                , gpt_category
                , gpt_subcategory
                , CAST(profile AS STRING)
                , size
                , 6
            FROM
                tmp.amz_stck_cate_matt_by_gpt_260406
            WHERE
                gpt_category IN ( 'Spring Mattress', 'Foam Mattress' )

            -- ZNS-2850 / 2026년 4월 데이터 추가 (2026-04-01 ~ 2026-05-02)
            UNION ALL

            SELECT
                asin
                , gpt_category
                , gpt_subcategory
                , CAST(profile AS STRING)
                , size
                , 6
            FROM
                tmp.amz_stck_cate_matt_by_gpt_260518
            WHERE
                gpt_category IN ( 'Spring Mattress', 'Foam Mattress' )


            -- ZNS-3063 / 2026년 7월 데이터 추가 (2026-05-03 ~ 2026-07-21)
            UNION ALL

            SELECT
                asin
                , gpt_category
                , gpt_subcategory
                , CAST(profile AS STRING)
                , size
                , 6
            FROM
                tmp.amz_stck_cate_matt_by_gpt_260721
            WHERE
                gpt_category IN ( 'Spring Mattress', 'Foam Mattress' )


            -- nrr mattress master 추가
            UNION ALL
            SELECT
                asin
                , category
                , subcategory
                , profile
                , size
                , 4
            FROM
                tmp.amz_stck_cate_nrr_matt_by_gpt_250730
            WHERE
                category IN ( 'Spring Mattress', 'Foam Mattress' )
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY asin ORDER BY ord DESC) = 1
    )
    , cte_stackline as (
        with cte_stck_filter as (
            SELECT RetailerSku
            FROM stck.atlas_sales_all
            WHERE SubCategory = 'Mattresses' AND WeekEnding >= '2023-01-01'
        )
        SELECT
            a.RetailerSku as asin
            , MIN(WeekEnding) as first_date
        FROM
            stck.atlas_sales_all a
                join cte_stck_filter b
                    on a.RetailerSku = b.RetailerSku
        WHERE
            SubCategory = 'Mattresses'
        GROUP BY 1
    )
SELECT
    DISTINCT
        a.asin
        , if(b.asin is not null, trim(b.category), trim(a.category)) AS category
        , if(b.asin is not null, trim(b.subcategory), trim(a.subcategory)) AS subcategory
        , if(b.asin is not null, 'PI', "PDP") AS mst_src
        , COALESCE(if(b.asin is not null, trim(b.profile), trim(a.profile)), 'OTHERS') AS profile
        , if(b.asin is not null, trim(b.size), trim(a.size)) AS size

        , COALESCE(c.size_label, d.size, 'OTHERS') AS size_adj
        , coalesce(e.first_date, f.first_date) as first_date
        , e.* EXCEPT (asin, first_date)
FROM
    -- tmp1.amazon_mattress_pdp_master a
    -- tmp.amz_stck_cate_by_gpt_250715 a
    cte_mattress_gpt_mst a
        LEFT JOIN cte_mattress_pi_mst b
            ON a.asin = b.asin
        LEFT JOIN meta.amz_pdt_mst_size_label_mapping c
            ON cast(REGEXP_REPLACE(LOWER(REGEXP_REPLACE(if(b.asin is not null, trim(b.size), trim(a.size)), '"|”|\'\'', '')), r'\s*x\s*', 'x') as string) = c.size_key

        -- for size_adj
        LEFT JOIN (
            SELECT
                *
            FROM
                tmp.amz_stck_cate_matt_by_gpt_260312
            QUALIFY
                ROW_NUMBER() OVER (PARTITION BY asin ORDER BY CASE
                                                                  WHEN profile IS NOT NULL AND size IS NOT NULL THEN 1
                                                                  WHEN profile IS NOT NULL OR size IS NOT NULL  THEN 2
                                                                  ELSE 3
                                                              END
                    ) = 1
        ) d
            ON a.asin = d.asin
        LEFT JOIN tmp.amazon_mattress_first_date_master e
            ON a.asin = e.asin

        -- filter stackline 2023
        JOIN
            cte_stackline f
                on a.asin = f.asin
WHERE
    NOT EXISTS (select 1 from meta.amz_comp_mst_bubble_pi c where category not in ('Spring Mattress', 'Foam Mattress') and a.asin = c.asin)

-- and a.asin='B08XMF8KR8'
;


--CREATE OR REPLACE TABLE meta.amazon_mattress_master AS
CREATE OR REPLACE TABLE wook.amazon_mattress_master AS
WITH cte_mattress_pi_mst AS (
    SELECT asin
        , category
        -- , if(category = 'Spring Mattress', IF(LOWER(TRIM(subcategory)) LIKE '%bonnell%', 'Bonnell', TRIM(subcategory)), null) AS spring_type
        , subcategory
        , profile
        , size
    FROM meta.amz_comp_mst_bubble_pi
    WHERE category IN ('Spring Mattress', 'Foam Mattress')
)
, cte_mattress_gpt_mst AS (
    SELECT asin
        , NULLIF(category, 'null')    AS category
        , NULLIF(subcategory, 'null') AS subcategory
        , NULLIF(profile, 'null')     AS profile
        , NULLIF(size, 'null')        AS size
        -- EXCEPT (ord)
    FROM (
        -- ZNS-2183 / tmp.amz_stck_cate_by_gpt_250715 / 2024-01-01 ~ 2025-07-15
        SELECT asin, category, subcategory, profile, size, 1 ord
        FROM tmp.amz_stck_cate_by_gpt_250715
        WHERE category IN ('Spring Mattress', 'Foam Mattress')
        UNION ALL
        SELECT asin, category, subcategory, profile, size, 2
        FROM tmp.amz_stck_cate_by_gpt_250716
        WHERE category IN ('Spring Mattress', 'Foam Mattress')
        UNION ALL
        SELECT asin, category, subcategory, profile, size, 3
        FROM tmp.amz_stck_cate_by_gpt_250717
        WHERE category IN ('Spring Mattress', 'Foam Mattress')
        UNION ALL
        SELECT asin, category, subcategory, profile, size, 5
        FROM tmp.amz_stck_cate_matt_by_gpt_250801
        WHERE category IN ('Spring Mattress', 'Foam Mattress')
        UNION ALL
        SELECT asin
        , gpt_category
        , gpt_subcategory
        , CAST(profile AS STRING)
        , size
        , 6
        FROM tmp.amz_stck_cate_matt_by_gpt_260312
        WHERE gpt_category IN ('Spring Mattress', 'Foam Mattress')

        -- ZNS-2721 / 2023년 mattress asins 추가
        UNION ALL

        SELECT asin
        , gpt_category
        , gpt_subcategory
        , CAST(profile AS STRING)
        , size
        , 6
        FROM tmp.amz_stck_cate_matt_by_gpt_260406
        WHERE gpt_category IN ('Spring Mattress', 'Foam Mattress')

        -- ZNS-2850 / 2026년 4월 데이터 추가 (2026-04-01 ~ 2026-05-02)
        UNION ALL

        SELECT asin
        , gpt_category
        , gpt_subcategory
        , CAST(profile AS STRING)
        , size
        , 6
        FROM tmp.amz_stck_cate_matt_by_gpt_260518
        WHERE gpt_category IN ('Spring Mattress', 'Foam Mattress')


        -- ZNS-3063 / 2026년 7월 데이터 추가 (2026-05-03 ~ 2026-07-21)
        UNION ALL

        SELECT asin
        , gpt_category
        , gpt_subcategory
        , CAST(profile AS STRING)
        , size
        , 6
        FROM tmp.amz_stck_cate_matt_by_gpt_260721
        WHERE gpt_category IN ('Spring Mattress', 'Foam Mattress')

        -- nrr mattress master 추가
        UNION ALL
        SELECT asin
        , category
        , subcategory
        , profile
        , size
        , 4
        FROM tmp.amz_stck_cate_nrr_matt_by_gpt_250730
        WHERE category IN ('Spring Mattress', 'Foam Mattress')
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY asin ORDER BY ord DESC) = 1
)
SELECT
    DISTINCT COALESCE(b.asin, a.asin)                                              AS asin
    , IF(b.asin IS NOT NULL, TRIM(b.category), TRIM(a.category))                   AS category
    -- , COALESCE(b.category, a.category) AS category
    -- , if(b.asin is not null, b.spring_type, a.spring_type) AS spring_type
    , IF(b.asin IS NOT NULL, TRIM(b.subcategory), TRIM(a.subcategory))             AS subcategory
    -- , COALESCE(b.spring_type, a.spring_type) AS spring_type
    -- , COALESCE(b.brand, a.brand) AS brand
    , IF(b.asin IS NOT NULL, 'PI', "PDP")                                          AS mst_src

    , COALESCE(IF(b.asin IS NOT NULL, TRIM(b.profile), TRIM(a.profile)), 'OTHERS') AS profile
    , IF(b.asin IS NOT NULL, TRIM(b.size), TRIM(a.size))                           AS size

    -- , COALESCE(c.size_label, '기타') AS size_adj
    , COALESCE(c.size_label, d.size, 'OTHERS')                                     AS size_adj

-- , a.* EXCEPT (asin, category, spring_type)
-- , a.* EXCEPT (asin, category, subcategory, profile, size)
              , e.* EXCEPT (asin)
FROM
    -- tmp1.amazon_mattress_pdp_master a
    -- tmp.amz_stck_cate_by_gpt_250715 a
    cte_mattress_gpt_mst a
        FULL OUTER JOIN cte_mattress_pi_mst b
                        ON a.asin = b.asin
        LEFT JOIN meta.amz_pdt_mst_size_label_mapping c
                  ON CAST(REGEXP_REPLACE(
                          LOWER(REGEXP_REPLACE(IF(b.asin IS NOT NULL, TRIM(b.size), TRIM(a.size)), '"|”|\'\'', '')),
                          r'\s*x\s*', 'x') AS STRING) = c.size_key

        -- for size_adj
        LEFT JOIN (SELECT *
                   FROM tmp.amz_stck_cate_matt_by_gpt_260312
                   QUALIFY ROW_NUMBER() OVER (PARTITION BY asin ORDER BY CASE
                                                                             WHEN profile IS NOT NULL AND size IS NOT NULL
                                                                                 THEN 1
                                                                             WHEN profile IS NOT NULL OR size IS NOT NULL
                                                                                 THEN 2
                                                                             ELSE 3
                       END
                   ) = 1) d
                   ON a.asin = d.asin
        LEFT JOIN tmp.amazon_mattress_first_date_master e
                  ON COALESCE(b.asin, a.asin) = e.asin

WHERE NOT EXISTS (SELECT 1
                  FROM meta.amz_comp_mst_bubble_pi c
                  WHERE category NOT IN ('Spring Mattress', 'Foam Mattress')
                    AND COALESCE(a.asin, b.asin) = c.asin)
;


--- test -------------------------
SELECT count(*) FROM wook.amazon_mattress_master WHERE first_date is null;
SELECT count(DISTINCT asin) FROM meta.amazon_mattress_master;
SELECT COUNT(DISTINCT RetailerSku)
FROM stck.atlas_sales_all
WHERE SubCategory = 'Mattresses'
  AND FORMAT_DATE('%Y', WeekEnding) >= '2024';
SELECT count(DISTINCT asin), count(if(size is null, 1, NULL)) FROM meta.amazon_mattress_master_by_stackline;
SELECT asin, count(*), max(size) FROM meta.amazon_mattress_master_by_stackline group by 1 order by 2 desc;

