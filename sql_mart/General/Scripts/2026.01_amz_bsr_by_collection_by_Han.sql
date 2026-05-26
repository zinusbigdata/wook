/*
 * Zinus Collection 단위 Amazon BSR 점유율
 * Writer : Nam Sik Han
 */

WITH
    cte_brand_list   AS (
        SELECT DISTINCT
            asin
            , REGEXP_REPLACE(COALESCE(UPPER(brand), 'No Brand Info'), r'[^[:print:]]', '') AS brand
        FROM
            dw.rf_amz_pdt_zns_comp_daily

        UNION
        DISTINCT

        SELECT DISTINCT
            request_asin AS asin
            , REGEXP_REPLACE(COALESCE(UPPER(brand), 'No Brand Info'), r'[^[:print:]]', '') AS brand
        FROM
            dw.rf_amz_pdt_zns_comp_daily
    ) 
    , cte_zinus_asis AS (
        SELECT DISTINCT asin
        FROM cte_brand_list
        WHERE brand LIKE '%ZINUS%'
    )
    select * from cte_zinus_asis 
    , cte_bsr_src               AS (
        SELECT
            IF(fullCategory = 'Any Department > Home & Kitchen > Furniture > Bedroom Furniture > Mattresses & Box Springs > Mattresses', TRUE, FALSE) AS is_mattress
            , bestsellers_rank AS rank
            , bestsellers_asin AS asin
            , DATE(initialTime) AS bsr_date
            , FORMAT_DATE('%yY_%QQ', DATE(initialTime)) AS yr_qt
            , FORMAT_DATE('%Y', DATE(initialTime)) AS bsr_year
        FROM
            rfapi.rf_amz_bsr_hourly
        WHERE
            bestsellers_rank <= 100
            and DATE(initialTime) between '2024-01-01' and '2025-12-31'
        QUALIFY
            ROW_NUMBER() OVER (PARTITION BY fullCategory, DATE (initialTime), bestsellers_rank ORDER BY initialTime DESC) = 1
    )
    , cte_collection_merge   AS (
        SELECT
            a.asin
            , COALESCE(b.collection_name, NULLIF(a.collection, 'nan')) AS collection
            , NULLIF(a.collection, 'nan') AS pi_collection
            , b.collection_name AS glb_collection
            , IF(a.financial_category IN ( 'Foam Mattresses', 'Spring Mattresses' ), TRUE, FALSE) AS is_mattress
        FROM
            meta.amz_zinus_master_pdt_pi a
                LEFT JOIN wook.global_sku_master b
                    ON a.zinus_sku = b.zinus_sku
        --                         AND b.sales_channel = 'AMAZON DI'
        --                         AND b.sales_channel like '%AMAZON%'
        --                         AND b.v_w = FALSE
        --         WHERE
        --             b.sales_channel = 'AMAZON DI'
        --             AND b.v_w = FALSE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY a.asin
            ORDER BY
                IF(collection_name IS NOT NULL, 1, 2)
                , CASE
                      WHEN sales_channel = 'AMAZON DI' THEN 1
                      WHEN remark like 'Tracy' THEN 2
                      WHEN sales_channel like '%AMAZON%' THEN 3
                      ELSE 4
                  END
                , CASE
                      WHEN b.v_w = FALSE
                          THEN 1
                      WHEN b.v_w IS NULL
                          THEN 2
                      ELSE 3
                  END
            ) = 1
        --         select sales_channel from wook.global_sku_master GROUP BY 1;
    )
    , cte_adj_collection as (
        SELECT
            a.* EXCEPT (collection)
            , COALESCE(b.glb_collection, a.collection) AS collection
        FROM
            cte_collection_merge a
                LEFT JOIN (
                SELECT '2in MGT w WonderBox' AS pi_collection,	'MGT' AS glb_collection
                UNION ALL
                SELECT '3in MGT w WonderBox'	,	'MGT'
                UNION ALL
                SELECT 'Aidan Sling Chair, 2PK'	,	'Aidan'
                UNION ALL
                SELECT 'BIFD 5in'	,	'Cool Grey BIFD'
                UNION ALL
                SELECT 'Justina, 16in'	,	'Justina'
                UNION ALL
                SELECT 'Quinn Sleeper Sofa'	,	'Quinn'
                UNION ALL
                SELECT 'SB w Bamboo Slats'	,	'Bamboo Slat SB'
            ) b
                    ON a.collection = b.pi_collection
    )
    , cte_add_collection      AS (
        SELECT
            f.*
            -- zinus 제품 인 경우 collection 정보 추가, OTHERS = master 에 asin 은 존재하나 collection 없는 케이스, MISSING_COLLECTION = master 에 누락된 케이스
            , IF(z.asin IS NOT NULL OR m.asin IS NOT NULL, COALESCE(m.collection, if(z.asin is not null, 'MISSING_COLLECTION', 'OTHERS')), null) as collection
            , IF(z.asin IS NOT NULL OR m.asin IS NOT NULL, TRUE, FALSE) AS is_zinus
            , IF(f.rank <= 20, TRUE, FALSE) AS is_top20
            , IF(f.rank <= 100, TRUE, FALSE) AS is_top100
            , IF(f.rank BETWEEN 21 AND 100, TRUE, FALSE) AS is_top21_100
        FROM
            cte_bsr_src f
                LEFT JOIN cte_adj_collection m
                    ON f.asin = m.asin
                LEFT JOIN cte_zinus_asis z
                    ON f.asin = z.asin
    )
    , cte_add_rank_level      AS (
        SELECT
            * EXCEPT (is_top20, is_top100, is_top21_100)
            , 'Top20' AS rank_level
        FROM
            cte_add_collection
        WHERE
            is_top20 = TRUE

        UNION ALL

        SELECT
            * EXCEPT (is_top20, is_top100, is_top21_100)
            , 'Top100' AS rank_level
        FROM
            cte_add_collection
        WHERE
            is_top100 = TRUE

        UNION ALL

        SELECT
            * EXCEPT (is_top20, is_top100, is_top21_100)
            , 'Top21_100' AS rank_level
        FROM
            cte_add_collection
        WHERE
            is_top21_100 = TRUE
    )
    , cte_count_by_collection AS (
        SELECT
            yr_qt
            , collection
            , rank_level
            , is_zinus
            , is_mattress
            , COUNT(1) AS number_of_bsr_listings
        FROM
            cte_add_rank_level
        GROUP BY 1, 2, 3, 4, 5
    )
    , cte_add_total_count as (
        SELECT
            *
            , SUM(number_of_bsr_listings) OVER (PARTITION BY yr_qt, is_mattress, rank_level ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_count
        FROM cte_count_by_collection
    )
    , cte_final as (
        SELECT
            IF(a.is_mattress = TRUE, 'Mattress', 'Non-Mattress') AS category_division
            , rank_level
            , yr_qt
            , SUM(number_of_bsr_listings) AS number_of_bsr_listings
            , COUNT(DISTINCT collection) AS number_of_collections
            , ANY_VALUE(total_count) AS total_count
            , SUM(number_of_bsr_listings) / ANY_VALUE(total_count) AS market_share
        FROM
            cte_add_total_count a
        WHERE
            is_zinus = TRUE
        GROUP BY 1, 2, 3
    )
SELECT
    *
FROM
    cte_final
        PIVOT (
        SUM(number_of_collections) AS number_of_collections
            , SUM(number_of_bsr_listings) AS number_of_bsr_listings
            , SUM(total_count) AS total_count
            , SUM(market_share) AS market_share
        FOR yr_qt IN ('24Y_1Q', '24Y_2Q', '24Y_3Q', '24Y_4Q', '25Y_1Q', '25Y_2Q', '25Y_3Q', '25Y_4Q') )
ORDER BY
    category_division, CASE
                           WHEN rank_level = 'Top20'  THEN 1
                           WHEN rank_level = 'Top100' THEN 2
                           ELSE 3
                       END