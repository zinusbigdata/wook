/*
 * Zinus Collection 단위 Amazon BSR 점유율 , ver 2
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
    , cte_zinus_asin_in_the_pdp_list AS (
        SELECT DISTINCT asin
        FROM cte_brand_list
        WHERE UPPER(brand) LIKE '%ZINUS%'
    )
    , cte_bsr_src               AS (
        SELECT
            IF(fullCategory = 'Any Department > Home & Kitchen > Furniture > Bedroom Furniture > Mattresses & Box Springs > Mattresses', TRUE, FALSE) AS is_mattress
--             , bestsellers_rank AS rank
            , rank
--             , bestsellers_asin AS asin
            , asin
            , DATE(initialTime) AS bsr_date
            , FORMAT_DATE('%yY_%QQ', DATE(initialTime)) AS yr_qt
            , FORMAT_DATE('%Y', DATE(initialTime)) AS bsr_year
--             , TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) as fullCategory
        FROM
--             rfapi.rf_amz_bsr_hourly
            rfapi.rf_amz_bsr_all
        WHERE
--             bestsellers_rank <= 100
            rank <= 100
--             and DATE(initialTime) between '2024-01-01' and '2025-12-31'
            and DATE(initialTime) between '2022-01-01' and '2025-12-31'
        QUALIFY
--             ROW_NUMBER() OVER (PARTITION BY fullCategory, DATE (initialTime), bestsellers_rank ORDER BY initialTime DESC) = 1
            ROW_NUMBER() OVER (PARTITION BY fullCategory, DATE (initialTime), rank ORDER BY initialTime DESC) = 1
--         select min(initialTime) from rfapi.rf_amz_bsr_all;
    )
    , cte_collection_merge   AS (
        SELECT
            a.asin
--             , COALESCE(b.collection_name, NULLIF(a.collection, 'nan')) AS collection
            , COALESCE(m.collection_name, COALESCE(NULLIF(b.collection_name, '#N/A'), NULLIF(a.collection, 'nan'))) AS collection
--             , NULLIF(a.collection, 'nan') AS pi_collection
            , COALESCE(NULLIF(m.collection_name, '#N/A'), NULLIF(a.collection, 'nan')) AS pi_collection
            , b.collection_name AS glb_collection
--             , IF(a.category IN ( 'Foam Mattresses', 'Spring Mattresses' ), TRUE, FALSE) AS is_mattress
            , a.financial_category as category
--             , b.category as category
        FROM
            meta.amz_zinus_master_pdt_pi a
                LEFT JOIN wook.global_sku_collection_mapping m
                    ON a.collection = m.collection

                LEFT JOIN wook.global_sku_master b
                    ON a.zinus_sku = b.zinus_sku
--
--                                 AND b.sales_channel = 'AMAZON DI'
--                                 AND b.sales_channel like '%AMAZON%'
--                                 AND b.v_w = FALSE
--                 WHERE
--                     b.sales_channel = 'AMAZON DI'
--                     AND b.v_w = FALSE

        QUALIFY ROW_NUMBER() OVER (PARTITION BY a.asin
            ORDER BY
--                 IF(collection_name IS NOT NULL, 1, 2)

                IF(COALESCE(m.collection_name, COALESCE(NULLIF(b.collection_name, '#N/A'), NULLIF(a.collection, 'nan'))) IS NOT NULL, 1, 2)
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
--    select * from cte_collection_merge where collection != glb_collection and glb_collection is not null;
--     , cte_adj_collection as (
--         SELECT
--             a.* EXCEPT (collection)
--             , COALESCE(b.glb_collection, a.collection) AS collection
--         FROM
--             cte_collection_merge a
--                 LEFT JOIN (
--                     SELECT '2in MGT w WonderBox' AS pi_collection,	'MGT' AS glb_collection
--                     UNION ALL
--                     SELECT '3in MGT w WonderBox',	'MGT'
--                     UNION ALL
--                     SELECT 'Aidan Sling Chair, 2PK',	'Aidan'
--                     UNION ALL
--                     SELECT 'BIFD 5in',	'Cool Grey BIFD'
--                     UNION ALL
--                     SELECT 'Justina, 16in',	'Justina'
--                     UNION ALL
--                     SELECT 'Quinn Sleeper Sofa',	'Quinn'
--                     UNION ALL
--                     SELECT 'SB w Bamboo Slats',	'Bamboo Slat SB'
--                 ) b
--                     ON a.collection = b.pi_collection
--     )
    , cte_add_collection      AS (
        SELECT
            f.*
            -- zinus 제품 인 경우 collection 정보 추가, OTHERS = master 에 asin 은 존재하나 collection 없는 케이스, UNKNOWN = master 에 누락된 케이스
            , IF(z.asin IS NOT NULL OR m.asin IS NOT NULL, COALESCE(m.collection, if(z.asin IS NOT NULL, 'UNKNOWN', 'OTHERS')), null) as collection

            , IF(z.asin IS NOT NULL OR m.asin IS NOT NULL
                -- zinus category
                , COALESCE(m.category, if(z.asin IS NOT NULL, 'UNKNOWN', 'OTHERS'))
                -- 경쟁사 카테고리
                , null
--                 , IF(f.is_mattress
--                         -- mattress
--                      , REPLACE(c.category, 'Mattress', 'Mattresses')
--                         -- non-mattress
-- --                      , REPLACE(d.category, 'OTH.FRAMES&BEDS', 'OTHER FRAMES & BEDS') )
--                      , fullCategory)
              ) AS category

            , IF(z.asin IS NOT NULL OR m.asin IS NOT NULL, TRUE, FALSE) AS is_zinus
            , IF(f.rank <= 20, TRUE, FALSE) AS is_top20
            , IF(f.rank <= 100, TRUE, FALSE) AS is_top100
            , IF(f.rank BETWEEN 21 AND 100, TRUE, FALSE) AS is_top21_100
        FROM
            cte_bsr_src f
                LEFT JOIN cte_collection_merge m
                    ON f.asin = m.asin
                LEFT JOIN cte_zinus_asin_in_the_pdp_list z
                    ON f.asin = z.asin

--                 LEFT JOIN meta.amazon_mattress_master c
--                     ON f.asin = c.asin

--                 LEFT JOIN meta.amazon_non_mattress_master d
--                     ON f.asin = d.asin

--         select asin from meta.amazon_mattress_master GROUP BY asin HAVING COUNT(asin) > 1;
--         select asin from meta.amazon_non_mattress_master GROUP BY asin HAVING COUNT(asin) > 1;
    )
--     select * from cte_add_collection;
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
--    SELECT * FROM cte_add_rank_level;
--    SELECT * FROM cte_add_rank_level where is_mattress=true and category='Platform Beds';
-- B073JVPPWF
-- B074QWX9V9
    , cte_qt_group AS (
        SELECT
            is_mattress
            , rank
            , asin
            , yr_qt
            , bsr_year
            , collection
            , category
            , is_zinus
            , rank_level
--             , bsr_date

            , COUNT(1) AS number_of_bsr_listings
        FROM
            cte_add_rank_level
        WHERE
            is_zinus = FALSE
           OR (is_zinus = TRUE AND is_mattress=TRUE AND category IN ('Foam Mattresses', 'Spring Mattresses'))
           OR (is_zinus = TRUE AND is_mattress=FALSE AND category NOT IN ('Foam Mattresses', 'Spring Mattresses'))
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    , cte_retail_sales as (
        SELECT
            FORMAT_DATE('%yY_%QQ', WeekEnding) as yr_qt
            , RetailerSku as asin
            , SUM(RetailSales) as RetailSales
        FROM stck.atlas_sales_all
        GROUP BY 1,2

--         select min(weekending) from stck.atlas_sales_all;
    )
    , cte_merge_sales as (
        SELECT
            a.*
            , b.RetailSales
        FROM
            cte_qt_group a
                LEFT JOIN cte_retail_sales b
                    ON a.asin = b.asin AND a.yr_qt = b.yr_qt
    )
    , cte_count_by_collection AS (
        SELECT
            yr_qt
            , collection
            , rank_level
            , is_zinus
            , is_mattress
            , COALESCE(category, 'UNKNOWN') as category
--             , COUNT(1) AS number_of_bsr_listings
            , SUM(number_of_bsr_listings) AS number_of_bsr_listings
            , SUM(RetailSales) AS RetailSales
        FROM
--             cte_add_rank_level
            cte_merge_sales
        GROUP BY 1, 2, 3, 4, 5, 6
    )
--    select * from cte_count_by_collection where yr_qt='25Y_4Q' and rank_level='Top20' and is_mattress=true; and category='Spring Mattresses';
-- 24Y_1Q,Top100,false,Sofa
--     category_division,category,rank_level,yr_qt
--     Mattress,Spring Mattresses,Top20,25Y_4Q
--     64


    , cte_add_total_count as (
        SELECT
            *
--             , SUM(number_of_bsr_listings) OVER (PARTITION BY yr_qt, is_mattress, category, rank_level ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_count
            , SUM(number_of_bsr_listings) OVER (PARTITION BY yr_qt, is_mattress, rank_level ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_count
        FROM cte_count_by_collection
    )
--    select * from cte_add_total_count where yr_qt='25Y_4Q' and rank_level='Top20' and is_mattress=true;
    , cte_final as (
        SELECT
            IF(a.is_mattress = TRUE, 'Mattress', 'Non-Mattress') AS bsr_category_division
            , IF(GROUPING ( category ) = 1, 'TOTAL', category) AS financial_category
--             , category AS category
            , rank_level
            , yr_qt
            , SUM(if(is_zinus, number_of_bsr_listings, 0)) AS number_of_bsr_listings
            , COUNT(DISTINCT if(is_zinus, collection, null)) AS number_of_collections

--             , SUM(if(is_zinus, RetailSales, 0)) as zinus_retail_sales
            , SUM(RetailSales) as retail_sales

            , ANY_VALUE(total_count) AS total_count
            , SUM(if(is_zinus, number_of_bsr_listings, 0)) / ANY_VALUE(total_count) AS market_share

        FROM
            cte_add_total_count a
        WHERE
            is_zinus = TRUE
--         GROUP BY 1, 2, 3, 4
        GROUP BY GROUPING SETS (
            (1,category,3,4)
            , (1,3,4)
        )

    )
--     , cte_add_total as (
--         SELECT
--             *
--             , number_of_bsr_listings / total_count as market_share
--         FROM
--             cte_final
--
--         UNION ALL
--
--         SELECT
--             category_division
--             , 'TOTAL' as category
--             , rank_level
--             , yr_qt
--             , SUM(number_of_bsr_listings)
--             , SUM(number_of_collections)
--             , SUM(zinus_retail_sales)
--             , SUM(retail_sales)
--             , SUM(total_count)
--             , SUM(number_of_bsr_listings) / SUM(total_count)
--         FROM cte_final
--         GROUP BY 1,2,3,4
--     )
-- select * from cte_final where category_division = 'Non-Mattress' and rank_level = 'Top20' and yr_qt = '24Y_1Q' order by total_count desc;
-- select * from cte_final where category_division = 'Mattress' and rank_level = 'Top20' and yr_qt = '24Y_1Q' order by total_count desc;

-- meta.amazon_mattress_master, meta.amazon_non_mattress_gpt_master(or meta.amazon_non_mattress_master)  로 경쟁사 카테고리 추가 검토
SELECT
    *
FROM
--     cte_add_total
    cte_final
        PIVOT (
            SUM(number_of_collections) AS number_of_collections
            , SUM(number_of_bsr_listings) AS number_of_bsr_listings
            , SUM(total_count) AS total_count
            , SUM(market_share) AS market_share
--             , SUM(zinus_retail_sales) AS zinus_retail_sales
            , SUM(retail_sales) AS retail_sales
        FOR yr_qt IN ('22Y_1Q', '22Y_2Q', '22Y_3Q', '22Y_4Q', '23Y_1Q', '23Y_2Q', '23Y_3Q', '23Y_4Q', '24Y_1Q', '24Y_2Q', '24Y_3Q', '24Y_4Q', '25Y_1Q', '25Y_2Q', '25Y_3Q', '25Y_4Q') )
-- WHERE
--     bsr_category_division = 'Mattress'
--     AND rank_level = 'Top20'
--     bsr_category_division = 'Non-Mattress'
--     AND rank_level = 'Top20'
ORDER BY
    bsr_category_division
       , CASE
           WHEN rank_level = 'Top20'  THEN 1
           WHEN rank_level = 'Top100' THEN 2
           ELSE 3
       END
       , if(financial_category = 'TOTAL', 1, 2)