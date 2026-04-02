/*
 * Small-Box 출시 이후 Review 평점 변화 추적을 위한 Mart Ver.2 : Original Collection 단위 집계 (이전거는 New Collection 단위로 집계함.) 
 * Source Table : dw.amz_us_zinus_rvw, dw.rf_amz_pdt_zns_comp_daily, tmp1.zns_f_cate_m_col_mst, vc.amz_vc_sales_monthly  
 * Made by Han : Feb 2026  
 */


CREATE OR REPLACE TABLE wook.review_sales_by_collection_category_anchor_asin_of_origin_col AS
WITH
    cte_anchor_asins AS (
        WITH
            cte_agg AS (
                SELECT
                    b.origin_collection
                    , a.asin AS acnhor_asin
                    , COUNT(1) AS cnt
                    , MIN(FORMAT_DATE("%Y%m", DATE(crawlTime_utc))) AS min_month
                    , MAX(FORMAT_DATE("%Y%m", DATE(crawlTime_utc))) AS max_month
                FROM
                    dw.rf_amz_pdt_zns_comp_daily a
                        LEFT JOIN tmp1.zns_f_cate_m_col_mst b
                            ON a.asin = b.asin
                WHERE
                    SUBSTRING(a.crawlTime_utc, 1, 10) >= '2024-01-01'
                    AND b.origin_collection IS NOT NULL
                GROUP BY 1, 2
                ORDER BY 1, 4 DESC, 3 DESC
            )
        SELECT
            *
        FROM
            cte_agg
        QUALIFY
            -- 가장 최근까지 수집, 가장 많이 수집, 가장 예전에 수집
            ROW_NUMBER() OVER (PARTITION BY origin_collection ORDER BY max_month DESC, cnt DESC, min_month) = 1
    )
    , cte_sales_src as (
        SELECT
            CAST(FORMAT_DATE('%Y%m', date) AS INT64) as yr_month
            , m.financial_category

            , m.origin_collection
            , m.main_collection
            , m.new_collection

            , ordered_revenue
            , ordered_units
        FROM
            vc.amz_vc_sales_monthly f
                LEFT JOIN tmp1.zns_f_cate_m_col_mst m
                    ON f.asin = m.asin
        WHERE
            date >= '2024-01-01'
    )
    , cte_sales_agg   AS (
        SELECT
            yr_month
            , financial_category

            , origin_collection
            , main_collection
            , new_collection

            , SUM(ordered_revenue) AS amt
            , SUM(ordered_units) AS qty
        FROM
            cte_sales_src
        GROUP BY
            1, 2, 3, 4, 5

        UNION ALL

        SELECT
            yr_month
            , financial_category

            , '__TOTAL__' as origin_collection
            , '__TOTAL__' as main_collection
            , '__TOTAL__' as new_collection

            , SUM(ordered_revenue) AS amt
            , SUM(ordered_units) AS qty
        FROM
            cte_sales_src f
        GROUP BY
            1, 2

    )
    , cte_written_rvw_agg     AS (
        SELECT
            CAST(FORMAT_DATE('%Y%m', review_date) AS INT64) AS yr_month
            , m.financial_category

            , m.origin_collection
            , m.main_collection
            , m.new_collection

            --             , AVG(rating) AS avg_rating
            , SUM(IF(rating < 3, 1, 0)) AS written_12_cnt
            , COUNT(1) AS written_all_cnt
            , SUM(IF(rating < 3, 1, 0)) / COUNT(1) AS written_12_ratio
            , SUM(IF(rating = 1, 1, 0)) AS written_1_cnt
            , SUM(IF(rating = 2, 1, 0)) AS written_2_cnt
            , SUM(IF(rating = 3, 1, 0)) AS written_3_cnt
            , SUM(IF(rating = 4, 1, 0)) AS written_4_cnt
            , SUM(IF(rating = 5, 1, 0)) AS written_5_cnt
        FROM
            dw.amz_us_zinus_rvw f
                LEFT JOIN tmp1.zns_f_cate_m_col_mst m
                    ON f.asin = m.asin
        WHERE
            -- review_date >= '2022-01-01'
            review_date between '2024-01-01' and (SELECT MAX(date) FROM vc.amz_vc_sales_monthly)
        GROUP BY
            1, 2, 3, 4, 5

        UNION ALL

        SELECT
            CAST(FORMAT_DATE('%Y%m', review_date) AS INT64) AS yr_month
            , m.financial_category

            , '__TOTAL__' as origin_collection
            , '__TOTAL__' as main_collection
            , '__TOTAL__' as new_collection

            -- 평점 (rating) 자체가 cnt1~5합계산결과/cnt1~5갯수 이므로 avg 처리시 평균의 평균과 같은 수치 오류 발생
            -- , AVG(rating) AS avg_rating

            , SUM(IF(rating < 3, 1, 0)) AS written_12_cnt
            , COUNT(1) AS witten_all_cnt
            , SUM(IF(rating < 3, 1, 0)) / COUNT(1) AS written_12_ratio
            , SUM(IF(rating = 1, 1, 0)) AS witten_1_cnt
            , SUM(IF(rating = 2, 1, 0)) AS witten_2_cnt
            , SUM(IF(rating = 3, 1, 0)) AS witten_3_cnt
            , SUM(IF(rating = 4, 1, 0)) AS witten_4_cnt
            , SUM(IF(rating = 5, 1, 0)) AS witten_5_cnt

        FROM
            dw.amz_us_zinus_rvw f
                LEFT JOIN tmp1.zns_f_cate_m_col_mst m
                    on f.asin = m.asin
        WHERE
            review_date between '2024-01-01' and (SELECT MAX(date) FROM vc.amz_vc_sales_monthly)
        GROUP BY
            1, 2
    )
    -- select * from cte_pdp_for_new_collection;
    , cte_written_rvw_add_avg_rating as (
        SELECT
            a.*
            --             , a.avg_rating AS written_avg_rating
            , (a.written_5_cnt * 5 + a.written_4_cnt * 4 + a.written_3_cnt * 3 + a.written_2_cnt * 2 + a.written_1_cnt)
                / ( a.written_5_cnt + a.written_4_cnt + a.written_3_cnt + a.written_2_cnt + a.written_1_cnt )
                AS written_avg_rating
        FROM
            cte_written_rvw_agg a
    )
    -- ZNS-2632 / add amazon category
    , cte_pdp_for_amz_category as (
        SELECT
            --             p.asin
            m.origin_collection
            , p.salesrank_category1
            , p.salesrank_category2
            , p.salesrank_category3
            , p.salesrank_category4
            , p.salesrank1
            , p.salesrank2
            , p.salesrank3
            , p.salesrank4
            , (
                SELECT
                    AS STRUCT
                    rank AS main_sales_rank
                    , cat AS main_sales_category
                FROM
                    UNNEST([ STRUCT ( p.salesrank1 AS rank, p.salesrank_category1 AS cat ), STRUCT ( p.salesrank2 AS rank, p.salesrank_category2 AS cat ), STRUCT ( p.salesrank3 AS rank, p.salesrank_category3 AS cat ), STRUCT ( p.salesrank4 AS rank, p.salesrank_category4 AS cat ) ])
                WHERE
                    rank IS NOT NULL
                ORDER BY cast(rank as INT64)
                LIMIT 1
            ).*
            , p.categories_flat
        FROM
            dw.rf_amz_pdt_zns_comp_daily p
                INNER JOIN cte_anchor_asins m
                    ON REGEXP_REPLACE(TRIM(p.asin), r'[^[:print:]]|&lrm;', '') = m.acnhor_asin
        --         QUALIFY ROW_NUMBER() OVER (PARTITION BY p.asin ORDER BY p.crawlTime_utc DESC) = 1
        QUALIFY ROW_NUMBER() OVER (PARTITION BY m.origin_collection ORDER BY p.crawlTime_utc DESC) = 1
    )
    --     select * from cte_pdp_for_sales_category;
    , cte_pdp_for_origin_collection as (
        WITH
            cte_pdp_daily_src as (
                SELECT
                    m.origin_collection
                    , DATE(p.crawlTime_utc) as pdp_date
                    , CAST(FORMAT_DATE("%Y%m", DATE(p.crawlTime_utc)) AS INT64) AS yr_month
                    , p.*
                FROM
                    dw.rf_amz_pdt_zns_comp_daily p
                        INNER JOIN cte_anchor_asins m
                            ON REGEXP_REPLACE(TRIM(p.asin), r'[^[:print:]]|&lrm;', '') = m.acnhor_asin
                QUALIFY
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            m.origin_collection, DATE(crawlTime_utc)
                        ORDER BY
                            crawlTime_utc DESC
                        ) = 1
            )
            , cte_pdp_yr_month_snap AS (
                SELECT
                    origin_collection
                    , yr_month
                    --             , COALESCE(p.ratings_total, last_value(p.ratings_total ignore nulls) over(PARTITION BY m.main_collection order by date(p.crawlTime_utc) desc)) AS all_cnt

                    --                     , p.ratings_total
                    --                     , p.rating_breakdown_five_star_count AS rt5_cnt
                    --                     , p.rating_breakdown_four_star_count AS rt4_cnt
                    --                     , p.rating_breakdown_three_star_count AS rt3_cnt
                    --                     , p.rating_breakdown_two_star_count AS rt2_cnt
                    --                     , p.rating_breakdown_one_star_count AS rt1_cnt

                    , AVG(rating) OVER (PARTITION BY origin_collection, yr_month ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) AS month_avg_rating
                    , SUM(rating) OVER (PARTITION BY origin_collection, yr_month ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) AS month_sum_rating
                    , COUNT(rating) OVER (PARTITION BY origin_collection, yr_month ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) AS month_cnt_rating

                    , FIRST_VALUE(ratings_total IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS first_day_ratings_total
                    , FIRST_VALUE(rating_breakdown_five_star_count IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS first_day_rt5_cnt
                    , FIRST_VALUE(rating_breakdown_four_star_count IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS first_day_rt4_cnt
                    , FIRST_VALUE(rating_breakdown_three_star_count IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS first_day_rt3_cnt
                    , FIRST_VALUE(rating_breakdown_two_star_count IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS first_day_rt2_cnt
                    , FIRST_VALUE(rating_breakdown_one_star_count IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS first_day_rt1_cnt

                    , LAST_VALUE(ratings_total IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS last_day_ratings_total
                    , LAST_VALUE(rating_breakdown_five_star_count IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS last_day_rt5_cnt
                    , LAST_VALUE(rating_breakdown_four_star_count IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS last_day_rt4_cnt
                    , LAST_VALUE(rating_breakdown_three_star_count IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS last_day_rt3_cnt
                    , LAST_VALUE(rating_breakdown_two_star_count IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS last_day_rt2_cnt
                    , LAST_VALUE(rating_breakdown_one_star_count IGNORE NULLS) OVER (PARTITION BY origin_collection, yr_month ORDER BY crawlTime_utc) AS last_day_rt1_cnt
                FROM
                    cte_pdp_daily_src
                WHERE
                    --             p.asin = 'B071FVJ3XY'
                    --                     m.origin_collection = 'GTFT'
                    --                     AND
                    origin_collection IS NOT NULL
                QUALIFY
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            origin_collection, yr_month
                        ORDER BY
                            --                             IF(ratings_total > 0 AND ratings_total != CAST(NULL AS INT64), 1, 0)
                            --                             , ABS(0 - ABS(ratings_total - ( COALESCE(rating_breakdown_five_star_count, 0) + COALESCE(rating_breakdown_four_star_count, 0) + COALESCE(rating_breakdown_three_star_count, 0) + COALESCE(rating_breakdown_two_star_count, 0) + COALESCE(rating_breakdown_one_star_count, 0) )))
                            --                             , ratings_total DESC
                            --                             ,
                            crawlTime_utc DESC
                        ) = 1
            )
            --             , cte_pdp_fill_month_snap as (
            --                 SELECT
            --                     * EXCEPT (ratings_total, rt5_cnt, rt4_cnt, rt3_cnt, rt2_cnt, rt1_cnt)
            --                     , COALESCE(ratings_total, LAST_VALUE(ratings_total ignore nulls) over(PARTITION BY new_collection order by yr_month)) AS all_cnt
            --                     , COALESCE(rt5_cnt, LAST_VALUE(rt5_cnt ignore nulls) over(PARTITION BY new_collection order by yr_month)) AS rt5_cnt
            --                     , COALESCE(rt4_cnt, LAST_VALUE(rt4_cnt ignore nulls) over(PARTITION BY new_collection order by yr_month)) AS rt4_cnt
            --                     , COALESCE(rt3_cnt, LAST_VALUE(rt3_cnt ignore nulls) over(PARTITION BY new_collection order by yr_month)) AS rt3_cnt
            --                     , COALESCE(rt2_cnt, LAST_VALUE(rt2_cnt ignore nulls) over(PARTITION BY new_collection order by yr_month)) AS rt2_cnt
            --                     , COALESCE(rt1_cnt, LAST_VALUE(rt1_cnt ignore nulls) over(PARTITION BY new_collection order by yr_month)) AS rt1_cnt
            --                 FROM cte_pdp_yr_month_snap
            --             )
            --             , cte_pdp_snap_with_inc as (
            --                 SELECT
            --                     *
            --
            -- --                     , all_cnt - LAG(all_cnt) OVER (PARTITION BY new_collection ORDER BY yr_month) AS inc_all_cnt
            -- --                     , rt5_cnt - LAG(rt5_cnt) OVER (PARTITION BY new_collection ORDER BY yr_month) AS inc_rt5_cnt
            -- --                     , rt4_cnt - LAG(rt4_cnt) OVER (PARTITION BY new_collection ORDER BY yr_month) AS inc_rt4_cnt
            -- --                     , rt3_cnt - LAG(rt3_cnt) OVER (PARTITION BY new_collection ORDER BY yr_month) AS inc_rt3_cnt
            -- --                     , rt2_cnt - LAG(rt2_cnt) OVER (PARTITION BY new_collection ORDER BY yr_month) AS inc_rt2_cnt
            -- --                     , rt1_cnt - LAG(rt1_cnt) OVER (PARTITION BY new_collection ORDER BY yr_month) AS inc_rt1_cnt
            --                 FROM
            -- --                     cte_pdp_fill_month_snap
            --                     cte_pdp_yr_month_snap
            --             )
            -- ZNS-2632 / add amazon category
            , cte_pdp_add_amz_category as (
                SELECT
                    a.*
                    , b.* EXCEPT (origin_collection)
                FROM
                    cte_pdp_yr_month_snap a
                        left join cte_pdp_for_amz_category b
                            on a.origin_collection = b.origin_collection
            )
        SELECT
            --             new_collection
            --             , yr_month

            --             , all_cnt as new_col_all_cnt
            --             , rt5_cnt as new_col_rt5_cnt
            --             , rt4_cnt as new_col_rt4_cnt
            --             , rt3_cnt as new_col_rt3_cnt
            --             , rt2_cnt as new_col_rt2_cnt
            --             , rt1_cnt as new_col_rt1_cnt
            --             , inc_all_cnt as new_col_inc_all_cnt
            --             , inc_rt5_cnt as new_col_inc_rt5_cnt
            --             , inc_rt4_cnt as new_col_inc_rt4_cnt
            --             , inc_rt3_cnt as new_col_inc_rt3_cnt
            --             , inc_rt2_cnt as new_col_inc_rt2_cnt
            --             , inc_rt1_cnt as new_col_inc_rt1_cnt

            origin_collection
            , yr_month
            , first_day_ratings_total
            , first_day_rt5_cnt
            , first_day_rt4_cnt
            , first_day_rt3_cnt
            , first_day_rt2_cnt
            , first_day_rt1_cnt
            , last_day_ratings_total
            , last_day_rt5_cnt
            , last_day_rt4_cnt
            , last_day_rt3_cnt
            , last_day_rt2_cnt
            , last_day_rt1_cnt

            , last_day_ratings_total - first_day_ratings_total AS inc_all_cnt
            , last_day_rt5_cnt - first_day_rt5_cnt AS inc_rt5_cnt
            , last_day_rt4_cnt - first_day_rt4_cnt AS inc_rt4_cnt
            , last_day_rt3_cnt - first_day_rt3_cnt AS inc_rt3_cnt
            , last_day_rt2_cnt - first_day_rt2_cnt AS inc_rt2_cnt
            , last_day_rt1_cnt - first_day_rt1_cnt AS inc_rt1_cnt

            , month_avg_rating
            --             , month_sum_rating
            --             , month_cnt_rating

            -- b.
            , salesrank_category1, salesrank_category2, salesrank_category3, salesrank_category4, salesrank1, salesrank2, salesrank3, salesrank4, main_sales_rank, main_sales_category, categories_flat

        FROM
            cte_pdp_add_amz_category
        --             cte_pdp_snap_with_inc

        UNION ALL

        SELECT
            '__TOTAL__' AS new_collection
            , yr_month

            , SUM(first_day_ratings_total) AS first_day_ratings_total
            , SUM(first_day_rt5_cnt) AS first_day_rt5_cnt
            , SUM(first_day_rt4_cnt) AS first_day_rt4_cnt
            , SUM(first_day_rt3_cnt) AS first_day_rt3_cnt
            , SUM(first_day_rt2_cnt) AS first_day_rt2_cnt
            , SUM(first_day_rt1_cnt) AS first_day_rt1_cnt
            , SUM(last_day_ratings_total) AS last_day_ratings_total
            , SUM(last_day_rt5_cnt) AS last_day_rt5_cnt
            , SUM(last_day_rt4_cnt) AS last_day_rt4_cnt
            , SUM(last_day_rt3_cnt) AS last_day_rt3_cnt
            , SUM(last_day_rt2_cnt) AS last_day_rt2_cnt
            , SUM(last_day_rt1_cnt) AS last_day_rt1_cnt

            , SUM(last_day_ratings_total) - SUM(first_day_ratings_total) AS inc_all_cnt
            , SUM(last_day_rt5_cnt) - SUM(first_day_rt5_cnt) AS inc_rt5_cnt
            , SUM(last_day_rt4_cnt) - SUM(first_day_rt4_cnt) AS inc_rt4_cnt
            , SUM(last_day_rt3_cnt) - SUM(first_day_rt3_cnt) AS inc_rt3_cnt
            , SUM(last_day_rt2_cnt) - SUM(first_day_rt2_cnt) AS inc_rt2_cnt
            , SUM(last_day_rt1_cnt) - SUM(first_day_rt1_cnt) AS inc_rt1_cnt

            , SUM(month_sum_rating) / SUM(month_cnt_rating) AS month_avg_rating

            --             , SUM(all_cnt) AS all_cnt
            --             , SUM(rt5_cnt) AS rt5_cnt
            --             , SUM(rt4_cnt) AS rt4_cnt
            --             , SUM(rt3_cnt) AS rt3_cnt
            --             , SUM(rt2_cnt) AS rt2_cnt
            --             , SUM(rt1_cnt) AS rt1_cnt
            --             , SUM(inc_all_cnt) AS inc_all_cnt
            --             , SUM(inc_rt5_cnt) AS inc_rt5_cnt
            --             , SUM(inc_rt4_cnt) AS inc_rt4_cnt
            --             , SUM(inc_rt3_cnt) AS inc_rt3_cnt
            --             , SUM(inc_rt2_cnt) AS inc_rt2_cnt
            --             , SUM(inc_rt1_cnt) AS inc_rt1_cnt

                -- b.
            , NULL AS salesrank_category1
            , NULL AS salesrank_category2
            , NULL AS salesrank_category3
            , NULL AS salesrank_category4
            , NULL AS salesrank1
            , NULL AS salesrank2
            , NULL AS salesrank3
            , NULL AS salesrank4
            , NULL AS main_sales_rank
            , NULL AS main_sales_category
            , NULL AS categories_flat

        FROM
            cte_pdp_add_amz_category
        GROUP BY 1, 2
    )

SELECT
    COALESCE(r.yr_month, s.yr_month) AS yr_month
    , IFNULL(COALESCE(r.financial_category, s.financial_category), '__UNKNOWN__') AS financial_category

    , IFNULL(COALESCE(r.origin_collection, s.origin_collection), '__UNKNOWN__') AS origin_collection
    , IFNULL(COALESCE(r.main_collection, s.main_collection), '__UNKNOWN__') AS main_collection
    , IFNULL(COALESCE(r.new_collection, s.new_collection), '__UNKNOWN__') AS new_collection

    , amt AS sales_amount
    , qty AS sales_qty

    , r.* EXCEPT (yr_month, financial_category, origin_collection, main_collection, new_collection)

    , pdp.* EXCEPT (origin_collection, yr_month)
    , IF(COALESCE(pdp.inc_rt5_cnt, 0) + COALESCE(pdp.inc_rt4_cnt, 0) + COALESCE(pdp.inc_rt3_cnt, 0) + COALESCE(pdp.inc_rt2_cnt, 0) + COALESCE(pdp.inc_rt1_cnt, 0) > 0
    , (COALESCE(pdp.inc_rt5_cnt, 0) * 5 + COALESCE(pdp.inc_rt4_cnt, 0) * 4 + COALESCE(pdp.inc_rt3_cnt, 0) * 3 + COALESCE(pdp.inc_rt2_cnt, 0) * 2 + COALESCE(pdp.inc_rt1_cnt, 0)) / ( COALESCE(pdp.inc_rt5_cnt, 0) + COALESCE(pdp.inc_rt4_cnt, 0) + COALESCE(pdp.inc_rt3_cnt, 0) + COALESCE(pdp.inc_rt2_cnt, 0) + COALESCE(pdp.inc_rt1_cnt, 0) )
    , 0
      ) AS inc_avg_rating
    , IF(COALESCE(pdp.last_day_rt5_cnt, 0) + COALESCE(pdp.last_day_rt4_cnt, 0) + COALESCE(pdp.last_day_rt3_cnt, 0) + COALESCE(pdp.last_day_rt2_cnt, 0) + COALESCE(pdp.last_day_rt1_cnt, 0) > 0
    , (COALESCE(pdp.last_day_rt5_cnt, 0) * 5 + COALESCE(pdp.last_day_rt4_cnt, 0) * 4 + COALESCE(pdp.last_day_rt3_cnt, 0) * 3 + COALESCE(pdp.last_day_rt2_cnt, 0) * 2 + COALESCE(pdp.last_day_rt1_cnt, 0)) / ( COALESCE(pdp.last_day_rt5_cnt, 0) + COALESCE(pdp.last_day_rt4_cnt, 0) + COALESCE(pdp.last_day_rt3_cnt, 0) + COALESCE(pdp.last_day_rt2_cnt, 0) + COALESCE(pdp.last_day_rt1_cnt, 0) )
    , 0
      ) AS last_day_avg_rating
    , IF(COALESCE(pdp.first_day_rt5_cnt, 0) + COALESCE(pdp.first_day_rt4_cnt, 0) + COALESCE(pdp.first_day_rt3_cnt, 0) + COALESCE(pdp.first_day_rt2_cnt, 0) + COALESCE(pdp.first_day_rt1_cnt, 0) > 0
    , (COALESCE(pdp.first_day_rt5_cnt, 0) * 5 + COALESCE(pdp.first_day_rt4_cnt, 0) * 4 + COALESCE(pdp.first_day_rt3_cnt, 0) * 3 + COALESCE(pdp.first_day_rt2_cnt, 0) * 2 + COALESCE(pdp.first_day_rt1_cnt, 0)) / ( COALESCE(pdp.first_day_rt5_cnt, 0) + COALESCE(pdp.first_day_rt4_cnt, 0) + COALESCE(pdp.first_day_rt3_cnt, 0) + COALESCE(pdp.first_day_rt2_cnt, 0) + COALESCE(pdp.first_day_rt1_cnt, 0) )
    , 0
      ) AS first_day_avg_rating
FROM
    cte_written_rvw_add_avg_rating r
        FULL OUTER JOIN cte_sales_agg s
            ON
        r.yr_month = s.yr_month
            -- r.yr = s.yr
            AND r.financial_category = s.financial_category

            AND IFNULL(r.origin_collection, '-') = IFNULL(s.origin_collection, '-')
            AND IFNULL(r.main_collection, '-') = IFNULL(s.main_collection, '-')
            AND IFNULL(r.new_collection, '-') = IFNULL(s.new_collection, '-')

        LEFT JOIN cte_pdp_for_origin_collection pdp
            ON COALESCE(r.origin_collection, s.origin_collection) = pdp.origin_collection
        AND COALESCE(r.yr_month, s.yr_month) = pdp.yr_month


ORDER BY 1, 2, 4 DESC
;
 