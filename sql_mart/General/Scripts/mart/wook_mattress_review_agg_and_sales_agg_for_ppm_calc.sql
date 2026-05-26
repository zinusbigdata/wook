-- written review + vc sales qty ---------------------------------------------------------------------------------------
create or replace table wook.mattress_review_agg_and_sales_agg_for_ppm_calc as
WITH
    cte_vc_daily_sales                 AS (
        SELECT
            date
            , t.asin
            , m.financial_category
            , m.origin_collection
            , m.main_collection
            , m.new_collection
            , SUM(ordered_units) AS ordered_units
        FROM
            vc.amz_vc_sales_daily_all t
                INNER JOIN meta.amz_zns_cat_col_mst m
                    ON t.asin = m.asin
        WHERE
            m.asin IS NOT NULL
            AND m.financial_category IN ('Foam Mattresses', 'Spring Mattresses')
        GROUP BY
            1, 2, 3, 4, 5, 6
    )
    , cte_written_rvw              AS (
        SELECT
            rvw.asin
            , rvw.review_date
            , m.financial_category
            , m.origin_collection
            , m.main_collection
            , m.new_collection
            , rvw.rating
        FROM
            dw.amz_us_zinus_rvw rvw
                INNER JOIN meta.amz_zns_cat_col_mst m
                    ON rvw.asin = m.asin
        WHERE
            m.asin IS NOT NULL
            AND m.financial_category IN ('Foam Mattresses', 'Spring Mattresses')
    )
    , cte_rvw_cnt   AS (
        SELECT
            asin
            , review_date
            , financial_category
            , origin_collection
            , main_collection
            , new_collection
            , SUM(IF(rating = 5, 1, 0)) AS written_5star_cnt
            , SUM(IF(rating = 4, 1, 0)) AS written_4star_cnt
            , SUM(IF(rating = 3, 1, 0)) AS written_3star_cnt
            , SUM(IF(rating = 2, 1, 0)) AS written_2star_cnt
            , SUM(IF(rating = 1, 1, 0)) AS written_1star_cnt
        FROM
            cte_written_rvw
        GROUP BY 1, 2, 3, 4, 5, 6
    )
SELECT
    COALESCE(s.asin, r.asin) AS asin
    , COALESCE(s.date, r.review_date) as date
    , COALESCE(s.financial_category, r.financial_category) as financial_category
    , COALESCE(s.origin_collection, r.origin_collection) as origin_collection
    , COALESCE(s.main_collection, r.main_collection) as main_collection
    , COALESCE(s.new_collection, r.new_collection) as new_collection
    , s.ordered_units
    , r.written_5star_cnt
    , r.written_4star_cnt
    , r.written_3star_cnt
    , r.written_2star_cnt
    , r.written_1star_cnt
    , r.written_5star_cnt + r.written_4star_cnt + r.written_3star_cnt + r.written_2star_cnt + r.written_1star_cnt AS written_total_cnt
    , r.written_1star_cnt + r.written_2star_cnt AS written_12star_cnt
FROM
    cte_vc_daily_sales s
        FULL OUTER JOIN cte_rvw_cnt r
            on s.asin = r.asin  and s.date = r.review_date
;
