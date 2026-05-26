/*
 *  Amazon US BSR 분석 마트 : wook.amz_us_bsr_add_seller_price_rating
 *  Last updated : 1/20/2026
 */


CREATE OR REPLACE TABLE wook.amz_us_bsr_add_seller_price_rating AS
WITH
    cte_date     AS (
        SELECT
            asin
            , fill_date
        FROM
            ( (
                SELECT
                    asin
                    , MIN(DATE(crawlTime_utc)) AS min_dt
                    , MAX(DATE(crawlTime_utc)) AS max_dt
                FROM
                    dw.rf_amz_pdt_zns_comp_daily AS t_i
                GROUP BY asin

            ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
    )
    , cte_extra  AS (
        SELECT
            t_d.asin
            , t_d.fill_date AS date
            , IFNULL(t_rf.bw_price_value, FIRST_VALUE(t_rf.bw_price_value IGNORE NULLS)
                                                      OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS retail_price
            , IFNULL(t_rf.rating, FIRST_VALUE(t_rf.rating IGNORE NULLS)
                                              OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS rating
            , IFNULL(t_rf.ratings_total, FIRST_VALUE(t_rf.ratings_total IGNORE NULLS)
                                                     OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS review_count
            , UPPER(k.bw_ff_type) AS seller_type
        FROM
            cte_date t_d
                LEFT JOIN (
                SELECT
                    --                         DISTINCT
                    a.asin
                    , a.rating
                    , a.ratings_total
                    , a.bw_price_value
                    , PARSE_DATE('%Y-%m-%d', SUBSTRING(crawlTime_utc, 0, 10)) AS date
                FROM
                    dw.rf_amz_pdt_zns_comp_daily a
                QUALIFY
                    ROW_NUMBER() OVER (PARTITION BY a.asin, PARSE_DATE('%Y-%m-%d', SUBSTRING(crawlTime_utc, 0, 10)) ORDER BY crawlTime_utc DESC) =
                    1
            ) t_rf
                    ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
                LEFT JOIN dw.rf_amz_stck_asin_pdt k
                    ON t_d.asin = k.asin
    )
SELECT
    COALESCE(mst.parent_asin, v.asin) AS parent_asin
    , v.*
    , ex.* EXCEPT (asin, date, seller_type)
    , COALESCE(seller_type, '3P') AS seller_type
FROM
    vs.amz_bsr_shr_daily_acc v
        LEFT JOIN cte_extra ex
            ON v.asin = ex.asin AND v.bsr_date = ex.date
        LEFT JOIN wook.amz_bsr_mapping_parent_asin_202512 mst
            ON v.asin = mst.asin
WHERE
    v.asin IS NOT NULL
    AND v.bsr_rank_range = 'Top 50'
ORDER BY
    bsr_date DESC
;
 