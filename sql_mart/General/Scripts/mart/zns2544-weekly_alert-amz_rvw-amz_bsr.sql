-- [Review] ------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------
-- 지난주 대비 평점 4.0 이하로 하락 -------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
--     SET STD_DT = CURRENT_DATE();
    SET STD_DT = '2025-12-13';

    WITH cte_src AS (
            SELECT
                a.asin
                , PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc) AS crawl_datetime
                , DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) as crawl_date
                , cal.yr
                , cal.yr_month
                , cal.yr_wk
                , cal2.yr_wk AS yr_last_wk
                , NULLIF(b.collection, 'nan') AS collection
                , b.abbre
                , b.financial_category
                , upper(REGEXP_REPLACE(TRIM(brand), r'[^[:print:]]', '')) AS brand
                , rating
                , ratings_total AS cnt_all
            FROM
                dw.rf_amz_pdt_zns_comp_daily a
                    JOIN meta.amz_zinus_master_pdt_pi b
                        ON a.asin = b.asin
                    LEFT JOIN meta.wk_calendar_new cal
                        ON DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) BETWEEN cal.start_date AND cal.end_date
                    LEFT JOIN meta.wk_calendar_new cal2
                        ON DATE_SUB(DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)), INTERVAL 1 WEEK) BETWEEN cal2.start_date AND cal2.end_date
        )
        , cte_wk_summ as (
            SELECT
                asin
                , yr_wk
                , yr_last_wk
                , collection
                , abbre
                , financial_category
                , brand
                , AVG(rating) AS wk_avg_rating
                , MAX(cnt_all) AS rvw_cnt
            FROM
                cte_src
            GROUP BY
                1, 2, 3, 4, 5, 6, 7
            ORDER BY 8, 9 DESC
        )
    SELECT
        tw.* EXCEPT (yr_last_wk)
        , lw.yr_wk AS lw_yr_wk
        , lw.wk_avg_rating AS lw_avg_rating
        , lw.rvw_cnt AS lw_rvw_cnt
    FROM
        cte_wk_summ tw
            LEFT JOIN cte_wk_summ lw
                ON tw.asin = lw.asin AND tw.yr_last_wk = lw.yr_wk
    WHERE
        tw.wk_avg_rating < 4
        AND lw.wk_avg_rating >= 4
        AND tw.yr_wk = CAST(FORMAT_DATE('%G%V', DATE(DATE_SUB(STD_DT, INTERVAL 0 WEEK))) AS INT64)
    ;
END;

-------------------------------------------------------------------------------------------------------------
-- 전주 대비 평점 -0.1 이상 하락 or 4주 평균 대비 -0.1 이상 하락 --------------------------------------------
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
--     SET STD_DT = CURRENT_DATE();
    SET STD_DT = '2025-12-13';

    WITH cte_src AS (
        SELECT
            a.asin
            , PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc) AS crawl_datetime
            , DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) AS crawl_date
            , cal.yr
            , cal.yr_month
            , cal.yr_wk
            , NULLIF(b.collection, 'nan') AS collection
            , b.abbre
            , b.financial_category
            , UPPER(REGEXP_REPLACE(TRIM(brand), r'[^[:print:]]', '')) AS brand
            , rating
            , ratings_total AS cnt_all
        FROM
            dw.rf_amz_pdt_zns_comp_daily a
                JOIN meta.amz_zinus_master_pdt_pi b
                    ON a.asin = b.asin
                LEFT JOIN meta.wk_calendar_new cal
                    ON DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) BETWEEN cal.start_date AND cal.end_date
        WHERE
            cal.yr_wk >= CAST(FORMAT_DATE('%G%V', DATE(DATE_SUB(STD_DT, INTERVAL 5 WEEK))) AS INT64)
    )
    , cte_this_wk as (
            SELECT
                asin
                , yr
                , yr_month
                , yr_wk
                , collection
                , abbre
                , financial_category
                , brand
                , AVG(rating) AS rating
                , MAX(cnt_all) AS cnt_all
            FROM
                cte_src
            WHERE
                yr_wk = CAST(FORMAT_DATE('%G%V', DATE(DATE_SUB(STD_DT, INTERVAL 0 WEEK))) AS INT64)
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        )
    , cte_last_wk as (
            SELECT
                asin
                , collection
                , abbre
                , financial_category
                , brand
                , AVG(rating) AS rating
                , MAX(cnt_all) AS cnt_all
            FROM
                cte_src
            WHERE
                yr_wk = CAST(FORMAT_DATE('%G%V', DATE(DATE_SUB(STD_DT, INTERVAL 1 WEEK))) AS INT64)
            GROUP BY 1, 2, 3, 4, 5
    )
    , cte_4_wk as (
            SELECT
                asin
                , collection
                , abbre
                , financial_category
                , brand
                , AVG(rating) AS rating
                , MAX(cnt_all) AS cnt_all
            FROM
                cte_src
            WHERE
                yr_wk < CAST(FORMAT_DATE('%G%V', DATE(DATE_SUB(STD_DT, INTERVAL 0 WEEK))) AS INT64)
            GROUP BY 1, 2, 3, 4, 5
    )
    , cte_merge as (
            SELECT
                a.*
                , b.rating AS lw_rating
                , b.cnt_all AS lw_cnt_all
                , c.rating AS lw_4_rating
                , c.cnt_all AS lw_4_cnt_all
                , a.rating - b.rating AS diff1
                , a.rating - c.rating AS diff2
            FROM
                cte_this_wk a
                    LEFT JOIN cte_last_wk b
                        ON a.asin = b.asin
                    LEFT JOIN cte_4_wk c
                        ON a.asin = c.asin
        )
    SELECT * FROM cte_merge
    WHERE
        diff1 <= -0.1
        OR diff2 <= -0.1
    ORDER BY collection, abbre
    ;

END;

-------------------------------------------------------------------------------------------------------------
-- 전주대비 1~2 star 비중이 -10% 이상 하락 (금주 1~2 star 갯수가 전주 1~2 star 갯수보다 10% 늘어난 것) ------
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
--     SET STD_DT = CURRENT_DATE();
    SET STD_DT = '2025-12-13';

    WITH cte_src AS (
        SELECT
            a.asin
            , PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc) AS crawl_datetime
            , DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) as crawl_date
            , cal.yr
            , cal.yr_month
            , cal.yr_wk
            , cal2.yr_wk AS yr_last_wk
            , NULLIF(b.collection, 'nan') AS collection
            , b.abbre
            , b.financial_category
            , REGEXP_REPLACE(TRIM(brand), r'[^[:print:]]', '') AS brand
            , rating_breakdown_one_star_count + rating_breakdown_two_star_count as cnt12
            , (rating_breakdown_one_star_count + rating_breakdown_two_star_count) * 1.1 as cnt12_add10
            , rating_breakdown_one_star_percentage + rating_breakdown_two_star_percentage as ratio12
        FROM
            dw.rf_amz_pdt_zns_comp_daily a
                JOIN meta.amz_zinus_master_pdt_pi b
                    ON a.asin = b.asin
                LEFT JOIN meta.wk_calendar_new cal
                    ON DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) BETWEEN cal.start_date AND cal.end_date
                LEFT JOIN meta.wk_calendar_new cal2
                    ON DATE_SUB(DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)), INTERVAL 1 WEEK) BETWEEN cal2.start_date AND cal2.end_date

    )
    , cte_agg as (
            SELECT
                asin
                , yr
                , yr_month
                , yr_wk
                , yr_last_wk
                , collection
                , abbre
                , financial_category
                , brand
                , MAX(cnt12) AS cnt12
                , MAX(cnt12_add10) AS cnt12_add10
            FROM
                cte_src
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    SELECT
        -- tw.* EXCEPT (ratio12, cnt12_add10)
        tw.* EXCEPT (cnt12_add10)
        , lw.cnt12 AS lw_cnt12
        -- , lw.ratio12 AS lw_ratio12
        -- , lw.cnt12_add10 as lw_cnt12_add10
    FROM
        cte_agg tw
            LEFT JOIN cte_agg lw
                ON tw.asin = lw.asin AND tw.yr_last_wk = lw.yr_wk
    WHERE
        tw.cnt12 >= lw.cnt12_add10
        and tw.yr_wk = CAST(FORMAT_DATE('%G%V', DATE(DATE_SUB(STD_DT, INTERVAL 0 WEEK))) AS INT64)
        AND NOT(tw.cnt12 = 0 AND lw.cnt12 = 0)
    ORDER BY collection, abbre, tw.cnt12 - lw.cnt12_add10 DESC
    ;

END;

-------------------------------------------------------------------------------------------------------------
-- 경쟁사 평균 평점 대비 ZINUS 평점 차이가 0.2 이상 하락 (= 현재 주차의 경쟁사 평균 평점 - 지누스 평균 평점 < -0.2, 평점 차이가 0.2 이상인 경우 Alert)
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
    --     SET STD_DT = CURRENT_DATE();
    SET STD_DT = '2025-12-13';
    WITH
        cte_cal AS (
            SELECT
                CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) AS yr_wk_str
                , CONCAT('Y', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 3, 2), ' W', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 5, 2)) AS yr_last_wk_str
                , yr_wk
                , start_date
                , end_date
                , LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS yr_last_wk
            FROM
                meta.wk_calendar
        )
        , cte_is_top10_asin as (
            SELECT
                cal.yr_wk
                , bsr_ctgry_label
                , brand
                , asin
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                bsr_rank_range = 'Top 10'
                AND asin_cnt_brand_dt > 0
                AND STD_DT BETWEEN cal.start_date AND cal.end_date
    --             AND bsr_ctgry_label = '01. Mattresses'
                AND asin IS NOT NULL
        )
        , cte_wk_pdp_rating as (
            SELECT
                a.asin
                , cal.yr_wk
                , b.bsr_ctgry_label
                , b.brand
                , a.rating
            FROM
                dw.rf_amz_pdt_zns_comp_daily a
                    JOIN cte_is_top10_asin b
                        ON a.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) BETWEEN cal.start_date AND cal.end_date
            WHERE
                cal.yr_wk = b.yr_wk
        )
    SELECT
        bsr_ctgry_label
        , AVG(IF(brand = 'ZINUS', rating, NULL)) AS avg_zinus
        , AVG(IF(brand != 'ZINUS', rating, NULL)) AS avg_non_zinus
        , AVG(IF(brand = 'ZINUS', rating, NULL)) - AVG(IF(brand != 'ZINUS', rating, NULL)) AS diff_avg_rating
        , AVG(IF(brand = 'ZINUS', rating, NULL)) - AVG(IF(brand != 'ZINUS', rating, NULL)) < -0.2 as detected
    FROM
        cte_wk_pdp_rating
    GROUP BY
        1
    -- HAVING
    --     AVG(IF(brand = 'ZINUS', rating, NULL)) - AVG(IF(brand != 'ZINUS', rating, NULL)) < -0.2
    --     OR avg_zinus IS NULL
    ORDER BY
        1
    ;
END;

-- [BSR] ---------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------
-- 경쟁사 전주대비 점유율 5% 이상 상승 (Top 20)
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
    SET STD_DT = CURRENT_DATE();
--     SET STD_DT = '2025-12-13';

    with cte_cal as (
        SELECT
            CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) as yr_wk_str
            , CONCAT('Y', SUBSTR(CAST(lag(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 3, 2), ' W', SUBSTR(CAST(lag(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 5, 2)) as yr_last_wk_str
            , yr_wk
            , start_date
            , end_date
            , LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS yr_last_wk
        FROM meta.wk_calendar
    )
    , cte_agg as (
            SELECT
                acc.yr_week
                , yr_last_wk_str
                , if(STD_DT BETWEEN cal.start_date AND cal.end_date, True, False) as is_current_week
                , bsr_ctgry_label
                , brand

                , AVG(asin_cnt_brand_dt) / 20 * 100 AS share_ratio
                -- , AVG(asin_cnt_brand_dt) / 50 * 100 AS share_ratio
            -- , asin_cnt_brand_dt
            -- *
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                asin IS NOT NULL
                AND UPPER(brand) != 'ZINUS'
                AND bsr_rank_range = 'Top 20'
                -- bsr_rank_range = 'Top 50'
                -- AND yr_week = 'Y25 W50'
                AND (
                    STD_DT BETWEEN cal.start_date AND cal.end_date
                        OR DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
                )
--                 AND bsr_ctgry_label = '01. Mattresses'
            GROUP BY 1, 2, 3, 4, 5
            -- ORDER BY 4 DESC
        )
    SELECT
        tw.*
        , lw.share_ratio AS lw_share_ratio
        , tw.share_ratio - lw.share_ratio as diff
    FROM
        cte_agg tw
            LEFT JOIN cte_agg lw
                ON tw.brand = lw.brand
                    AND tw.bsr_ctgry_label = lw.bsr_ctgry_label
                    AND tw.yr_last_wk_str = lw.yr_week
    WHERE
        tw.is_current_week = true
        AND tw.share_ratio - lw.share_ratio >= 5
    ORDER BY
        tw.share_ratio - lw.share_ratio DESC
    ;

END;

-------------------------------------------------------------------------------------------------------------
-- (진행중..) 지누스 전주 대비 BSR RANK 10등 이상 변동 (Top 50) OR Top 20 / 50 진입,이탈 AND 2주 연속 유지
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
    SET STD_DT = CURRENT_DATE();
--     SET STD_DT = '2025-11-01';

    -- 전주 대비 BSR RANK 10등 이상 변동 (Top 50)
    WITH cte_cal AS (
            SELECT
                CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) AS yr_wk_str
                , CONCAT('Y', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 3, 2), ' W', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 5, 2)) AS yr_last_wk_str
                , yr_wk
                , start_date
                , end_date
                , LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS yr_last_wk
            FROM meta.wk_calendar
        )
        , cte_lw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) = 'ZINUS'
                AND bsr_rank_range = 'Top 50'
                -- last week
                AND DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4
        )
        , cte_tw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) = 'ZINUS'
                AND bsr_rank_range = 'Top 50'
                -- this week
                AND STD_DT BETWEEN cal.start_date AND cal.end_date

--                 AND bsr_ctgry_label = '01. Mattresses'
            GROUP BY 1, 2, 3, 4
        )
    SELECT
        COALESCE(t.asin, l.asin) AS asin
        , COALESCE(t.bsr_ctgry_label, l.bsr_ctgry_label) AS bsr_ctgry_label
        , t.* EXCEPT (asin, yr_week, bsr_ctgry_label, brand)
        , l.rank AS lw_rank
        , t.rank - l.rank AS diff_rank
    FROM
        cte_tw t
            FULL JOIN cte_lw l
                ON t.asin = l.asin
                    AND t.bsr_ctgry_label = l.bsr_ctgry_label
    WHERE
        ABS(t.rank - l.rank) > 10
        OR t.asin IS NULL OR l.asin IS NULL
    ORDER BY 2
    ;

END;
-- BSR TOP 20 / 50 - 진입 또는 이탈, (2주 연속 여부)
BEGIN
    DECLARE STD_DT DATE;
    SET STD_DT = CURRENT_DATE();

    WITH cte_cal AS (
            SELECT
                CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) AS yr_wk_str
                , CONCAT('Y', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 3, 2), ' W', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 5, 2)) AS yr_last_wk_str
                , yr_wk
                , start_date
                , end_date
                , LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS yr_last_wk
            FROM meta.wk_calendar
        )
        , cte_ltw as (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , bsr_rank_range
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) = 'ZINUS'
                AND bsr_rank_range in ('Top 20', 'Top 50')
                -- 2 weeks ago
                AND DATE_SUB(STD_DT, INTERVAL 2 WEEK) BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4, 5
        )
        , cte_lw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , bsr_rank_range
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) = 'ZINUS'
                AND bsr_rank_range in ('Top 20', 'Top 50')
                -- last week
                AND DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4, 5
        )
        , cte_tw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , bsr_rank_range
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) = 'ZINUS'
                AND bsr_rank_range in ('Top 20', 'Top 50')
                AND STD_DT BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4, 5
        )
    SELECT
        COALESCE(t.asin, l.asin, ltw.asin) AS asin
        , COALESCE(t.bsr_ctgry_label, l.bsr_ctgry_label, ltw.bsr_ctgry_label) AS bsr_ctgry_label
        , COALESCE(t.bsr_rank_range, l.bsr_rank_range, ltw.bsr_rank_range) AS bsr_rank_range
        , t.* EXCEPT (asin, yr_week, bsr_ctgry_label, bsr_rank_range, brand)
        , l.rank AS last_week_rank
        , ltw.rank AS two_weeks_ago_rank
--         , t.rank - l.rank AS diff_rank
--         , IF(t.asin IS NOT NULL, 'IN', 'OUT') AS diff_rank_type
        , CASE

              WHEN t.asin IS NOT NULL AND l.asin IS NULL THEN 'IN'
              WHEN l.asin IS NOT NULL AND t.asin IS NULL THEN 'OUT'
              WHEN ltw.asin IS NOT NULL AND l.asin IS NULL AND t.asin IS NULL THEN 'OUT (LEFT_FOR_2_WEEKS_IN_A_ROW)'
              WHEN ltw.asin IS NULL AND l.asin IS NOT NULL AND t.asin IS NOT NULL THEN 'IN (ENTERED_FOR_2_WEEKS_IN_A_ROW)'
          END AS in_out_type
--         , ltw.asin
    FROM
        cte_tw t
            FULL JOIN cte_lw l
                ON t.asin = l.asin
                    AND t.bsr_ctgry_label = l.bsr_ctgry_label
                    AND t.bsr_rank_range = l.bsr_rank_range
            FULL JOIN cte_ltw ltw
                ON COALESCE(t.asin, l.asin) = ltw.asin
                    AND COALESCE(t.bsr_ctgry_label, l.bsr_ctgry_label) = ltw.bsr_ctgry_label
                    AND COALESCE(t.bsr_rank_range, l.bsr_rank_range) = ltw.bsr_rank_range
    WHERE
        t.asin IS NULL OR l.asin IS NULL OR ltw.asin IS NULL
    ORDER BY 3, 2
    ;

END;

-------------------------------------------------------------------------------------------------------------
-- 경쟁사 전주 대비 BSR RANK 10등 이상 변동 (Top 50) OR Top 20 / 50 진입,이탈 AND 2주 연속 유지 -------------
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
    SET STD_DT = CURRENT_DATE();
    --     SET STD_DT = '2025-11-01';

    -- 경쟁사 전주 대비 BSR RANK 10등 이상 변동 (Top 50)
    WITH cte_cal AS (
            SELECT
                CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) AS yr_wk_str
                , CONCAT('Y', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 3, 2), ' W', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 5, 2)) AS yr_last_wk_str
                , yr_wk
                , start_date
                , end_date
                , LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS yr_last_wk
            FROM meta.wk_calendar
        )
        , cte_lw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) != 'ZINUS'
                AND bsr_rank_range = 'Top 50'
                -- last week
                AND DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4
        )
        , cte_tw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) != 'ZINUS'
                AND bsr_rank_range = 'Top 50'
                -- this week
                AND STD_DT BETWEEN cal.start_date AND cal.end_date

            --                 AND bsr_ctgry_label = '01. Mattresses'
            GROUP BY 1, 2, 3, 4
        )
    SELECT
        COALESCE(t.asin, l.asin) AS asin
        , COALESCE(t.bsr_ctgry_label, l.bsr_ctgry_label) AS bsr_ctgry_label
        , t.* EXCEPT (asin, yr_week, bsr_ctgry_label, brand)
        , l.rank AS lw_rank
        , t.rank - l.rank AS diff_rank
    FROM
        cte_tw t
            FULL JOIN cte_lw l
                ON t.asin = l.asin
            AND t.bsr_ctgry_label = l.bsr_ctgry_label
    WHERE
        ABS(t.rank - l.rank) > 10
        OR t.asin IS NULL OR l.asin IS NULL
    ORDER BY 2
    ;

END;
BEGIN
    DECLARE STD_DT DATE;
    SET STD_DT = CURRENT_DATE();
    -- BSR TOP 20 / 50 - 진입 또는 이탈, (2주 연속 여부)
    WITH cte_cal AS (
            SELECT
                CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) AS yr_wk_str
                , CONCAT('Y', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 3, 2), ' W', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 5, 2)) AS yr_last_wk_str
                , yr_wk
                , start_date
                , end_date
                , LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS yr_last_wk
            FROM meta.wk_calendar
        )
        , cte_ltw as (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , bsr_rank_range
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) != 'ZINUS'
                AND bsr_rank_range in ('Top 20', 'Top 50')
                -- 2 weeks ago
                AND DATE_SUB(STD_DT, INTERVAL 2 WEEK) BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4, 5
        )
        , cte_lw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , bsr_rank_range
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) != 'ZINUS'
                AND bsr_rank_range in ('Top 20', 'Top 50')
                -- last week
                AND DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4, 5
        )
        , cte_tw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , bsr_rank_range
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) != 'ZINUS'
                AND bsr_rank_range in ('Top 20', 'Top 50')
                AND STD_DT BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4, 5
        )
    SELECT
        COALESCE(t.asin, l.asin, ltw.asin) AS asin
        , COALESCE(t.bsr_ctgry_label, l.bsr_ctgry_label, ltw.bsr_ctgry_label) AS bsr_ctgry_label
        , COALESCE(t.bsr_rank_range, l.bsr_rank_range, ltw.bsr_rank_range) AS bsr_rank_range
        , t.* EXCEPT (asin, yr_week, bsr_ctgry_label, bsr_rank_range, brand)
        , l.rank AS last_week_rank
        , ltw.rank AS two_weeks_ago_rank
        --         , t.rank - l.rank AS diff_rank
        --         , IF(t.asin IS NOT NULL, 'IN', 'OUT') AS diff_rank_type
        , CASE

              WHEN t.asin IS NOT NULL AND l.asin IS NULL THEN 'IN'
              WHEN l.asin IS NOT NULL AND t.asin IS NULL THEN 'OUT'
              WHEN ltw.asin IS NOT NULL AND l.asin IS NULL AND t.asin IS NULL THEN 'OUT (LEFT_FOR_2_WEEKS_IN_A_ROW)'
              WHEN ltw.asin IS NULL AND l.asin IS NOT NULL AND t.asin IS NOT NULL THEN 'IN (ENTERED_FOR_2_WEEKS_IN_A_ROW)'
          END AS in_out_type
    --         , ltw.asin
    FROM
        cte_tw t
            FULL JOIN cte_lw l
                ON t.asin = l.asin
            AND t.bsr_ctgry_label = l.bsr_ctgry_label
            AND t.bsr_rank_range = l.bsr_rank_range
            FULL JOIN cte_ltw ltw
                ON COALESCE(t.asin, l.asin) = ltw.asin
            AND COALESCE(t.bsr_ctgry_label, l.bsr_ctgry_label) = ltw.bsr_ctgry_label
            AND COALESCE(t.bsr_rank_range, l.bsr_rank_range) = ltw.bsr_rank_range
    WHERE
        t.asin IS NULL OR l.asin IS NULL OR ltw.asin IS NULL
    ORDER BY 3, 2
    ;

END;


-------------------------------------------------------------------------------------------------------------
-- 지누스 전주대비 점유율 5% 이상 하락 (Top 20) -------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
--     SET STD_DT = CURRENT_DATE();
    SET STD_DT = '2025-10-16';

    with cte_cal as (
            SELECT
                CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) as yr_wk_str
                , CONCAT('Y', SUBSTR(CAST(lag(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 3, 2), ' W', SUBSTR(CAST(lag(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 5, 2)) as yr_last_wk_str
                , yr_wk
                , start_date
                , end_date
                , LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS yr_last_wk
            FROM meta.wk_calendar
        )
        , cte_agg as (
            SELECT
                acc.yr_week
                , yr_last_wk_str
                , if(STD_DT BETWEEN cal.start_date AND cal.end_date, True, False) as is_current_week
                , bsr_ctgry_label
                , brand
                , AVG(asin_cnt_brand_dt) / 20 * 100 AS share_ratio
--                 , AVG(asin_cnt_brand_dt) / 50 * 100 AS share_ratio
            -- , asin_cnt_brand_dt
            -- *
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                asin IS NOT NULL
                AND UPPER(brand) = 'ZINUS'
                AND bsr_rank_range = 'Top 20'
--                 AND bsr_rank_range = 'Top 50'
                AND (
                    STD_DT BETWEEN cal.start_date AND cal.end_date
                        OR DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
                    )
            GROUP BY 1, 2, 3, 4, 5
        )
    SELECT
        tw.*
        , lw.share_ratio AS lw_share_ratio
        , tw.share_ratio - lw.share_ratio as diff
    FROM
        cte_agg tw
            LEFT JOIN cte_agg lw
                ON tw.brand = lw.brand
            AND tw.bsr_ctgry_label = lw.bsr_ctgry_label
            AND tw.yr_last_wk_str = lw.yr_week
    WHERE
        tw.is_current_week = true
        AND tw.share_ratio - lw.share_ratio <= -5
    ORDER BY
        tw.share_ratio - lw.share_ratio DESC
    ;

END;

-------------------------------------------------------------------------------------------------------------
-- 지누스 TOP 10 '점유율' '순위' 에서 하락 이동하는 경우 Alert, 2단계 이상 하락시 강한 Alert ----------------
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
    DECLARE YR_WK STRING;
    DECLARE LAST_YR_WK STRING;

--     SET STD_DT = CURRENT_DATE();
        SET STD_DT = '2025-11-01';

    SET YR_WK =  (select CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) from meta.wk_calendar where STD_DT BETWEEN start_date AND end_date);
    SET LAST_YR_WK =  (SELECT CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) FROM meta.wk_calendar WHERE DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN start_date AND end_date);

    WITH
--         cte_cal  AS (
--             SELECT
--                 CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) AS yr_wk_str
--                 , CONCAT('Y', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 3, 2), ' W', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 5, 2)) AS yr_last_wk_str
--                 , yr_wk
--                 , start_date
--                 , end_date
--                 , LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS yr_last_wk
--             FROM
--                 meta.wk_calendar
--         )
--         ,
        cte_agg as (
            SELECT
                acc.yr_week
--                 , LAST_YR_WK
--                 , if(STD_DT BETWEEN cal.start_date AND cal.end_date, True, False) as is_current_week
--                 , if(CURRENT_DATE() BETWEEN cal.start_date AND cal.end_date, True, False) as is_current_week
                , if(acc.yr_week = YR_WK, True, False) as is_current_week
                , bsr_ctgry_label
                , brand
                , AVG(asin_cnt_brand_dt) / 20 * 100 AS share_ratio
                , rank() OVER (PARTITION BY yr_week, bsr_ctgry_label ORDER BY AVG(asin_cnt_brand_dt) DESC) AS share_rank
            --                 , AVG(asin_cnt_brand_dt) / 50 * 100 AS share_ratio
            -- , asin_cnt_brand_dt
            -- *
            FROM
                vs.amz_bsr_shr_daily_acc acc
--                     LEFT JOIN cte_cal cal
--                         ON acc.yr_week = cal.yr_wk_str
            WHERE
                asin IS NOT NULL
--                 AND UPPER(brand) = 'ZINUS'
                AND bsr_rank_range = 'Top 20'
                --                 AND bsr_rank_range = 'Top 50'
                AND
                    yr_week IN (YR_WK, LAST_YR_WK)
--                     (
--                     STD_DT BETWEEN cal.start_date AND cal.end_date
--                         OR DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date

--                     CURRENT_DATE() BETWEEN cal.start_date AND cal.end_date
--                         OR DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
--                     )
            GROUP BY 1, 2, 3, 4, 5
        )
    SELECT
        YR_WK as yr_week
        , COALESCE(a.bsr_ctgry_label, b.bsr_ctgry_label) as bsr_ctgry_label
        , COALESCE(a.brand, b.brand) as brand
        , a.share_ratio
        , b.share_ratio AS lw_share_ratio
        , a.share_rank
        , b.share_rank AS lw_share_rank
        , -(COALESCE(a.share_rank, 21) - COALESCE(b.share_rank, 21)) AS delta_rank
        , if(-(COALESCE(a.share_rank, 21) - COALESCE(b.share_rank, 21)) <= -2, TRUE, FALSE) as is_critical
    FROM
        (SELECT bsr_ctgry_label, brand, share_ratio, share_rank FROM cte_agg WHERE is_current_week = TRUE) a
            FULL JOIN (SELECT bsr_ctgry_label, brand, share_ratio, share_rank FROM cte_agg WHERE is_current_week = FALSE) b
                ON
--                     a.yr_last_wk_str = b.yr_week
--                     and
                    a.bsr_ctgry_label = b.bsr_ctgry_label
                    and a.brand = b.brand
    WHERE
        -(COALESCE(a.share_rank, 21) - COALESCE(b.share_rank, 21)) < 0
        and COALESCE(a.brand, b.brand) = 'ZINUS'
    ;

END;

-------------------------------------------------------------------------------------------------------------
-- 경쟁사 Top 10 점유율 순위 상승 Alert OR (2단계 이상 상승 OR 2주 연속 유지시) 강한 Alert ------------------
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
    DECLARE YR_WK STRING;
    DECLARE LAST_YR_WK STRING;
    DECLARE TWO_WEEKS_AGO_YR_WK STRING;

        SET STD_DT = CURRENT_DATE();
--     SET STD_DT = '2025-11-01';

    SET YR_WK =  (select CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) from meta.wk_calendar where STD_DT BETWEEN start_date AND end_date);
    SET LAST_YR_WK =  (SELECT CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) FROM meta.wk_calendar WHERE DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN start_date AND end_date);
    SET TWO_WEEKS_AGO_YR_WK =  (SELECT CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) FROM meta.wk_calendar WHERE DATE_SUB(STD_DT, INTERVAL 2 WEEK) BETWEEN start_date AND end_date);

    WITH
        cte_agg as (
            SELECT
                acc.yr_week
--                 , if(acc.yr_week = YR_WK, True, False) as is_current_week
                , bsr_ctgry_label
                , brand
                , AVG(asin_cnt_brand_dt) / 20 * 100 AS share_ratio
                --                 , AVG(asin_cnt_brand_dt) / 50 * 100 AS share_ratio
                , rank() OVER (PARTITION BY yr_week, bsr_ctgry_label ORDER BY AVG(asin_cnt_brand_dt) DESC) AS share_rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
            WHERE
                asin IS NOT NULL
                AND bsr_rank_range = 'Top 20'
                --                 AND bsr_rank_range = 'Top 50'
                AND yr_week IN (YR_WK, LAST_YR_WK, TWO_WEEKS_AGO_YR_WK)
            GROUP BY 1, 2, 3
        )
    SELECT
        YR_WK as yr_week
        , COALESCE(a.bsr_ctgry_label, b.bsr_ctgry_label) as bsr_ctgry_label
        , COALESCE(a.brand, b.brand) as brand
        , a.share_ratio
        , b.share_ratio AS lw_share_ratio
        , a.share_rank
        , b.share_rank AS share_rank_1
        , c.share_rank as share_rank_2
        , -(COALESCE(a.share_rank, 21) - COALESCE(b.share_rank, 21)) AS delta_rank
        , IF(
                -(COALESCE(a.share_rank, 21) - COALESCE(b.share_rank, 21)) >= 2
                OR
                (-(COALESCE(b.share_rank, 21) - COALESCE(c.share_rank, 21)) > 0 and -(COALESCE(a.share_rank, 21) - COALESCE(b.share_rank, 21)) >= 0)
            , TRUE, FALSE) AS is_critical
    FROM
        (SELECT bsr_ctgry_label, brand, share_ratio, share_rank FROM cte_agg WHERE yr_week = YR_WK) a
            FULL JOIN (SELECT bsr_ctgry_label, brand, share_ratio, share_rank FROM cte_agg WHERE yr_week = LAST_YR_WK) b
                ON
                    a.bsr_ctgry_label = b.bsr_ctgry_label
                    AND a.brand = b.brand
            FULL JOIN (SELECT bsr_ctgry_label, brand, share_ratio, share_rank FROM cte_agg WHERE yr_week = TWO_WEEKS_AGO_YR_WK) c
                ON
                    COALESCE(a.bsr_ctgry_label, b.bsr_ctgry_label) = c.bsr_ctgry_label
                    AND COALESCE(a.brand, b.brand) = c.brand
    WHERE
        -(COALESCE(a.share_rank, 21) - COALESCE(b.share_rank, 21)) > 0
        AND COALESCE(a.brand, b.brand, c.brand) != 'ZINUS'
    ;

END;


-------------------------------------------------------------------------------------------------------------
-- 지누스 TOP 10 Brand Rank 순위에서 하락 이동하는 경우 Alert, 2단계 이상 하락시 강한 Alert -----------------
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
    SET STD_DT = CURRENT_DATE();
    --     SET STD_DT = '2025-11-01';

    -- 전주 대비 BSR RANK 10등 이상 변동 (Top 50)
    WITH
        cte_cal  AS (
            SELECT
                CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) AS yr_wk_str
                , CONCAT('Y', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 3, 2), ' W',
                         SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 5, 2)) AS yr_last_wk_str
                , yr_wk
                , start_date
                , end_date
                , LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS yr_last_wk
            FROM
                meta.wk_calendar
        )
        , cte_lw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) = 'ZINUS'
                AND bsr_rank_range = 'Top 10'
                -- last week
                AND DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4
        )
        , cte_tw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) = 'ZINUS'
                AND bsr_rank_range = 'Top 10'
                -- this week
                AND STD_DT BETWEEN cal.start_date AND cal.end_date

            --                 AND bsr_ctgry_label = '01. Mattresses'
            GROUP BY 1, 2, 3, 4
        )
    SELECT
        COALESCE(t.asin, l.asin) AS asin
        , COALESCE(t.bsr_ctgry_label, l.bsr_ctgry_label) AS bsr_ctgry_label
        , t.* EXCEPT (asin, yr_week, bsr_ctgry_label, brand)
        , l.rank AS lw_rank
        , COALESCE(t.rank, 0) - l.rank AS diff_rank
        , IF(COALESCE(t.rank, 0) - l.rank > -2 , 'warn', 'danger') AS alert_type
    FROM
        cte_tw t
            FULL JOIN cte_lw l
                ON t.asin = l.asin AND t.bsr_ctgry_label = l.bsr_ctgry_label
    WHERE
        t.rank - l.rank < 0
        OR t.asin IS NULL
        OR l.asin IS NULL
    ORDER BY 2;

END;


-------------------------------------------------------------------------------------------------------------
-- 경쟁사 Top 10 브랜드 Rank 순위 상승 Alert OR (2단계 이상 상승 OR 2주 연속 유지시) 강한 Alert -------------
-------------------------------------------------------------------------------------------------------------
BEGIN
    DECLARE STD_DT DATE;
    SET STD_DT = CURRENT_DATE();
    --     SET STD_DT = '2025-11-01';

    WITH cte_cal AS (
            SELECT
                CONCAT('Y', SUBSTR(CAST(yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(yr_wk AS STRING), 5, 2)) AS yr_wk_str
                , CONCAT('Y', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 3, 2), ' W', SUBSTR(CAST(LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS STRING), 5, 2)) AS yr_last_wk_str
                , yr_wk
                , start_date
                , end_date
                , LAG(yr_wk, 1) OVER (ORDER BY yr_wk) AS yr_last_wk
            FROM meta.wk_calendar
        )
        , cte_ltw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) != 'ZINUS'
                AND bsr_rank_range = 'Top 10'
                -- last week
                --                 AND DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
                AND DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK) BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4
        )
        , cte_lw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) != 'ZINUS'
                AND bsr_rank_range = 'Top 10'
                -- last week
--                 AND DATE_SUB(STD_DT, INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
                AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK) BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4
        )
--        SELECT * FROM cte_lw;
        , cte_tw AS (
            SELECT
                acc.yr_week
                , bsr_ctgry_label
                , brand
                , acc.asin
                , AVG(rank) AS rank
            FROM
                vs.amz_bsr_shr_daily_acc acc
                    LEFT JOIN meta.amz_zinus_master_pdt_pi b
                        ON acc.asin = b.asin
                    LEFT JOIN cte_cal cal
                        ON acc.yr_week = cal.yr_wk_str
            WHERE
                acc.asin IS NOT NULL
                AND UPPER(brand) != 'ZINUS'
                AND bsr_rank_range = 'Top 10'
                -- this week
--                 AND STD_DT BETWEEN cal.start_date AND cal.end_date
                AND CURRENT_DATE() BETWEEN cal.start_date AND cal.end_date
            GROUP BY 1, 2, 3, 4
        )
    SELECT
        COALESCE(t.asin, l.asin, ltw.asin) AS asin
        , COALESCE(t.bsr_ctgry_label, l.bsr_ctgry_label, ltw.bsr_ctgry_label) AS bsr_ctgry_label
        , t.* EXCEPT (asin, yr_week, bsr_ctgry_label, brand)
        , l.rank AS last_week_rank
        , ltw.rank AS two_weeks_ago_rank
        , IF(l.rank IS NULL , t.rank-10, t.rank - l.rank) AS diff_rank
--         , IF(ABS(IF(l.rank IS NULL , -t.rank, t.rank - l.rank)) >= 2, 'danger', 'warn') AS alert_type
        , CASE
              WHEN IF(ltw.rank IS NULL AND l.rank IS NOT NULL, l.rank - 11, l.rank - ltw.rank) < 0 AND IF(ltw.rank IS NULL AND t.rank IS NOT NULL, t.rank - 10, t.rank - ltw.rank) < 0  THEN 'danger (rising for 2 consecutive weeks)' -- 2주 연속 상승 유지
              WHEN IF(l.rank IS NULL, t.rank - 11, t.rank - l.rank) <= -2 THEN 'danger' -- 전주 대비 2단계 상승
              WHEN IF(l.rank IS NULL, t.rank - 11, t.rank - l.rank) < 0 THEN 'warn' -- 전주 대비 상승
        END AS alert_type
    FROM
        cte_tw t
            FULL JOIN cte_lw l
                ON t.asin = l.asin
                    AND t.bsr_ctgry_label = l.bsr_ctgry_label
            FULL JOIN cte_ltw ltw
                ON COALESCE(t.asin, l.asin) = ltw.asin
                    AND COALESCE(t.bsr_ctgry_label, l.bsr_ctgry_label) = ltw.bsr_ctgry_label
    WHERE
        IF(l.rank IS NULL , t.rank-10, t.rank - l.rank)  < 0 -- 순위 상승 및 진입
        AND t.asin is not null
    ORDER BY 2
    ;

END;

-- -- 지난주 대비 평점 4.0 이하로 하락
-- WITH cte_src AS (
--         SELECT
--             a.asin
--             , PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc) AS crawl_datetime
--             , DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) as crawl_date
--             , cal.yr
--             , cal.yr_month
--             , cal.yr_wk
--             , cal2.yr_wk AS yr_last_wk
--             , NULLIF(b.collection, 'nan') AS collection
--             , b.abbre
--             , b.financial_category
--             , REGEXP_REPLACE(TRIM(brand), r'[^[:print:]]', '') AS brand
--             , rating
--             , ratings_total AS cnt_all
--         -- , rating_breakdown_five_star_count AS cnt5
--         -- , rating_breakdown_four_star_count AS cnt4
--         -- , rating_breakdown_three_star_count AS cnt3
--         -- , rating_breakdown_two_star_count AS cnt2
--         -- , rating_breakdown_one_star_count AS cnt1
--         -- , rating_breakdown_five_star_percentage AS ratio5
--         -- , rating_breakdown_four_star_percentage AS ratio4
--         -- , rating_breakdown_three_star_percentage AS ratio3
--         -- , rating_breakdown_two_star_percentage AS ratio2
--         -- , rating_breakdown_one_star_percentage AS ratio1
--         FROM
--             dw.rf_amz_pdt_zns_comp_daily a
--                 JOIN meta.amz_zinus_master_pdt_pi b
--                     ON a.asin = b.asin
--                 LEFT JOIN meta.wk_calendar_new cal
--                     ON DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) BETWEEN cal.start_date AND cal.end_date
--                 LEFT JOIN meta.wk_calendar_new cal2
--                     ON DATE_SUB(DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)), INTERVAL 1 WEEK) BETWEEN cal2.start_date AND cal2.end_date
--     )
--     , cte_wk_summ as (
--         SELECT
--             asin
--             , yr_wk
--             , yr_last_wk
--             , collection
--             , abbre
--             , financial_category
--             , brand
--             , AVG(rating) AS wk_avg_rating
--             , MAX(cnt_all) AS rvw_cnt
--         FROM
--             cte_src
--         -- WHERE
--         --     yr_wk = CAST(FORMAT_DATE('%G%V', DATE(DATE_SUB(STD_DT, INTERVAL 0 WEEK))) AS INT64)
--         GROUP BY 1, 2, 3, 4, 5, 6, 7
--         -- HAVING
--         --     AVG(rating) < 4
--         ORDER BY 8, 9 DESC
--     )
-- SELECT
--     tw.* EXCEPT (yr_last_wk)
--     , lw.yr_wk AS lw_yr_wk
--     , lw.wk_avg_rating AS lw_avg_rating
--     , lw.rvw_cnt AS lw_rvw_cnt
-- FROM
--     cte_wk_summ tw
--         LEFT JOIN cte_wk_summ lw ON tw.asin = lw.asin AND tw.yr_wk = lw.yr_last_wk
-- WHERE
--     tw.wk_avg_rating < 4
--     AND lw.wk_avg_rating >= 4
--     AND tw.yr_wk = CAST(FORMAT_DATE('%G%V', DATE(DATE_SUB(STD_DT, INTERVAL 1 WEEK))) AS INT64)
-- ;


-- 전주 대비 평점 -0.1 이상 하락 or 4주 평균 대비 -0.1 이상 하락
-- WITH cte_src AS (
--         SELECT
--             a.asin
--             , PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc) AS crawl_datetime
--             , DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) as crawl_date
--             , cal.yr
--             , cal.yr_month
--             , cal.yr_wk
--             , cal2.yr_wk AS yr_last_wk
--             , NULLIF(b.collection, 'nan') AS collection
--             , b.abbre
--             , b.financial_category
--             , upper(REGEXP_REPLACE(TRIM(brand), r'[^[:print:]]', '')) AS brand
--             , rating
--             , ratings_total AS cnt_all
--         FROM
--             dw.rf_amz_pdt_zns_comp_daily a
--                 JOIN meta.amz_zinus_master_pdt_pi b
--                     ON a.asin = b.asin
--                 LEFT JOIN meta.wk_calendar_new cal
--                     ON DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)) BETWEEN cal.start_date AND cal.end_date
--                 LEFT JOIN meta.wk_calendar_new cal2
--                     ON DATE_SUB(DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', a.crawlTime_utc)), INTERVAL 1 WEEK) BETWEEN cal2.start_date AND cal2.end_date
--     )
--     , cte_wk_summ as (
--         SELECT
--             asin
--             , yr_wk
--             , yr_last_wk
--             , collection
--             , abbre
--             , financial_category
--             , brand
--             , AVG(rating) AS wk_avg_rating
--             , MAX(cnt_all) AS rvw_cnt
--         FROM
--             cte_src
--         GROUP BY
--             1, 2, 3, 4, 5, 6, 7
--         ORDER BY 8, 9 DESC
--     )
-- SELECT
--     tw.* EXCEPT (yr_last_wk)
--     , lw.yr_wk AS lw_yr_wk
--     , lw.wk_avg_rating AS lw_avg_rating
--     , lw.rvw_cnt AS lw_rvw_cnt
-- FROM
--     cte_wk_summ tw
--         LEFT JOIN cte_wk_summ lw ON tw.asin = lw.asin AND tw.yr_last_wk = lw.yr_wk
-- WHERE
--     tw.wk_avg_rating - lw.wk_avg_rating < -0.1
--     -- AND lw.wk_avg_rating >= 4
--     AND tw.yr_wk = CAST(FORMAT_DATE('%G%V', DATE(DATE_SUB(STD_DT, INTERVAL 0 WEEK))) AS INT64)
-- ;


-- SELECT
--     *
-- FROM
--     dw.rf_amz_pdt_zns_comp_daily
--         TABLESAMPLE SYSTEM ( 1 percent )
-- ;

