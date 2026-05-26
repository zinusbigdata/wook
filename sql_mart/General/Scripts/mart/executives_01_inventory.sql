-- [erp master] --------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp1.erp_sku_category_mst as
WITH
    cte_sku as (
        -- unique check ---------------------------
        -- SELECT
        --     zinus_sku
        --     , count(DISTINCT Zinus_SKU)
        --     , count(Zinus_SKU)
        --     , count(OMSID)
        --     , count(DISTINCT OMSID)
        -- FROM meta.erp_material_mapping
        -- where
        --     LOWER(Customer_Name) LIKE 'amazon%'
        --     AND OMSID IS NOT NULL
        --     AND Zinus_SKU IS NOT NULL
        -- GROUP BY 1
        -- HAVING count(DISTINCT Zinus_SKU) > 1

        SELECT
            OMSID as asin
            , MAX(Zinus_SKU) as zinus_sku
        FROM
            meta.erp_material_mapping
        WHERE
            LOWER(Customer_Name) LIKE 'amazon%'
            AND OMSID IS NOT NULL
            AND Zinus_SKU IS NOT NULL
        GROUP BY 1
    )
    , cte_category AS (
        SELECT
            *
        FROM
            (
                SELECT
                    zinus_sku
                    , prdct_h_lv1 AS category
                    , collection
                    , ROW_NUMBER() OVER (PARTITION BY zinus_sku ORDER BY erp_date DESC) AS filter_rnk
                FROM
                    erp.do_di_us_daily
            )
        WHERE filter_rnk = 1
    )
SELECT
    a.asin
    , a.zinus_sku
    , b.category
    , b.collection
FROM
    cte_sku a
        left join cte_category b
            on a.zinus_sku = b.zinus_sku;

------------------------------------------------------------------------------------------------------------------------


-- 아마존 보유 재고 / di inv & monthly category shipped_cogs, sellable_on_hand_inventory
-- source data : vendor central inventory (monthly + weekly)
-- select max(yr_month) from hq.di_monthly_inv;

BEGIN
    DECLARE LAST_UPDATE DATE;
    set LAST_UPDATE = (
        -- SELECT MAX(date) FROM vc.amz_vc_inv_all
        SELECT MAX(date) FROM vc.amz_vc_inv_daily_all
    );

    CREATE OR REPLACE TABLE tmp1.vc_inv_sales_fact AS
    WITH
        -- cte_weekly_inv      AS (
        --     SELECT
        --         asin
        --         , sellable_on_hand_inventory
        --         , sellable_on_hand_units
        --         , CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month
        --     FROM
        --         vc.amz_vc_inv_all
        --     WHERE
        --         date = LAST_UPDATE
        -- )
        cte_daily_inv      AS (
            SELECT
                asin
                , sellable_on_hand_inventory
                , sellable_on_hand_units
                , CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month
            FROM
                vc.amz_vc_inv_daily_all
            WHERE
                date = LAST_UPDATE
        )
        , cte_monthly_inv   AS (
            SELECT
                asin
                , sellable_on_hand_inventory
                , sellable_on_hand_units
                , CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS INT64) AS yr_month
            FROM
                vc.amz_vc_inv_monthly
        )
        , cte_inv           AS (
            SELECT
                *
            FROM
                cte_monthly_inv

            UNION ALL

            SELECT
                *
            FROM
                -- cte_weekly_inv w
                cte_daily_inv d
            WHERE
                NOT EXISTS (
                    SELECT 1
                    FROM cte_monthly_inv AS m
                    -- WHERE w.yr_month = m.yr_month
                    WHERE d.yr_month = m.yr_month
                )
        )
        , cte_monthly_sales AS (
            SELECT
                asin
                , Ordered_Revenue
                , ordered_units
                , shipped_cogs
                , shipped_units
                , CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month
            FROM
                vc.amz_vc_sales_monthly
        )
        -- , cte_weekly_sales  AS (
        --     SELECT
        --         asin
        --         , SUM(Ordered_Revenue) AS Ordered_Revenue
        --         , SUM(ordered_units) AS Ordered_units
        --         , SUM(shipped_cogs) AS shipped_cogs
        --         , SUM(shipped_units) AS shipped_units
        --         , CAST(FORMAT_DATE('%Y%m', MAX(date)) AS INT64) AS yr_month
        --     FROM
        --         vc.amz_vc_sales_weekly_all
        --     WHERE
        --         -- 현재 월인 경우 window로 한달간의 sales 합
        --         date BETWEEN
        --                 DATE_SUB((SELECT MAX(date) FROM vc.amz_vc_sales_weekly_all), INTERVAL 1 MONTH)
        --                 AND
        --                 (SELECT MAX(date) FROM vc.amz_vc_sales_weekly_all)
        --
        --     -- WHERE
        --     --     FORMAT_DATE('%Y%m', date) = (
        --     --         SELECT MAX(FORMAT_DATE('%Y%m', date))
        --     --         FROM vc.amz_vc_sales_weekly_all
        --     --     )
        --     --     AND asin = 'B006MIPW70'
        --     GROUP BY 1
        -- )
        , cte_daily_sales  AS (
            SELECT
                asin
                , SUM(Ordered_Revenue) AS Ordered_Revenue
                , SUM(ordered_units) AS Ordered_units
                , SUM(shipped_cogs) AS shipped_cogs
                , SUM(shipped_units) AS shipped_units
                , CAST(FORMAT_DATE('%Y%m', MAX(date)) AS INT64) AS yr_month
            FROM
                vc.amz_vc_sales_daily_all
            WHERE
                -- 현재 월인 경우 window로 한달간의 sales 합
                date BETWEEN
                    DATE_SUB((SELECT MAX(date) FROM vc.amz_vc_sales_daily_all), INTERVAL 1 MONTH)
                    AND
                    (SELECT MAX(date) FROM vc.amz_vc_sales_daily_all)
            GROUP BY 1
        )
        , cte_current_month_daily_sales  AS (
            SELECT
                asin
                , SUM(Ordered_Revenue) AS Ordered_Revenue
                , SUM(ordered_units) AS Ordered_units
                , SUM(shipped_cogs) AS shipped_cogs
                , SUM(shipped_units) AS shipped_units
                , CAST(FORMAT_DATE('%Y%m', MAX(date)) AS INT64) AS yr_month
            FROM
                vc.amz_vc_sales_daily_all
            WHERE
                -- 현재 월 sales
                FORMAT_DATE('%Y%m', date) = FORMAT_DATE('%Y%m', (SELECT MAX(date) FROM vc.amz_vc_sales_daily_all))
            GROUP BY 1
        )
        , cte_sales         AS (
            SELECT
                *
            FROM
                cte_monthly_sales

            UNION ALL

            SELECT
                *
            FROM
                cte_daily_sales d
            WHERE
                NOT EXISTS (
                    SELECT 1
                    FROM cte_monthly_sales AS m
                    WHERE d.yr_month = m.yr_month
                )
        )
        , cte_sales2         AS (
            SELECT
                *
            FROM
                cte_monthly_sales

            UNION ALL

            SELECT
                *
            FROM
                cte_current_month_daily_sales d
            WHERE
                NOT EXISTS (
                    SELECT 1
                    FROM cte_monthly_sales AS m
                    WHERE d.yr_month = m.yr_month
                )
        )
        , cte_inv_sales     AS (
            SELECT
                COALESCE(inv.asin, sales.asin) AS asin
                , COALESCE(inv.yr_month, sales.yr_month) AS yr_month
                , inv.sellable_on_hand_inventory
                , inv.sellable_on_hand_units
                , sales.Ordered_Revenue AS ordered_revenue
                , sales.ordered_units AS ordered_units
                , sales.shipped_cogs AS shipped_cogs
                , sales.shipped_units AS shipped_units

            FROM
                cte_inv inv
                    FULL OUTER JOIN cte_sales AS sales
                        ON inv.asin = sales.asin AND inv.yr_month = sales.yr_month
        )
        , cte_inv_sales2    AS (
            SELECT
                COALESCE(s1.asin, s2.asin) AS asin
                , COALESCE(s1.yr_month, s2.yr_month) AS yr_month
                , s1.sellable_on_hand_inventory
                , s1.sellable_on_hand_units

                --  wos 계산을 위한 컬럼 (현재월 = (오늘-4주 ~ 오늘)
                , s1.Ordered_Revenue
                , s1.ordered_units
                , s1.shipped_cogs
                , s1.shipped_units

                --  현재월의 합계를 위한 컬럼
                , s2.Ordered_Revenue AS ordered_revenue2
                , s2.ordered_units AS ordered_units2
                , s2.shipped_cogs AS shipped_cogs2
                , s2.shipped_units AS shipped_units2
            FROM
                cte_inv_sales s1
                    FULL OUTER JOIN cte_sales2 AS s2
                        ON s1.asin = s2.asin AND s1.yr_month = s2.yr_month
        )
    SELECT
        a.*
        , b.zinus_sku AS sku
        , b.category
        , LAST_DAY(PARSE_DATE('%Y%m%d', CONCAT(CAST(a.yr_month AS STRING), '01')), MONTH) AS end_date
    FROM
        cte_inv_sales2 a
            LEFT JOIN tmp1.erp_sku_category_mst b
                ON a.asin = b.asin;

    CREATE OR REPLACE TABLE hq.di_monthly_inv AS
    WITH
       cte_summ as (
            SELECT
                yr_month
                , IF(category IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS prd_type
                , SUM(sellable_on_hand_inventory) AS inv_amt
                , SUM(sellable_on_hand_units) AS inv_qty
                , SUM(sellable_on_hand_inventory) / ( SUM(shipped_cogs) * 7 / EXTRACT(DAY FROM MAX(end_date)) ) AS wos_shipped_cogs
            FROM
                tmp1.vc_inv_sales_fact
            GROUP BY
                GROUPING SETS (
                   ( 1, 2 ), ( 1 )
                )
        )
    SELECT
        yr_month
        , COALESCE(prd_type, 'T') AS prd_type
        , inv_amt
        , inv_qty
        , wos_shipped_cogs as wos
        , LAST_UPDATE as last_update
    FROM
        cte_summ
    WHERE
        yr_month >= 202201
    ORDER BY
        1, CASE
               WHEN prd_type = 'T' THEN 1
               WHEN prd_type = 'M' THEN 2
               WHEN prd_type = 'N' THEN 3
               ELSE 4
           END
    ;

    -- [vc wos] --------------------------------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE tmp1.vc_wos AS
    SELECT
        yr_month
        , IFNULL(IF(GROUPING ( f.category ) = 1, 'TOTAL', f.category), 'UNKNOWN') AS category

        , SUM(sellable_on_hand_inventory) AS inv_amt
        , SUM(sellable_on_hand_units) AS inv_qty

        , SUM(ordered_revenue) AS ordered_revenue
        , SUM(ordered_units) AS ordered_units

        , SUM(shipped_cogs) AS shipped_cogs
        , SUM(shipped_units) AS shipped_units

        , if(SUM(shipped_cogs) = 0, null, SUM(sellable_on_hand_inventory) / ( SUM(shipped_cogs) * 7 / EXTRACT(DAY FROM MAX(end_date)) )) AS wos

        --  오늘이 월말이 아닌 경우 최근 4주가 아닌 현재 월의 합계를 위한 컬럼
        , SUM(ordered_revenue2) AS ordered_revenue2
        , SUM(ordered_units2) AS ordered_units2
        , SUM(shipped_cogs2) AS shipped_cogs2
        , SUM(shipped_units2) AS shipped_units2
    FROM
        tmp1.vc_inv_sales_fact f
    GROUP BY
        GROUPING SETS (
            ( 1, f.category )
            , ( 1 )
        )
    ;

    -- [vc sellout & cogs / mart.vc_sales_sellout_cogs] ----------------------------------------------------------------
    CREATE OR REPLACE TABLE mart.vc_sales_sellout_cogs AS
    SELECT
        yr_month
        , category
        , 'WOS' as sell_type
        , CAST(NULL AS STRING) AS value_type
        , wos as val
        , LAST_UPDATE AS last_update
    FROM
        tmp1.vc_wos

    UNION ALL

    SELECT
        yr_month
        , category
        , 'Sell-Out' as sell_type
        , 'Amount' as value_type
        , ordered_revenue2 as val
        , LAST_UPDATE AS last_update
    FROM
        tmp1.vc_wos

    UNION all

    SELECT
        yr_month
        , category
        , 'Sell-Out' as sell_type
        , 'Quantity' as value_type
        , ordered_units2 as val
        , LAST_UPDATE AS last_update
    FROM
        tmp1.vc_wos

    UNION ALL

    SELECT
        yr_month
        , category
        , 'Shipped COGS' as sell_type
        , 'Amount' as value_type
        , shipped_cogs2 as val
        , LAST_UPDATE AS last_update
    FROM
        tmp1.vc_wos

    UNION all

    SELECT
        yr_month
        , category
        , 'Shipped COGS' as sell_type
        , 'Quantity' as value_type
        , shipped_units2 as val
        , LAST_UPDATE AS last_update
    FROM
        tmp1.vc_wos

    UNION ALL

    SELECT
        yr_month
        , category
        , 'Inventory' as sell_type
        , 'Amount' as value_type
        , inv_amt as val
        , LAST_UPDATE AS last_update
    FROM
        tmp1.vc_wos

    UNION all

    SELECT
        yr_month
        , category
        , 'Inventory' as sell_type
        , 'Quantity' as value_type
        , inv_qty as val
        , LAST_UPDATE AS last_update
    FROM
        tmp1.vc_wos
    ;

END;

------------------------------------------------------------------------------------------------------------------------

-- IBP / di inv & monthly category shipped_cogs, sellable_on_hand_inventory
-- source data : dw.ibp_amz_sell_out_fcst_scenario,  vc.amz_vc_inv_monthly & vc.amz_vc_inv_daily_all
BEGIN

    -- IBP WOS
    CREATE OR REPLACE TABLE tmp1.ibp_wos AS
    WITH
        cte_inv   AS (
            SELECT
                yr_month
                , CATEGORY
                , SUM(CAST(ZSPCHANNELRECOMMWTHPOQTY AS FLOAT64)) AS inv
            FROM
                (
                    SELECT
                        c.yr_month
                        , COALESCE(ibp.CATEGORY, mst.CATEGORY) as CATEGORY
                        , ZSPCHANNELRECOMMWTHPOQTY
                        , RANK() OVER (PARTITION BY c.yr_month, COALESCE(ibp.CATEGORY, mst.CATEGORY) ORDER BY PERIODID4_TSTAMP DESC) AS rnum
                    FROM
                        dw.ibp_amz_sell_out_fcst_scenario ibp
                            left JOIN meta.wk_calendar_new c
                                on date(ibp.PERIODID4_TSTAMP) between c.start_date and c.end_date
                            left join tmp1.erp_sku_category_mst mst
                                on ibp.PRDID = mst.zinus_sku
                    WHERE
                        ibp.DPSCENARIONAME = 'Growth'
                        AND ibp.CHANNEL = 'AMZ DI'

                        -- 25.05.09 / 시나리오명 변경 25.04.21 이전 Growth, 이후 Upside
                        AND date(ibp.PERIODID4_TSTAMP) < '2025-04-21'
                        -- AND ibp.CHANNEL NOT IN ('SuperOrdinary', 'AMZ DDS')

                    UNION ALL

                    SELECT
                        c.yr_month
                        , COALESCE(ibp.CATEGORY, mst.CATEGORY) as CATEGORY
                        , ZSPCHANNELRECOMMWTHPOQTY
                        , RANK() OVER (PARTITION BY c.yr_month, COALESCE(ibp.CATEGORY, mst.CATEGORY) ORDER BY PERIODID4_TSTAMP DESC) AS rnum
                    FROM
                        dw.ibp_amz_sell_out_fcst_scenario ibp
                        left JOIN meta.wk_calendar_new c
                    on date(ibp.PERIODID4_TSTAMP) between c.start_date and c.end_date
                        left join tmp1.erp_sku_category_mst mst
                        on ibp.PRDID = mst.zinus_sku
                    WHERE
                        ibp.DPSCENARIONAME = 'Upside'
                        AND ibp.CHANNEL = 'AMZ DI'

                        -- 25.05.09 / 시나리오명 변경 25.04.21 이전 Growth, 이후 Upside
                        AND date(ibp.PERIODID4_TSTAMP) >= '2025-04-21'
                )
            WHERE
                rnum = 1
            GROUP BY 1, 2
        )
        , cte_cogs_src AS (
            SELECT
                c.yr_month
                , PERIODID4_TSTAMP
                , COALESCE(ibp.CATEGORY, mst.CATEGORY, 'UNKNOWN') as CATEGORY
                , ZDPFINALALIGNEDFCST
                , ZDPFINALALIGNEDFCSTREV
                , DATE_SUB(DATE_ADD(DATE(c.thursday_date), INTERVAL 1 WEEK), INTERVAL 3 day) AS s
                , DATE_SUB(DATE_ADD(DATE(c.thursday_date), INTERVAL 13 WEEK), INTERVAL 3 day) AS e

                --  해당 월의 마지막주 이후  next wk13 구하기 위한 기준 값
                , RANK() OVER (PARTITION BY c.yr_month, COALESCE(ibp.CATEGORY, mst.CATEGORY) ORDER BY PERIODID4_TSTAMP DESC) AS rnum
            FROM
                dw.ibp_amz_sell_out_fcst_scenario ibp
                    left JOIN meta.wk_calendar_new c
                        on date(ibp.PERIODID4_TSTAMP) between c.start_date and c.end_date
                    left join tmp1.erp_sku_category_mst mst
                        on ibp.PRDID = mst.zinus_sku
            WHERE
                ibp.DPSCENARIONAME = 'Growth'
                AND ibp.CHANNEL = 'AMZ DI'
                -- AND ibp.CHANNEL NOT IN ('SuperOrdinary', 'AMZ DDS')

                -- 25.05.09 / 시나리오명 변경 25.04.21 이전 Growth, 이후 Upside
                AND date(ibp.PERIODID4_TSTAMP) < '2025-04-21'

            UNION ALL

            SELECT
                c.yr_month
                , PERIODID4_TSTAMP
                , COALESCE(ibp.CATEGORY, mst.CATEGORY, 'UNKNOWN') as CATEGORY
                , ZDPFINALALIGNEDFCST
                , ZDPFINALALIGNEDFCSTREV
                , DATE_SUB(DATE_ADD(DATE(c.thursday_date), INTERVAL 1 WEEK), INTERVAL 3 day) AS s
                , DATE_SUB(DATE_ADD(DATE(c.thursday_date), INTERVAL 13 WEEK), INTERVAL 3 day) AS e

                --  해당 월의 마지막주 이후  next wk13 구하기 위한 기준 값
                , RANK() OVER (PARTITION BY c.yr_month, COALESCE(ibp.CATEGORY, mst.CATEGORY) ORDER BY PERIODID4_TSTAMP DESC) AS rnum
            FROM
                dw.ibp_amz_sell_out_fcst_scenario ibp
                    left JOIN meta.wk_calendar_new c
                        on date(ibp.PERIODID4_TSTAMP) between c.start_date and c.end_date
                    left join tmp1.erp_sku_category_mst mst
                        on ibp.PRDID = mst.zinus_sku
            WHERE
                ibp.DPSCENARIONAME = 'Upside'
                AND ibp.CHANNEL = 'AMZ DI'

                -- 25.05.09 / 시나리오명 변경 25.04.21 이전 Growth, 이후 Upside
                AND date(ibp.PERIODID4_TSTAMP) >= '2025-04-21'
        )
        , cte_f13 AS (
            -- 각 월별 f1 ~ f13 추가
            SELECT
                yr_month
                , CATEGORY
                , MAX(s) AS s
                , MAX(e) AS e
            FROM
                cte_cogs_src
            WHERE
                rnum = 1
            GROUP BY 1, 2
        )
        , cte_cogs as (
            SELECT
                f13.yr_month
                , s.CATEGORY

                , SUM(CAST(s.ZDPFINALALIGNEDFCST AS FLOAT64)) / 13 AS f13
            FROM
                cte_cogs_src AS s
                    LEFT JOIN cte_f13 AS f13
                        ON DATE(s.PERIODID4_TSTAMP) BETWEEN f13.s AND f13.e AND s.CATEGORY = f13.CATEGORY
            GROUP BY
                1, 2
            HAVING
                COUNT(DISTINCT s.PERIODID4_TSTAMP) = 13
        )
        SELECT
            IFNULL(IF(GROUPING ( a.category ) = 1, 'TOTAL', a.category), 'UNKNOWN') AS category
            , a.yr_month
            , CAST(SUBSTRING(CAST(A.yr_month AS STRING), 1, 4) AS INT) AS year
            , sum(f13) as f13
            , sum(b.inv) as inv
            , IF(sum(f13) != 0, SUM(b.inv) / sum(f13), 0) AS wos
        FROM
            cte_cogs a
                LEFT JOIN cte_inv b
                    ON a.yr_month = b.yr_month AND a.CATEGORY = b.CATEGORY
        GROUP BY
            GROUPING SETS (
                ( a.category, 2, 3 ), ( 2, 3 )
            )
    ;

    -- [ wos / vc + ibp / mart.ibp_vc_wos] -----------------------------------------------------------------------------
    CREATE OR REPLACE TABLE mart.ibp_vc_wos AS
    SELECT
        yr_month
        , category
        , 'IBP' as src_type
        , wos
    FROM
        tmp1.ibp_wos

    UNION all

    SELECT
        yr_month
        , category
        , 'VC' as src_type
        , wos
    FROM
        tmp1.vc_wos
    ;

    -- [ibp cogs / mart.ibp_cogs] --------------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE mart.ibp_cogs as
    WITH
        cte_inv   AS (
            SELECT
                yr_month
                , CATEGORY
                , SUM(CAST(ZSPCHANNELRECOMMWTHPOQTY AS FLOAT64)) AS ending_inventory
            FROM
                (
                    SELECT
                        c.yr_month
                        , COALESCE(ibp.CATEGORY, mst.CATEGORY) as CATEGORY
                        , ZSPCHANNELRECOMMWTHPOQTY
                        , RANK() OVER (PARTITION BY c.yr_month, COALESCE(ibp.CATEGORY, mst.CATEGORY) ORDER BY PERIODID4_TSTAMP DESC) AS rnum
                    FROM
                        dw.ibp_amz_sell_out_fcst_scenario ibp
                            left JOIN meta.wk_calendar_new c
                                on date(ibp.PERIODID4_TSTAMP) between c.start_date and c.end_date
                            left join tmp1.erp_sku_category_mst mst
                                on ibp.PRDID = mst.zinus_sku
                    WHERE
                        ibp.DPSCENARIONAME = 'Growth'
                        AND ibp.CHANNEL = 'AMZ DI'
                        -- AND ibp.CHANNEL NOT IN ('SuperOrdinary', 'AMZ DDS')

                        -- 25.05.09 / 시나리오명 변경 25.04.21 이전 Growth, 이후 Upside
                        AND date(ibp.PERIODID4_TSTAMP) < '2025-04-21'

                    UNION ALL

                    SELECT
                        c.yr_month
                        , COALESCE(ibp.CATEGORY, mst.CATEGORY) as CATEGORY
                        , ZSPCHANNELRECOMMWTHPOQTY
                        , RANK() OVER (PARTITION BY c.yr_month, COALESCE(ibp.CATEGORY, mst.CATEGORY) ORDER BY PERIODID4_TSTAMP DESC) AS rnum
                    FROM
                        dw.ibp_amz_sell_out_fcst_scenario ibp
                        left JOIN meta.wk_calendar_new c
                    on date(ibp.PERIODID4_TSTAMP) between c.start_date and c.end_date
                        left join tmp1.erp_sku_category_mst mst
                        on ibp.PRDID = mst.zinus_sku
                    WHERE
                        ibp.DPSCENARIONAME = 'Upside'
                        AND ibp.CHANNEL = 'AMZ DI'

                        -- 25.05.09 / 시나리오명 변경 25.04.21 이전 Growth, 이후 Upside
                        AND date(ibp.PERIODID4_TSTAMP) >= '2025-04-21'

                )
            WHERE
                rnum = 1
            GROUP BY 1, 2
        )
        , cte_cogs as (
            SELECT
                c.yr_month
                , COALESCE(a.CATEGORY, mst.CATEGORY) as CATEGORY
                , SUM(CAST(ZDPFINALALIGNEDFCST AS FLOAT64)) AS shipped_cogs_units
                , SUM(CAST(ZDPFINALALIGNEDFCSTREV AS FLOAT64)) AS shipped_cogs
            FROM
                dw.ibp_amz_sell_out_fcst_scenario a
                    LEFT JOIN meta.wk_calendar_new c
                        ON DATE_SUB(DATE(PERIODID4_TSTAMP), INTERVAL 1 DAY) = c.start_date
                    left join tmp1.erp_sku_category_mst mst
                        on a.PRDID = mst.zinus_sku
            WHERE
                DPSCENARIONAME = 'Growth'
                AND CHANNEL = 'AMZ DI'
                -- AND CHANNEL NOT IN ('SuperOrdinary', 'AMZ DDS')

                -- 25.05.09 / 시나리오명 변경 25.04.21 이전 Growth, 이후 Upside
                AND date(a.PERIODID4_TSTAMP) < '2025-04-21'

            GROUP BY 1,2

            UNION ALL

            SELECT
                c.yr_month
                , COALESCE(a.CATEGORY, mst.CATEGORY) as CATEGORY
                , SUM(CAST(ZDPFINALALIGNEDFCST AS FLOAT64)) AS shipped_cogs_units
                , SUM(CAST(ZDPFINALALIGNEDFCSTREV AS FLOAT64)) AS shipped_cogs
            FROM
                dw.ibp_amz_sell_out_fcst_scenario a
                    LEFT JOIN meta.wk_calendar_new c
                        ON DATE_SUB(DATE(PERIODID4_TSTAMP), INTERVAL 1 DAY) = c.start_date
                    left join tmp1.erp_sku_category_mst mst
                        on a.PRDID = mst.zinus_sku
            WHERE
                DPSCENARIONAME = 'Upside'
                AND CHANNEL = 'AMZ DI'

                -- 25.05.09 / 시나리오명 변경 25.04.21 이전 Growth, 이후 Upside
                AND date(a.PERIODID4_TSTAMP) >= '2025-04-21'

            GROUP BY 1,2
        )
        , cte_cogs_with_inv as (
            SELECT
                a.yr_month
                , IFNULL(IF(GROUPING ( a.category ) = 1, 'TOTAL', a.category), 'UNKNOWN') AS category
                , SUM(CAST(shipped_cogs_units AS FLOAT64)) AS shipped_cogs_units
                , SUM(CAST(shipped_cogs AS FLOAT64)) AS shipped_cogs
                , SUM(CAST(ending_inventory AS FLOAT64)) AS ending_inventory
            -- , CHANNEL
            FROM
                cte_cogs a
                    LEFT JOIN cte_inv b
                        ON a.yr_month = b.yr_month AND a.CATEGORY = b.CATEGORY
            GROUP BY GROUPING SETS ( ( 1, a.category ), ( 1 ) )
        )
    SELECT
        a.*
        , w.wos
        , w.f13
    FROM
        cte_cogs_with_inv a
            LEFT JOIN tmp1.ibp_wos w
                ON a.yr_month = w.yr_month AND a.CATEGORY = w.CATEGORY
    ;

END;
------------------------------------------------------------------------------------------------------------------------
-- Non-US Inventory
BEGIN
    DECLARE LAST_UPDATE DATE;
    set LAST_UPDATE = (
        select max(date) from vc.amz_uk_vc_inv_all
    );

    CREATE OR REPLACE TABLE hq.di_monthly_glb_inv AS
    WITH
        cte_exchange AS (
            with cte_exchange_rnum as (
                SELECT
                    t_r.currency
                    , fill_dt AS date
                    , COALESCE(e.usd, LAST_VALUE(e.usd IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS usd
                    , COALESCE(e.currency_rate, LAST_VALUE(e.currency_rate IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS currency_rate
                    , ROW_NUMBER() OVER (PARTITION BY t_r.currency, FORMAT_DATE('%Y%m', date) order by fill_dt DESC ) as rnum
                FROM
                    (
                        SELECT currency, MIN(date) AS min_dt
                        FROM meta.exchange_usd
                        GROUP BY 1
                    ) AS t_r
                        JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_dt, CURRENT_DATE())) fill_dt
                        LEFT JOIN meta.exchange_usd e
                            ON fill_dt = e.date AND t_r.currency = e.currency
            )
            SELECT
                currency
                , cast(FORMAT_DATE('%Y%m', date) as int64) as yr_month
                , currency_rate

                --  월 마지막 값
                -- , usd

                --  월 평균 값
                , avg(usd) as usd
            FROM
                cte_exchange_rnum

            -- 월 마지막 값
            -- WHERE rnum = 1

            --  월 평균 값
            GROUP BY
                currency, 2, currency_rate
        )
       , cte_category_mst as (
            SELECT DISTINCT
                item_id
                , country
                , company
                , ordered_category AS category
                , zinus_sku
            FROM
                meta.zns_global_pdt_mst
        )
       , cte_weekly_inv            AS (

            SELECT
                asin
                , sellable_on_hand_inventory
                , 'CA' as country
                , 'CA' as company
                , 'CAD' as currency
                , CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month
            FROM
                vc.amz_ca_vc_inv_all
            where
                date = (select max(date) from vc.amz_ca_vc_inv_all)

            UNION all SELECT asin, sellable_on_hand_inventory, 'DE' as country, 'EU' as company, 'EUR' as currency, CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month FROM vc.amz_de_vc_inv_all where date = (select max(date) from vc.amz_de_vc_inv_all)
            UNION all SELECT asin, sellable_on_hand_inventory, 'FR' as country, 'EU' as company, 'EUR' as currency, CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month FROM vc.amz_fr_vc_inv_all where date = (select max(date) from vc.amz_fr_vc_inv_all)
            UNION all SELECT asin, sellable_on_hand_inventory, 'IT' as country, 'EU' as company, 'EUR' as currency, CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month FROM vc.amz_it_vc_inv_all where date = (select max(date) from vc.amz_it_vc_inv_all)
            UNION all SELECT asin, sellable_on_hand_inventory, 'ES' as country, 'EU' as company, 'EUR' as currency, CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month FROM vc.amz_es_vc_inv_all where date = (select max(date) from vc.amz_es_vc_inv_all)
            UNION all SELECT asin, sellable_on_hand_inventory, 'UK' as country, 'EU' as company, 'GBP' as currency, CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month FROM vc.amz_uk_vc_inv_all where date = (select max(date) from vc.amz_uk_vc_inv_all)

            UNION all SELECT asin, sellable_on_hand_inventory, 'AU' as country, 'AU' as company, 'AUD' as currency, CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month FROM vc.amz_au_vc_inv_all where date = (select max(date) from vc.amz_au_vc_inv_all)
            UNION all SELECT asin, sellable_on_hand_inventory, 'JP' as country, 'JP' as company, 'JPY' as currency, CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month FROM vc.amz_jp_vc_inv_all where date = (select max(date) from vc.amz_jp_vc_inv_all)
            UNION all SELECT asin, sellable_on_hand_inventory, 'US' as country, 'MELLOW' as company, 'USD' as currency, CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month FROM vc.amz_mellow_vc_inv_all where date = (select max(date) from vc.amz_mellow_vc_inv_all)

        )
       , cte_monthly_inv            AS (
            SELECT
                asin
                , sellable_on_hand_inventory
                , 'CA' as country
                , 'CA' as company
                , 'CAD' as currency
                , CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS INT64) AS yr_month
            FROM
                vc.amz_vc_ca_inv_monthly

            UNION ALL SELECT asin, sellable_on_hand_inventory, 'DE' as country, 'EU' as company, 'EUR' as currency, CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS INT64) AS yr_month FROM vc.amz_vc_de_inv_monthly
            UNION ALL SELECT asin, sellable_on_hand_inventory, 'FR' as country, 'EU' as company, 'EUR' as currency, CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS INT64) AS yr_month FROM vc.amz_vc_fr_inv_monthly
            UNION ALL SELECT asin, sellable_on_hand_inventory, 'IT' as country, 'EU' as company, 'EUR' as currency, CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS INT64) AS yr_month FROM vc.amz_vc_it_inv_monthly
            UNION ALL SELECT asin, sellable_on_hand_inventory, 'ES' as country, 'EU' as company, 'EUR' as currency, CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS INT64) AS yr_month FROM vc.amz_vc_es_inv_monthly
            UNION ALL SELECT asin, sellable_on_hand_inventory, 'UK' as country, 'EU' as company, 'GBP' as currency, CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS INT64) AS yr_month FROM vc.amz_vc_uk_inv_monthly

            UNION ALL SELECT asin, sellable_on_hand_inventory, 'AU' as country, 'AU' as company, 'AUD' as currency, CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS INT64) AS yr_month FROM vc.amz_vc_au_inv_monthly
            UNION ALL SELECT asin, sellable_on_hand_inventory, 'JP' as country, 'JP' as company, 'JPY' as currency, CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS INT64) AS yr_month FROM vc.amz_vc_jp_inv_monthly
            UNION ALL SELECT asin, sellable_on_hand_inventory, 'US' as country, 'MELLOW' as company, 'USD' as currency, CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS INT64) AS yr_month FROM vc.amz_vc_mellow_inv_monthly

        )
       , cte_inv as (
            with cte_month as (
                select yr_month from cte_monthly_inv group by 1
            )
            SELECT
                *
            FROM
                cte_monthly_inv

            UNION ALL

            SELECT
                *
            FROM
                cte_weekly_inv w
            WHERE
                NOT EXISTS (
                    SELECT 1
                    FROM cte_month AS m
                    WHERE w.yr_month = m.yr_month
                )
        )
       , cte_source_final AS (
            SELECT
                a.*
                , a.sellable_on_hand_inventory / e.currency_rate * e.usd as sellable_on_hand_inventory_usd
                , m.zinus_sku AS sku
                , m.category
                , LAST_DAY(PARSE_DATE('%Y%m%d', CONCAT(CAST(a.yr_month AS STRING), '01')), MONTH) AS end_date
            FROM
                cte_inv a
                    LEFT JOIN cte_category_mst m
                        ON a.asin = m.item_id
                            and a.country = m.country
                            and a.company = m.company
                    LEFT JOIN cte_exchange e
                        ON a.yr_month = e.yr_month
                               AND a.currency = e.currency
        )
       , cte_summ as (
            SELECT
                s.yr_month
                , if(grouping(company) = 1, 'T', company) as grp_company
                , IF(category IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS prd_type
                , SUM(sellable_on_hand_inventory_usd) AS inv_amt_usd
                , SUM(sellable_on_hand_inventory) as inv_amt
                , currency
            FROM
                cte_source_final s
            GROUP BY
                GROUPING SETS (
                    ( s.yr_month, company, 3, currency ), ( s.yr_month, company, currency ), (s.yr_month)
                )
        )
       , cte_glb as (
            SELECT
                yr_month
                , grp_company AS company
                , COALESCE(prd_type, 'T') AS prd_type
                , inv_amt_usd
                , IF(grp_company = 'T', inv_amt_usd, inv_amt) AS inv_amt
                , IF(grp_company = 'T', 'USD', currency) AS currency
            -- , wos
            FROM
                cte_summ
            WHERE
                yr_month >= 202201
        )
       , cte_eu_uk as (
            SELECT
                yr_month
                , 'EU(UK)' AS company
                , prd_type
                , NULL AS inv_amt_usd
                , inv_amt
                , currency
            FROM
                cte_glb
            WHERE
                company = 'EU'
                AND currency = 'GBP'

        )
       , cte_eu_others as (
            SELECT
                yr_month
                , 'EU(Others)' AS company
                , prd_type
                , NULL AS inv_amt_usd
                , inv_amt
                , currency
            FROM
                cte_glb
            WHERE
                company = 'EU'
                AND currency = 'EUR'

        )
       , cte_eu as (
            SELECT
                yr_month
                , company
                , prd_type
                , SUM(inv_amt_usd) AS inv_amt_usd
                , null AS inv_amt
                , cast(null as string) AS currency
            FROM
                cte_glb
            WHERE
                company = 'EU'
            GROUP BY 1, 2, 3
        )
    SELECT
        *
        , LAST_UPDATE as last_update
    FROM
        cte_glb
    WHERE
        company != 'EU'
    UNION ALL
    SELECT
        *
        , LAST_UPDATE as last_update
    FROM
        cte_eu_uk
    UNION ALL
    SELECT
        *
        , LAST_UPDATE as last_update
    FROM
        cte_eu_others
    UNION ALL
    SELECT
        *
        , LAST_UPDATE as last_update
    FROM
        cte_eu
    ;

END;

------------------------------------------------------------------------------------------------------------------------
-- so (super ordinary) -------------------------------------------------------------------------------------------------

BEGIN
    DECLARE LAST_UPDATE STRING;
    set LAST_UPDATE = (
        SELECT SUBSTR(MAX(load_time_utc), 1, 10) FROM dw.amz_superordinary_inventory_summaries_history
    );

    CREATE OR REPLACE TABLE hq.so_monthly_inv AS
    WITH
       cte_pr00 as (
            SELECT
                Material_Number
                , MIN(Sales_Price_PR00) AS Sales_Price_PR00
                -- , MAX(Sales_Price_PR00) AS Sales_Price_PR00
            FROM meta.pr00_salescost
            -- WHERE Customer_Desc="Amazon DDS" AND Sales_Org=2000
            WHERE LOWER(Customer_Desc) LIKE "%amazon%" AND Sales_Org=2000
            GROUP BY 1
        )
       , end_tmp AS (
            SELECT
                if(mst.category in ('10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS'), 'M', 'N') is_mattress
                , DATE(load_time_pst) AS date
                , cast(FORMAT_DATE('%Y%m', DATE(load_time_pst)) as int64) AS yr_month
                , ( COALESCE(Fulfillable_Quantity, 0) + COALESCE(Pending_Transshipment_Quantity, 0) ) * C.Sales_Price_PR00 AS sellable_on_hand_inventory
                , ROW_NUMBER() OVER (PARTITION BY A.ASIN, DATE(load_time_pst) ORDER BY load_time_pst DESC) AS FILTER_RNK
            FROM dw.amz_superordinary_inventory_summaries_history A
                     LEFT OUTER JOIN cte_pr00 C ON A.SKU=C.Material_Number
                     left join tmp1.erp_sku_category_mst mst on a.sku = mst.zinus_sku
        )
    SELECT
        yr_month
        , IF(GROUPING ( is_mattress ) = 1, 'Total', is_mattress) AS prd_type
        , SUM(sellable_on_hand_inventory) AS inv_amt
        , LAST_UPDATE as last_update
    FROM
        end_tmp
    WHERE
        FILTER_RNK = 1
        AND
        (
            DATE(date) = LAST_DAY(DATE(date), MONTH)
            OR DATE(date) = ( SELECT DATE(MAX(date)) FROM end_tmp )
        )
        -- DATE(date) = LAST_DAY(DATE(date), MONTH)
        -- OR DATE(date) = (select date(max(date)) from end_tmp)
    GROUP BY GROUPING SETS (( yr_month, is_mattress ), ( yr_month ))
    ;


END;


------------------------------------------------------------------------------------------------------------------------
-- DO

-- DELETE
-- FROM
--     us_it.ml_plant
-- WHERE
--     yr_month = '202412'
-- ;


-- CREATE TABLE ods.ml_plant AS
-- SELECT
--     *
--     , CURRENT_DATETIME() as load_datetime
-- FROM
--     us_it.ml_plant
-- ;
BEGIN
    DECLARE LAST_UPDATE STRING;

    -- SET LAST_UPDATE = "2025-01-12";
--     SET LAST_UPDATE = "{{ ti.xcom_pull(task_ids='task_01_do_inv_sp2ods') }}";
    SET LAST_UPDATE = (SELECT FORMAT_TIMESTAMP('%Y-%m-%d', max(load_datetime)) from ods.do_us_zco1r2002_inv);

    -- IF LAST_UPDATE IS NOT NULL AND TRIM(LAST_UPDATE) != '' THEN
    IF LAST_UPDATE IS NOT NULL AND TRIM(LAST_UPDATE) != '' AND LAST_UPDATE != '[]' THEN

        CREATE OR REPLACE TABLE hq.do_monthly_inv AS
        WITH
            cte_deduplicated as (
                SELECT
                    *
                FROM
                    (
                        SELECT
                            plant
                            , mat_co_group
                            , material
                            , valuation_type
                            , material_description
                            , ending_qty
                            , ending_amt
                            , yr_month
                            , load_datetime
                        FROM
                            ods.ml_plant

                        UNION ALL

                        -- 250520 new format (~202505)
                        SELECT
                            plant
                            , mat_co_group
                            , material
                            , valuation_type
                            , material_description
                            , ending_qty
                            , ending_amt
                            , yr_month
                            , load_datetime
                        FROM
                            ods.inv_ending_zco1r2002
                        WHERE
                            yr_month <= '202505'

                        UNION ALL

                        -- 202506 ~ (us do from rpa)
                        SELECT
                            plant
                            , mat_co_group
                            , material
                            , valuation_type
                            , material_description
                            , ending_qty
                            , ending_amt
                            , yr_month
--                             , load_datetime
                            , DATETIME(load_datetime) as load_datetime
                        FROM
                            ods.do_us_zco1r2002_inv
                        WHERE
                            yr_month > '202505'
                        -- 25.09.18 (last_update 는 다르고) yr_month 는 동일한 원본 파일이 두개 이상이면 load_datetime 이 동일한 중복된 yr_month 가 발생하여 qualify 에 last_update 조건 추가
                        QUALIFY RANK() OVER (PARTITION BY yr_month ORDER BY last_update DESC, load_datetime desc) = 1

                    )
                QUALIFY RANK() OVER (PARTITION BY yr_month ORDER BY load_datetime DESC) = 1
            )
            , cte_agg as (
                SELECT
                    yr_month
                    , IF(mat_co_group IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS prd_type
                    , SUM(ending_amt) AS inv_amt
                FROM
                    -- us_it.ml_plant
                    cte_deduplicated
                WHERE
                    plant in ('2000', '2001', '2002', '2100', '2800', '2900') -- do plant
                GROUP BY
                    -- 1
                    GROUPING SETS (
                    ( 1, 2 ) -- yr_month, prd_type
                    , ( 1 ) -- yr_month
                    )
            )
            , cte_total as (
                SELECT
                    CAST(yr_month AS INT64) AS yr_month
                    , COALESCE(prd_type, 'T') AS prd_type -- M, N, T
                    , inv_amt
                FROM
                    cte_agg
            )
        SELECT
            yr_month
            , prd_type
            , inv_amt
            , LAST_UPDATE as last_update
        FROM
            cte_total
        ;

    END IF;

END;