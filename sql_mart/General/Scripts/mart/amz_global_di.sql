------------------------------------------------------------------------------------------------------------------------
-- [SOURCE] ------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- [US] ----------------------------------------------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                    (
                                                        SELECT LEAST(
                                                                (SELECT MAX(date) FROM vc.amz_vc_sales_daily_all),
                                                                (SELECT MAX(date) FROM vc.amz_vc_inv_daily_all)
                                                               )
                                                    )
                                                , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_vc_sales_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_vc_inv_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));

    CREATE OR REPLACE TABLE tmp1.amz_di_us AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin
            FROM vc.vc_catalog
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_vc_sales_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_vc_sales_daily_all GROUP BY 1

                        ) AS t_i
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_rf              AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(bw_price_value, FIRST_VALUE(bw_price_value IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , a.bw_price_value
                        , PARSE_DATE('%Y-%m-%d', SUBSTRING(crawlTime_utc, 0, 10)) AS date
                    FROM
                        dw.rf_amz_pdt_zns_comp_daily a
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
        )
        , cte_month_rf        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(retail_price) AS retail_price
            FROM
                cte_rf a
            GROUP BY 1, 2
        )
        , cte_week_rf         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_rf a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_keepa           AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(t_keepa.LISTPRICE, FIRST_VALUE(t_keepa.LISTPRICE IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS LISTPRICE
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        DATE(LISTPRICE_time) AS date
                        , a.asin
                        , LISTPRICE
                        , ROW_NUMBER() OVER (PARTITION BY a.asin, DATE (LISTPRICE_time) ORDER BY LISTPRICE_time DESC) AS rnum
                    FROM
                        keepa.zinus_amz_list_price a
                            JOIN cte_target b
                                ON a.asin = b.asin
                    WHERE
                        a.LISTPRICE IS NOT NULL
                ) t_keepa
                        ON t_d.asin = t_keepa.asin AND t_d.fill_date = t_keepa.date AND rnum = 1
        )
        , cte_month_keepa     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
            GROUP BY 1, 2
        )
        , cte_week_keepa      AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_inv_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        , net_received
                        , net_received_units

                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units

                        , c.unhealthy_inventory
                        , c.unhealthy_units
                        , a.date <= LAST_MONTH_DAY as is_closed
                    FROM
                        vc.amz_vc_inv_daily_all a
                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units

                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , b.yr_wk
                        , net_received
                        , net_received_units

                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_vc_inv_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )

        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                -- , a.date
                , SUM(shipped_revenue) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_vc_sales_daily_all a
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_vc_sales_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_vc_netppm_daily_all a
            GROUP BY 1, 2
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'US' AS country

        , vc.* EXCEPT (asin, yr_month)
        , keepa.list_price
        , rf.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_month = keepa.yr_month
            LEFT JOIN cte_month_rf rf
                ON vc.asin = rf.asin AND vc.yr_month = rf.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'US' AS country

        , vc.* EXCEPT (asin, yr_wk)
        , keepa.list_price
        , rf.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_wk = keepa.yr_wk
            LEFT JOIN cte_week_rf rf
                ON vc.asin = rf.asin AND vc.yr_wk = rf.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;

END
;
-- [JP] ----------------------------------------------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_jp_vc_sales_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_jp_vc_inv_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                            (
                                                                SELECT LEAST(
                                                                        (SELECT MAX(date) FROM vc.amz_jp_vc_sales_daily_all),
                                                                        (SELECT MAX(date) FROM vc.amz_jp_vc_inv_daily_all)
                                                                       )
                                                            )
                                                        , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));

    CREATE OR REPLACE TABLE tmp1.amz_di_jp AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin FROM vc.vc_jp_catalog
        )
        , cte_exchange AS (
            SELECT
                t_r.currency
                , fill_dt AS date
                , COALESCE(e.usd, LAST_VALUE(e.usd IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS usd
                , COALESCE(e.currency_rate, LAST_VALUE(e.currency_rate IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS currency_rate
            FROM
                (
                    SELECT currency, MIN(date) AS min_dt FROM meta.exchange_usd GROUP BY 1
                ) AS t_r
                    JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_dt, CURRENT_DATE())) fill_dt
                    LEFT JOIN meta.exchange_usd e
                        ON fill_dt = e.date AND t_r.currency = e.currency
            WHERE
                e.currency = 'JPY'
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_jp_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_jp_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_jp_vc_sales_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_jp_vc_sales_daily_all GROUP BY 1

                        ) AS t_i
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_rf              AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(bw_price_value, FIRST_VALUE(bw_price_value IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , a.bw_price_value
                        , PARSE_DATE('%Y-%m-%d', SUBSTRING(crawlTime_utc, 0, 10)) AS date
                    FROM
                        dw.rf_amzjp_pdt_price_daily a
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_rf        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(retail_price) AS retail_price
            FROM
                cte_rf a
            GROUP BY 1, 2
        )
        , cte_week_rf         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_rf a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_keepa           AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(t_keepa.LISTPRICE, FIRST_VALUE(t_keepa.LISTPRICE IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * 100 * e.usd AS LISTPRICE
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        DATE(LISTPRICE_time) AS date
                        , a.asin
                        , LISTPRICE
                        , ROW_NUMBER() OVER (PARTITION BY a.asin, DATE (LISTPRICE_time) ORDER BY LISTPRICE_time DESC) AS rnum
                    FROM
                        -- keepa.zinus_amz_list_price a
                        dw.amzjp_list_price_all a
                            JOIN cte_target b
                                ON a.asin = b.asin
                    WHERE
                        a.LISTPRICE IS NOT NULL
                ) t_keepa
                        ON t_d.asin = t_keepa.asin AND t_d.fill_date = t_keepa.date AND rnum = 1
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
            GROUP BY 1, 2
        )
        , cte_week_keepa      AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_jp_inv_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units

                        , c.unhealthy_inventory * e.usd AS unhealthy_inventory
                        , c.unhealthy_units
                        , a.date <= LAST_MONTH_DAY as is_closed
                    FROM
                        vc.amz_jp_vc_inv_daily_all a
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units

                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_jp_vc_inv_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )
        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_jp_vc_sales_daily_all a
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_jp_vc_sales_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_jp_vc_netppm_daily_all a
            GROUP BY 1, 2
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_jp_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'JP' AS country
        , vc.* EXCEPT (asin, yr_month)
        , keepa.list_price
        , rf.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_month = keepa.yr_month
            LEFT JOIN cte_month_rf rf
                ON vc.asin = rf.asin AND vc.yr_month = rf.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'JP' AS country
        , vc.* EXCEPT (asin, yr_wk)
        , keepa.list_price
        , rf.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_wk = keepa.yr_wk
            LEFT JOIN cte_week_rf rf
                ON vc.asin = rf.asin AND vc.yr_wk = rf.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;

END
;
-- [AU - missing keepa] ------------------------------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_au_vc_sales_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_au_vc_inv_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                            (
                                                                SELECT LEAST(
                                                                        (SELECT MAX(date) FROM vc.amz_au_vc_sales_daily_all),
                                                                        (SELECT MAX(date) FROM vc.amz_au_vc_inv_daily_all)
                                                                       )
                                                            )
                                                        , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));


    CREATE OR REPLACE TABLE tmp1.amz_di_au AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin FROM vc.vc_au_catalog
        )
        , cte_exchange AS (
            SELECT
                t_r.currency
                , fill_dt AS date
                , COALESCE(e.usd, LAST_VALUE(e.usd IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS usd
                , COALESCE(e.currency_rate, LAST_VALUE(e.currency_rate IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS currency_rate
            FROM
                (
                    SELECT currency, MIN(date) AS min_dt FROM meta.exchange_usd GROUP BY 1
                ) AS t_r
                    JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_dt, CURRENT_DATE())) fill_dt
                    LEFT JOIN meta.exchange_usd e
                        ON fill_dt = e.date AND t_r.currency = e.currency
            WHERE
                e.currency = 'AUD'
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_au_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_au_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_au_vc_sales_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_au_vc_sales_daily_all GROUP BY 1

                        ) AS t_i
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_rf              AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(bw_price_value, FIRST_VALUE(bw_price_value IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , a.bw_price_value
                        , PARSE_DATE('%Y-%m-%d', SUBSTRING(crawlTime_utc, 0, 10)) AS date
                    FROM
                        dw.rf_amzau_pdt_daily a
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date

        )
        , cte_month_rf        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                -- , a.date
                , AVG(retail_price) AS retail_price
            FROM
                cte_rf a
            GROUP BY 1, 2
        )
        , cte_week_rf         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_rf a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_au_inv_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units

                        , c.unhealthy_inventory * e.usd AS unhealthy_inventory
                        , c.unhealthy_units
                        , a.date <= LAST_MONTH_DAY as is_closed
                    FROM
                        vc.amz_au_vc_inv_daily_all a
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date

                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_au_vc_inv_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )
        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_au_vc_sales_daily_all a
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_au_vc_sales_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_au_vc_netppm_daily_all a
            GROUP BY 1, 2
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                -- , a.date
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_au_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'AU' AS country
        , vc.* EXCEPT (asin, yr_month)
        , CAST(NULL AS FLOAT64) AS list_price
        , rf.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_rf rf
                ON vc.asin = rf.asin AND vc.yr_month = rf.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'AU' AS country
        , vc.* EXCEPT (asin, yr_wk)
        , CAST(NULL AS FLOAT64) AS list_price
        , rf.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_rf rf
                ON vc.asin = rf.asin AND vc.yr_wk = rf.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;

END
;
-- [MX - missing keepa, missing rainforest] ----------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_mx_vc_sales_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_mx_vc_inv_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                            (
                                                                SELECT LEAST(
                                                                        (SELECT MAX(date) FROM vc.amz_mx_vc_sales_daily_all),
                                                                        (SELECT MAX(date) FROM vc.amz_mx_vc_inv_daily_all)
                                                                       )
                                                            )
                                                        , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));

    CREATE OR REPLACE TABLE tmp1.amz_di_mx AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin FROM vc.vc_mx_catalog
        )
        , cte_exchange AS (
            SELECT
                t_r.currency
                , fill_dt AS date
                , COALESCE(e.usd, LAST_VALUE(e.usd IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS usd
                , COALESCE(e.currency_rate, LAST_VALUE(e.currency_rate IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS currency_rate
            FROM
                (
                    SELECT currency, MIN(date) AS min_dt FROM meta.exchange_usd GROUP BY 1
                ) AS t_r
                    JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_dt, CURRENT_DATE())) fill_dt
                    LEFT JOIN meta.exchange_usd e
                        ON fill_dt = e.date AND t_r.currency = e.currency
            WHERE
                e.currency = 'MXN'
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_mx_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_mx_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_mx_vc_sales_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_mx_vc_sales_daily_all GROUP BY 1

                        ) AS t_i
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_price              AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(retail_price, FIRST_VALUE(retail_price IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , IF(
                                NULLIF(a.ordered_revenue, 0) IS NULL
                                    OR NULLIF(a.ordered_units, 0) IS NULL
                                    OR a.ordered_revenue < 0
                                    OR a.ordered_units < 0
                            , NULL
                            , a.ordered_revenue / a.ordered_units) AS retail_price
                        , a.date
                    FROM
                        vc.amz_mx_vc_sales_daily_all a
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date

        )
        , cte_month_price        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(retail_price) AS retail_price
            FROM
                cte_price a
            GROUP BY 1, 2
        )
        , cte_week_price         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_price a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_mx_inv_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units

                        , c.unhealthy_inventory * e.usd AS unhealthy_inventory
                        , c.unhealthy_units
                        , a.date <= LAST_MONTH_DAY as is_closed
                    FROM
                        vc.amz_mx_vc_inv_daily_all a
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date

                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units

                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_mx_vc_inv_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )
        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_mx_vc_sales_daily_all a
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_mx_vc_sales_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_mx_vc_netppm_daily_all a
            GROUP BY 1, 2
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                -- , a.date
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_mx_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'MX' AS country
        , vc.* EXCEPT (asin, yr_month)
        , CAST(NULL AS FLOAT64) AS list_price
        , mp.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_price mp
                ON vc.asin = mp.asin AND vc.yr_month = mp.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'MX' AS country
        , vc.* EXCEPT (asin, yr_wk)
        , CAST(NULL AS FLOAT64) AS list_price
        , wp.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_price wp
                ON vc.asin = wp.asin AND vc.yr_wk = wp.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;
END
;
-- [CA] ----------------------------------------------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_ca_vc_sales_sourcing_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_ca_vc_inv_sourcing_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                            (
                                                                SELECT LEAST(
                                                                        (SELECT MAX(date) FROM vc.amz_ca_vc_sales_sourcing_daily_all),
                                                                        (SELECT MAX(date) FROM vc.amz_ca_vc_inv_sourcing_daily_all)
                                                                       )
                                                            )
                                                        , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));


    CREATE OR REPLACE TABLE tmp1.amz_di_ca AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin FROM vc.vc_ca_catalog
        )
        , cte_exchange AS (
            SELECT
                t_r.currency
                , fill_dt AS date
                , COALESCE(e.usd, LAST_VALUE(e.usd IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS usd
                , COALESCE(e.currency_rate, LAST_VALUE(e.currency_rate IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS currency_rate
            FROM
                (
                    SELECT currency, MIN(date) AS min_dt FROM meta.exchange_usd GROUP BY 1
                ) AS t_r
                    JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_dt, CURRENT_DATE())) fill_dt
                    LEFT JOIN meta.exchange_usd e
                        ON fill_dt = e.date AND t_r.currency = e.currency
            WHERE
                e.currency = 'CAD'
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_ca_vc_inv_sourcing_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_ca_vc_inv_sourcing_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_ca_vc_sales_sourcing_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_ca_vc_sales_sourcing_daily_all GROUP BY 1

                        ) AS t_i
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_rf              AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(bw_price_value, FIRST_VALUE(bw_price_value IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , a.bw_price_value
                        , DATE(crawl_time_utc) AS date
                    FROM
                        dw.rf_amz_ca_pdt a
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_rf        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(retail_price) AS retail_price
            FROM
                cte_rf a
            GROUP BY 1, 2
        )
        , cte_week_rf         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_rf a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_keepa           AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(t_keepa.LISTPRICE, FIRST_VALUE(t_keepa.LISTPRICE IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS LISTPRICE
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        DATE(LISTPRICE_time) AS date
                        , a.asin
                        , LISTPRICE
                        , ROW_NUMBER() OVER (PARTITION BY a.asin, DATE (LISTPRICE_time) ORDER BY LISTPRICE_time DESC) AS rnum
                    FROM
                        dw.amzca_list_price_all a
                            JOIN cte_target b
                                ON a.asin = b.asin
                    WHERE
                        a.LISTPRICE IS NOT NULL
                ) t_keepa
                        ON t_d.asin = t_keepa.asin AND t_d.fill_date = t_keepa.date AND rnum = 1
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
            GROUP BY 1, 2
        )
        , cte_week_keepa      AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_ca_inv_sourcing_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)

            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units

                        , c.unhealthy_inventory * e.usd AS unhealthy_inventory
                        , c.unhealthy_units

                        , a.date <= LAST_MONTH_DAY as is_closed
                    FROM
                        vc.amz_ca_vc_inv_sourcing_daily_all a
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date

                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)

            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units

                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_ca_vc_inv_sourcing_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )
        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_ca_vc_sales_sourcing_daily_all a
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_ca_vc_sales_sourcing_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_ca_vc_netppm_daily_all a
            GROUP BY 1, 2
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                -- , a.date
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_ca_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'CA' AS country
        , vc.* EXCEPT (asin, yr_month)
        , keepa.list_price
        , rf.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_month = keepa.yr_month
            LEFT JOIN cte_month_rf rf
                ON vc.asin = rf.asin AND vc.yr_month = rf.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'CA' AS country
        , vc.* EXCEPT (asin, yr_wk)
        , keepa.list_price
        , rf.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_wk = keepa.yr_wk
            LEFT JOIN cte_week_rf rf
                ON vc.asin = rf.asin AND vc.yr_wk = rf.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;

END
;
-- [MELLOW] ------------------------------------------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_mellow_vc_sales_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_mellow_vc_inv_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                            (
                                                                SELECT LEAST(
                                                                        (SELECT MAX(date) FROM vc.amz_mellow_vc_sales_daily_all),
                                                                        (SELECT MAX(date) FROM vc.amz_mellow_vc_inv_daily_all)
                                                                       )
                                                            )
                                                        , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));


    CREATE OR REPLACE TABLE tmp1.amz_di_mellow AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin FROM vc.vc_mellow_catalog
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_mellow_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_mellow_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_mellow_vc_sales_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_mellow_vc_sales_daily_all GROUP BY 1

                        ) AS t_i
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_rf              AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(bw_price_value, FIRST_VALUE(bw_price_value IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , a.bw_price_value
                        , PARSE_DATE('%Y-%m-%d', SUBSTRING(crawlTime_utc, 0, 10)) AS date
                    FROM
                        dw.rf_amz_pdt_zns_comp_daily a
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
        )
        , cte_month_rf        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(retail_price) AS retail_price
            FROM
                cte_rf a
            GROUP BY 1, 2
        )
        , cte_week_rf         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_rf a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_keepa           AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(t_keepa.LISTPRICE, FIRST_VALUE(t_keepa.LISTPRICE IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS LISTPRICE
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        DATE(LISTPRICE_time) AS date
                        , a.asin
                        , LISTPRICE
                        , ROW_NUMBER() OVER (PARTITION BY a.asin, DATE (LISTPRICE_time) ORDER BY LISTPRICE_time DESC) AS rnum
                    FROM
                        keepa.zinus_amz_list_price a
                            JOIN cte_target b
                                ON a.asin = b.asin
                    WHERE
                        a.LISTPRICE IS NOT NULL
                ) t_keepa
                        ON t_d.asin = t_keepa.asin AND t_d.fill_date = t_keepa.date AND rnum = 1
        )
        , cte_month_keepa     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
            GROUP BY 1, 2
        )
        , cte_week_keepa      AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_mellow_inv_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        , net_received
                        , net_received_units
                        , open_purchase_order_quantity AS daily_open_purchase_order_quantity
                        , sellable_on_hand_inventory AS daily_sellable_on_hand_inventory
                        , sellable_on_hand_units AS daily_sellable_on_hand_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units

                        , c.unhealthy_inventory
                        , c.unhealthy_units

                        , a.date <= LAST_MONTH_DAY as is_closed
                    FROM
                        vc.amz_mellow_vc_inv_daily_all a
                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , b.yr_wk
                        , net_received
                        , net_received_units
                        , open_purchase_order_quantity AS daily_open_purchase_order_quantity
                        , sellable_on_hand_inventory AS daily_sellable_on_hand_inventory
                        , sellable_on_hand_units AS daily_sellable_on_hand_units

                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_mellow_vc_inv_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )
        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , SUM(shipped_revenue) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_mellow_vc_sales_daily_all a
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_mellow_vc_sales_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_mellow_vc_netppm_daily_all a
            GROUP BY 1, 2
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_mellow_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'MELLOW' AS country

        , vc.* EXCEPT (asin, yr_month)
        , keepa.list_price
        , rf.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_month = keepa.yr_month
            LEFT JOIN cte_month_rf rf
                ON vc.asin = rf.asin AND vc.yr_month = rf.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'MELLOW' AS country

        , vc.* EXCEPT (asin, yr_wk)
        , keepa.list_price
        , rf.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_wk = keepa.yr_wk
            LEFT JOIN cte_week_rf rf
                ON vc.asin = rf.asin AND vc.yr_wk = rf.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;

END
;
-- [DE] ----------------------------------------------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_de_vc_sales_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_de_vc_inv_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                            (
                                                                SELECT LEAST(
                                                                        (SELECT MAX(date) FROM vc.amz_de_vc_sales_daily_all),
                                                                        (SELECT MAX(date) FROM vc.amz_de_vc_inv_daily_all)
                                                                       )
                                                            )
                                                        , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));

    CREATE OR REPLACE TABLE tmp1.amz_di_de AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin FROM vc.vc_de_catalog
        )
        , cte_exchange AS (
            SELECT
                t_r.currency
                , fill_dt AS date
                , COALESCE(e.usd, LAST_VALUE(e.usd IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS usd
                , COALESCE(e.currency_rate, LAST_VALUE(e.currency_rate IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS currency_rate
            FROM
                (
                    SELECT currency, MIN(date) AS min_dt FROM meta.exchange_usd GROUP BY 1
                ) AS t_r
                    JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_dt, CURRENT_DATE())) fill_dt
                    LEFT JOIN meta.exchange_usd e
                        ON fill_dt = e.date AND t_r.currency = e.currency
            WHERE
                e.currency = 'EUR'
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_de_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_de_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_de_vc_sales_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_de_vc_sales_daily_all GROUP BY 1

                        ) AS t_i
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_keepa_retail_price              AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(bw_price_value, FIRST_VALUE(bw_price_value IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , a.buyBoxPrice AS bw_price_value
                        , DATE(a.date) AS date
                    FROM
                        dw.amzde_pdt_all a -- keepa
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa_retail_price        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(retail_price) AS retail_price
            FROM
                cte_keepa_retail_price a
            GROUP BY 1, 2
        )
        , cte_week_keepa_retail_price         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_keepa_retail_price a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_keepa           AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(t_keepa.LISTPRICE, FIRST_VALUE(t_keepa.LISTPRICE IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS LISTPRICE
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        DATE(LISTPRICE_time) AS date
                        , a.asin
                        , LISTPRICE
                        , ROW_NUMBER() OVER (PARTITION BY a.asin, DATE (LISTPRICE_time) ORDER BY LISTPRICE_time DESC) AS rnum
                    FROM
                        dw.amzde_list_price_all a
                            JOIN cte_target b
                                ON a.asin = b.asin
                    WHERE
                        a.LISTPRICE IS NOT NULL
                ) t_keepa
                        ON t_d.asin = t_keepa.asin AND t_d.fill_date = t_keepa.date AND rnum = 1
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date

        )
        , cte_month_keepa     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
            GROUP BY 1, 2
        )
        , cte_week_keepa      AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_de_inv_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units

                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , c.unhealthy_inventory * e.usd AS unhealthy_inventory
                        , c.unhealthy_units

                        , a.date <= LAST_MONTH_DAY as is_closed
                    FROM
                        vc.amz_de_vc_inv_daily_all a
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units

                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units

                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_de_vc_inv_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )
        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                -- , a.date
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_de_vc_sales_daily_all a
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_de_vc_sales_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_de_vc_netppm_daily_all a
            GROUP BY 1, 2
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_de_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'DE' AS country
        , vc.* EXCEPT (asin, yr_month)
        , keepa.list_price
        , ap.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_month = keepa.yr_month
            LEFT JOIN cte_month_keepa_retail_price ap
                ON vc.asin = ap.asin AND vc.yr_month = ap.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'DE' AS country
        , vc.* EXCEPT (asin, yr_wk)
        , keepa.list_price
        , ap.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_wk = keepa.yr_wk
            LEFT JOIN cte_week_keepa_retail_price ap
                ON vc.asin = ap.asin AND vc.yr_wk = ap.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;

END
;
-- [UK] ----------------------------------------------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_uk_vc_sales_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_uk_vc_inv_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                            (
                                                                SELECT LEAST(
                                                                        (SELECT MAX(date) FROM vc.amz_uk_vc_sales_daily_all),
                                                                        (SELECT MAX(date) FROM vc.amz_uk_vc_inv_daily_all)
                                                                       )
                                                            )
                                                        , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));


    CREATE OR REPLACE TABLE tmp1.amz_di_uk AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin FROM vc.vc_uk_catalog
        )
        , cte_exchange AS (
            SELECT
                t_r.currency
                , fill_dt AS date
                , COALESCE(e.usd, LAST_VALUE(e.usd IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS usd
                , COALESCE(e.currency_rate, LAST_VALUE(e.currency_rate IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS currency_rate
            FROM
                (
                    SELECT currency, MIN(date) AS min_dt FROM meta.exchange_usd GROUP BY 1
                ) AS t_r
                    JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_dt, CURRENT_DATE())) fill_dt
                    LEFT JOIN meta.exchange_usd e
                        ON fill_dt = e.date AND t_r.currency = e.currency
            WHERE
                e.currency = 'GBP'
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_uk_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_uk_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_uk_vc_sales_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_uk_vc_sales_daily_all GROUP BY 1

                        ) AS t_i
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_keepa_retail_price              AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(bw_price_value, FIRST_VALUE(bw_price_value IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , a.buyBoxPrice AS bw_price_value
                        , DATE(date) AS date
                    FROM
                        dw.amzuk_pdt_all a -- keepa
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa_retail_price        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                -- , a.date
                , AVG(retail_price) AS retail_price
            FROM
                cte_keepa_retail_price a
            GROUP BY 1, 2
        )
        , cte_week_keepa_retail_price         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_keepa_retail_price a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_keepa           AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(t_keepa.LISTPRICE, FIRST_VALUE(t_keepa.LISTPRICE IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS LISTPRICE
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        DATE(LISTPRICE_time) AS date
                        , a.asin
                        , LISTPRICE
                        , ROW_NUMBER() OVER (PARTITION BY a.asin, DATE (LISTPRICE_time) ORDER BY LISTPRICE_time DESC) AS rnum
                    FROM
                        dw.amzuk_list_price_all a
                            JOIN cte_target b
                                ON a.asin = b.asin
                    WHERE
                        a.LISTPRICE IS NOT NULL
                ) t_keepa
                        ON t_d.asin = t_keepa.asin AND t_d.fill_date = t_keepa.date AND rnum = 1
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                -- , a.date
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
            GROUP BY 1, 2
        )
        , cte_week_keepa      AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_uk_inv_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units

                        , c.unhealthy_inventory * e.usd AS unhealthy_inventory
                        , c.unhealthy_units

                        , a.date <= LAST_MONTH_DAY as is_closed

                    FROM
                        vc.amz_uk_vc_inv_daily_all a
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_uk_vc_inv_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )
        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_uk_vc_sales_daily_all a
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_uk_vc_sales_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_uk_vc_netppm_daily_all a
            GROUP BY 1, 2
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_uk_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'UK' AS country
        , vc.* EXCEPT (asin, yr_month)
        , keepa.list_price
        , ap.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_month = keepa.yr_month
            LEFT JOIN cte_month_keepa_retail_price ap
                ON vc.asin = ap.asin AND vc.yr_month = ap.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'UK' AS country
        , vc.* EXCEPT (asin, yr_wk)
        , keepa.list_price
        , ap.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_wk = keepa.yr_wk
            LEFT JOIN cte_week_keepa_retail_price ap
                ON vc.asin = ap.asin AND vc.yr_wk = ap.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;

END
;
-- [FR] ----------------------------------------------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_fr_vc_sales_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_fr_vc_inv_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                            (
                                                                SELECT LEAST(
                                                                        (SELECT MAX(date) FROM vc.amz_fr_vc_sales_daily_all),
                                                                        (SELECT MAX(date) FROM vc.amz_fr_vc_inv_daily_all)
                                                                       )
                                                            )
                                                        , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));


    CREATE OR REPLACE TABLE tmp1.amz_di_fr AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin FROM vc.vc_fr_catalog
        )
        , cte_exchange AS (
            SELECT
                t_r.currency
                , fill_dt AS date
                , COALESCE(e.usd, LAST_VALUE(e.usd IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS usd
                , COALESCE(e.currency_rate, LAST_VALUE(e.currency_rate IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS currency_rate
            FROM
                (
                    SELECT currency, MIN(date) AS min_dt FROM meta.exchange_usd GROUP BY 1
                ) AS t_r
                    JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_dt, CURRENT_DATE())) fill_dt
                    LEFT JOIN meta.exchange_usd e
                        ON fill_dt = e.date AND t_r.currency = e.currency
            WHERE
                e.currency = 'EUR'
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_fr_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_fr_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_fr_vc_sales_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_fr_vc_sales_daily_all GROUP BY 1

                        ) AS t_i
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_keepa_retail_price              AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(bw_price_value, FIRST_VALUE(bw_price_value IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , a.buyBoxPrice AS bw_price_value
                        , DATE(date) AS date
                    FROM
                        dw.amzfr_pdt_all a -- keepa
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa_retail_price        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(retail_price) AS retail_price
            FROM
                cte_keepa_retail_price a
            GROUP BY 1, 2
        )
        , cte_week_keepa_retail_price         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_keepa_retail_price a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_keepa           AS (
            SELECT
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(t_keepa.LISTPRICE, FIRST_VALUE(t_keepa.LISTPRICE IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS LISTPRICE
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        DATE(LISTPRICE_time) AS date
                        , a.asin
                        , LISTPRICE
                        , ROW_NUMBER() OVER (PARTITION BY a.asin, DATE (LISTPRICE_time) ORDER BY LISTPRICE_time DESC) AS rnum
                    FROM
                        dw.amzfr_list_price_all a
                            JOIN cte_target b
                                ON a.asin = b.asin
                    WHERE
                        a.LISTPRICE IS NOT NULL
                ) t_keepa
                        ON t_d.asin = t_keepa.asin AND t_d.fill_date = t_keepa.date AND rnum = 1
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
            GROUP BY 1, 2
        )
        , cte_week_keepa      AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_fr_inv_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , open_purchase_order_quantity AS daily_open_purchase_order_quantity
                        , sellable_on_hand_inventory AS daily_sellable_on_hand_inventory
                        , sellable_on_hand_units AS daily_sellable_on_hand_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units

                        , c.unhealthy_inventory * e.usd AS unhealthy_inventory
                        , c.unhealthy_units

                        , a.date <= LAST_MONTH_DAY as is_closed
                    FROM
                        vc.amz_fr_vc_inv_daily_all a
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date

                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_fr_vc_inv_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )
        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_fr_vc_sales_daily_all a
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_fr_vc_sales_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_fr_vc_netppm_daily_all a
            GROUP BY 1, 2
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_fr_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'FR' AS country
        , vc.* EXCEPT (asin, yr_month)
        , keepa.list_price
        , ap.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_month = keepa.yr_month
            LEFT JOIN cte_month_keepa_retail_price ap
                ON vc.asin = ap.asin AND vc.yr_month = ap.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'FR' AS country
        , vc.* EXCEPT (asin, yr_wk)
        , keepa.list_price
        , ap.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_wk = keepa.yr_wk
            LEFT JOIN cte_week_keepa_retail_price ap
                ON vc.asin = ap.asin AND vc.yr_wk = ap.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;

END
;
-- [IT] ----------------------------------------------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_it_vc_sales_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_it_vc_inv_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                            (
                                                                SELECT LEAST(
                                                                        (SELECT MAX(date) FROM vc.amz_it_vc_sales_daily_all),
                                                                        (SELECT MAX(date) FROM vc.amz_it_vc_inv_daily_all)
                                                                       )
                                                            )
                                                        , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));


    CREATE OR REPLACE TABLE tmp1.amz_di_it AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin FROM vc.vc_it_catalog
        )
        , cte_exchange AS (
            SELECT
                t_r.currency
                , fill_dt AS date
                , COALESCE(e.usd, LAST_VALUE(e.usd IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS usd
                , COALESCE(e.currency_rate, LAST_VALUE(e.currency_rate IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS currency_rate
            FROM
                (
                    SELECT currency, MIN(date) AS min_dt FROM meta.exchange_usd GROUP BY 1
                ) AS t_r
                    JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_dt, CURRENT_DATE())) fill_dt
                    LEFT JOIN meta.exchange_usd e
                        ON fill_dt = e.date AND t_r.currency = e.currency
            WHERE
                e.currency = 'EUR'
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_it_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_it_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_it_vc_sales_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_it_vc_sales_daily_all GROUP BY 1

                        ) AS t_i
                    -- WHERE
                    -- asin IN ( 'B087GYSG3K', 'B087GYSG3K' )
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_keepa_retail_price              AS (
            SELECT
                -- * EXCEPT (rnum)
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(bw_price_value, FIRST_VALUE(bw_price_value IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , a.buyBoxPrice AS bw_price_value
                        , DATE(date) AS date
                    FROM
                        dw.amzit_pdt_all a -- keepa
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa_retail_price        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(retail_price) AS retail_price
            FROM
                cte_keepa_retail_price a
            GROUP BY 1, 2
        )
        , cte_week_keepa_retail_price         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_keepa_retail_price a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        -- select * from cte_week_rf order by yr_wk desc;
        , cte_keepa           AS (
            SELECT
                -- * EXCEPT (rnum)
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(t_keepa.LISTPRICE, FIRST_VALUE(t_keepa.LISTPRICE IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS LISTPRICE
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        -- FORMAT_DATE('%Y-%m-%d', DATE(LISTPRICE_time)) AS date
                        DATE(LISTPRICE_time) AS date
                        , a.asin
                        , LISTPRICE
                        , ROW_NUMBER() OVER (PARTITION BY a.asin, DATE (LISTPRICE_time) ORDER BY LISTPRICE_time DESC) AS rnum
                    FROM
                        -- keepa.zinus_amz_list_price a
                        dw.amzit_list_price_all a
                            JOIN cte_target b
                                ON a.asin = b.asin
                    WHERE
                        a.LISTPRICE IS NOT NULL
                ) t_keepa
                        ON t_d.asin = t_keepa.asin AND t_d.fill_date = t_keepa.date AND rnum = 1
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                -- , a.date
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
            GROUP BY 1, 2
        )
        , cte_week_keepa      AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        -- select * from cte_week_keepa order by yr_wk desc;
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_it_inv_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        -- , b.yr_month
                        -- , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , open_purchase_order_quantity AS daily_open_purchase_order_quantity
                        , sellable_on_hand_inventory AS daily_sellable_on_hand_inventory
                        , sellable_on_hand_units AS daily_sellable_on_hand_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units

                        --                     , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        --                     , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unhealthy_units
                        , c.unhealthy_inventory * e.usd AS unhealthy_inventory
                        , c.unhealthy_units

                        , a.date <= LAST_MONTH_DAY as is_closed
                    FROM
                        vc.amz_it_vc_inv_daily_all a
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date

                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        -- , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        -- , b.yr_month
                        , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_it_vc_inv_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )
        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_it_vc_sales_daily_all a
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_it_vc_sales_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
            -- ORDER BY 1 DESC
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_it_vc_netppm_daily_all a
            GROUP BY 1, 2
            -- ORDER BY 1 DESC
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                -- , a.date
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_it_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'IT' AS country
        , vc.* EXCEPT (asin, yr_month)
        , keepa.list_price
        , ap.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_month = keepa.yr_month
            LEFT JOIN cte_month_keepa_retail_price ap
                ON vc.asin = ap.asin AND vc.yr_month = ap.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'IT' AS country
        , vc.* EXCEPT (asin, yr_wk)
        , keepa.list_price
        , ap.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_wk = keepa.yr_wk
            LEFT JOIN cte_week_keepa_retail_price ap
                ON vc.asin = ap.asin AND vc.yr_wk = ap.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;

END
;
-- [ES] ----------------------------------------------------------------------------------------------------------------
BEGIN

    DECLARE LAST_WEEK_DAY DATE;
    DECLARE LAST_MONTH_DAY DATE;
    SET LAST_WEEK_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                           (
                                                               SELECT LEAST(
                                                                       (SELECT MAX(date) FROM vc.amz_es_vc_sales_daily_all),
                                                                       (SELECT MAX(date) FROM vc.amz_es_vc_inv_daily_all)
                                                                      )
                                                           )
                                                       , INTERVAL 1 DAY), INTERVAL 1 WEEK), WEEK ));
    SET LAST_MONTH_DAY = (SELECT LAST_DAY( DATE_SUB(DATE_ADD(
                                                            (
                                                                SELECT LEAST(
                                                                        (SELECT MAX(date) FROM vc.amz_es_vc_sales_daily_all),
                                                                        (SELECT MAX(date) FROM vc.amz_es_vc_inv_daily_all)
                                                                       )
                                                            )
                                                        , INTERVAL 1 DAY), INTERVAL 1 MONTH), MONTH ));


    CREATE OR REPLACE TABLE tmp1.amz_di_es AS
    WITH
        cte_target            AS (
            SELECT DISTINCT asin FROM vc.vc_es_catalog
        )
        , cte_exchange AS (
            SELECT
                t_r.currency
                , fill_dt AS date
                , COALESCE(e.usd, LAST_VALUE(e.usd IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS usd
                , COALESCE(e.currency_rate, LAST_VALUE(e.currency_rate IGNORE NULLS) OVER (PARTITION BY t_r.currency ORDER BY fill_dt)) AS currency_rate
            FROM
                (
                    SELECT currency, MIN(date) AS min_dt FROM meta.exchange_usd GROUP BY 1
                ) AS t_r
                    JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_dt, CURRENT_DATE())) fill_dt
                    LEFT JOIN meta.exchange_usd e
                        ON fill_dt = e.date AND t_r.currency = e.currency
            WHERE
                e.currency = 'EUR'
        )
        , cte_date            AS (
            SELECT
                asin
                , fill_date
            FROM
                ( (
                    SELECT
                        asin
                        , MIN(dt) AS min_dt
                        , MAX(dt) AS max_dt
                    FROM
                        (

                            SELECT asin, MIN(date) AS dt FROM vc.amz_es_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_es_vc_inv_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MIN(date) AS dt FROM vc.amz_es_vc_sales_daily_all GROUP BY 1

                            UNION DISTINCT

                            SELECT asin, MAX(date) AS dt FROM vc.amz_es_vc_sales_daily_all GROUP BY 1

                        ) AS t_i
                    GROUP BY asin

                ) AS t_r JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(t_r.min_dt AS DATE), CAST(t_r.max_dt AS DATE))) fill_date )
        )
        , cte_keepa_retail_price              AS (
            SELECT
                -- * EXCEPT (rnum)
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(bw_price_value, FIRST_VALUE(bw_price_value IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS retail_price
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        a.asin
                        , a.buyBoxPrice AS bw_price_value
                        , DATE(date) AS date
                    FROM
                        dw.amzes_pdt_all a -- keepa
                            JOIN cte_target b
                                ON a.asin = b.asin
                ) t_rf
                        ON t_d.asin = t_rf.asin AND t_d.fill_date = t_rf.date
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa_retail_price        AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(retail_price) AS retail_price
            FROM
                cte_keepa_retail_price a
            GROUP BY 1, 2
        )
        , cte_week_keepa_retail_price         AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(retail_price) AS retail_price
            FROM
                cte_keepa_retail_price a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        -- select * from cte_week_rf order by yr_wk desc;
        , cte_keepa           AS (
            SELECT
                -- * EXCEPT (rnum)
                t_d.asin
                , t_d.fill_date AS date
                , IFNULL(t_keepa.LISTPRICE, FIRST_VALUE(t_keepa.LISTPRICE IGNORE NULLS) OVER (PARTITION BY t_d.asin ORDER BY t_d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) * e.usd AS LISTPRICE
            FROM
                cte_date t_d
                    LEFT JOIN (
                    SELECT
                        DATE(LISTPRICE_time) AS date
                        , a.asin
                        , LISTPRICE
                        , ROW_NUMBER() OVER (PARTITION BY a.asin, DATE (LISTPRICE_time) ORDER BY LISTPRICE_time DESC) AS rnum
                    FROM
                        -- keepa.zinus_amz_list_price a
                        dw.amzes_list_price_all a
                            JOIN cte_target b
                                ON a.asin = b.asin
                    WHERE
                        a.LISTPRICE IS NOT NULL
                ) t_keepa
                        ON t_d.asin = t_keepa.asin AND t_d.fill_date = t_keepa.date AND rnum = 1
                    LEFT JOIN cte_exchange e
                        ON t_d.fill_date = e.date
        )
        , cte_month_keepa     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                -- , a.date
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
            GROUP BY 1, 2
        )
        , cte_week_keepa      AS (
            SELECT
                a.asin
                , b.yr_wk
                , AVG(LISTPRICE) AS list_price
            FROM
                cte_keepa a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        -- select * from cte_week_keepa order by yr_wk desc;
        , cte_month_healthy_inv AS (
            SELECT
                asin
                , FORMAT_DATE('%Y%m', PARSE_DATE('%m-%d-%Y', end_date)) AS yr_month
                , unhealthy_inventory
                , unhealthy_units
            FROM
                vc.amz_vc_es_inv_monthly
        )
        , cte_month_inv       AS (
            SELECT
                asin
                , yr_month
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        -- , b.yr_month
                        -- , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , open_purchase_order_quantity AS daily_open_purchase_order_quantity
                        , sellable_on_hand_inventory AS daily_sellable_on_hand_inventory
                        , sellable_on_hand_units AS daily_sellable_on_hand_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units

                        --                     , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        --                     , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY FORMAT_DATE('%Y%m', a.date), a.asin ORDER BY a.date DESC) AS unhealthy_units
                        , c.unhealthy_inventory * e.usd AS unhealthy_inventory
                        , c.unhealthy_units

                        , a.date <= LAST_MONTH_DAY as is_closed
                    FROM
                        vc.amz_es_vc_inv_daily_all a
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date

                            LEFT JOIN cte_month_healthy_inv c
                                ON a.asin = c.asin AND FORMAT_DATE('%Y%m', a.date) = c.yr_month
--                     WHERE
--                         a.date <= LAST_MONTH_DAY
                )
            GROUP BY 1, 2
        )
        , cte_week_inv        AS (
            SELECT
                asin
                , yr_wk
                , SUM(net_received) AS net_received
                , SUM(net_received_units) AS net_received_units
                , ANY_VALUE(open_purchase_order_quantity) AS open_purchase_order_quantity
                , ANY_VALUE(sellable_on_hand_inventory) AS sellable_on_hand_inventory
                , ANY_VALUE(sellable_on_hand_units) AS sellable_on_hand_units

                , ANY_VALUE(aged_90_days_sellable_inventory) AS aged_90_days_sellable_inventory
                , ANY_VALUE(aged_90_days_sellable_units) AS aged_90_days_sellable_units
                , ANY_VALUE(unsellable_on_hand_inventory) AS unsellable_on_hand_inventory
                , ANY_VALUE(unsellable_on_hand_units) AS unsellable_on_hand_units
                , ANY_VALUE(unhealthy_inventory) AS unhealthy_inventory
                , ANY_VALUE(unhealthy_units) AS unhealthy_units

                , MIN(is_closed) as is_closed -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                (
                    SELECT
                        a.asin
                        , a.date
                        -- , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                        -- , b.yr_month
                        , b.yr_wk
                        , net_received * e.usd AS net_received
                        , net_received_units
                        , FIRST_VALUE(open_purchase_order_quantity) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS open_purchase_order_quantity
                        , FIRST_VALUE(sellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS sellable_on_hand_inventory
                        , FIRST_VALUE(sellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS sellable_on_hand_units

                        , FIRST_VALUE(aged_90_days_sellable_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS aged_90_days_sellable_inventory
                        , FIRST_VALUE(aged_90_days_sellable_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS aged_90_days_sellable_units
                        , FIRST_VALUE(unsellable_on_hand_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unsellable_on_hand_inventory
                        , FIRST_VALUE(unsellable_on_hand_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unsellable_on_hand_units
                        , FIRST_VALUE(unhealthy_inventory) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) * e.usd AS unhealthy_inventory
                        , FIRST_VALUE(unhealthy_units) OVER (PARTITION BY b.yr_wk, a.asin ORDER BY a.date DESC) AS unhealthy_units

                        , a.date <= LAST_WEEK_DAY as is_closed
                    FROM
                        vc.amz_es_vc_inv_daily_all a
                            LEFT JOIN meta.wk_calendar_new b
                                ON a.date BETWEEN b.start_date AND b.end_date
                            LEFT JOIN cte_exchange e
                                ON a.date = e.date
--                     WHERE
--                         a.date <= LAST_WEEK_DAY
                )
            GROUP BY 1, 2
        )
        , cte_month_sales     AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_MONTH_DAY) as is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_es_vc_sales_daily_all a
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_MONTH_DAY
            GROUP BY 1, 2
        )
        , cte_week_sales      AS (
            SELECT
                a.asin
                , b.yr_wk
                , SUM(shipped_revenue * e.usd) AS shipped_revenue
                , SUM(shipped_units) AS shipped_units
                , SUM(customer_returns) AS customer_returns

                , MIN(a.date <= LAST_WEEK_DAY) AS is_closed  -- false 가 하나라도 있으면 open 상태 (마감 미완료), 모두 true 일때 close 상태 (마감)
            FROM
                vc.amz_es_vc_sales_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
                    LEFT JOIN cte_exchange e
                        ON a.date = e.date
--             WHERE
--                 a.date <= LAST_WEEK_DAY
            GROUP BY 1, 2
        )
        , cte_month_netppm    AS (
            SELECT
                a.asin
                , CAST(FORMAT_DATE('%Y%m', a.date) AS INT64) AS yr_month
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_es_vc_netppm_daily_all a
            GROUP BY 1, 2
            -- ORDER BY 1 DESC
        )
        , cte_week_netppm     AS (
            SELECT
                a.asin
                , b.yr_wk
                -- , a.date
                , AVG(net_ppm) AS net_ppm
            FROM
                vc.amz_es_vc_netppm_daily_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON a.date BETWEEN b.start_date AND b.end_date
            GROUP BY 1, 2
        )
        , cte_sales_inv_month AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_month, inv.yr_month) AS yr_month
                , sales.* EXCEPT (asin, yr_month, is_closed)
                , inv.* EXCEPT (asin, yr_month, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_month_sales sales
                    FULL OUTER JOIN cte_month_inv inv
                        ON sales.asin = inv.asin AND sales.yr_month = inv.yr_month
        )
        , cte_sales_inv_week  AS (
            SELECT
                COALESCE(sales.asin, inv.asin) AS asin
                , COALESCE(sales.yr_wk, inv.yr_wk) AS yr_wk
                , sales.* EXCEPT (asin, yr_wk, is_closed)
                , inv.* EXCEPT (asin, yr_wk, is_closed)
                , COALESCE(sales.is_closed, inv.is_closed) AS is_closed
            FROM
                cte_week_sales sales
                    FULL OUTER JOIN cte_week_inv inv
                        ON sales.asin = inv.asin AND sales.yr_wk = inv.yr_wk
        )
    SELECT
        vc.asin
        , vc.yr_month AS yr_month_or_week
        , 'MONTH' AS period_type
        , 'ES' AS country
        , vc.* EXCEPT (asin, yr_month)
        , keepa.list_price
        , ap.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_month vc
            LEFT JOIN cte_month_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_month = keepa.yr_month
            LEFT JOIN cte_month_keepa_retail_price ap
                ON vc.asin = ap.asin AND vc.yr_month = ap.yr_month
            LEFT JOIN cte_month_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_month = netppm.yr_month

    UNION ALL

    SELECT
        vc.asin
        , vc.yr_wk AS yr_month_or_week
        , 'WEEK' AS period_type
        , 'ES' AS country
        , vc.* EXCEPT (asin, yr_wk)
        , keepa.list_price
        , ap.retail_price
        , netppm.net_ppm
    FROM
        cte_sales_inv_week vc
            LEFT JOIN cte_week_keepa keepa
                ON vc.asin = keepa.asin AND vc.yr_wk = keepa.yr_wk
            LEFT JOIN cte_week_keepa_retail_price ap
                ON vc.asin = ap.asin AND vc.yr_wk = ap.yr_wk
            LEFT JOIN cte_week_netppm netppm
                ON vc.asin = netppm.asin AND vc.yr_wk = netppm.yr_wk
    ;

END
;

------------------------------------------------------------------------------------------------------------------------
-- [MASTER] ------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp1.glb_di_mst AS
WITH
    cte_new_collection_mst as (
        -- select zinus_sku, sales_country from meta.global_sku_master GROUP BY zinus_sku, sales_country HAVING count(DISTINCT collection_name) > 1;

--         SELECT DISTINCT  sales_country FROM meta.global_sku_master; -- MELLOW IS US

        SELECT DISTINCT
            zinus_sku
            , IF(sales_country = 'GB', 'UK', sales_country) AS sales_country
            , collection_name
        FROM
            meta.global_sku_master
            -- ods.global_sku_master
    )
    , cte_global_mst AS (
        SELECT
            mdm.sku as zinus_sku
            , mdm.zcustomer_code AS asin
            , IF(TRIM(mdm.zfincat) = '90.SmartBases', '25.SMARTBASES', TRIM(mdm.zfincat)) AS category
            , COALESCE(new_col_mst.collection_name, COALESCE(col_additions.collection, 'UNKNOWN')) AS collection
            , IF(UPPER(mdm.zjde_channel) LIKE '%MELLOW%', 'MELLOW', IF(UPPER(mdm.sales_land) = 'GB', 'UK', UPPER(mdm.sales_land))) AS country
            , mdm.mfg_land AS coo
            , mdm.zbig AS big
            , mdm.zmid AS middle
            , mdm.zsing AS single
            , c.single_cat_desc
        FROM

            meta.mdm_portal_sku mdm -- GB IS UK / EU ?

                -- add zinus_sku
                LEFT JOIN meta.mdm_category_mst c
                    ON
                        mdm.zbig = c.big_cat
                        AND mdm.zmid = c.middle_cat
                        AND mdm.zsing = c.single_cat

                -- add single cat desc
                LEFT JOIN cte_new_collection_mst new_col_mst
                    ON
                        mdm.sku = new_col_mst.zinus_sku
                        AND IF(mdm.sales_land = 'GB', 'UK', mdm.sales_land) = new_col_mst.sales_country

                -- new col additions
                LEFT JOIN tmp.mapping_new_collection_additions col_additions
                    ON
                        -- col_additions 의 us 제외한 데이터는 zinus_sku 가 없으면 collection 없음
                        mdm.sku = col_additions.zinus_sku
                            -- mdm - GB, UK 혼재되어 있음
                            -- channel 이 mellow 인 경우 us country is mellow
                            AND IF(mdm.sales_land = 'GB', 'UK', IF(mdm.sales_land = 'US' AND UPPER(mdm.zjde_channel) LIKE '%MELLOW%', 'MELLOW', mdm.sales_land)) = col_additions.country
                            AND col_additions.country != 'US'
        WHERE
            -- us 를 제외하면 mellow 도 제외됨
            ( UPPER(mdm.sales_land) != 'US' OR (UPPER(mdm.sales_land) = 'US' AND UPPER(mdm.zjde_channel) LIKE '%MELLOW%') )
--             select DISTINCT zjde_channel from meta.mdm_portal_sku order by 1;
--             select DISTINCT zjde_channel from meta.mdm_portal_sku where upper(zjde_channel) like '%MELLOW%' order by 1;
            AND mdm.zcustomer_code IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY
                mdm.zcustomer_code, IF(UPPER(mdm.zjde_channel) LIKE '%MELLOW%', 'MELLOW', UPPER(mdm.sales_land))
            ORDER BY
                IF(UPPER(mdm.zjde_channel) LIKE '%AMAZON%', 1, 2)
                , SAFE_CAST(mdm.zvalid_from AS INT64) DESC
        ) = 1

        -- mdm sales channel check
        -- select distinct zjde_channel from meta.mdm_portal_sku mdm where ( UPPER(mdm.sales_land) != 'US' OR (UPPER(mdm.sales_land) = 'US' AND mdm.zjde_channel LIKE 'MELLOW%') ) order by 1;

    )
    , cte_mdm_us_mst AS (
        SELECT
            mdm.sku as zinus_sku
            , mdm.zcustomer_code AS asin
            , IF(TRIM(mdm.zfincat) = '90.SmartBases', '25.SMARTBASES', TRIM(mdm.zfincat)) AS category
            , COALESCE(col_mst.collection_name, COALESCE(col_additions.collection, 'UNKNOWN')) AS collection
            , UPPER(mdm.sales_land) AS country
            , mdm.mfg_land AS coo
            , mdm.zbig AS big
            , mdm.zmid AS middle
            , mdm.zsing AS single
            , c.single_cat_desc
        FROM

            meta.mdm_portal_sku mdm

            -- add zinus_sku
                LEFT JOIN meta.mdm_category_mst c
                    ON
                        mdm.zbig = c.big_cat
                            AND mdm.zmid = c.middle_cat
                            AND mdm.zsing = c.single_cat

                -- add single cat desc
                LEFT JOIN cte_new_collection_mst col_mst
                    ON
                        mdm.sku = col_mst.zinus_sku
                            AND mdm.sales_land = col_mst.sales_country

                -- new col additions
                LEFT JOIN tmp.mapping_new_collection_additions col_additions
                    ON
                        (mdm.sku = col_additions.zinus_sku OR mdm.zcustomer_code = col_additions.asin)
                        AND mdm.sales_land = col_additions.country
        WHERE
            UPPER(mdm.sales_land) = 'US' AND UPPER(mdm.zjde_channel) NOT LIKE '%MELLOW%'
            AND mdm.zcustomer_code IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY
                mdm.zcustomer_code
            ORDER BY
                CASE
                    WHEN UPPER(mdm.zjde_channel) = 'AMAZON DI' THEN 1
                    WHEN UPPER(mdm.zjde_channel) = 'AMAZON DI (AMZ DI EXCLUSIVE)' THEN 2
                    WHEN UPPER(mdm.zjde_channel) = 'AMAZON DI FBA' THEN 3
                    WHEN UPPER(mdm.zjde_channel) LIKE '%AMAZON%' THEN 4
                    ELSE 5
                END
                , SAFE_CAST(mdm.zvalid_from AS INT64) DESC
            ) = 1

        -- mdm sales channel check
        -- select distinct zjde_channel from meta.mdm_portal_sku mdm where ( UPPER(mdm.sales_land) != 'US' OR (UPPER(mdm.sales_land) = 'US' AND mdm.zjde_channel LIKE 'MELLOW%') ) order by 1;

    )
    , cte_pi_us_mst AS (
        SELECT
            pi.zinus_sku
            , pi.asin
            , COALESCE(
                    IF(TRIM(mdm.zfincat) = '90.SmartBases', '25.SMARTBASES', TRIM(mdm.zfincat))
                    , CASE TRIM(pi.financial_category)
                          WHEN 'Foam Mattresses' THEN '10.FOAM MATTRESSES'
                          WHEN 'Spring Mattresses' THEN '15.SPRING MATTRESS'
                          WHEN 'Platform Beds' THEN '20.PLATFORM BEDS'
                          WHEN 'SmartBases' THEN '25.SMARTBASES'
                          WHEN 'Box Springs' THEN '30.BOX SPRINGS'
                          WHEN 'Other Frames & Beds' THEN '35.OTH.FRAMES&BEDS'
                          WHEN 'Sofa' THEN '40.SOFA'
                          WHEN 'Non Bedroom Furniture' THEN '45.NON BEDROOM FUR'
                          WHEN 'Toppers' THEN '50.TOPPERS'
                          WHEN 'Others' THEN '95.OTHERS'
                        -- ELSE 'UNKNOWN'
                  END
              ) AS category

            , COALESCE(COALESCE(pi.new_collection, col_mst.collection_name), COALESCE(col_additions.collection, 'UNKNOWN')) AS collection
            , 'US' AS country
            , COALESCE(mdm.mfg_land, pi.coo) AS coo
            , mdm.zbig AS big
            , mdm.zmid AS middle
            , mdm.zsing AS single
            --                 , mdm.big
            --                 , mdm.middle
            --                 , mdm.single
            , c.single_cat_desc
        FROM
            -- meta.amz_zinus_master_pdt_pi pi
            meta.amz_zinus_master_pdt_pi_add_new_col pi

                LEFT JOIN meta.mdm_portal_sku mdm
                    ON pi.asin = mdm.zcustomer_code
                        AND UPPER(mdm.sales_land) = 'US'

                LEFT JOIN meta.mdm_category_mst c
                    ON mdm.zbig = c.big_cat
                        AND mdm.zmid = c.middle_cat
                        AND mdm.zsing = c.single_cat

                LEFT JOIN cte_new_collection_mst col_mst
                    ON pi.zinus_sku = col_mst.zinus_sku AND col_mst.sales_country = 'US'

                -- new col additions
                LEFT JOIN tmp.mapping_new_collection_additions col_additions
                    ON
                        pi.asin = col_additions.asin
                        AND col_additions.country = 'US'
        -- WHERE
            -- UPPER(mdm.sales_channel) IN ( 'AMAZON DI', 'AMAZON DI (AMZ DI EXCLUSIVE)', 'AMAZON DI FBA' )
            -- UPPER(sales_chan) IN ( 'AMAZON DI')
            -- -- UPPER(mdm.sales_land)  = 'US'
            -- -- AND mdm.zcustomer_code IS NOT NULL
            -- AND pi.asin IS NOT NULL
        --  QUALIFY ROW_NUMBER() OVER (PARTITION BY mdm.customer_code, mdm.sales_channel ORDER BY mdm.create_date DESC) = 1
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY
                pi.asin
            ORDER BY
                CASE
                      WHEN UPPER(mdm.zjde_channel) = 'AMAZON DI' THEN 1
                      WHEN UPPER(mdm.zjde_channel) = 'AMAZON DI (AMZ DI EXCLUSIVE)' THEN 2
                      WHEN UPPER(mdm.zjde_channel) = 'AMAZON DI FBA' THEN 3
                      WHEN UPPER(mdm.zjde_channel) LIKE '%AMAZON%' THEN 4
                      ELSE 5
                  END
                , SAFE_CAST(mdm.zvalid_from AS INT64) DESC
        ) = 1
        -- select asin from meta.amz_zinus_master_pdt_pi_add_new_col GROUP BY 1 having count(1) > 1;
    )
    , cte_us_mst as (
        SELECT *, 1 as ord FROM cte_pi_us_mst
        union all
        SELECT *, 2 as ord FROM cte_mdm_us_mst
    )
SELECT * FROM cte_global_mst
UNION ALL
SELECT
    * EXCEPT (ord)
FROM
    cte_us_mst
QUALIFY ROW_NUMBER() OVER (PARTITION BY asin ORDER BY ord) = 1
;

------------------------------------------------------------------------------------------------------------------------
-- [MART] --------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- [amz_di_global] -----------------------------------------------------------------------------------------------------
-- select asin from mart.amz_di_global where country IN ('DE', 'UK', 'FR', 'IT', 'ES') and zinus_sku is not null GROUP BY asin, period_type, yr_month_or_week HAVING count(1) > 2 and count(yr_month_or_week) = 3;
-- B087K1L59X

BEGIN

    DECLARE LAST_UPDATE DATE;
    SET LAST_UPDATE = (
        SELECT MAX(date) FROM vc.amz_vc_sales_daily_all
    );

    CREATE OR REPLACE TABLE mart.amz_di_global AS
        WITH cte as (
            SELECT
                a.asin
                , yr_month_or_week
                , period_type
                , a.country
                , b.zinus_sku
                , IF(b.category IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS division
                , b.category AS financial_category
                , b.collection
                , b.coo
                , b.big
                , b.middle
                , b.single
                , b.single_cat_desc
                , shipped_revenue
                , shipped_units
                , customer_returns
                , net_received
                , net_received_units
                , open_purchase_order_quantity
                , sellable_on_hand_inventory
                , sellable_on_hand_units

                , aged_90_days_sellable_inventory
                , aged_90_days_sellable_units
                , unsellable_on_hand_inventory
                , unsellable_on_hand_units
                , unhealthy_inventory
                , unhealthy_units

                , list_price
                , retail_price
                , net_ppm
                , a.is_closed
                , LAST_UPDATE AS last_updated
            FROM
                (
                    SELECT * FROM tmp1.amz_di_jp
                    UNION ALL
                    SELECT * FROM tmp1.amz_di_au
                    UNION ALL
                    SELECT * FROM tmp1.amz_di_ca
                    UNION ALL
                    SELECT * FROM tmp1.amz_di_de
                    UNION ALL
                    SELECT * FROM tmp1.amz_di_uk
                    UNION ALL
                    SELECT * FROM tmp1.amz_di_fr
                    UNION ALL
                    SELECT * FROM tmp1.amz_di_it
                    UNION ALL
                    SELECT * FROM tmp1.amz_di_es

                    UNION ALL
                    SELECT * FROM tmp1.amz_di_mellow

                    UNION ALL
                    SELECT * FROM tmp1.amz_di_mx

                    UNION ALL
                    SELECT * FROM tmp1.amz_di_us

                ) a
                    LEFT JOIN tmp1.glb_di_mst b
                        ON a.asin = b.asin AND a.country = b.country
    )
    , cte_eu as (
        SELECT
            asin
            , yr_month_or_week
            , period_type
            , 'EU' as country
            , zinus_sku
            , division
            , financial_category
            , collection
            , coo
            , big
            , middle
            , single
            , single_cat_desc

            , SUM(shipped_revenue) as shipped_revenue
            , SUM(shipped_units) as shipped_units
            , SUM(customer_returns) as customer_returns
            , SUM(net_received) as net_received
            , SUM(net_received_units) as net_received_units
            , SUM(open_purchase_order_quantity) as open_purchase_order_quantity
            , SUM(sellable_on_hand_inventory) as sellable_on_hand_inventory
            , SUM(sellable_on_hand_units) as sellable_on_hand_units

            , SUM(aged_90_days_sellable_inventory) as aged_90_days_sellable_inventory
            , SUM(aged_90_days_sellable_units) as aged_90_days_sellable_units
            , SUM(unsellable_on_hand_inventory) as unsellable_on_hand_inventory
            , SUM(unsellable_on_hand_units) as unsellable_on_hand_units
            , SUM(unhealthy_inventory) as unhealthy_inventory
            , SUM(unhealthy_units) as unhealthy_units

            , AVG(list_price) as list_price
            , AVG(retail_price) as retail_price
            , AVG(net_ppm) as net_ppm

            , MIN(is_closed) AS is_closed

            , MAX(last_updated) as last_updated
       FROM cte
       WHERE
           country IN ('DE', 'UK', 'FR', 'IT', 'ES')
       GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12,13
    )
    SELECT * FROM cte
    UNION ALL
    SELECT * FROM cte_eu
    ;

END;

-- [amz_global_fcst_all] -----------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart.amz_global_fcst_all AS
    WITH cte as (
        WITH
            base_data AS (
                SELECT *, 'AU' AS Company, 'p70' AS Type FROM `vc.amz_vc_au_fcst_inv_new_all` UNION ALL
                SELECT *, 'AU' AS Company, 'mean' AS Type FROM `vc.amz_vc_au_fcst_mean_inv_new_all` UNION ALL
                SELECT *, 'AU' AS Company, 'p80' AS Type FROM `vc.amz_vc_au_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'AU' AS Company, 'p90' AS Type FROM `vc.amz_vc_au_fcst_p90_inv_new_all` UNION ALL

                SELECT *, 'CA' AS Company, 'p70' AS Type FROM `vc.amz_vc_ca_fcst_inv_new_all` UNION ALL
                SELECT *, 'CA' AS Company, 'mean' AS Type FROM `vc.amz_vc_ca_fcst_mean_inv_new_all` UNION ALL
                SELECT *, 'CA' AS Company, 'p80' AS Type FROM `vc.amz_vc_ca_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'CA' AS Company, 'p90' AS Type FROM `vc.amz_vc_ca_fcst_p90_inv_new_all` UNION ALL

                SELECT *, 'DE' AS Company, 'p70' AS Type FROM `vc.amz_vc_de_fcst_inv_new_all` UNION ALL
                SELECT *, 'DE' AS Company, 'mean' AS Type FROM `vc.amz_vc_de_fcst_mean_inv_new_all` UNION ALL
                SELECT *, 'DE' AS Company, 'p80' AS Type FROM `vc.amz_vc_de_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'DE' AS Company, 'p90' AS Type FROM `vc.amz_vc_de_fcst_p90_inv_new_all` UNION ALL

                SELECT *, 'ES' AS Company, 'p70' AS Type FROM `vc.amz_vc_es_fcst_inv_new_all` UNION ALL
                SELECT *, 'ES' AS Company, 'mean' AS Type FROM `vc.amz_vc_es_fcst_mean_inv_new_all` UNION ALL
                SELECT *, 'ES' AS Company, 'p80' AS Type FROM `vc.amz_vc_es_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'ES' AS Company, 'p90' AS Type FROM `vc.amz_vc_es_fcst_p90_inv_new_all` UNION ALL

                SELECT *, 'US' AS Company, 'p70' AS Type FROM `vc.amz_vc_fcst_inv_new_all` UNION ALL
                SELECT *, 'US' AS Company, 'p80' AS Type FROM `vc.amz_vc_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'US' AS Company, 'p90' AS Type FROM `vc.amz_vc_fcst_p90_inv_new_all` UNION ALL
                SELECT *, 'US' AS Company, 'mean' AS Type FROM `vc.amz_vc_fcst_mean_inv_new_all` UNION ALL

                SELECT *, 'FR' AS Company, 'p70' AS Type FROM `vc.amz_vc_fr_fcst_inv_new_all` UNION ALL
                SELECT *, 'FR' AS Company, 'mean' AS Type FROM `vc.amz_vc_fr_fcst_mean_inv_new_all` UNION ALL
                SELECT *, 'FR' AS Company, 'p80' AS Type FROM `vc.amz_vc_fr_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'FR' AS Company, 'p90' AS Type FROM `vc.amz_vc_fr_fcst_p90_inv_new_all` UNION ALL

                SELECT *, 'IT' AS Company, 'p70' AS Type FROM `vc.amz_vc_it_fcst_inv_new_all` UNION ALL
                SELECT *, 'IT' AS Company, 'mean' AS Type FROM `vc.amz_vc_it_fcst_mean_inv_new_all` UNION ALL
                SELECT *, 'IT' AS Company, 'p80' AS Type FROM `vc.amz_vc_it_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'IT' AS Company, 'p90' AS Type FROM `vc.amz_vc_it_fcst_p90_inv_new_all` UNION ALL

                SELECT *, 'JP' AS Company, 'p70' AS Type FROM `vc.amz_vc_jp_fcst_inv_new_all` UNION ALL
                SELECT *, 'JP' AS Company, 'mean' AS Type FROM `vc.amz_vc_jp_fcst_mean_inv_new_all` UNION ALL
                SELECT *, 'JP' AS Company, 'p80' AS Type FROM `vc.amz_vc_jp_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'JP' AS Company, 'p90' AS Type FROM `vc.amz_vc_jp_fcst_p90_inv_new_all` UNION ALL

                SELECT *, 'Mellow' AS Company, 'p70' AS Type FROM `vc.amz_vc_mellow_us_fcst_inv_new_all` UNION ALL
                SELECT *, 'Mellow' AS Company, 'mean' AS Type FROM `vc.amz_vc_mellow_us_fcst_mean_inv_new_all` UNION ALL
                SELECT *, 'Mellow' AS Company, 'p80' AS Type FROM `vc.amz_vc_mellow_us_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'Mellow' AS Company, 'p90' AS Type FROM `vc.amz_vc_mellow_us_fcst_p90_inv_new_all` UNION ALL

                SELECT *, 'UK' AS Company, 'p70' AS Type FROM `vc.amz_vc_uk_fcst_inv_new_all` UNION ALL
                SELECT *, 'UK' AS Company, 'mean' AS Type FROM `vc.amz_vc_uk_fcst_mean_inv_new_all` UNION ALL
                SELECT *, 'UK' AS Company, 'p80' AS Type FROM `vc.amz_vc_uk_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'UK' AS Company, 'p90' AS Type FROM `vc.amz_vc_uk_fcst_p90_inv_new_all` UNION ALL

                SELECT *, 'MX' AS Company, 'p70' AS Type FROM `vc.amz_vc_mx_fcst_inv_new_all` UNION ALL
                SELECT *, 'MX' AS Company, 'mean' AS Type FROM `vc.amz_vc_mx_fcst_mean_inv_new_all` UNION ALL
                SELECT *, 'MX' AS Company, 'p80' AS Type FROM `vc.amz_vc_mx_fcst_p80_inv_new_all` UNION ALL
                SELECT *, 'MX' AS Company, 'p90' AS Type FROM `vc.amz_vc_mx_fcst_p90_inv_new_all`
            )
        SELECT
            b.*
            , m.zinus_sku
            , m.category AS financial_category
            , m.collection
            , m.coo
            , m.big
            , m.middle
            , m.single
            , m.single_cat_desc
            , IF((SELECT MAX(date) FROM base_data)> CURRENT_DATE(), CURRENT_DATE(), (SELECT MAX(date) FROM base_data)) AS last_updated
        FROM
            base_data b
                LEFT JOIN tmp1.glb_di_mst m
                    ON b.asin = m.asin AND b.Company = m.country
    ),
    cte_eu as (
        SELECT
            asin, MAX(product_title) as product_title
             , SUM(week0) AS week0, SUM(week1) AS week1, SUM(week2) AS week2, SUM(week3) AS week3, SUM(week4) AS week4, SUM(week5) AS week5, SUM(week6) AS week6, SUM(week7) AS week7, SUM(week8) AS week8, SUM(week9) AS week9, SUM(week10) AS week10, SUM(week11) AS week11, SUM(week12) AS week12, SUM(week13) AS week13, SUM(week14) AS week14, SUM(week15) AS week15, SUM(week16) AS week16, SUM(week17) AS week17, SUM(week18) AS week18, SUM(week19) AS week19, SUM(week20) AS week20, SUM(week21) AS week21, SUM(week22) AS week22, SUM(week23) AS week23, SUM(week24) AS week24, SUM(week25) AS week25, SUM(week26) AS week26, SUM(week27) AS week27, SUM(week28) AS week28, SUM(week29) AS week29, SUM(week30) AS week30, SUM(week31) AS week31, SUM(week32) AS week32, SUM(week33) AS week33, SUM(week34) AS week34, SUM(week35) AS week35, SUM(week36) AS week36, SUM(week37) AS week37, SUM(week38) AS week38, SUM(week39) AS week39, SUM(week40) AS week40, SUM(week41) AS week41, SUM(week42) AS week42, SUM(week43) AS week43, SUM(week44) AS week44, SUM(week45) AS week45, SUM(week46) AS week46, SUM(week47) AS week47
             , date
             , MAX(brand) as brand
             , 'EU' as Company
             , Type, zinus_sku, financial_category, collection, coo, big, middle, single, single_cat_desc
             , MAX(last_updated) AS last_updated
        FROM cte
        WHERE Company IN ('DE', 'UK', 'FR', 'IT', 'ES')
        GROUP BY asin, date, Type, zinus_sku, financial_category, collection, coo, big, middle, single, single_cat_desc
    )
    SELECT * FROM cte
    UNION ALL
    SELECT * FROM cte_eu
;

-- [psi report] --------------------------------------------------------------------------------------------------------
-- select count(1) from mart.amz_di_global_psi_report;
-- 166,920

CREATE OR REPLACE TABLE tmp1.amz_di_global_psi_report AS
WITH
    cte_fcst as (
        WITH cte_fcst_src as (
            SELECT
                a.asin
                , a.Company as country
                , a.financial_category
                , a.type
                , week0, week1, week2, week3, week4, week5, week6, week7, week8, week9, week10, week11, week12, week13, week14, week15, week16, week17, week18, week19, week20, week21, week22, week23, week24, week25, week26, week27, week28, week29, week30, week31, week32, week33, week34, week35, week36, week37, week38, week39, week40, week41, week42, week43, week44, week45, week46, week47
                , b.yr_month
                , b.yr_wk
            FROM
                mart.amz_global_fcst_all a
                    LEFT JOIN meta.wk_calendar_new b
                        ON DATE_SUB(a.date, INTERVAL 1 WEEK) BETWEEN b.start_date AND b.end_date
--             WHERE
--                 a.Type='p70'

--                 AND asin = 'B006MIPW70'
--                 AND Company = 'US'
        )
        , cte_fcst_month_week_union as (
                SELECT
                    * EXCEPT (yr_wk, yr_month)

                    , yr_month AS yr_month_or_week
                    , 'MONTH' AS period_type
                FROM
                    cte_fcst_src
                QUALIFY
                    ROW_NUMBER() OVER (PARTITION BY asin, country, type, yr_month ORDER BY yr_wk DESC) = 1

                UNION ALL

                SELECT
                    * EXCEPT (yr_wk, yr_month)
                    , yr_wk AS yr_month_or_week
                    , 'WEEK' AS period_type
                FROM
                    cte_fcst_src
            )
        SELECT
            COALESCE(country, 'UNKNOWN') AS country
            , IF(COALESCE(financial_category, 'UNKNOWN') IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS division
            , COALESCE(financial_category, 'UNKNOWN') AS category
            , type
            , period_type
            , CAST(yr_month_or_week AS STRING) as yr_month_or_week
            , SUM(week0) AS week0, SUM(week1) AS week1, SUM(week2) AS week2, SUM(week3) AS week3, SUM(week4) AS week4, SUM(week5) AS week5, SUM(week6) AS week6, SUM(week7) AS week7, SUM(week8) AS week8, SUM(week9) AS week9, SUM(week10) AS week10, SUM(week11) AS week11, SUM(week12) AS week12, SUM(week13) AS week13, SUM(week14) AS week14, SUM(week15) AS week15, SUM(week16) AS week16, SUM(week17) AS week17, SUM(week18) AS week18, SUM(week19) AS week19, SUM(week20) AS week20, SUM(week21) AS week21, SUM(week22) AS week22, SUM(week23) AS week23, SUM(week24) AS week24, SUM(week25) AS week25, SUM(week26) AS week26, SUM(week27) AS week27, SUM(week28) AS week28, SUM(week29) AS week29, SUM(week30) AS week30, SUM(week31) AS week31, SUM(week32) AS week32, SUM(week33) AS week33, SUM(week34) AS week34, SUM(week35) AS week35, SUM(week36) AS week36, SUM(week37) AS week37, SUM(week38) AS week38, SUM(week39) AS week39, SUM(week40) AS week40, SUM(week41) AS week41, SUM(week42) AS week42, SUM(week43) AS week43, SUM(week44) AS week44, SUM(week45) AS week45, SUM(week46) AS week46, SUM(week47) AS week47
        FROM
            cte_fcst_month_week_union
--         GROUP BY country, financial_category, period_type, yr_month_or_week
        GROUP BY 1, 2, 3, 4, 5, 6
    )
--     select max(yr_month_or_week) from cte_fcst_p70 where period_type='WEEK';
--     select max(date) from mart.amz_global_fcst_all;

-- SELECT
--     *
-- FROM
--     mart.amz_di_global a
--         LEFT JOIN cte_fcst_p70 b
--             ON a.asin = b.asin AND a.country = b.Company AND a.yr_month_or_week = b.yr_month_or_week AND a.period_type = b.period_type
--     and a.country='US'
-- ;

    , cte_actual_src AS (
        SELECT
            COALESCE(a.country, 'UNKNOWN') AS country
            , IF(COALESCE(a.financial_category, 'UNKNOWN') IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS division
            , COALESCE(a.financial_category, 'UNKNOWN') AS category

--             , COALESCE(coo, 'UNKNOWN') AS origin
--             , '-' AS origin

            , CAST(a.yr_month_or_week AS STRING) AS yr_month_or_week
            , a.period_type

            , SUM(open_purchase_order_quantity) as open_purchase_order_quantity
            , SUM(open_purchase_order_quantity) * ( SUM(sellable_on_hand_inventory) / IF(SUM(COALESCE(sellable_on_hand_units, 0)) = 0, 1, SUM(sellable_on_hand_units)) ) AS open_purchase_order_amount
            , SUM(net_received_units) as net_received_units
            , SUM(net_received) as net_received
            , SUM(sellable_on_hand_units) as sellable_on_hand_units
            , SUM(sellable_on_hand_inventory) as sellable_on_hand_inventory
            , SUM(shipped_units) as shipped_units
            , SUM(shipped_revenue) as shipped_revenue

--             , IF(period_type = 'WEEK', CAST(MAX(b.yr_month) AS STRING), NULL) AS yr_month_for_week
        FROM
            mart.amz_di_global a

--                 LEFT JOIN meta.wk_calendar_new b
--                     ON a.yr_month_or_week = b.yr_wk
        WHERE
            a.is_closed = True
--             and a.country='US'
--             and a.country='CA'
--             and a.country='FR'

--             and a.asin = 'B006MIPW70'
--             and a.country='US'
        GROUP BY 1, 2, 3, 4, 5
    )
    , cte_final_week_row_value as (
        SELECT
            a.country
            , a.division
            , a.category
--             , a.origin
            , a.period_type
            , a.yr_month_or_week

            --             , c.open_purchase_order_quantity
            --             , c.open_purchase_order_amount

            , a.open_purchase_order_quantity
            , a.open_purchase_order_amount

            , a.sellable_on_hand_units -- ending inv
            , a.sellable_on_hand_inventory -- ending inv usd

            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN CURRENT ROW AND 25 FOLLOWING) AS sell_out_13_sum
            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN 52 FOLLOWING AND 77 FOLLOWING) AS sell_out_last_year_13_sum

            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN CURRENT ROW AND 25 FOLLOWING) AS sell_out_usd_13_sum
            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN 52 FOLLOWING AND 77 FOLLOWING) AS sell_out_usd_last_year_13_sum

            , LEAD(shipped_units, 51) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst1
            , LEAD(shipped_units, 50) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst2
            , LEAD(shipped_units, 49) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst3
            , LEAD(shipped_units, 48) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst4
            , LEAD(shipped_units, 47) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst5
            , LEAD(shipped_units, 46) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst6
            , LEAD(shipped_units, 45) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst7
            , LEAD(shipped_units, 44) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst8
            , LEAD(shipped_units, 43) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst9
            , LEAD(shipped_units, 42) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst10
            , LEAD(shipped_units, 41) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst11
            , LEAD(shipped_units, 40) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst12
            , LEAD(shipped_units, 39) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst13
            , LEAD(shipped_units, 38) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst14
            , LEAD(shipped_units, 37) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst15
            , LEAD(shipped_units, 36) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst16
            , LEAD(shipped_units, 35) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst17
            , LEAD(shipped_units, 34) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst18
            , LEAD(shipped_units, 33) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst19
            , LEAD(shipped_units, 32) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst20
            , LEAD(shipped_units, 31) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst21
            , LEAD(shipped_units, 30) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst22
            , LEAD(shipped_units, 29) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst23
            , LEAD(shipped_units, 28) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst24
            , LEAD(shipped_units, 27) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst25
            , LEAD(shipped_units, 26) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst26

            , LEAD(shipped_revenue, 51) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd1
            , LEAD(shipped_revenue, 50) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd2
            , LEAD(shipped_revenue, 49) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd3
            , LEAD(shipped_revenue, 48) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd4
            , LEAD(shipped_revenue, 47) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd5
            , LEAD(shipped_revenue, 46) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd6
            , LEAD(shipped_revenue, 45) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd7
            , LEAD(shipped_revenue, 44) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd8
            , LEAD(shipped_revenue, 43) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd9
            , LEAD(shipped_revenue, 42) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd10
            , LEAD(shipped_revenue, 41) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd11
            , LEAD(shipped_revenue, 40) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd12
            , LEAD(shipped_revenue, 39) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd13
            , LEAD(shipped_revenue, 38) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd14
            , LEAD(shipped_revenue, 37) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd15
            , LEAD(shipped_revenue, 36) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd16
            , LEAD(shipped_revenue, 35) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd17
            , LEAD(shipped_revenue, 34) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd18
            , LEAD(shipped_revenue, 33) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd19
            , LEAD(shipped_revenue, 32) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd20
            , LEAD(shipped_revenue, 31) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd21
            , LEAD(shipped_revenue, 30) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd22
            , LEAD(shipped_revenue, 29) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd23
            , LEAD(shipped_revenue, 28) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd24
            , LEAD(shipped_revenue, 27) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd25
            , LEAD(shipped_revenue, 26) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd26

            , p70.week0 as p70_week0, p70.week1 as p70_week1, p70.week2 as p70_week2, p70.week3 as p70_week3, p70.week4 as p70_week4, p70.week5 as p70_week5, p70.week6 as p70_week6, p70.week7 as p70_week7, p70.week8 as p70_week8, p70.week9 as p70_week9, p70.week10 as p70_week10, p70.week11 as p70_week11, p70.week12 as p70_week12, p70.week13 as p70_week13, p70.week14 as p70_week14, p70.week15 as p70_week15, p70.week16 as p70_week16, p70.week17 as p70_week17, p70.week18 as p70_week18, p70.week19 as p70_week19, p70.week20 as p70_week20, p70.week21 as p70_week21, p70.week22 as p70_week22, p70.week23 as p70_week23, p70.week24 as p70_week24, p70.week25 as p70_week25, p70.week26 as p70_week26, p70.week27 as p70_week27, p70.week28 as p70_week28, p70.week29 as p70_week29, p70.week30 as p70_week30, p70.week31 as p70_week31, p70.week32 as p70_week32, p70.week33 as p70_week33, p70.week34 as p70_week34, p70.week35 as p70_week35, p70.week36 as p70_week36, p70.week37 as p70_week37, p70.week38 as p70_week38, p70.week39 as p70_week39, p70.week40 as p70_week40, p70.week41 as p70_week41, p70.week42 as p70_week42, p70.week43 as p70_week43, p70.week44 as p70_week44, p70.week45 as p70_week45, p70.week46 as p70_week46, p70.week47 as p70_week47
            , p80.week0 as p80_week0, p80.week1 as p80_week1, p80.week2 as p80_week2, p80.week3 as p80_week3, p80.week4 as p80_week4, p80.week5 as p80_week5, p80.week6 as p80_week6, p80.week7 as p80_week7, p80.week8 as p80_week8, p80.week9 as p80_week9, p80.week10 as p80_week10, p80.week11 as p80_week11, p80.week12 as p80_week12, p80.week13 as p80_week13, p80.week14 as p80_week14, p80.week15 as p80_week15, p80.week16 as p80_week16, p80.week17 as p80_week17, p80.week18 as p80_week18, p80.week19 as p80_week19, p80.week20 as p80_week20, p80.week21 as p80_week21, p80.week22 as p80_week22, p80.week23 as p80_week23, p80.week24 as p80_week24, p80.week25 as p80_week25, p80.week26 as p80_week26, p80.week27 as p80_week27, p80.week28 as p80_week28, p80.week29 as p80_week29, p80.week30 as p80_week30, p80.week31 as p80_week31, p80.week32 as p80_week32, p80.week33 as p80_week33, p80.week34 as p80_week34, p80.week35 as p80_week35, p80.week36 as p80_week36, p80.week37 as p80_week37, p80.week38 as p80_week38, p80.week39 as p80_week39, p80.week40 as p80_week40, p80.week41 as p80_week41, p80.week42 as p80_week42, p80.week43 as p80_week43, p80.week44 as p80_week44, p80.week45 as p80_week45, p80.week46 as p80_week46, p80.week47 as p80_week47
            , p90.week0 as p90_week0, p90.week1 as p90_week1, p90.week2 as p90_week2, p90.week3 as p90_week3, p90.week4 as p90_week4, p90.week5 as p90_week5, p90.week6 as p90_week6, p90.week7 as p90_week7, p90.week8 as p90_week8, p90.week9 as p90_week9, p90.week10 as p90_week10, p90.week11 as p90_week11, p90.week12 as p90_week12, p90.week13 as p90_week13, p90.week14 as p90_week14, p90.week15 as p90_week15, p90.week16 as p90_week16, p90.week17 as p90_week17, p90.week18 as p90_week18, p90.week19 as p90_week19, p90.week20 as p90_week20, p90.week21 as p90_week21, p90.week22 as p90_week22, p90.week23 as p90_week23, p90.week24 as p90_week24, p90.week25 as p90_week25, p90.week26 as p90_week26, p90.week27 as p90_week27, p90.week28 as p90_week28, p90.week29 as p90_week29, p90.week30 as p90_week30, p90.week31 as p90_week31, p90.week32 as p90_week32, p90.week33 as p90_week33, p90.week34 as p90_week34, p90.week35 as p90_week35, p90.week36 as p90_week36, p90.week37 as p90_week37, p90.week38 as p90_week38, p90.week39 as p90_week39, p90.week40 as p90_week40, p90.week41 as p90_week41, p90.week42 as p90_week42, p90.week43 as p90_week43, p90.week44 as p90_week44, p90.week45 as p90_week45, p90.week46 as p90_week46, p90.week47 as p90_week47
            , mean.week0 as mean_week0, mean.week1 as mean_week1, mean.week2 as mean_week2, mean.week3 as mean_week3, mean.week4 as mean_week4, mean.week5 as mean_week5, mean.week6 as mean_week6, mean.week7 as mean_week7, mean.week8 as mean_week8, mean.week9 as mean_week9, mean.week10 as mean_week10, mean.week11 as mean_week11, mean.week12 as mean_week12, mean.week13 as mean_week13, mean.week14 as mean_week14, mean.week15 as mean_week15, mean.week16 as mean_week16, mean.week17 as mean_week17, mean.week18 as mean_week18, mean.week19 as mean_week19, mean.week20 as mean_week20, mean.week21 as mean_week21, mean.week22 as mean_week22, mean.week23 as mean_week23, mean.week24 as mean_week24, mean.week25 as mean_week25, mean.week26 as mean_week26, mean.week27 as mean_week27, mean.week28 as mean_week28, mean.week29 as mean_week29, mean.week30 as mean_week30, mean.week31 as mean_week31, mean.week32 as mean_week32, mean.week33 as mean_week33, mean.week34 as mean_week34, mean.week35 as mean_week35, mean.week36 as mean_week36, mean.week37 as mean_week37, mean.week38 as mean_week38, mean.week39 as mean_week39, mean.week40 as mean_week40, mean.week41 as mean_week41, mean.week42 as mean_week42, mean.week43 as mean_week43, mean.week44 as mean_week44, mean.week45 as mean_week45, mean.week46 as mean_week46, mean.week47 as mean_week47

        FROM
            cte_actual_src a
        --                 LEFT JOIN cte_final_month_row_value c
        --                     ON a.country = c.country AND a.division = c.division AND a.category = c.category AND a.origin = c.origin
                LEFT JOIN cte_fcst p70
                    on a.country = p70.country and a.division = p70.division and a.category = p70.category and a.period_type = p70.period_type and a.yr_month_or_week = p70.yr_month_or_week and p70.type = 'p70'

                LEFT JOIN cte_fcst p80
                    on a.country = p80.country and a.division = p80.division and a.category = p80.category and a.period_type = p80.period_type and a.yr_month_or_week = p80.yr_month_or_week and p80.type = 'p80'

                LEFT JOIN cte_fcst p90
                    on a.country = p90.country and a.division = p90.division and a.category = p90.category and a.period_type = p90.period_type and a.yr_month_or_week = p90.yr_month_or_week and p90.type = 'p90'

                LEFT JOIN cte_fcst mean
                    on a.country = mean.country and a.division = mean.division and a.category = mean.category and a.period_type = mean.period_type and a.yr_month_or_week = mean.yr_month_or_week and mean.type = 'mean'
        WHERE
            a.period_type = 'WEEK'
        --         QUALIFY ROW_NUMBER() OVER (PARTITION BY a.country, a.division, a.category, a.origin, a.period_type ORDER BY a.yr_month_or_week DESC) = 1
        QUALIFY RANK() OVER (PARTITION BY a.country, a.period_type ORDER BY a.yr_month_or_week DESC) = 1
    )
    -- SELECT * FROM cte_final_week_row_value where country='MX';
    , cte_final_month_row_value AS (
        SELECT
            a.country
            , a.division
            , a.category
--             , a.origin
            , a.period_type
            , a.yr_month_or_week

            , w.open_purchase_order_quantity
            , w.open_purchase_order_amount
--             , a.open_purchase_order_quantity -- fcst received po
--             , a.open_purchase_order_amount -- fcst received po usd

            , a.sellable_on_hand_units -- ending inv
            , a.sellable_on_hand_inventory -- ending inv usd

            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) AS sell_out_6_sum
            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN 12 FOLLOWING AND 17 FOLLOWING) AS sell_out_last_year_6_sum

            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) AS sell_out_usd_6_sum
            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN 12 FOLLOWING AND 17 FOLLOWING) AS sell_out_usd_last_year_6_sum

            , LEAD(shipped_units, 11) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst1
            , LEAD(shipped_units, 10) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst2
            , LEAD(shipped_units, 9) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst3
            , LEAD(shipped_units, 8) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst4
            , LEAD(shipped_units, 7) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst5
            , LEAD(shipped_units, 6) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst6
            , LEAD(shipped_units, 5) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst7
            , LEAD(shipped_units, 4) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst8
            , LEAD(shipped_units, 3) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst9
            , LEAD(shipped_units, 2) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst10
            , LEAD(shipped_units, 1) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst11
            , shipped_units AS sell_out_fcst12

            , LEAD(shipped_revenue, 11) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd1
            , LEAD(shipped_revenue, 10) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd2
            , LEAD(shipped_revenue, 9) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd3
            , LEAD(shipped_revenue, 8) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd4
            , LEAD(shipped_revenue, 7) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd5
            , LEAD(shipped_revenue, 6) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd6
            , LEAD(shipped_revenue, 5) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd7
            , LEAD(shipped_revenue, 4) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd8
            , LEAD(shipped_revenue, 3) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd9
            , LEAD(shipped_revenue, 2) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd10
            , LEAD(shipped_revenue, 1) OVER (PARTITION BY a.country, a.division, a.category, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd11
            , shipped_revenue AS sell_out_fcst_usd12

            , w.p70_week0, w.p70_week1, w.p70_week2, w.p70_week3, w.p70_week4, w.p70_week5, w.p70_week6, w.p70_week7, w.p70_week8, w.p70_week9, w.p70_week10, w.p70_week11, w.p70_week12, w.p70_week13, w.p70_week14, w.p70_week15, w.p70_week16, w.p70_week17, w.p70_week18, w.p70_week19, w.p70_week20, w.p70_week21, w.p70_week22, w.p70_week23, w.p70_week24, w.p70_week25, w.p70_week26, w.p70_week27, w.p70_week28, w.p70_week29, w.p70_week30, w.p70_week31, w.p70_week32, w.p70_week33, w.p70_week34, w.p70_week35, w.p70_week36, w.p70_week37, w.p70_week38, w.p70_week39, w.p70_week40, w.p70_week41, w.p70_week42, w.p70_week43, w.p70_week44, w.p70_week45, w.p70_week46, w.p70_week47
            , w.p80_week0, w.p80_week1, w.p80_week2, w.p80_week3, w.p80_week4, w.p80_week5, w.p80_week6, w.p80_week7, w.p80_week8, w.p80_week9, w.p80_week10, w.p80_week11, w.p80_week12, w.p80_week13, w.p80_week14, w.p80_week15, w.p80_week16, w.p80_week17, w.p80_week18, w.p80_week19, w.p80_week20, w.p80_week21, w.p80_week22, w.p80_week23, w.p80_week24, w.p80_week25, w.p80_week26, w.p80_week27, w.p80_week28, w.p80_week29, w.p80_week30, w.p80_week31, w.p80_week32, w.p80_week33, w.p80_week34, w.p80_week35, w.p80_week36, w.p80_week37, w.p80_week38, w.p80_week39, w.p80_week40, w.p80_week41, w.p80_week42, w.p80_week43, w.p80_week44, w.p80_week45, w.p80_week46, w.p80_week47
            , w.p90_week0, w.p90_week1, w.p90_week2, w.p90_week3, w.p90_week4, w.p90_week5, w.p90_week6, w.p90_week7, w.p90_week8, w.p90_week9, w.p90_week10, w.p90_week11, w.p90_week12, w.p90_week13, w.p90_week14, w.p90_week15, w.p90_week16, w.p90_week17, w.p90_week18, w.p90_week19, w.p90_week20, w.p90_week21, w.p90_week22, w.p90_week23, w.p90_week24, w.p90_week25, w.p90_week26, w.p90_week27, w.p90_week28, w.p90_week29, w.p90_week30, w.p90_week31, w.p90_week32, w.p90_week33, w.p90_week34, w.p90_week35, w.p90_week36, w.p90_week37, w.p90_week38, w.p90_week39, w.p90_week40, w.p90_week41, w.p90_week42, w.p90_week43, w.p90_week44, w.p90_week45, w.p90_week46, w.p90_week47
            , w.mean_week0, w.mean_week1, w.mean_week2, w.mean_week3, w.mean_week4, w.mean_week5, w.mean_week6, w.mean_week7, w.mean_week8, w.mean_week9, w.mean_week10, w.mean_week11, w.mean_week12, w.mean_week13, w.mean_week14, w.mean_week15, w.mean_week16, w.mean_week17, w.mean_week18, w.mean_week19, w.mean_week20, w.mean_week21, w.mean_week22, w.mean_week23, w.mean_week24, w.mean_week25, w.mean_week26, w.mean_week27, w.mean_week28, w.mean_week29, w.mean_week30, w.mean_week31, w.mean_week32, w.mean_week33, w.mean_week34, w.mean_week35, w.mean_week36, w.mean_week37, w.mean_week38, w.mean_week39, w.mean_week40, w.mean_week41, w.mean_week42, w.mean_week43, w.mean_week44, w.mean_week45, w.mean_week46, w.mean_week47

        FROM
            cte_actual_src a
                LEFT JOIN cte_final_week_row_value w
                    ON a.country = w.country AND a.division = w.division AND a.category = w.category
        WHERE
            a.period_type = 'MONTH'
--         QUALIFY ROW_NUMBER() OVER (PARTITION BY country, division, category, origin, period_type ORDER BY yr_month_or_week DESC) = 1
        QUALIFY RANK() OVER (PARTITION BY a.country, a.period_type ORDER BY a.yr_month_or_week DESC) = 1
    )
    --     SELECT sell_out_fcst1 * (sell_out_6_sum / sell_out_last_year_6_sum), * FROM cte_final_month_row_value;
    --    select * from cte_final_month_row_value;
    , cte_fcst_src as (
        SELECT
            country
            , division
            , category

            , FORMAT_DATE('%Y%m', DATE_ADD(PARSE_DATE('%Y%m', yr_month_or_week), INTERVAL arr MONTH)) AS yr_month_or_week
            , period_type

            , IF(arr <= 3, open_purchase_order_quantity / 3, 0) AS net_received_units
            , IF(arr <= 3, open_purchase_order_amount / 3, 0) AS net_received

            , sellable_on_hand_units
            , sellable_on_hand_inventory

            , CASE arr
                  WHEN 1 THEN sell_out_fcst1 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 2 THEN sell_out_fcst2 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 3 THEN sell_out_fcst3 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 4 THEN sell_out_fcst4 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 5 THEN sell_out_fcst5 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 6 THEN sell_out_fcst6 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 7 THEN sell_out_fcst7 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 8 THEN sell_out_fcst8 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 9 THEN sell_out_fcst9 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 10 THEN sell_out_fcst10 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 11 THEN sell_out_fcst11 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 12 THEN sell_out_fcst12 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
              END AS shipped_units
            , CASE arr
                  WHEN 1 THEN sell_out_fcst_usd1 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 2 THEN sell_out_fcst_usd2 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 3 THEN sell_out_fcst_usd3 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 4 THEN sell_out_fcst_usd4 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 5 THEN sell_out_fcst_usd5 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 6 THEN sell_out_fcst_usd6 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 7 THEN sell_out_fcst_usd7 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 8 THEN sell_out_fcst_usd8 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 9 THEN sell_out_fcst_usd9 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 10 THEN sell_out_fcst_usd10 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 11 THEN sell_out_fcst_usd11 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 12 THEN sell_out_fcst_usd12 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
              END AS shipped_revenue

            , CASE arr
                  WHEN 1 THEN sell_out_fcst1
                  WHEN 2 THEN sell_out_fcst2
                  WHEN 3 THEN sell_out_fcst3
                  WHEN 4 THEN sell_out_fcst4
                  WHEN 5 THEN sell_out_fcst5
                  WHEN 6 THEN sell_out_fcst6
                  WHEN 7 THEN sell_out_fcst7
                  WHEN 8 THEN sell_out_fcst8
                  WHEN 9 THEN sell_out_fcst9
                  WHEN 10 THEN sell_out_fcst10
                  WHEN 11 THEN sell_out_fcst11
                  WHEN 12 THEN sell_out_fcst12
              END AS sell_out_fcst_target

            , CASE arr
                  WHEN 1 THEN sell_out_fcst_usd1
                  WHEN 2 THEN sell_out_fcst_usd2
                  WHEN 3 THEN sell_out_fcst_usd3
                  WHEN 4 THEN sell_out_fcst_usd4
                  WHEN 5 THEN sell_out_fcst_usd5
                  WHEN 6 THEN sell_out_fcst_usd6
                  WHEN 7 THEN sell_out_fcst_usd7
                  WHEN 8 THEN sell_out_fcst_usd8
                  WHEN 9 THEN sell_out_fcst_usd9
                  WHEN 10 THEN sell_out_fcst_usd10
                  WHEN 11 THEN sell_out_fcst_usd11
                  WHEN 12 THEN sell_out_fcst_usd12
              END AS sell_out_fcst_target_usd

            , sell_out_6_sum as sell_out_sum
            , sell_out_last_year_6_sum as sell_out_last_year_sum
            , sell_out_usd_6_sum as sell_out_sum_usd
            , sell_out_usd_last_year_6_sum as sell_out_last_year_sum_usd

            , CASE arr
                  WHEN 1 THEN p70_week0+p70_week1+p70_week2+p70_week3
                  WHEN 2 THEN p70_week4+p70_week5+p70_week6+p70_week7
                  WHEN 3 THEN p70_week8+p70_week9+p70_week10+p70_week11
                  WHEN 4 THEN p70_week12+p70_week13+p70_week14+p70_week15
                  WHEN 5 THEN p70_week16+p70_week17+p70_week18+p70_week19
                  WHEN 6 THEN p70_week20+p70_week21+p70_week22+p70_week23
                  WHEN 7 THEN p70_week24+p70_week25+p70_week26+p70_week27
                  WHEN 8 THEN p70_week28+p70_week29+p70_week30+p70_week31
                  WHEN 9 THEN p70_week32+p70_week33+p70_week34+p70_week35
                  WHEN 10 THEN p70_week36+p70_week37+p70_week38+p70_week39
                  WHEN 11 THEN p70_week40+p70_week41+p70_week42+p70_week43
                  WHEN 12 THEN p70_week44+p70_week45+p70_week46+p70_week47
              END AS p70
            , CASE arr
                  WHEN 1 THEN p80_week0+p80_week1+p80_week2+p80_week3
                  WHEN 2 THEN p80_week4+p80_week5+p80_week6+p80_week7
                  WHEN 3 THEN p80_week8+p80_week9+p80_week10+p80_week11
                  WHEN 4 THEN p80_week12+p80_week13+p80_week14+p80_week15
                  WHEN 5 THEN p80_week16+p80_week17+p80_week18+p80_week19
                  WHEN 6 THEN p80_week20+p80_week21+p80_week22+p80_week23
                  WHEN 7 THEN p80_week24+p80_week25+p80_week26+p80_week27
                  WHEN 8 THEN p80_week28+p80_week29+p80_week30+p80_week31
                  WHEN 9 THEN p80_week32+p80_week33+p80_week34+p80_week35
                  WHEN 10 THEN p80_week36+p80_week37+p80_week38+p80_week39
                  WHEN 11 THEN p80_week40+p80_week41+p80_week42+p80_week43
                  WHEN 12 THEN p80_week44+p80_week45+p80_week46+p80_week47
              END AS p80
            , CASE arr
                  WHEN 1 THEN p90_week0+p90_week1+p90_week2+p90_week3
                  WHEN 2 THEN p90_week4+p90_week5+p90_week6+p90_week7
                  WHEN 3 THEN p90_week8+p90_week9+p90_week10+p90_week11
                  WHEN 4 THEN p90_week12+p90_week13+p90_week14+p90_week15
                  WHEN 5 THEN p90_week16+p90_week17+p90_week18+p90_week19
                  WHEN 6 THEN p90_week20+p90_week21+p90_week22+p90_week23
                  WHEN 7 THEN p90_week24+p90_week25+p90_week26+p90_week27
                  WHEN 8 THEN p90_week28+p90_week29+p90_week30+p90_week31
                  WHEN 9 THEN p90_week32+p90_week33+p90_week34+p90_week35
                  WHEN 10 THEN p90_week36+p90_week37+p90_week38+p90_week39
                  WHEN 11 THEN p90_week40+p90_week41+p90_week42+p90_week43
                  WHEN 12 THEN p90_week44+p90_week45+p90_week46+p90_week47
              END AS p90
            , CASE arr
                  WHEN 1 THEN mean_week0+mean_week1+mean_week2+mean_week3
                  WHEN 2 THEN mean_week4+mean_week5+mean_week6+mean_week7
                  WHEN 3 THEN mean_week8+mean_week9+mean_week10+mean_week11
                  WHEN 4 THEN mean_week12+mean_week13+mean_week14+mean_week15
                  WHEN 5 THEN mean_week16+mean_week17+mean_week18+mean_week19
                  WHEN 6 THEN mean_week20+mean_week21+mean_week22+mean_week23
                  WHEN 7 THEN mean_week24+mean_week25+mean_week26+mean_week27
                  WHEN 8 THEN mean_week28+mean_week29+mean_week30+mean_week31
                  WHEN 9 THEN mean_week32+mean_week33+mean_week34+mean_week35
                  WHEN 10 THEN mean_week36+mean_week37+mean_week38+mean_week39
                  WHEN 11 THEN mean_week40+mean_week41+mean_week42+mean_week43
                  WHEN 12 THEN mean_week44+mean_week45+mean_week46+mean_week47
              END AS mean

        FROM
            cte_final_month_row_value
                CROSS JOIN UNNEST(GENERATE_ARRAY(1, 12, 1)) AS arr

        UNION ALL

        SELECT
            country
            , division
            , category
            -- , yr_month_or_week
            , FORMAT_DATE('%G%V', DATE_ADD(PARSE_DATE('%G-%V', concat(SUBSTRING(yr_month_or_week, 1, 4), '-', SUBSTRING(yr_month_or_week, 5, 2))), INTERVAL arr WEEK)) AS yr_month_or_week
            , period_type

            -- , open_purchase_order_quantity
            , IF(arr <= 13, open_purchase_order_quantity/13, 0) AS net_received_units
            , IF(arr <= 13, open_purchase_order_amount/13, 0) AS net_received

            , sellable_on_hand_units
            , sellable_on_hand_inventory

            , CASE arr
                  WHEN 1  THEN sell_out_fcst1 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 2  THEN sell_out_fcst2 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 3  THEN sell_out_fcst3 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 4  THEN sell_out_fcst4 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 5  THEN sell_out_fcst5 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 6  THEN sell_out_fcst6 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 7  THEN sell_out_fcst7 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 8  THEN sell_out_fcst8 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 9  THEN sell_out_fcst9 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 10 THEN sell_out_fcst10 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 11 THEN sell_out_fcst11 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 12 THEN sell_out_fcst12 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 13 THEN sell_out_fcst13 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 14 THEN sell_out_fcst14 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 15 THEN sell_out_fcst15 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 16 THEN sell_out_fcst16 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 17 THEN sell_out_fcst17 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 18 THEN sell_out_fcst18 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 19 THEN sell_out_fcst19 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 20 THEN sell_out_fcst20 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 21 THEN sell_out_fcst21 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 22 THEN sell_out_fcst22 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 23 THEN sell_out_fcst23  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 24 THEN sell_out_fcst24  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 25 THEN sell_out_fcst25  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 26 THEN sell_out_fcst26  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
              END AS shipped_units
            , CASE arr
                  WHEN 1  THEN sell_out_fcst_usd1 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 2  THEN sell_out_fcst_usd2 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 3  THEN sell_out_fcst_usd3 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 4  THEN sell_out_fcst_usd4 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 5  THEN sell_out_fcst_usd5 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 6  THEN sell_out_fcst_usd6 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 7  THEN sell_out_fcst_usd7 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 8  THEN sell_out_fcst_usd8 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 9  THEN sell_out_fcst_usd9 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 10 THEN sell_out_fcst_usd10 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 11 THEN sell_out_fcst_usd11 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 12 THEN sell_out_fcst_usd12 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 13 THEN sell_out_fcst_usd13 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 14 THEN sell_out_fcst_usd14 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 15 THEN sell_out_fcst_usd15 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 16 THEN sell_out_fcst_usd16 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 17 THEN sell_out_fcst_usd17 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 18 THEN sell_out_fcst_usd18 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 19 THEN sell_out_fcst_usd19 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 20 THEN sell_out_fcst_usd20 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 21 THEN sell_out_fcst_usd21 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 22 THEN sell_out_fcst_usd22 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 23 THEN sell_out_fcst_usd23  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 24 THEN sell_out_fcst_usd24  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 25 THEN sell_out_fcst_usd25  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 26 THEN sell_out_fcst_usd26  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
              END AS shipped_revenue

            , CASE arr
                  WHEN 1 THEN sell_out_fcst1
                  WHEN 2 THEN sell_out_fcst2
                  WHEN 3 THEN sell_out_fcst3
                  WHEN 4 THEN sell_out_fcst4
                  WHEN 5 THEN sell_out_fcst5
                  WHEN 6 THEN sell_out_fcst6
                  WHEN 7 THEN sell_out_fcst7
                  WHEN 8 THEN sell_out_fcst8
                  WHEN 9 THEN sell_out_fcst9
                  WHEN 10 THEN sell_out_fcst10
                  WHEN 11 THEN sell_out_fcst11
                  WHEN 12 THEN sell_out_fcst12
                  WHEN 13 THEN sell_out_fcst13
                  WHEN 14 THEN sell_out_fcst14
                  WHEN 15 THEN sell_out_fcst15
                  WHEN 16 THEN sell_out_fcst16
                  WHEN 17 THEN sell_out_fcst17
                  WHEN 18 THEN sell_out_fcst18
                  WHEN 19 THEN sell_out_fcst19
                  WHEN 20 THEN sell_out_fcst20
                  WHEN 21 THEN sell_out_fcst21
                  WHEN 22 THEN sell_out_fcst22
                  WHEN 23 THEN sell_out_fcst23
                  WHEN 24 THEN sell_out_fcst24
                  WHEN 25 THEN sell_out_fcst25
                  WHEN 26 THEN sell_out_fcst26
              END AS sell_out_fcst_target

            , CASE arr
                  WHEN 1 THEN sell_out_fcst_usd1
                  WHEN 2 THEN sell_out_fcst_usd2
                  WHEN 3 THEN sell_out_fcst_usd3
                  WHEN 4 THEN sell_out_fcst_usd4
                  WHEN 5 THEN sell_out_fcst_usd5
                  WHEN 6 THEN sell_out_fcst_usd6
                  WHEN 7 THEN sell_out_fcst_usd7
                  WHEN 8 THEN sell_out_fcst_usd8
                  WHEN 9 THEN sell_out_fcst_usd9
                  WHEN 10 THEN sell_out_fcst_usd10
                  WHEN 11 THEN sell_out_fcst_usd11
                  WHEN 12 THEN sell_out_fcst_usd12
                  WHEN 13 THEN sell_out_fcst_usd13
                  WHEN 14 THEN sell_out_fcst_usd14
                  WHEN 15 THEN sell_out_fcst_usd15
                  WHEN 16 THEN sell_out_fcst_usd16
                  WHEN 17 THEN sell_out_fcst_usd17
                  WHEN 18 THEN sell_out_fcst_usd18
                  WHEN 19 THEN sell_out_fcst_usd19
                  WHEN 20 THEN sell_out_fcst_usd20
                  WHEN 21 THEN sell_out_fcst_usd21
                  WHEN 22 THEN sell_out_fcst_usd22
                  WHEN 23 THEN sell_out_fcst_usd23
                  WHEN 24 THEN sell_out_fcst_usd24
                  WHEN 25 THEN sell_out_fcst_usd25
                  WHEN 26 THEN sell_out_fcst_usd26
              END AS sell_out_fcst_target_usd

            ,sell_out_13_sum
            , sell_out_last_year_13_sum
            , sell_out_usd_13_sum
            , sell_out_usd_last_year_13_sum

            , CASE arr
                  WHEN 1  THEN p70_week0
                  WHEN 2  THEN p70_week1
                  WHEN 3  THEN p70_week2
                  WHEN 4  THEN p70_week3
                  WHEN 5  THEN p70_week4
                  WHEN 6  THEN p70_week5
                  WHEN 7  THEN p70_week6
                  WHEN 8  THEN p70_week7
                  WHEN 9  THEN p70_week8
                  WHEN 10 THEN p70_week9
                  WHEN 11 THEN p70_week10
                  WHEN 12 THEN p70_week11
                  WHEN 13 THEN p70_week12
                  WHEN 14 THEN p70_week13
                  WHEN 15 THEN p70_week14
                  WHEN 16 THEN p70_week15
                  WHEN 17 THEN p70_week16
                  WHEN 18 THEN p70_week17
                  WHEN 19 THEN p70_week18
                  WHEN 20 THEN p70_week19
                  WHEN 21 THEN p70_week20
                  WHEN 22 THEN p70_week21
                  WHEN 23 THEN p70_week22
                  WHEN 24 THEN p70_week23
                  WHEN 25 THEN p70_week24
                  WHEN 26 THEN p70_week25
              END AS p70
            , CASE arr
                  WHEN 1  THEN p80_week0
                  WHEN 2  THEN p80_week1
                  WHEN 3  THEN p80_week2
                  WHEN 4  THEN p80_week3
                  WHEN 5  THEN p80_week4
                  WHEN 6  THEN p80_week5
                  WHEN 7  THEN p80_week6
                  WHEN 8  THEN p80_week7
                  WHEN 9  THEN p80_week8
                  WHEN 10 THEN p80_week9
                  WHEN 11 THEN p80_week10
                  WHEN 12 THEN p80_week11
                  WHEN 13 THEN p80_week12
                  WHEN 14 THEN p80_week13
                  WHEN 15 THEN p80_week14
                  WHEN 16 THEN p80_week15
                  WHEN 17 THEN p80_week16
                  WHEN 18 THEN p80_week17
                  WHEN 19 THEN p80_week18
                  WHEN 20 THEN p80_week19
                  WHEN 21 THEN p80_week20
                  WHEN 22 THEN p80_week21
                  WHEN 23 THEN p80_week22
                  WHEN 24 THEN p80_week23
                  WHEN 25 THEN p80_week24
                  WHEN 26 THEN p80_week25
              END AS p80
            , CASE arr
                  WHEN 1  THEN p90_week0
                  WHEN 2  THEN p90_week1
                  WHEN 3  THEN p90_week2
                  WHEN 4  THEN p90_week3
                  WHEN 5  THEN p90_week4
                  WHEN 6  THEN p90_week5
                  WHEN 7  THEN p90_week6
                  WHEN 8  THEN p90_week7
                  WHEN 9  THEN p90_week8
                  WHEN 10 THEN p90_week9
                  WHEN 11 THEN p90_week10
                  WHEN 12 THEN p90_week11
                  WHEN 13 THEN p90_week12
                  WHEN 14 THEN p90_week13
                  WHEN 15 THEN p90_week14
                  WHEN 16 THEN p90_week15
                  WHEN 17 THEN p90_week16
                  WHEN 18 THEN p90_week17
                  WHEN 19 THEN p90_week18
                  WHEN 20 THEN p90_week19
                  WHEN 21 THEN p90_week20
                  WHEN 22 THEN p90_week21
                  WHEN 23 THEN p90_week22
                  WHEN 24 THEN p90_week23
                  WHEN 25 THEN p90_week24
                  WHEN 26 THEN p90_week25
              END AS p90
            , CASE arr
                  WHEN 1  THEN mean_week0
                  WHEN 2  THEN mean_week1
                  WHEN 3  THEN mean_week2
                  WHEN 4  THEN mean_week3
                  WHEN 5  THEN mean_week4
                  WHEN 6  THEN mean_week5
                  WHEN 7  THEN mean_week6
                  WHEN 8  THEN mean_week7
                  WHEN 9  THEN mean_week8
                  WHEN 10 THEN mean_week9
                  WHEN 11 THEN mean_week10
                  WHEN 12 THEN mean_week11
                  WHEN 13 THEN mean_week12
                  WHEN 14 THEN mean_week13
                  WHEN 15 THEN mean_week14
                  WHEN 16 THEN mean_week15
                  WHEN 17 THEN mean_week16
                  WHEN 18 THEN mean_week17
                  WHEN 19 THEN mean_week18
                  WHEN 20 THEN mean_week19
                  WHEN 21 THEN mean_week20
                  WHEN 22 THEN mean_week21
                  WHEN 23 THEN mean_week22
                  WHEN 24 THEN mean_week23
                  WHEN 25 THEN mean_week24
                  WHEN 26 THEN mean_week25
              END AS mean

        FROM
            cte_final_week_row_value
                CROSS JOIN UNNEST(GENERATE_ARRAY(1, 26, 1)) AS arr
    )
-- SELECT
--     *
-- FROM
--     cte_fcst_src
-- ;
    , cte_fcst_act_union as (
        SELECT
            country
            , division
            , category

            , CAST(yr_month_or_week AS STRING) AS time
--             , CAST(yr_month_or_week AS STRING) AS end_time

            , period_type AS time_level
            , CAST(MAX(yr_month_or_week) OVER (PARTITION BY COALESCE(country, 'UNKNOWN'), period_type) AS STRING) AS max_time

            , net_received_units
            , net_received

--             , sellable_on_hand_units - shipped_units + net_received_units AS sellable_on_hand_units
--             , sellable_on_hand_inventory - shipped_revenue + net_received AS sellable_on_hand_inventory

            -- 260108 / cube (GROUPING SETS) 단에서 계산
            , sellable_on_hand_units
            , sellable_on_hand_inventory

            -- 260108 / ratio 의 결과의 합 != 합 * ratio 문제 / cube (GROUPING SETS) 단에서 재계산
            , shipped_units
            , shipped_revenue

            , sell_out_fcst_target
            , sell_out_fcst_target_usd

            , sell_out_sum
            , sell_out_last_year_sum

            , sell_out_sum_usd
            , sell_out_last_year_sum_usd

            , p70
            , p80
            , p90
            , mean

            , 'fcst' AS row_type
        FROM
            cte_fcst_src

        UNION ALL

        SELECT
            country
            , division
            , category

            , CAST(yr_month_or_week AS STRING) AS time

            , period_type AS time_level
            , CAST(MAX(yr_month_or_week) OVER (PARTITION BY COALESCE(country, 'UNKNOWN'), period_type) AS STRING) AS max_time

            , net_received_units
            , net_received

            , sellable_on_hand_units
            , sellable_on_hand_inventory

            , shipped_units
            , shipped_revenue

            , null
            , null

            , null
            , null

            , null
            , null

            , null as p70
            , null as p80
            , null as p90
            , null as mean

            , 'act' AS row_type
        FROM
            cte_actual_src
    )
    , cte_sum_for_wos as (
        SELECT
            *
            , 'MONTH' as time_base_level
            , SUM(shipped_units) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout
            , SUM(shipped_revenue) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_usd
            , SUM(received_po) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po
            , SUM(received_po_usd) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po_usd

            , SUM(shipped_units_with_p70) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p70
            , SUM(shipped_units_with_p80) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p80
            , SUM(shipped_units_with_p90) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p90
            , SUM(shipped_units_with_mean) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_mean

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                  THEN
                      SUM(shipped_units) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                  THEN
                      SUM(shipped_revenue) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_sum_for_wos_usd

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p70) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p70_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p80) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p80_sum_for_wos

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p90) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p90_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_mean) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_mean_sum_for_wos

        FROM
            (
                SELECT
                    country
                    , IF(GROUPING(division) = 1, 'T', division) AS division_att
                    , IF(GROUPING(category) = 1, 'T', category) AS category_att

                    , time
                    , FORMAT_DATE('%Y%m', DATE_ADD(PARSE_DATE('%Y%m', time), INTERVAL 1 YEAR)) AS prev_ytd_time

--                     , SUM(shipped_units) AS shipped_units
--                     , SUM(shipped_revenue) AS shipped_revenue
                    , IF(row_type = 'fcst', SUM(sell_out_fcst_target) * (if(SUM(sell_out_last_year_sum) is null or SUM(sell_out_last_year_sum) = 0, 1, SUM(sell_out_sum) / SUM(sell_out_last_year_sum))), SUM(shipped_units)) AS shipped_units
                    , IF(row_type = 'fcst', SUM(sell_out_fcst_target_usd) * (if(SUM(sell_out_last_year_sum_usd) is null or SUM(sell_out_last_year_sum_usd) = 0, 1, SUM(sell_out_sum_usd) / SUM(sell_out_last_year_sum_usd))), SUM(shipped_revenue)) AS shipped_revenue

                    , IF(row_type = 'fcst', SUM(p70), SUM(shipped_units)) AS shipped_units_with_p70
                    , IF(row_type = 'fcst', SUM(p80), SUM(shipped_units)) AS shipped_units_with_p80
                    , IF(row_type = 'fcst', SUM(p90), SUM(shipped_units)) AS shipped_units_with_p90
                    , IF(row_type = 'fcst', SUM(mean), SUM(shipped_units)) AS shipped_units_with_mean

                    , SUM(net_received_units) AS received_po
                    , SUM(net_received) AS received_po_usd
                FROM
                    cte_fcst_act_union
                WHERE
                    time_level = 'MONTH'
                GROUP BY
                    GROUPING SETS (
                        ( country, time, time_level, row_type, division, category )
                        , ( country, time, time_level, row_type, division )
                        , ( country, time, time_level, row_type, category )
                        , ( country, time, time_level, row_type )
                    )
            )

        UNION ALL

        SELECT
            *
            , 'WEEK' as time_base_level
            , SUM(shipped_units) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout
            , SUM(shipped_revenue) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_usd
            , SUM(received_po) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po
            , SUM(received_po_usd) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po_usd

            , SUM(shipped_units_with_p70) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p70
            , SUM(shipped_units_with_p80) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p80
            , SUM(shipped_units_with_p90) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p90
            , SUM(shipped_units_with_mean) OVER (PARTITION BY country, division_att, category_att, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_mean

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING) = 13
                  THEN
                      SUM(shipped_units) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING)
              END AS sell_out_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING) = 13
                  THEN
                      SUM(shipped_revenue) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING)
              END AS sell_out_sum_for_wos_usd

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p70) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p70_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p80) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p80_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p90) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p90_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_mean) OVER (PARTITION BY country, division_att, category_att ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_mean_sum_for_wos
        FROM
            (
                SELECT
                    country
                    , IF(GROUPING(division) = 1, 'T', division) AS division_att
                    , IF(GROUPING(category) = 1, 'T', category) AS category_att

                    , time
                    , CAST(CAST(SUBSTRING(time,1,4) AS INT64) +1 AS STRING) || SUBSTRING(time, 5) AS prev_ytd_time

--                     , SUM(shipped_units) AS shipped_units
--                     , SUM(shipped_revenue) AS shipped_revenue
                    , IF(row_type = 'fcst', SUM(sell_out_fcst_target) * (if(SUM(sell_out_last_year_sum) is null or SUM(sell_out_last_year_sum) = 0, 1, SUM(sell_out_sum) / SUM(sell_out_last_year_sum))), SUM(shipped_units)) AS shipped_units
                    , IF(row_type = 'fcst', SUM(sell_out_fcst_target_usd) * (if(SUM(sell_out_last_year_sum_usd) is null or SUM(sell_out_last_year_sum_usd) = 0, 1, SUM(sell_out_sum_usd) / SUM(sell_out_last_year_sum_usd))), SUM(shipped_revenue)) AS shipped_revenue

                    , IF(row_type = 'fcst', SUM(p70), SUM(shipped_units)) AS shipped_units_with_p70
                    , IF(row_type = 'fcst', SUM(p80), SUM(shipped_units)) AS shipped_units_with_p80
                    , IF(row_type = 'fcst', SUM(p90), SUM(shipped_units)) AS shipped_units_with_p90
                    , IF(row_type = 'fcst', SUM(mean), SUM(shipped_units)) AS shipped_units_with_mean

                    , SUM(net_received_units) AS received_po
                    , SUM(net_received) AS received_po_usd
                FROM
                    cte_fcst_act_union
                WHERE
                    time_level = 'WEEK'
                GROUP BY
                    GROUPING SETS (
                        ( country, time, time_level, row_type, division, category )
                        , ( country, time, time_level, row_type, division )
                        , ( country, time, time_level, row_type, category )
                        , ( country, time, time_level, row_type )
                    )
            )
    )
    , cte_cube as (
        SELECT
            country
            , IF(GROUPING(division) = 1, 'T', division) AS division_att
            , IF(GROUPING(category) = 1, 'T', category) AS category_att

            , time_level
            , time

            , IF(row_type = 'fcst'
                    , SUM(sell_out_fcst_target) * (if(SUM(sell_out_last_year_sum) is null or SUM(sell_out_last_year_sum) = 0, 1, SUM(sell_out_sum) / SUM(sell_out_last_year_sum)))
                    , SUM(shipped_units)
                ) AS sell_out
            , IF(row_type = 'fcst'
                    , SUM(sell_out_fcst_target_usd) * (if(SUM(sell_out_last_year_sum_usd) is null or SUM(sell_out_last_year_sum_usd) = 0, 1, SUM(sell_out_sum_usd) / SUM(sell_out_last_year_sum_usd)))
                    , SUM(shipped_revenue)
                ) AS sell_out_usd
--             , SUM(shipped_units) AS sell_out
--             , SUM(shipped_revenue) AS sell_out_usd

            , IF(row_type = 'fcst'
                    , SUM(p70)
                    , SUM(shipped_units)
                ) AS sell_out_with_p70
            , IF(row_type = 'fcst'
                , SUM(p80)
                , SUM(shipped_units)
              ) AS sell_out_with_p80
            , IF(row_type = 'fcst'
                , SUM(p90)
                , SUM(shipped_units)
              ) AS sell_out_with_p90
            , IF(row_type = 'fcst'
                , SUM(mean)
                , SUM(shipped_units)
              ) AS sell_out_with_mean

            , SUM(net_received_units) AS received_po
            , SUM(sellable_on_hand_units) AS ending_inv

            , SUM(net_received) AS received_po_usd
            , SUM(sellable_on_hand_inventory) AS ending_inv_usd

            , SUM(sell_out_fcst_target) as sell_out_fcst_target
            , SUM(sell_out_fcst_target_usd) as sell_out_fcst_target_usd

            , SUM(sell_out_sum) as sell_out_sum
            , SUM(sell_out_last_year_sum) as sell_out_last_year_sum
            , SUM(sell_out_sum_usd) as sell_out_sum_usd
            , SUM(sell_out_last_year_sum_usd) as sell_out_last_year_sum_usd

            , row_type
        FROM
            cte_fcst_act_union
        GROUP BY GROUPING SETS (
                (country, time, time_level, row_type, division, category),
                (country, time, time_level, row_type, division),
                (country, time, time_level, row_type, category),
                (country, time, time_level, row_type)
        )
    )
    , cte_semi_final as (
        SELECT
            f.* -- EXCEPT (ending_inv, ending_inv_usd)
--             , IF(f.row_type = 'act', 0, ROW_NUMBER() OVER (PARTITION BY f.country, f.division_att, f.category_att, f.origin_att, f.time_level, f.row_type ORDER BY f.time)) AS row_num
            , SUBSTRING(f.time, 1, 4) AS year

            , w.ytd_sellout
            , w.ytd_sellout_usd
            , w.ytd_received_po
            , w.ytd_received_po_usd
            , w.ytd_sellout_with_p70
            , w.ytd_sellout_with_p80
            , w.ytd_sellout_with_p90
            , w.ytd_sellout_with_mean
--             , f.ending_inv
--             , f.ending_inv_usd
            --     , if(f.row_type = 'fcst', f.ending_inv - f.sell_out + f.received_po , f.ending_inv) as ending_inv
            --     , if(f.row_type = 'fcst', f.ending_inv - f.sell_out_usd + f.received_po_usd , f.ending_inv_usd) as ending_inv_usd

            , w2.ytd_sellout AS prev_ytd_sellout
            , w2.ytd_sellout_usd AS prev_ytd_sellout_usd
            , w2.ytd_received_po AS prev_ytd_received_po
            , w2.ytd_received_po_usd AS prev_ytd_received_po_usd
            , w2.ytd_sellout_with_p70 AS prev_ytd_sellout_with_p70
            , w2.ytd_sellout_with_p80 AS prev_ytd_sellout_with_p80
            , w2.ytd_sellout_with_p90 AS prev_ytd_sellout_with_p90
            , w2.ytd_sellout_with_mean AS prev_ytd_sellout_with_mean

            , w.sell_out_sum_for_wos
            , w.sell_out_sum_for_wos_usd
            , w.sell_out_with_p70_sum_for_wos
            , w.sell_out_with_p80_sum_for_wos
            , w.sell_out_with_p90_sum_for_wos
            , w.sell_out_with_mean_sum_for_wos
        FROM
            cte_cube f
                LEFT JOIN cte_sum_for_wos w
                    ON f.country = w.country
                           AND f.division_att = w.division_att
                           AND f.category_att = w.category_att
                           AND f.time = w.time
                           AND IF(f.time_level = 'WEEK', 'WEEK', 'MONTH') = w.time_base_level
                LEFT JOIN cte_sum_for_wos w2
                    ON f.country = w2.country
                           AND f.division_att = w2.division_att
                           AND f.category_att = w2.category_att
                           AND f.time = w2.prev_ytd_time
                           AND IF(f.time_level = 'WEEK', 'WEEK', 'MONTH') = w2.time_base_level
    )
    , cte_inventory_logic AS (
        SELECT
            *

            , LAST_VALUE(IF(row_type = 'act', ending_inv, NULL) IGNORE NULLS) OVER( PARTITION BY country, division_att, category_att, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS start_inv_qty
            , SUM(IF(row_type = 'fcst', sell_out, 0)) OVER( PARTITION BY country, division_att, category_att, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out
            , SUM(IF(row_type = 'fcst', received_po, 0)) OVER( PARTITION BY country, division_att, category_att, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_po

            , SUM(IF(row_type = 'fcst', sell_out_with_p70, 0)) OVER( PARTITION BY country, division_att, category_att, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_p70
            , SUM(IF(row_type = 'fcst', sell_out_with_p80, 0)) OVER( PARTITION BY country, division_att, category_att, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_p80
            , SUM(IF(row_type = 'fcst', sell_out_with_p90, 0)) OVER( PARTITION BY country, division_att, category_att, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_p90
            , SUM(IF(row_type = 'fcst', sell_out_with_mean, 0)) OVER( PARTITION BY country, division_att, category_att, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_mean

            , LAST_VALUE(IF(row_type = 'act', ending_inv_usd, NULL) IGNORE NULLS) OVER(PARTITION BY country, division_att, category_att, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS start_inv_amt
            , SUM(IF(row_type = 'fcst', sell_out_usd, 0)) OVER( PARTITION BY country, division_att, category_att, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_usd
            , SUM(IF(row_type = 'fcst', received_po_usd, 0)) OVER( PARTITION BY country, division_att, category_att, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_po_usd
        FROM
            cte_semi_final
    )
    , cte_month_n_week_final as (
        SELECT
            * EXCEPT (start_inv_qty, cumulative_sell_out, start_inv_amt, cumulative_sell_out_usd, ending_inv, ending_inv_usd, cumulative_po, cumulative_po_usd, cumulative_sell_out_with_p70, cumulative_sell_out_with_p80, cumulative_sell_out_with_p90, cumulative_sell_out_with_mean)

            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out + cumulative_po, ending_inv) AS ending_inv
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_p70 + cumulative_po, ending_inv) AS ending_inv_with_p70
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_p80 + cumulative_po, ending_inv) AS ending_inv_with_p80
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_p90 + cumulative_po, ending_inv) AS ending_inv_with_p90
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_mean + cumulative_po, ending_inv) AS ending_inv_with_mean
            , IF(row_type = 'fcst', start_inv_amt - cumulative_sell_out_usd + cumulative_po_usd, ending_inv_usd) AS ending_inv_usd
        FROM
            cte_inventory_logic
    )
    , cte_year_n_quarter_final as (
        SELECT
            country
            , division_att
            , category_att
            , 'QUARTER' AS time_level
            , CONCAT(SUBSTRING(time, 1, 4), 'Q', CAST(CEIL(CAST(SUBSTRING(time, 5, 2) AS INT64) / 3) AS STRING)) AS time
            , SUM(sell_out) AS sell_out
            , SUM(sell_out_usd) AS sell_out_usd
            , SUM(sell_out_with_p70) AS sell_out_with_p70
            , SUM(sell_out_with_p80) AS sell_out_with_p80
            , SUM(sell_out_with_p90) AS sell_out_with_p90
            , SUM(sell_out_with_mean) AS sell_out_with_mean

            , SUM(received_po) AS received_po
            , SUM(received_po_usd) AS received_po_usd
            --                 , ARRAY_AGG(received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS received_po
            --                 , ARRAY_AGG(received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS received_po_usd


            , SUM(sell_out_fcst_target) as sell_out_fcst_target
            , SUM(sell_out_fcst_target_usd) as sell_out_fcst_target_usd

            , SUM(sell_out_sum) as sell_out_sum
            , SUM(sell_out_last_year_sum) as sell_out_last_year_sum
            , SUM(sell_out_sum_usd) as sell_out_sum_usd
            , SUM(sell_out_last_year_sum_usd) as sell_out_last_year_sum_usd

            , ARRAY_AGG(row_type ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS row_type

            --     , 0 as row_num

            , SUBSTRING(MAX(time), 1, 4) AS year
            , ARRAY_AGG(ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout
            , ARRAY_AGG(ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_usd
            , ARRAY_AGG(ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po
            , ARRAY_AGG(ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po_usd
            , ARRAY_AGG(ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p70
            , ARRAY_AGG(ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p80
            , ARRAY_AGG(ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p90
            , ARRAY_AGG(ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_mean

            , ARRAY_AGG(prev_ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout
            , ARRAY_AGG(prev_ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_usd
            , ARRAY_AGG(prev_ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po
            , ARRAY_AGG(prev_ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po_usd
            , ARRAY_AGG(prev_ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p70
            , ARRAY_AGG(prev_ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p80
            , ARRAY_AGG(prev_ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p90
            , ARRAY_AGG(prev_ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_mean

            , ARRAY_AGG(sell_out_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos
            , ARRAY_AGG(sell_out_sum_for_wos_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos_usd
            , ARRAY_AGG(sell_out_with_p70_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p70_sum_for_wos
            , ARRAY_AGG(sell_out_with_p80_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p80_sum_for_wos
            , ARRAY_AGG(sell_out_with_p90_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p90_sum_for_wos
            , ARRAY_AGG(sell_out_with_mean_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_mean_sum_for_wos

            , ARRAY_AGG(ending_inv ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv
            , ARRAY_AGG(ending_inv_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p70
            , ARRAY_AGG(ending_inv_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p80
            , ARRAY_AGG(ending_inv_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p90
            , ARRAY_AGG(ending_inv_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_mean
            , ARRAY_AGG(ending_inv_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_usd

        FROM
            cte_month_n_week_final
        WHERE
            time_level = 'MONTH'
        GROUP BY 1, 2, 3, 4, 5

        UNION ALL

        SELECT
            country
            , division_att
            , category_att
            , 'YEAR' AS time_level
            , SUBSTRING(time, 1, 4) AS time
            , SUM(sell_out) AS sell_out
            , SUM(sell_out_usd) AS sell_out_usd
            , SUM(sell_out_with_p70) AS sell_out_with_p70
            , SUM(sell_out_with_p80) AS sell_out_with_p80
            , SUM(sell_out_with_p90) AS sell_out_with_p90
            , SUM(sell_out_with_mean) AS sell_out_with_mean

            , SUM(received_po) AS received_po
            , SUM(received_po_usd) AS received_po_usd


            , SUM(sell_out_fcst_target) as sell_out_fcst_target
            , SUM(sell_out_fcst_target_usd) as sell_out_fcst_target_usd

            , SUM(sell_out_sum) as sell_out_sum
            , SUM(sell_out_last_year_sum) as sell_out_last_year_sum
            , SUM(sell_out_sum_usd) as sell_out_sum_usd
            , SUM(sell_out_last_year_sum_usd) as sell_out_last_year_sum_usd


            , ARRAY_AGG(row_type ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS row_type

            --     , 0 as row_num

            , SUBSTRING(MAX(time), 1, 4) AS year
            , ARRAY_AGG(ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout
            , ARRAY_AGG(ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_usd
            , ARRAY_AGG(ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po
            , ARRAY_AGG(ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po_usd
            , ARRAY_AGG(ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p70
            , ARRAY_AGG(ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p80
            , ARRAY_AGG(ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p90
            , ARRAY_AGG(ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_mean

            , ARRAY_AGG(prev_ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout
            , ARRAY_AGG(prev_ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_usd
            , ARRAY_AGG(prev_ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po
            , ARRAY_AGG(prev_ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po_usd
            , ARRAY_AGG(prev_ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p70
            , ARRAY_AGG(prev_ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p80
            , ARRAY_AGG(prev_ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p90
            , ARRAY_AGG(prev_ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_mean

            , ARRAY_AGG(sell_out_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos
            , ARRAY_AGG(sell_out_sum_for_wos_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos_usd
            , ARRAY_AGG(sell_out_with_p70_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p70_sum_for_wos
            , ARRAY_AGG(sell_out_with_p80_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p80_sum_for_wos
            , ARRAY_AGG(sell_out_with_p90_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p90_sum_for_wos
            , ARRAY_AGG(sell_out_with_mean_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_mean_sum_for_wos

            , ARRAY_AGG(ending_inv ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv
            , ARRAY_AGG(ending_inv_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p70
            , ARRAY_AGG(ending_inv_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p80
            , ARRAY_AGG(ending_inv_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p90
            , ARRAY_AGG(ending_inv_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_mean
            , ARRAY_AGG(ending_inv_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_usd

        FROM
            cte_month_n_week_final
        WHERE
            time_level = 'MONTH'
        GROUP BY 1, 2, 3, 4, 5
    )
SELECT
    * -- EXCEPT (end_time, max_time)
    , IF(row_type = 'fcst', ROW_NUMBER() OVER (PARTITION BY country, division_att, category_att, time_level, row_type ORDER BY time), 0) AS row_num
FROM
    cte_month_n_week_final

UNION ALL

SELECT
    *
    , IF(row_type = 'fcst', ROW_NUMBER() OVER (PARTITION BY country, division_att, category_att, time_level, row_type ORDER BY time), 0) AS row_num
FROM
    cte_year_n_quarter_final;

CREATE OR REPLACE TABLE mart.amz_di_global_psi_report AS
    with cte_final as (
        SELECT
            *
            ,   CASE time_level
                    WHEN 'YEAR' THEN
                        CAST(CAST(time AS INT64) - 1 AS STRING)

                    WHEN 'QUARTER' THEN
                        CONCAT(
                                CAST(IF(SAFE_CAST(REGEXP_EXTRACT(time, r'Q([1-4])') AS INT64) = 1, SAFE_CAST(SUBSTR(time, 1, 4) AS INT64) - 1, SAFE_CAST(SUBSTR(time, 1, 4) AS INT64)) AS STRING),
                                'Q',
                                CAST(IF(SAFE_CAST(REGEXP_EXTRACT(time, r'Q([1-4])') AS INT64) = 1, 4, SAFE_CAST(REGEXP_EXTRACT(time, r'Q([1-4])') AS INT64) - 1) AS STRING)
                        )

                    WHEN 'MONTH' THEN
                        FORMAT_DATE('%Y%m', DATE_SUB(PARSE_DATE('%Y%m', time), INTERVAL 1 MONTH))

                    WHEN 'WEEK' THEN
                        FORMAT_DATE('%G%V', DATE_SUB(PARSE_DATE('%G-%V', SUBSTR(time, 1,4)||'-'||SUBSTR(time, 5,2)), INTERVAL 7 DAY))

                    ELSE NULL
                END AS last_time
        FROM
            tmp1.amz_di_global_psi_report
        )
SELECT
    a.*
    , b.sell_out as last_sell_out
    , b.sell_out_usd as last_sell_out_usd
    , b.received_po as last_received_po
    , b.received_po_usd as last_received_po_usd

    , b.ending_inv as last_ending_inv
    , b.ending_inv_usd as last_ending_inv_usd

    , b.ending_inv_with_p70 as last_ending_inv_with_p70
    , b.ending_inv_with_p80 as last_ending_inv_with_p80
    , b.ending_inv_with_p90 as last_ending_inv_with_p90
    , b.ending_inv_with_mean as last_ending_inv_with_mean

    , b.sell_out_sum_for_wos as last_sell_out_sum_for_wos
    , b.sell_out_with_p70_sum_for_wos as last_sell_out_with_p70_sum_for_wos
    , b.sell_out_with_p80_sum_for_wos as last_sell_out_with_p80_sum_for_wos
    , b.sell_out_with_p90_sum_for_wos as last_sell_out_with_p90_sum_for_wos
    , b.sell_out_with_mean_sum_for_wos as last_sell_out_with_mean_sum_for_wos

FROM
    cte_final a
        LEFT JOIN cte_final b
            ON a.time_level = b.time_level AND a.last_time = b.time
                AND a.country = b.country
                AND a.division_att = b.division_att
                AND a.category_att = b.category_att
;


-- [psi detail report] -------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp1.amz_di_global_psi_report_detail AS
WITH
    cte_fcst as (
        WITH cte_fcst_src as (
                SELECT
                    a.asin
                    , COALESCE(zinus_sku, 'UNKNOWN') AS zinus_sku
--                     , a.Company as country
--                     , a.financial_category
                    , COALESCE(Company, 'UNKNOWN') AS country
                    , IF(COALESCE(financial_category, 'UNKNOWN') IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS division
                    , COALESCE(financial_category, 'UNKNOWN') AS category
                    , a.type
                    , week0, week1, week2, week3, week4, week5, week6, week7, week8, week9, week10, week11, week12, week13, week14, week15, week16, week17, week18, week19, week20, week21, week22, week23, week24, week25, week26, week27, week28, week29, week30, week31, week32, week33, week34, week35, week36, week37, week38, week39, week40, week41, week42, week43, week44, week45, week46, week47
                    , b.yr_month
                    , b.yr_wk
                FROM
                    mart.amz_global_fcst_all a
                        LEFT JOIN meta.wk_calendar_new b
                            ON DATE_SUB(a.date, INTERVAL 1 WEEK) BETWEEN b.start_date AND b.end_date
--                 WHERE
--                     a.Type='p70'

--                     AND asin = 'B006MIPW70'
--                     AND Company = 'US'
            )
            , cte_union as (
                SELECT
                    * EXCEPT (yr_wk, yr_month)

                    , CAST(yr_month AS STRING) AS yr_month_or_week
                    , 'MONTH' AS period_type
                FROM
                    cte_fcst_src
                QUALIFY
                    ROW_NUMBER() OVER (PARTITION BY asin, country, type, yr_month ORDER BY yr_wk DESC) = 1

                UNION ALL

                SELECT
                    * EXCEPT (yr_wk, yr_month)

                    , CAST(yr_wk AS STRING) AS yr_month_or_week
                    , 'WEEK' AS period_type
                FROM
                    cte_fcst_src
            )
        SELECT
            zinus_sku, country, division, category, type
             , SUM(week0) AS week0, SUM(week1) AS week1, SUM(week2) AS week2, SUM(week3) AS week3, SUM(week4) AS week4, SUM(week5) AS week5, SUM(week6) AS week6, SUM(week7) AS week7, SUM(week8) AS week8, SUM(week9) AS week9, SUM(week10) AS week10, SUM(week11) AS week11, SUM(week12) AS week12, SUM(week13) AS week13, SUM(week14) AS week14, SUM(week15) AS week15, SUM(week16) AS week16, SUM(week17) AS week17, SUM(week18) AS week18, SUM(week19) AS week19, SUM(week20) AS week20, SUM(week21) AS week21, SUM(week22) AS week22, SUM(week23) AS week23, SUM(week24) AS week24, SUM(week25) AS week25, SUM(week26) AS week26, SUM(week27) AS week27, SUM(week28) AS week28, SUM(week29) AS week29, SUM(week30) AS week30, SUM(week31) AS week31, SUM(week32) AS week32, SUM(week33) AS week33, SUM(week34) AS week34, SUM(week35) AS week35, SUM(week36) AS week36, SUM(week37) AS week37, SUM(week38) AS week38, SUM(week39) AS week39, SUM(week40) AS week40, SUM(week41) AS week41, SUM(week42) AS week42, SUM(week43) AS week43, SUM(week44) AS week44, SUM(week45) AS week45, SUM(week46) AS week46, SUM(week47) AS week47
             , yr_month_or_week, period_type
        FROM cte_union
        GROUP BY zinus_sku, country, division, category, type, yr_month_or_week, period_type
    )
    , cte_origin_src AS (
        SELECT
            COALESCE(country, 'UNKNOWN') AS country
            , IF(COALESCE(financial_category, 'UNKNOWN') IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS division
            , COALESCE(financial_category, 'UNKNOWN') AS category
            , COALESCE(single, 'UNKNOWN') AS single_category
            , COALESCE(single_cat_desc, 'UNKNOWN') AS single_cat_desc
            , COALESCE(zinus_sku, 'UNKNOWN') AS zinus_sku
            , asin

            , CAST(yr_month_or_week AS STRING) AS yr_month_or_week

            , period_type

            , open_purchase_order_quantity
            , open_purchase_order_quantity * ( sellable_on_hand_inventory / IF(COALESCE(sellable_on_hand_units, 0) = 0, 1, sellable_on_hand_units) ) AS open_purchase_order_amount
            , net_received_units
            , net_received
            , sellable_on_hand_units
            , sellable_on_hand_inventory
            , shipped_units
            , shipped_revenue
        FROM
            mart.amz_di_global a
        WHERE
            is_closed=TRUE

--             and a.asin = 'B006MIPW70'
--             and a.country='US'
    )
    , cte_fill_date as (
        SELECT
            country
            , asin
            , 'WEEK' as period_type
            , fill_date
        FROM
            (
                SELECT
                    country
                    , asin
                    , PARSE_DATE('%G-%V', CONCAT(SUBSTRING(MIN(yr_month_or_week), 1, 4), '-', SUBSTRING(MIN(yr_month_or_week), 5, 2))) AS min_wk
                    , PARSE_DATE('%G-%V', CONCAT(SUBSTRING(MAX(yr_month_or_week), 1, 4), '-', SUBSTRING(MAX(yr_month_or_week), 5, 2))) AS max_wk
--                     , PARSE_DATE('%G-%V', CONCAT(SUBSTRING(CAST(MIN(yr_month_or_week) AS STRING), 1, 4), '-', SUBSTRING(CAST(MIN(yr_month_or_week) AS STRING), 5, 2))) AS min_wk
--                     , PARSE_DATE('%G-%V', CONCAT(SUBSTRING(CAST(MAX(yr_month_or_week) AS STRING), 1, 4), '-', SUBSTRING(CAST(MAX(yr_month_or_week) AS STRING), 5, 2))) AS max_wk
                FROM
                    cte_origin_src
                    -- mart.amz_di_global
                WHERE
                    period_type='WEEK'
                    -- AND is_closed=TRUE

                GROUP BY 1, 2

            ) AS t_r
                CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_wk, t_r.max_wk, INTERVAL 1 WEEK)) AS date_val
                , UNNEST([CAST(FORMAT_DATE('%G%V', date_val) AS INT64)]) AS fill_date

        UNION ALL

        SELECT
            country
            , asin
            , 'MONTH' as period_type
            , fill_date
        FROM
            (
                SELECT
                    country
                    , asin
                    , PARSE_DATE('%Y%m', MIN(yr_month_or_week)) AS min_m
                    , PARSE_DATE('%Y%m', MAX(yr_month_or_week)) AS max_m
                FROM
                    cte_origin_src
                WHERE
                    period_type='MONTH'
                GROUP BY 1, 2

            ) AS t_r
                CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_m, t_r.max_m, INTERVAL 1 MONTH )) AS date_val
                , UNNEST([CAST(FORMAT_DATE('%Y%m', date_val) AS INT64)]) AS fill_date
    )
    , cte_actual_src as (
        SELECT
            country
            , division
            , category
            , single_category
            , single_cat_desc
            , zinus_sku
            , yr_month_or_week
            , period_type
            , SUM(open_purchase_order_quantity) AS open_purchase_order_quantity
            , SUM(open_purchase_order_amount) AS open_purchase_order_amount
            , SUM(net_received_units) AS net_received_units
            , SUM(net_received) AS net_received
            , SUM(sellable_on_hand_units) AS sellable_on_hand_units
            , SUM(sellable_on_hand_inventory) AS sellable_on_hand_inventory
            , SUM(shipped_units) AS shipped_units
            , SUM(shipped_revenue) AS shipped_revenue
        FROM (
            SELECT
                d.country
                , IF(division IS NULL, FIRST_VALUE(division IGNORE NULLS) OVER (PARTITION BY d.country, d.asin ORDER BY d.fill_date DESC), division) AS division
                , IF(category IS NULL, FIRST_VALUE(category IGNORE NULLS) OVER (PARTITION BY d.country, d.asin ORDER BY d.fill_date DESC), category) AS category
                , IF(single_category IS NULL, FIRST_VALUE(single_category IGNORE NULLS) OVER (PARTITION BY d.country, d.asin ORDER BY d.fill_date DESC), single_category) AS single_category
                , IF(single_cat_desc IS NULL, FIRST_VALUE(single_cat_desc IGNORE NULLS) OVER (PARTITION BY d.country, d.asin ORDER BY d.fill_date DESC), single_cat_desc) AS single_cat_desc
                , IF(zinus_sku IS NULL, FIRST_VALUE(zinus_sku IGNORE NULLS) OVER (PARTITION BY d.country, d.asin ORDER BY d.fill_date DESC), zinus_sku) AS zinus_sku
                , CAST(d.fill_date AS STRING) AS yr_month_or_week

                , d.period_type
                , open_purchase_order_quantity
                , open_purchase_order_amount
                , net_received_units
                , net_received
                , sellable_on_hand_units
                , sellable_on_hand_inventory
                , shipped_units
                , shipped_revenue

    --             , IF(d.period_type = 'WEEK', CAST(b.yr_month AS STRING), NULL) AS yr_month_for_week
            FROM
                cte_fill_date d
                    LEFT JOIN cte_origin_src s
                        ON d.asin = s.asin
                               AND d.country = s.country
                               AND d.period_type = s.period_type
                               AND d.fill_date = CAST(s.yr_month_or_week as INT64)

    --                 LEFT JOIN meta.wk_calendar_new b
    --                     ON d.fill_date = b.yr_wk
        )
        -- zinus sku 단위 grouping
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    )
    , cte_final_week_row_value as (
        SELECT
            a.country
            , a.division
            , a.category
            , a.single_category
            , a.single_cat_desc
            , a.zinus_sku
            , a.period_type
            , a.yr_month_or_week

            --             , c.open_purchase_order_quantity
            --             , c.open_purchase_order_amount
            , a.open_purchase_order_quantity
            , a.open_purchase_order_amount

            , a.sellable_on_hand_units -- ending inv
            , a.sellable_on_hand_inventory -- ending inv usd

            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc ROWS BETWEEN CURRENT ROW AND 25 FOLLOWING) AS sell_out_13_sum
            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week  desc ROWS BETWEEN 52 FOLLOWING AND 77 FOLLOWING) AS sell_out_last_year_13_sum

            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week  desc ROWS BETWEEN CURRENT ROW AND 25 FOLLOWING) AS sell_out_usd_13_sum
            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week  desc ROWS BETWEEN 52 FOLLOWING AND 77 FOLLOWING) AS sell_out_usd_last_year_13_sum

            , LEAD(shipped_units, 51) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst1
            , LEAD(shipped_units, 50) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst2
            , LEAD(shipped_units, 49) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst3
            , LEAD(shipped_units, 48) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst4
            , LEAD(shipped_units, 47) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst5
            , LEAD(shipped_units, 46) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst6
            , LEAD(shipped_units, 45) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst7
            , LEAD(shipped_units, 44) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst8
            , LEAD(shipped_units, 43) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst9
            , LEAD(shipped_units, 42) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst10
            , LEAD(shipped_units, 41) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst11
            , LEAD(shipped_units, 40) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst12
            , LEAD(shipped_units, 39) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst13
            , LEAD(shipped_units, 38) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst14
            , LEAD(shipped_units, 37) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst15
            , LEAD(shipped_units, 36) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst16
            , LEAD(shipped_units, 35) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst17
            , LEAD(shipped_units, 34) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst18
            , LEAD(shipped_units, 33) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst19
            , LEAD(shipped_units, 32) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst20
            , LEAD(shipped_units, 31) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst21
            , LEAD(shipped_units, 30) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst22
            , LEAD(shipped_units, 29) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst23
            , LEAD(shipped_units, 28) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst24
            , LEAD(shipped_units, 27) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst25
            , LEAD(shipped_units, 26) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst26

            , LEAD(shipped_revenue, 51) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd1
            , LEAD(shipped_revenue, 50) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd2
            , LEAD(shipped_revenue, 49) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd3
            , LEAD(shipped_revenue, 48) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd4
            , LEAD(shipped_revenue, 47) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd5
            , LEAD(shipped_revenue, 46) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd6
            , LEAD(shipped_revenue, 45) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd7
            , LEAD(shipped_revenue, 44) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd8
            , LEAD(shipped_revenue, 43) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd9
            , LEAD(shipped_revenue, 42) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd10
            , LEAD(shipped_revenue, 41) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd11
            , LEAD(shipped_revenue, 40) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd12
            , LEAD(shipped_revenue, 39) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) as sell_out_fcst_usd13
            , LEAD(shipped_revenue, 38) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd14
            , LEAD(shipped_revenue, 37) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd15
            , LEAD(shipped_revenue, 36) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd16
            , LEAD(shipped_revenue, 35) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd17
            , LEAD(shipped_revenue, 34) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd18
            , LEAD(shipped_revenue, 33) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd19
            , LEAD(shipped_revenue, 32) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd20
            , LEAD(shipped_revenue, 31) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd21
            , LEAD(shipped_revenue, 30) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd22
            , LEAD(shipped_revenue, 29) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd23
            , LEAD(shipped_revenue, 28) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd24
            , LEAD(shipped_revenue, 27) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd25
            , LEAD(shipped_revenue, 26) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd26

            , p70.week0 as p70_week0, p70.week1 as p70_week1, p70.week2 as p70_week2, p70.week3 as p70_week3, p70.week4 as p70_week4, p70.week5 as p70_week5, p70.week6 as p70_week6, p70.week7 as p70_week7, p70.week8 as p70_week8, p70.week9 as p70_week9, p70.week10 as p70_week10, p70.week11 as p70_week11, p70.week12 as p70_week12, p70.week13 as p70_week13, p70.week14 as p70_week14, p70.week15 as p70_week15, p70.week16 as p70_week16, p70.week17 as p70_week17, p70.week18 as p70_week18, p70.week19 as p70_week19, p70.week20 as p70_week20, p70.week21 as p70_week21, p70.week22 as p70_week22, p70.week23 as p70_week23, p70.week24 as p70_week24, p70.week25 as p70_week25, p70.week26 as p70_week26, p70.week27 as p70_week27, p70.week28 as p70_week28, p70.week29 as p70_week29, p70.week30 as p70_week30, p70.week31 as p70_week31, p70.week32 as p70_week32, p70.week33 as p70_week33, p70.week34 as p70_week34, p70.week35 as p70_week35, p70.week36 as p70_week36, p70.week37 as p70_week37, p70.week38 as p70_week38, p70.week39 as p70_week39, p70.week40 as p70_week40, p70.week41 as p70_week41, p70.week42 as p70_week42, p70.week43 as p70_week43, p70.week44 as p70_week44, p70.week45 as p70_week45, p70.week46 as p70_week46, p70.week47 as p70_week47
            , p80.week0 as p80_week0, p80.week1 as p80_week1, p80.week2 as p80_week2, p80.week3 as p80_week3, p80.week4 as p80_week4, p80.week5 as p80_week5, p80.week6 as p80_week6, p80.week7 as p80_week7, p80.week8 as p80_week8, p80.week9 as p80_week9, p80.week10 as p80_week10, p80.week11 as p80_week11, p80.week12 as p80_week12, p80.week13 as p80_week13, p80.week14 as p80_week14, p80.week15 as p80_week15, p80.week16 as p80_week16, p80.week17 as p80_week17, p80.week18 as p80_week18, p80.week19 as p80_week19, p80.week20 as p80_week20, p80.week21 as p80_week21, p80.week22 as p80_week22, p80.week23 as p80_week23, p80.week24 as p80_week24, p80.week25 as p80_week25, p80.week26 as p80_week26, p80.week27 as p80_week27, p80.week28 as p80_week28, p80.week29 as p80_week29, p80.week30 as p80_week30, p80.week31 as p80_week31, p80.week32 as p80_week32, p80.week33 as p80_week33, p80.week34 as p80_week34, p80.week35 as p80_week35, p80.week36 as p80_week36, p80.week37 as p80_week37, p80.week38 as p80_week38, p80.week39 as p80_week39, p80.week40 as p80_week40, p80.week41 as p80_week41, p80.week42 as p80_week42, p80.week43 as p80_week43, p80.week44 as p80_week44, p80.week45 as p80_week45, p80.week46 as p80_week46, p80.week47 as p80_week47
            , p90.week0 as p90_week0, p90.week1 as p90_week1, p90.week2 as p90_week2, p90.week3 as p90_week3, p90.week4 as p90_week4, p90.week5 as p90_week5, p90.week6 as p90_week6, p90.week7 as p90_week7, p90.week8 as p90_week8, p90.week9 as p90_week9, p90.week10 as p90_week10, p90.week11 as p90_week11, p90.week12 as p90_week12, p90.week13 as p90_week13, p90.week14 as p90_week14, p90.week15 as p90_week15, p90.week16 as p90_week16, p90.week17 as p90_week17, p90.week18 as p90_week18, p90.week19 as p90_week19, p90.week20 as p90_week20, p90.week21 as p90_week21, p90.week22 as p90_week22, p90.week23 as p90_week23, p90.week24 as p90_week24, p90.week25 as p90_week25, p90.week26 as p90_week26, p90.week27 as p90_week27, p90.week28 as p90_week28, p90.week29 as p90_week29, p90.week30 as p90_week30, p90.week31 as p90_week31, p90.week32 as p90_week32, p90.week33 as p90_week33, p90.week34 as p90_week34, p90.week35 as p90_week35, p90.week36 as p90_week36, p90.week37 as p90_week37, p90.week38 as p90_week38, p90.week39 as p90_week39, p90.week40 as p90_week40, p90.week41 as p90_week41, p90.week42 as p90_week42, p90.week43 as p90_week43, p90.week44 as p90_week44, p90.week45 as p90_week45, p90.week46 as p90_week46, p90.week47 as p90_week47
            , mean.week0 as mean_week0, mean.week1 as mean_week1, mean.week2 as mean_week2, mean.week3 as mean_week3, mean.week4 as mean_week4, mean.week5 as mean_week5, mean.week6 as mean_week6, mean.week7 as mean_week7, mean.week8 as mean_week8, mean.week9 as mean_week9, mean.week10 as mean_week10, mean.week11 as mean_week11, mean.week12 as mean_week12, mean.week13 as mean_week13, mean.week14 as mean_week14, mean.week15 as mean_week15, mean.week16 as mean_week16, mean.week17 as mean_week17, mean.week18 as mean_week18, mean.week19 as mean_week19, mean.week20 as mean_week20, mean.week21 as mean_week21, mean.week22 as mean_week22, mean.week23 as mean_week23, mean.week24 as mean_week24, mean.week25 as mean_week25, mean.week26 as mean_week26, mean.week27 as mean_week27, mean.week28 as mean_week28, mean.week29 as mean_week29, mean.week30 as mean_week30, mean.week31 as mean_week31, mean.week32 as mean_week32, mean.week33 as mean_week33, mean.week34 as mean_week34, mean.week35 as mean_week35, mean.week36 as mean_week36, mean.week37 as mean_week37, mean.week38 as mean_week38, mean.week39 as mean_week39, mean.week40 as mean_week40, mean.week41 as mean_week41, mean.week42 as mean_week42, mean.week43 as mean_week43, mean.week44 as mean_week44, mean.week45 as mean_week45, mean.week46 as mean_week46, mean.week47 as mean_week47
        FROM
            cte_actual_src a
        --                 LEFT JOIN cte_final_month_row_value c
        --                     ON a.country = c.country AND a.division = c.division AND a.category = c.category AND a.single_category = c.single_category AND a.zinus_sku = c.zinus_sku
                LEFT JOIN cte_fcst p70
                    ON a.zinus_sku = p70.zinus_sku AND a.country = p70.country AND a.division = p70.division AND a.category = p70.category AND a.period_type = p70.period_type AND a.yr_month_or_week = p70.yr_month_or_week AND p70.type='p70'
                LEFT JOIN cte_fcst p80
                    ON a.zinus_sku = p80.zinus_sku AND a.country = p80.country AND a.division = p80.division AND a.category = p80.category AND a.period_type = p80.period_type AND a.yr_month_or_week = p80.yr_month_or_week AND p80.type='p80'
                LEFT JOIN cte_fcst p90
                    ON a.zinus_sku = p90.zinus_sku AND a.country = p90.country AND a.division = p90.division AND a.category = p90.category AND a.period_type = p90.period_type AND a.yr_month_or_week = p90.yr_month_or_week AND p90.type='p90'
                LEFT JOIN cte_fcst mean
                    ON a.zinus_sku = mean.zinus_sku AND a.country = mean.country AND a.division = mean.division AND a.category = mean.category AND a.period_type = mean.period_type AND a.yr_month_or_week = mean.yr_month_or_week AND mean.type='mean'
        WHERE
            a.period_type = 'WEEK'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.zinus_sku, a.period_type ORDER BY a.yr_month_or_week desc) = 1
    )
    , cte_final_month_row_value AS (
        SELECT
            a.country
            , a.division
            , a.category
            , a.single_category
            , a.single_cat_desc
            , a.zinus_sku
            , a.period_type
            , a.yr_month_or_week

            , w.open_purchase_order_quantity -- fcst received po
            , w.open_purchase_order_amount -- fcst received po usd

--             , a.open_purchase_order_quantity -- fcst received po
--             , a.open_purchase_order_amount -- fcst received po usd

            , a.sellable_on_hand_units -- ending inv
            , a.sellable_on_hand_inventory -- ending inv usd

            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) AS sell_out_6_sum
            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week  DESC ROWS BETWEEN 12 FOLLOWING AND 17 FOLLOWING) AS sell_out_last_year_6_sum

            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week  DESC ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) AS sell_out_usd_6_sum
            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week  DESC ROWS BETWEEN 12 FOLLOWING AND 17 FOLLOWING) AS sell_out_usd_last_year_6_sum

            , LEAD(shipped_units, 11) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst1
            , LEAD(shipped_units, 10) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst2
            , LEAD(shipped_units, 9) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst3
            , LEAD(shipped_units, 8) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst4
            , LEAD(shipped_units, 7) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst5
            , LEAD(shipped_units, 6) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst6
            , LEAD(shipped_units, 5) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst7
            , LEAD(shipped_units, 4) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst8
            , LEAD(shipped_units, 3) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst9
            , LEAD(shipped_units, 2) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst10
            , LEAD(shipped_units, 1) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst11
            , shipped_units AS sell_out_fcst12

            , LEAD(shipped_revenue, 11) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd1
            , LEAD(shipped_revenue, 10) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd2
            , LEAD(shipped_revenue, 9) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd3
            , LEAD(shipped_revenue, 8) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd4
            , LEAD(shipped_revenue, 7) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd5
            , LEAD(shipped_revenue, 6) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd6
            , LEAD(shipped_revenue, 5) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd7
            , LEAD(shipped_revenue, 4) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd8
            , LEAD(shipped_revenue, 3) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd9
            , LEAD(shipped_revenue, 2) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd10
            , LEAD(shipped_revenue, 1) OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd11
            , shipped_revenue AS sell_out_fcst_usd12

            , w.p70_week0, w.p70_week1, w.p70_week2, w.p70_week3, w.p70_week4, w.p70_week5, w.p70_week6, w.p70_week7, w.p70_week8, w.p70_week9, w.p70_week10, w.p70_week11, w.p70_week12, w.p70_week13, w.p70_week14, w.p70_week15, w.p70_week16, w.p70_week17, w.p70_week18, w.p70_week19, w.p70_week20, w.p70_week21, w.p70_week22, w.p70_week23, w.p70_week24, w.p70_week25, w.p70_week26, w.p70_week27, w.p70_week28, w.p70_week29, w.p70_week30, w.p70_week31, w.p70_week32, w.p70_week33, w.p70_week34, w.p70_week35, w.p70_week36, w.p70_week37, w.p70_week38, w.p70_week39, w.p70_week40, w.p70_week41, w.p70_week42, w.p70_week43, w.p70_week44, w.p70_week45, w.p70_week46, w.p70_week47
            , w.p80_week0, w.p80_week1, w.p80_week2, w.p80_week3, w.p80_week4, w.p80_week5, w.p80_week6, w.p80_week7, w.p80_week8, w.p80_week9, w.p80_week10, w.p80_week11, w.p80_week12, w.p80_week13, w.p80_week14, w.p80_week15, w.p80_week16, w.p80_week17, w.p80_week18, w.p80_week19, w.p80_week20, w.p80_week21, w.p80_week22, w.p80_week23, w.p80_week24, w.p80_week25, w.p80_week26, w.p80_week27, w.p80_week28, w.p80_week29, w.p80_week30, w.p80_week31, w.p80_week32, w.p80_week33, w.p80_week34, w.p80_week35, w.p80_week36, w.p80_week37, w.p80_week38, w.p80_week39, w.p80_week40, w.p80_week41, w.p80_week42, w.p80_week43, w.p80_week44, w.p80_week45, w.p80_week46, w.p80_week47
            , w.p90_week0, w.p90_week1, w.p90_week2, w.p90_week3, w.p90_week4, w.p90_week5, w.p90_week6, w.p90_week7, w.p90_week8, w.p90_week9, w.p90_week10, w.p90_week11, w.p90_week12, w.p90_week13, w.p90_week14, w.p90_week15, w.p90_week16, w.p90_week17, w.p90_week18, w.p90_week19, w.p90_week20, w.p90_week21, w.p90_week22, w.p90_week23, w.p90_week24, w.p90_week25, w.p90_week26, w.p90_week27, w.p90_week28, w.p90_week29, w.p90_week30, w.p90_week31, w.p90_week32, w.p90_week33, w.p90_week34, w.p90_week35, w.p90_week36, w.p90_week37, w.p90_week38, w.p90_week39, w.p90_week40, w.p90_week41, w.p90_week42, w.p90_week43, w.p90_week44, w.p90_week45, w.p90_week46, w.p90_week47
            , w.mean_week0, w.mean_week1, w.mean_week2, w.mean_week3, w.mean_week4, w.mean_week5, w.mean_week6, w.mean_week7, w.mean_week8, w.mean_week9, w.mean_week10, w.mean_week11, w.mean_week12, w.mean_week13, w.mean_week14, w.mean_week15, w.mean_week16, w.mean_week17, w.mean_week18, w.mean_week19, w.mean_week20, w.mean_week21, w.mean_week22, w.mean_week23, w.mean_week24, w.mean_week25, w.mean_week26, w.mean_week27, w.mean_week28, w.mean_week29, w.mean_week30, w.mean_week31, w.mean_week32, w.mean_week33, w.mean_week34, w.mean_week35, w.mean_week36, w.mean_week37, w.mean_week38, w.mean_week39, w.mean_week40, w.mean_week41, w.mean_week42, w.mean_week43, w.mean_week44, w.mean_week45, w.mean_week46, w.mean_week47
        FROM
            cte_actual_src a
                LEFT JOIN cte_final_week_row_value w
                    ON a.country = w.country AND a.division = w.division AND a.category = w.category AND a.single_category = w.single_category AND a.zinus_sku = w.zinus_sku
        WHERE
            a.period_type = 'MONTH'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY a.country, a.division, a.category, a.single_category, a.period_type, a.zinus_sku ORDER BY a.yr_month_or_week desc) = 1
    )
    , cte_fcst_src as (
        SELECT
            country
            , division
            , category
            , single_category
            , single_cat_desc
            , zinus_sku
            -- , yr_month_or_week
            , FORMAT_DATE('%Y%m', DATE_ADD(PARSE_DATE('%Y%m', yr_month_or_week), INTERVAL arr MONTH)) AS yr_month_or_week
            , period_type

            -- , open_purchase_order_quantity
            , IF(arr <= 3, open_purchase_order_quantity / 3, 0) AS net_received_units
            , IF(arr <= 3, open_purchase_order_amount / 3, 0) AS net_received
            , sellable_on_hand_units
            , sellable_on_hand_inventory

            , CASE arr
                  WHEN 1 THEN sell_out_fcst1 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 2 THEN sell_out_fcst2 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 3 THEN sell_out_fcst3 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 4 THEN sell_out_fcst4 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 5 THEN sell_out_fcst5 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 6 THEN sell_out_fcst6 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 7 THEN sell_out_fcst7 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 8 THEN sell_out_fcst8 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 9 THEN sell_out_fcst9 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 10 THEN sell_out_fcst10 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 11 THEN sell_out_fcst11 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 12 THEN sell_out_fcst12 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
              END AS shipped_units
            , CASE arr
                  WHEN 1 THEN sell_out_fcst_usd1 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 2 THEN sell_out_fcst_usd2 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 3 THEN sell_out_fcst_usd3 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 4 THEN sell_out_fcst_usd4 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 5 THEN sell_out_fcst_usd5 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 6 THEN sell_out_fcst_usd6 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 7 THEN sell_out_fcst_usd7 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 8 THEN sell_out_fcst_usd8 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 9 THEN sell_out_fcst_usd9 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 10 THEN sell_out_fcst_usd10 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 11 THEN sell_out_fcst_usd11 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 12 THEN sell_out_fcst_usd12 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
              END AS shipped_revenue

            , CASE arr
                  WHEN 1 THEN p70_week0+p70_week1+p70_week2+p70_week3
                  WHEN 2 THEN p70_week4+p70_week5+p70_week6+p70_week7
                  WHEN 3 THEN p70_week8+p70_week9+p70_week10+p70_week11
                  WHEN 4 THEN p70_week12+p70_week13+p70_week14+p70_week15
                  WHEN 5 THEN p70_week16+p70_week17+p70_week18+p70_week19
                  WHEN 6 THEN p70_week20+p70_week21+p70_week22+p70_week23
                  WHEN 7 THEN p70_week24+p70_week25+p70_week26+p70_week27
                  WHEN 8 THEN p70_week28+p70_week29+p70_week30+p70_week31
                  WHEN 9 THEN p70_week32+p70_week33+p70_week34+p70_week35
                  WHEN 10 THEN p70_week36+p70_week37+p70_week38+p70_week39
                  WHEN 11 THEN p70_week40+p70_week41+p70_week42+p70_week43
                  WHEN 12 THEN p70_week44+p70_week45+p70_week46+p70_week47
              END AS p70
            , CASE arr
                  WHEN 1 THEN p80_week0+p80_week1+p80_week2+p80_week3
                  WHEN 2 THEN p80_week4+p80_week5+p80_week6+p80_week7
                  WHEN 3 THEN p80_week8+p80_week9+p80_week10+p80_week11
                  WHEN 4 THEN p80_week12+p80_week13+p80_week14+p80_week15
                  WHEN 5 THEN p80_week16+p80_week17+p80_week18+p80_week19
                  WHEN 6 THEN p80_week20+p80_week21+p80_week22+p80_week23
                  WHEN 7 THEN p80_week24+p80_week25+p80_week26+p80_week27
                  WHEN 8 THEN p80_week28+p80_week29+p80_week30+p80_week31
                  WHEN 9 THEN p80_week32+p80_week33+p80_week34+p80_week35
                  WHEN 10 THEN p80_week36+p80_week37+p80_week38+p80_week39
                  WHEN 11 THEN p80_week40+p80_week41+p80_week42+p80_week43
                  WHEN 12 THEN p80_week44+p80_week45+p80_week46+p80_week47
              END AS p80
            , CASE arr
                  WHEN 1 THEN p90_week0+p90_week1+p90_week2+p90_week3
                  WHEN 2 THEN p90_week4+p90_week5+p90_week6+p90_week7
                  WHEN 3 THEN p90_week8+p90_week9+p90_week10+p90_week11
                  WHEN 4 THEN p90_week12+p90_week13+p90_week14+p90_week15
                  WHEN 5 THEN p90_week16+p90_week17+p90_week18+p90_week19
                  WHEN 6 THEN p90_week20+p90_week21+p90_week22+p90_week23
                  WHEN 7 THEN p90_week24+p90_week25+p90_week26+p90_week27
                  WHEN 8 THEN p90_week28+p90_week29+p90_week30+p90_week31
                  WHEN 9 THEN p90_week32+p90_week33+p90_week34+p90_week35
                  WHEN 10 THEN p90_week36+p90_week37+p90_week38+p90_week39
                  WHEN 11 THEN p90_week40+p90_week41+p90_week42+p90_week43
                  WHEN 12 THEN p90_week44+p90_week45+p90_week46+p90_week47
              END AS p90
            , CASE arr
                  WHEN 1 THEN mean_week0+mean_week1+mean_week2+mean_week3
                  WHEN 2 THEN mean_week4+mean_week5+mean_week6+mean_week7
                  WHEN 3 THEN mean_week8+mean_week9+mean_week10+mean_week11
                  WHEN 4 THEN mean_week12+mean_week13+mean_week14+mean_week15
                  WHEN 5 THEN mean_week16+mean_week17+mean_week18+mean_week19
                  WHEN 6 THEN mean_week20+mean_week21+mean_week22+mean_week23
                  WHEN 7 THEN mean_week24+mean_week25+mean_week26+mean_week27
                  WHEN 8 THEN mean_week28+mean_week29+mean_week30+mean_week31
                  WHEN 9 THEN mean_week32+mean_week33+mean_week34+mean_week35
                  WHEN 10 THEN mean_week36+mean_week37+mean_week38+mean_week39
                  WHEN 11 THEN mean_week40+mean_week41+mean_week42+mean_week43
                  WHEN 12 THEN mean_week44+mean_week45+mean_week46+mean_week47
              END AS mean
        FROM
            cte_final_month_row_value
                CROSS JOIN UNNEST(GENERATE_ARRAY(1, 12, 1)) AS arr


        UNION ALL

        SELECT
            country
            , division
            , category
            , single_category
            , single_cat_desc
            , zinus_sku

            , FORMAT_DATE('%G%V', DATE_ADD(PARSE_DATE('%G-%V', concat(SUBSTRING(yr_month_or_week, 1, 4), '-', SUBSTRING(yr_month_or_week, 5, 2))), INTERVAL arr WEEK)) AS yr_month_or_week

            , period_type

            -- , open_purchase_order_quantity
            , IF(arr <= 13, open_purchase_order_quantity/13, 0) AS net_received_units
            , IF(arr <= 13, open_purchase_order_amount/13, 0) AS net_received

            , sellable_on_hand_units
            , sellable_on_hand_inventory

            , CASE arr
                  WHEN 1  THEN sell_out_fcst1 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 2  THEN sell_out_fcst2 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 3  THEN sell_out_fcst3 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 4  THEN sell_out_fcst4 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 5  THEN sell_out_fcst5 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 6  THEN sell_out_fcst6 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 7  THEN sell_out_fcst7 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 8  THEN sell_out_fcst8 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 9  THEN sell_out_fcst9 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 10 THEN sell_out_fcst10 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 11 THEN sell_out_fcst11 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 12 THEN sell_out_fcst12 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 13 THEN sell_out_fcst13 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 14 THEN sell_out_fcst14 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 15 THEN sell_out_fcst15 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 16 THEN sell_out_fcst16 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 17 THEN sell_out_fcst17 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 18 THEN sell_out_fcst18 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 19 THEN sell_out_fcst19 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 20 THEN sell_out_fcst20 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 21 THEN sell_out_fcst21 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 22 THEN sell_out_fcst22 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 23 THEN sell_out_fcst23  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 24 THEN sell_out_fcst24  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 25 THEN sell_out_fcst25  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 26 THEN sell_out_fcst26  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
              END AS shipped_units
            , CASE arr
                  WHEN 1  THEN sell_out_fcst_usd1 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 2  THEN sell_out_fcst_usd2 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 3  THEN sell_out_fcst_usd3 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 4  THEN sell_out_fcst_usd4 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 5  THEN sell_out_fcst_usd5 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 6  THEN sell_out_fcst_usd6 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 7  THEN sell_out_fcst_usd7 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 8  THEN sell_out_fcst_usd8 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 9  THEN sell_out_fcst_usd9 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 10 THEN sell_out_fcst_usd10 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 11 THEN sell_out_fcst_usd11 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 12 THEN sell_out_fcst_usd12 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 13 THEN sell_out_fcst_usd13 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 14 THEN sell_out_fcst_usd14 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 15 THEN sell_out_fcst_usd15 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 16 THEN sell_out_fcst_usd16 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 17 THEN sell_out_fcst_usd17 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 18 THEN sell_out_fcst_usd18 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 19 THEN sell_out_fcst_usd19 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 20 THEN sell_out_fcst_usd20 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 21 THEN sell_out_fcst_usd21 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 22 THEN sell_out_fcst_usd22 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 23 THEN sell_out_fcst_usd23  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 24 THEN sell_out_fcst_usd24  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 25 THEN sell_out_fcst_usd25  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 26 THEN sell_out_fcst_usd26  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
              END AS shipped_revenue

            , CASE arr
                  WHEN 1 THEN p70_week0
                  WHEN 2 THEN p70_week1
                  WHEN 3 THEN p70_week2
                  WHEN 4 THEN p70_week3
                  WHEN 5 THEN p70_week4
                  WHEN 6 THEN p70_week5
                  WHEN 7 THEN p70_week6
                  WHEN 8 THEN p70_week7
                  WHEN 9 THEN p70_week8
                  WHEN 10 THEN p70_week9
                  WHEN 11 THEN p70_week10
                  WHEN 12 THEN p70_week11
                  WHEN 13 THEN p70_week12
                  WHEN 14 THEN p70_week13
                  WHEN 15 THEN p70_week14
                  WHEN 16 THEN p70_week15
                  WHEN 17 THEN p70_week16
                  WHEN 18 THEN p70_week17
                  WHEN 19 THEN p70_week18
                  WHEN 20 THEN p70_week19
                  WHEN 21 THEN p70_week20
                  WHEN 22 THEN p70_week21
                  WHEN 23 THEN p70_week22
                  WHEN 24 THEN p70_week23
                  WHEN 25 THEN p70_week24
                  WHEN 26 THEN p70_week25
              END AS p70
            , CASE arr
                  WHEN 1 THEN p80_week0
                  WHEN 2 THEN p80_week1
                  WHEN 3 THEN p80_week2
                  WHEN 4 THEN p80_week3
                  WHEN 5 THEN p80_week4
                  WHEN 6 THEN p80_week5
                  WHEN 7 THEN p80_week6
                  WHEN 8 THEN p80_week7
                  WHEN 9 THEN p80_week8
                  WHEN 10 THEN p80_week9
                  WHEN 11 THEN p80_week10
                  WHEN 12 THEN p80_week11
                  WHEN 13 THEN p80_week12
                  WHEN 14 THEN p80_week13
                  WHEN 15 THEN p80_week14
                  WHEN 16 THEN p80_week15
                  WHEN 17 THEN p80_week16
                  WHEN 18 THEN p80_week17
                  WHEN 19 THEN p80_week18
                  WHEN 20 THEN p80_week19
                  WHEN 21 THEN p80_week20
                  WHEN 22 THEN p80_week21
                  WHEN 23 THEN p80_week22
                  WHEN 24 THEN p80_week23
                  WHEN 25 THEN p80_week24
                  WHEN 26 THEN p80_week25
              END AS p80
            , CASE arr
                  WHEN 1 THEN p90_week0
                  WHEN 2 THEN p90_week1
                  WHEN 3 THEN p90_week2
                  WHEN 4 THEN p90_week3
                  WHEN 5 THEN p90_week4
                  WHEN 6 THEN p90_week5
                  WHEN 7 THEN p90_week6
                  WHEN 8 THEN p90_week7
                  WHEN 9 THEN p90_week8
                  WHEN 10 THEN p90_week9
                  WHEN 11 THEN p90_week10
                  WHEN 12 THEN p90_week11
                  WHEN 13 THEN p90_week12
                  WHEN 14 THEN p90_week13
                  WHEN 15 THEN p90_week14
                  WHEN 16 THEN p90_week15
                  WHEN 17 THEN p90_week16
                  WHEN 18 THEN p90_week17
                  WHEN 19 THEN p90_week18
                  WHEN 20 THEN p90_week19
                  WHEN 21 THEN p90_week20
                  WHEN 22 THEN p90_week21
                  WHEN 23 THEN p90_week22
                  WHEN 24 THEN p90_week23
                  WHEN 25 THEN p90_week24
                  WHEN 26 THEN p90_week25
              END AS p90
            , CASE arr
                  WHEN 1  THEN mean_week0
                  WHEN 2  THEN mean_week1
                  WHEN 3  THEN mean_week2
                  WHEN 4  THEN mean_week3
                  WHEN 5  THEN mean_week4
                  WHEN 6  THEN mean_week5
                  WHEN 7  THEN mean_week6
                  WHEN 8  THEN mean_week7
                  WHEN 9  THEN mean_week8
                  WHEN 10 THEN mean_week9
                  WHEN 11 THEN mean_week10
                  WHEN 12 THEN mean_week11
                  WHEN 13 THEN mean_week12
                  WHEN 14 THEN mean_week13
                  WHEN 15 THEN mean_week14
                  WHEN 16 THEN mean_week15
                  WHEN 17 THEN mean_week16
                  WHEN 18 THEN mean_week17
                  WHEN 19 THEN mean_week18
                  WHEN 20 THEN mean_week19
                  WHEN 21 THEN mean_week20
                  WHEN 22 THEN mean_week21
                  WHEN 23 THEN mean_week22
                  WHEN 24 THEN mean_week23
                  WHEN 25 THEN mean_week24
                  WHEN 26 THEN mean_week25
              END AS mean
        FROM
            cte_final_week_row_value
                CROSS JOIN UNNEST(GENERATE_ARRAY(1, 26, 1)) AS arr
    )
    , cte_fcst_act_union as (

        WITH
            cte_limit_fcst AS (
                SELECT
                    country
                    , time_level
                    , MAX(time) AS limit_time
                FROM
                    mart.amz_di_global_psi_report
                WHERE
                    row_type = 'act'
                GROUP BY 1, 2
            )
        SELECT
            f.* EXCEPT (fcst_start_date)
        FROM
            (
                SELECT
                    country
                    , division
                    , category
                    , single_category
                    , single_cat_desc
                    , zinus_sku
--                     , yr_month_or_week
                    , yr_month_or_week AS time
--                     , yr_month_or_week AS end_time

                    , period_type AS time_level
                    -- , open_purchase_order_quantity

                    , net_received_units AS received_po
                    , net_received AS received_po_usd
                    , sellable_on_hand_units - shipped_units + net_received_units AS ending_inv
                    , sellable_on_hand_inventory - shipped_revenue + net_received AS ending_inv_usd
                    , shipped_units AS sell_out
                    , shipped_revenue AS sell_out_usd

                    , p70 as sell_out_with_p70
                    , p80 as sell_out_with_p80
                    , p90 as sell_out_with_p90
                    , mean as sell_out_with_mean

                    , FIRST_VALUE(yr_month_or_week IGNORE NULLS) OVER (PARTITION BY country, division, category, single_category, period_type, zinus_sku ORDER BY yr_month_or_week) AS fcst_start_date
                    , 'fcst' AS row_type
                FROM
                    cte_fcst_src f
            ) f
                LEFT JOIN cte_limit_fcst as c
                    ON f.time_level = c.time_level
                        AND f.country = c.country
        WHERE
            f.fcst_start_date > c.limit_time

        UNION ALL

        SELECT
            country
            , division
            , category
            , single_category
            , single_cat_desc
            , zinus_sku
--             , yr_month_or_week
            , yr_month_or_week AS time
--             , yr_month_or_week AS end_time

            , period_type AS time_level
            -- , open_purchase_order_quantity
            , net_received_units
            , net_received
            , sellable_on_hand_units
            , sellable_on_hand_inventory
            , shipped_units
            , shipped_revenue
            , shipped_units as sell_out_with_p70
            , shipped_units as sell_out_with_p80
            , shipped_units as sell_out_with_p90
            , shipped_units as sell_out_with_mean
            , 'act' AS row_type
        FROM
            cte_actual_src
    )


    , cte_sum_wos as (
        SELECT
            country
            , division
            , category
            , single_category
            , single_cat_desc
            , zinus_sku

            , time
            , FORMAT_DATE('%Y%m', DATE_ADD(PARSE_DATE('%Y%m', time), INTERVAL 1 YEAR)) AS prev_ytd_time

            , 'MONTH' as time_base_level
            , AVG(sell_out) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout
            , AVG(sell_out_usd) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_usd
            , AVG(received_po) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po
            , AVG(received_po_usd) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po_usd

            , AVG(sell_out_with_p70) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p70
            , AVG(sell_out_with_p80) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p80
            , AVG(sell_out_with_p90) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p90
            , AVG(sell_out_with_mean) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_mean

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                  THEN
                      SUM(sell_out) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                  THEN
                      SUM(sell_out_usd) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_sum_for_wos_usd

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(sell_out_with_p70) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p70_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(sell_out_with_p80) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p80_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(sell_out_with_p90) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p90_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(sell_out_with_mean) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_mean_sum_for_wos
        FROM
            cte_fcst_act_union
        WHERE
            time_level = 'MONTH'

        UNION ALL

        SELECT
            country
            , division
            , category
            , single_category
            , single_cat_desc
            , zinus_sku

            , time
            , CAST(CAST(SUBSTRING(time,1,4) AS INT64) +1 AS STRING) || SUBSTRING(time, 5) AS prev_ytd_time

            , 'WEEK' as time_base_level
            , AVG(sell_out) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout
            , AVG(sell_out_usd) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_usd
            , AVG(received_po) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po
            , AVG(received_po_usd) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po_usd

            , AVG(sell_out_with_p70) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p70
            , AVG(sell_out_with_p80) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p80
            , AVG(sell_out_with_p90) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p90
            , AVG(sell_out_with_mean) OVER (PARTITION BY country, division, category, single_category, zinus_sku, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_mean

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING) = 13
                  THEN
                      SUM(sell_out) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING)
              END AS sell_out_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING) = 13
                  THEN
                      SUM(sell_out_usd) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING)
              END AS sell_out_sum_for_wos_usd

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(sell_out_with_p70) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p70_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(sell_out_with_p80) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p80_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(sell_out_with_p90) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p90_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(sell_out_with_mean) OVER (PARTITION BY country, division, category, single_category, zinus_sku ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_mean_sum_for_wos
        FROM
            cte_fcst_act_union
        WHERE
            time_level = 'WEEK'

    )
    , cte_semi_final as (
        SELECT
            f.*

--             , IF(f.row_type = 'act', 0, ROW_NUMBER() OVER (PARTITION BY f.country, f.division, f.category, f.single_category, f.zinus_sku, f.time_level, f.row_type ORDER BY f.time)) AS row_num

            , SUBSTRING(f.time, 1, 4) AS year

            , w.ytd_sellout
            , w.ytd_sellout_usd
            , w.ytd_received_po
            , w.ytd_received_po_usd
            , w.ytd_sellout_with_p70
            , w.ytd_sellout_with_p80
            , w.ytd_sellout_with_p90
            , w.ytd_sellout_with_mean

            , prev_wos.ytd_sellout AS prev_ytd_sellout
            , prev_wos.ytd_sellout_usd AS prev_ytd_sellout_usd
            , prev_wos.ytd_received_po AS prev_ytd_received_po
            , prev_wos.ytd_received_po_usd AS prev_ytd_received_po_usd
            , prev_wos.ytd_sellout_with_p70 AS prev_ytd_sellout_with_p70
            , prev_wos.ytd_sellout_with_p80 AS prev_ytd_sellout_with_p80
            , prev_wos.ytd_sellout_with_p90 AS prev_ytd_sellout_with_p90
            , prev_wos.ytd_sellout_with_mean AS prev_ytd_sellout_with_mean

            , w.sell_out_sum_for_wos
            , w.sell_out_sum_for_wos_usd
            , w.sell_out_with_p70_sum_for_wos
            , w.sell_out_with_p80_sum_for_wos
            , w.sell_out_with_p90_sum_for_wos
            , w.sell_out_with_mean_sum_for_wos
        FROM
            cte_fcst_act_union f

                LEFT JOIN cte_sum_wos w
                    ON f.country = w.country
                       AND f.division = w.division
                       AND f.category = w.category
                       AND f.single_category = w.single_category
                       AND f.zinus_sku = w.zinus_sku
                       AND f.time = w.time
--                        AND f.end_time = w.time
                       AND IF(f.time_level = 'WEEK', 'WEEK', 'MONTH') = w.time_base_level

                LEFT JOIN cte_sum_wos prev_wos
                    ON f.country = prev_wos.country
                       AND f.division = prev_wos.division
                       AND f.category = prev_wos.category
                       AND f.single_category = prev_wos.single_category
                       AND f.zinus_sku = prev_wos.zinus_sku
                       AND f.time = prev_wos.prev_ytd_time
                       AND IF(f.time_level = 'WEEK', 'WEEK', 'MONTH') = prev_wos.time_base_level
        --         LEFT JOIN cte_limit_fcst as c
        --             ON f.time_level = c.time_level
        -- WHERE
        --     NOT (f.row_type = 'fcst' AND f.time <= c.limit_time)
    )
    , cte_inventory_logic AS (
        SELECT
            *

            , LAST_VALUE(IF(row_type = 'act', ending_inv, NULL) IGNORE NULLS) OVER( PARTITION BY country, division, category, single_category, zinus_sku, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS start_inv_qty

            , SUM(IF(row_type = 'fcst', sell_out, 0)) OVER( PARTITION BY country, division, category, single_category, zinus_sku, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out
            , SUM(IF(row_type = 'fcst', received_po, 0)) OVER( PARTITION BY country, division, category, single_category, zinus_sku, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_po

            , SUM(IF(row_type = 'fcst', sell_out_with_p70, 0)) OVER( PARTITION BY country, division, category, single_category, zinus_sku, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_p70
            , SUM(IF(row_type = 'fcst', sell_out_with_p80, 0)) OVER( PARTITION BY country, division, category, single_category, zinus_sku, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_p80
            , SUM(IF(row_type = 'fcst', sell_out_with_p90, 0)) OVER( PARTITION BY country, division, category, single_category, zinus_sku, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_p90
            , SUM(IF(row_type = 'fcst', sell_out_with_mean, 0)) OVER( PARTITION BY country, division, category, single_category, zinus_sku, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_mean

            , LAST_VALUE(IF(row_type = 'act', ending_inv_usd, NULL) IGNORE NULLS) OVER(PARTITION BY country, division, category, single_category, zinus_sku, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS start_inv_amt
            , SUM(IF(row_type = 'fcst', sell_out_usd, 0)) OVER( PARTITION BY country, division, category, single_category, zinus_sku, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_usd
            , SUM(IF(row_type = 'fcst', received_po_usd, 0)) OVER( PARTITION BY country, division, category, single_category, zinus_sku, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_po_usd
        FROM
            cte_semi_final
    )
    , cte_month_n_week_final as (

        SELECT
            * EXCEPT (start_inv_qty, cumulative_sell_out, start_inv_amt, cumulative_sell_out_usd, ending_inv, ending_inv_usd, cumulative_po, cumulative_po_usd, cumulative_sell_out_with_p70, cumulative_sell_out_with_p80, cumulative_sell_out_with_p90, cumulative_sell_out_with_mean)

            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out + cumulative_po, ending_inv) AS ending_inv
            , IF(row_type = 'fcst', start_inv_amt - cumulative_sell_out_usd + cumulative_po_usd, ending_inv_usd) AS ending_inv_usd
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_p70 + cumulative_po, ending_inv) AS ending_inv_with_p70
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_p80 + cumulative_po, ending_inv) AS ending_inv_with_p80
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_p90 + cumulative_po, ending_inv) AS ending_inv_with_p90
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_mean + cumulative_po, ending_inv) AS ending_inv_with_mean
        FROM
            cte_inventory_logic
    )
    , cte_year_n_quarter_final as (
        SELECT
            country
            , division
            , category
            , single_category
            , single_cat_desc
            , zinus_sku
            , CONCAT(SUBSTRING(time, 1, 4), 'Q', CAST(CEIL(CAST(SUBSTRING(time, 5, 2) AS INT64) / 3) AS STRING)) AS time
            , 'QUARTER' AS time_level

            , SUM(received_po) AS received_po
            , SUM(received_po_usd) AS received_po_usd

            , SUM(sell_out) AS sell_out
            , SUM(sell_out_usd) AS sell_out_usd
            , SUM(sell_out_with_p70) AS sell_out_with_p70
            , SUM(sell_out_with_p80) AS sell_out_with_p80
            , SUM(sell_out_with_p90) AS sell_out_with_p90
            , SUM(sell_out_with_mean) AS sell_out_with_mean

            , ARRAY_AGG(row_type ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS row_type
            , SUBSTRING(MAX(time), 1, 4) AS year

            , ARRAY_AGG(ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout
            , ARRAY_AGG(ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_usd
            , ARRAY_AGG(ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po
            , ARRAY_AGG(ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po_usd
            , ARRAY_AGG(ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p70
            , ARRAY_AGG(ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p80
            , ARRAY_AGG(ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p90
            , ARRAY_AGG(ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_mean

            , ARRAY_AGG(prev_ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout
            , ARRAY_AGG(prev_ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_usd
            , ARRAY_AGG(prev_ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po
            , ARRAY_AGG(prev_ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po_usd
            , ARRAY_AGG(prev_ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p70
            , ARRAY_AGG(prev_ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p80
            , ARRAY_AGG(prev_ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p90
            , ARRAY_AGG(prev_ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_mean

            , ARRAY_AGG(sell_out_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos
            , ARRAY_AGG(sell_out_sum_for_wos_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos_usd
            , ARRAY_AGG(sell_out_with_p70_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p70_sum_for_wos
            , ARRAY_AGG(sell_out_with_p80_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p80_sum_for_wos
            , ARRAY_AGG(sell_out_with_p90_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p90_sum_for_wos
            , ARRAY_AGG(sell_out_with_mean_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_mean_sum_for_wos


            , ARRAY_AGG(ending_inv ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv
            , ARRAY_AGG(ending_inv_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_usd
            , ARRAY_AGG(ending_inv_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p70
            , ARRAY_AGG(ending_inv_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p80
            , ARRAY_AGG(ending_inv_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p90
            , ARRAY_AGG(ending_inv_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_mean


        FROM
            cte_month_n_week_final
        WHERE
            time_level = 'MONTH'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

        UNION ALL

        SELECT
            country
            , division
            , category
            , single_category
            , single_cat_desc
            , zinus_sku
            , SUBSTRING(time, 1, 4) AS time
            , 'YEAR' AS time_level

            , SUM(received_po) AS received_po
            , SUM(received_po_usd) AS received_po_usd

            , SUM(sell_out) AS sell_out
            , SUM(sell_out_usd) AS sell_out_usd
            , SUM(sell_out_with_p70) AS sell_out_with_p70
            , SUM(sell_out_with_p80) AS sell_out_with_p80
            , SUM(sell_out_with_p90) AS sell_out_with_p90
            , SUM(sell_out_with_mean) AS sell_out_with_mean

            , ARRAY_AGG(row_type ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS row_type
            , SUBSTRING(MAX(time), 1, 4) AS year

            , ARRAY_AGG(ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout
            , ARRAY_AGG(ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_usd
            , ARRAY_AGG(ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po
            , ARRAY_AGG(ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po_usd
            , ARRAY_AGG(ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p70
            , ARRAY_AGG(ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p80
            , ARRAY_AGG(ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p90
            , ARRAY_AGG(ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_mean

            , ARRAY_AGG(prev_ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout
            , ARRAY_AGG(prev_ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_usd
            , ARRAY_AGG(prev_ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po
            , ARRAY_AGG(prev_ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po_usd
            , ARRAY_AGG(prev_ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p70
            , ARRAY_AGG(prev_ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p80
            , ARRAY_AGG(prev_ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p90
            , ARRAY_AGG(prev_ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_mean

            , ARRAY_AGG(sell_out_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos
            , ARRAY_AGG(sell_out_sum_for_wos_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos_usd
            , ARRAY_AGG(sell_out_with_p70_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p70_sum_for_wos
            , ARRAY_AGG(sell_out_with_p80_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p80_sum_for_wos
            , ARRAY_AGG(sell_out_with_p90_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p90_sum_for_wos
            , ARRAY_AGG(sell_out_with_mean_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_mean_sum_for_wos


            , ARRAY_AGG(ending_inv ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv
            , ARRAY_AGG(ending_inv_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_usd
            , ARRAY_AGG(ending_inv_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p70
            , ARRAY_AGG(ending_inv_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p80
            , ARRAY_AGG(ending_inv_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p90
            , ARRAY_AGG(ending_inv_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_mean

        FROM
            cte_month_n_week_final
        WHERE
            time_level = 'MONTH'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    )
SELECT
    *
    , IF(row_type = 'fcst', ROW_NUMBER() OVER (PARTITION BY country, division, category, single_category, zinus_sku, time_level, row_type ORDER BY time), 0) AS row_num
FROM
    cte_month_n_week_final

UNION ALL

SELECT
    *
    , if(row_type = 'fcst', ROW_NUMBER() OVER (PARTITION BY country, division, category, single_category, zinus_sku, time_level, row_type ORDER BY time), 0) AS row_num
FROM
    cte_year_n_quarter_final
;

-- select PARSE_DATE('%G-%V', '2025-43');
-- select SUBSTR('202543', 1,4)||'-'||SUBSTR('202543', 5,2)

CREATE OR REPLACE TABLE mart.amz_di_global_psi_report_detail AS
WITH
    cte_add_last_time AS (
        SELECT
            *
            ,   CASE time_level
                    WHEN 'YEAR' THEN
                        CAST(CAST(time AS INT64) - 1 AS STRING)

                    WHEN 'QUARTER' THEN
                        CONCAT(
                                CAST(IF(SAFE_CAST(REGEXP_EXTRACT(time, r'Q([1-4])') AS INT64) = 1, SAFE_CAST(SUBSTR(time, 1, 4) AS INT64) - 1, SAFE_CAST(SUBSTR(time, 1, 4) AS INT64)) AS STRING),
                                'Q',
                                CAST(IF(SAFE_CAST(REGEXP_EXTRACT(time, r'Q([1-4])') AS INT64) = 1, 4, SAFE_CAST(REGEXP_EXTRACT(time, r'Q([1-4])') AS INT64) - 1) AS STRING)
                        )

                    WHEN 'MONTH' THEN
                        FORMAT_DATE('%Y%m', DATE_SUB(PARSE_DATE('%Y%m', time), INTERVAL 1 MONTH))

                    WHEN 'WEEK' THEN
                        FORMAT_DATE('%G%V', DATE_SUB(PARSE_DATE('%G-%V', SUBSTR(time, 1,4)||'-'||SUBSTR(time, 5,2)), INTERVAL 7 DAY))

                    ELSE NULL
                END AS last_time
        FROM
            tmp1.amz_di_global_psi_report_detail
    )
SELECT
    a.*
    , b.sell_out as last_sell_out
    , b.sell_out_usd as last_sell_out_usd
    , b.received_po as last_received_po
    , b.received_po_usd as last_received_po_usd

    , b.ending_inv as last_ending_inv
    , b.ending_inv_usd as last_ending_inv_usd

    , b.ending_inv_with_p70 as last_ending_inv_with_p70
    , b.ending_inv_with_p80 as last_ending_inv_with_p80
    , b.ending_inv_with_p90 as last_ending_inv_with_p90
    , b.ending_inv_with_mean as last_ending_inv_with_mean

    , b.sell_out_sum_for_wos as last_sell_out_sum_for_wos
    , b.sell_out_with_p70_sum_for_wos as last_sell_out_with_p70_sum_for_wos
    , b.sell_out_with_p80_sum_for_wos as last_sell_out_with_p80_sum_for_wos
    , b.sell_out_with_p90_sum_for_wos as last_sell_out_with_p90_sum_for_wos
    , b.sell_out_with_mean_sum_for_wos as last_sell_out_with_mean_sum_for_wos

FROM
    cte_add_last_time a
        LEFT JOIN cte_add_last_time b
            ON a.time_level = b.time_level AND a.last_time = b.time
                AND a.country = b.country
                AND a.division = b.division
                AND a.category = b.category
                AND a.single_category = b.single_category
                AND a.zinus_sku = b.zinus_sku
;

-- [psi collection report] -------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp1.amz_di_global_psi_report_by_collection AS
WITH
    cte_fcst as (
        WITH cte_fcst_src as (
                SELECT
                    a.asin
                    , a.Company as country
                    , a.type
                    , a.financial_category
                    , a.collection
                    , week0, week1, week2, week3, week4, week5, week6, week7, week8, week9, week10, week11, week12, week13, week14, week15, week16, week17, week18, week19, week20, week21, week22, week23, week24, week25, week26, week27, week28, week29, week30, week31, week32, week33, week34, week35, week36, week37, week38, week39, week40, week41, week42, week43, week44, week45, week46, week47
                    , b.yr_month
                    , b.yr_wk
                FROM
                    mart.amz_global_fcst_all a
                        LEFT JOIN meta.wk_calendar_new b
                            ON DATE_SUB(a.date, INTERVAL 1 WEEK) BETWEEN b.start_date AND b.end_date
--                 WHERE
--                     a.Type='p70'
--                     AND asin = 'B006MIPW70'
--                     AND Company = 'US'
            )
            , cte_fcst_month_week_union as (
                SELECT
                    * EXCEPT (yr_wk, yr_month)

                    , yr_month AS yr_month_or_week
                    , 'MONTH' AS period_type
                FROM
                    cte_fcst_src
                QUALIFY
                    ROW_NUMBER() OVER (PARTITION BY asin, country, type, yr_month ORDER BY yr_wk DESC) = 1

                UNION ALL

                SELECT
                    * EXCEPT (yr_wk, yr_month)
                    , yr_wk AS yr_month_or_week
                    , 'WEEK' AS period_type
                FROM
                    cte_fcst_src
            )
        SELECT
            COALESCE(country, 'UNKNOWN') AS country
            , IF(COALESCE(financial_category, 'UNKNOWN') IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS division
            , COALESCE(financial_category, 'UNKNOWN') AS category
            , COALESCE(collection, 'UNKNOWN') AS collection
            , type
            , period_type
            , CAST(yr_month_or_week AS STRING) as yr_month_or_week
            , sum(week0) as week0, sum(week1) as week1, sum(week2) as week2, sum(week3) as week3, sum(week4) as week4, sum(week5) as week5, sum(week6) as week6, sum(week7) as week7, sum(week8) as week8, sum(week9) as week9, sum(week10) as week10, sum(week11) as week11, sum(week12) as week12, sum(week13) as week13, sum(week14) as week14, sum(week15) as week15, sum(week16) as week16, sum(week17) as week17, sum(week18) as week18, sum(week19) as week19, sum(week20) as week20, sum(week21) as week21, sum(week22) as week22, sum(week23) as week23, sum(week24) as week24, sum(week25) as week25, sum(week26) as week26, sum(week27) as week27, sum(week28) as week28, sum(week29) as week29, sum(week30) as week30, sum(week31) as week31, sum(week32) as week32, sum(week33) as week33, sum(week34) as week34, sum(week35) as week35, sum(week36) as week36, sum(week37) as week37, sum(week38) as week38, sum(week39) as week39, sum(week40) as week40, sum(week41) as week41, sum(week42) as week42, sum(week43) as week43, sum(week44) as week44, sum(week45) as week45, sum(week46) as week46, sum(week47) as week47
        FROM
            cte_fcst_month_week_union
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    )
    , cte_origin_src AS (
        SELECT
            f.asin
            , COALESCE(country, 'UNKNOWN') AS country
            , IF(COALESCE(f.financial_category, 'UNKNOWN') IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS division
            , COALESCE(f.financial_category, 'UNKNOWN') AS category
            , COALESCE(collection, 'UNKNOWN') AS collection

            , yr_month_or_week

            , period_type

            , open_purchase_order_quantity
            , open_purchase_order_quantity * ( sellable_on_hand_inventory / IF(COALESCE(sellable_on_hand_units, 0) = 0, 1, sellable_on_hand_units) ) AS open_purchase_order_amount
            , net_received_units
            , net_received
            , sellable_on_hand_units
            , sellable_on_hand_inventory
            , shipped_units
            , shipped_revenue

        FROM
            mart.amz_di_global  f
        WHERE
            is_closed=TRUE

    )
    , cte_fill_date as (
        SELECT
            country
            , asin
            , 'WEEK' as period_type
            , fill_date
        FROM
            (
                SELECT
                    country
                    , asin
                    , PARSE_DATE('%G-%V', CONCAT(SUBSTRING(CAST(MIN(yr_month_or_week) AS STRING), 1, 4), '-', SUBSTRING(CAST(MIN(yr_month_or_week) AS STRING), 5, 2))) AS min_wk
                    , PARSE_DATE('%G-%V', CONCAT(SUBSTRING(CAST(MAX(yr_month_or_week) AS STRING), 1, 4), '-', SUBSTRING(CAST(MAX(yr_month_or_week) AS STRING), 5, 2))) AS max_wk
                FROM cte_origin_src
                WHERE
                    period_type='WEEK'
                GROUP BY 1, 2

            ) AS t_r
                CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_wk, t_r.max_wk, INTERVAL 1 WEEK)) AS date_val
                , UNNEST([CAST(FORMAT_DATE('%G%V', date_val) AS INT64)]) AS fill_date

        UNION ALL

        SELECT
            country
            , asin
            , 'MONTH' as period_type
            , fill_date
        FROM
            (
                SELECT
                    country
                    , asin
                    , PARSE_DATE('%Y%m', CAST(MIN(yr_month_or_week) AS STRING)) AS min_m
                    , PARSE_DATE('%Y%m', CAST(MAX(yr_month_or_week) AS STRING)) AS max_m
                FROM cte_origin_src
                WHERE
                    period_type='MONTH'
                GROUP BY 1, 2

            ) AS t_r
                CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(t_r.min_m, t_r.max_m, INTERVAL 1 MONTH )) AS date_val
                , UNNEST([CAST(FORMAT_DATE('%Y%m', date_val) AS INT64)]) AS fill_date
    )

    , cte_fill_src as (
        SELECT
            d.country
            , IF(division IS NULL, FIRST_VALUE(division IGNORE NULLS) OVER (PARTITION BY d.country, d.asin ORDER BY d.fill_date DESC), division) AS division
            , IF(category IS NULL, FIRST_VALUE(category IGNORE NULLS) OVER (PARTITION BY d.country, d.asin ORDER BY d.fill_date DESC), category) AS category
            , IF(collection IS NULL, FIRST_VALUE(collection IGNORE NULLS) OVER (PARTITION BY d.country, d.asin ORDER BY d.fill_date DESC), collection) AS collection
            , CAST(d.fill_date AS STRING) AS yr_month_or_week
            , fill_date

            , d.period_type
            , open_purchase_order_quantity
            , open_purchase_order_amount
            , net_received_units
            , net_received
            , sellable_on_hand_units
            , sellable_on_hand_inventory
            , shipped_units
            , shipped_revenue

--             , IF(d.period_type = 'WEEK', CAST(b.yr_month AS STRING), NULL) AS yr_month_for_week
        FROM
            cte_fill_date d
                LEFT JOIN cte_origin_src s
                    ON d.asin = s.asin AND d.country = s.country AND d.period_type = s.period_type AND d.fill_date = CAST(s.yr_month_or_week as INT64)
                LEFT JOIN meta.wk_calendar_new b
                    ON d.fill_date = b.yr_wk
    )
    , cte_actual_collection_src AS (
        SELECT
            country
            , division
            , category
            , collection
--             , CAST(yr_month_or_week AS STRING) AS yr_month_or_week
            , yr_month_or_week
            , period_type
            , SUM(open_purchase_order_quantity) as open_purchase_order_quantity
            , SUM(open_purchase_order_amount) AS open_purchase_order_amount
            , SUM(net_received_units) as net_received_units
            , SUM(net_received) as net_received
            , SUM(sellable_on_hand_units) as sellable_on_hand_units
            , SUM(sellable_on_hand_inventory) as sellable_on_hand_inventory
            , SUM(shipped_units) as shipped_units
            , SUM(shipped_revenue) as shipped_revenue

--             , IF(period_type = 'WEEK', CAST(MAX(b.yr_month) AS STRING), NULL) AS yr_month_for_week
        FROM
--             cte_origin_src a
            cte_fill_src a

--                 LEFT JOIN meta.wk_calendar_new b
--                     ON a.fill_date = b.yr_wk

--         WHERE
        --             and a.country='US'
        --             and a.country='CA'
        --             and a.country='FR'
        GROUP BY 1, 2, 3, 4, 5, 6
    )
    , cte_final_week_row_value as (
        SELECT
            a.country
            , a.division
            , a.category
            , a.collection
            , a.period_type
            , a.yr_month_or_week

            --             , c.open_purchase_order_quantity
            --             , c.open_purchase_order_amount

            , a.open_purchase_order_quantity
            , a.open_purchase_order_amount

            , a.sellable_on_hand_units -- ending inv
            , a.sellable_on_hand_inventory -- ending inv usd

            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN CURRENT ROW AND 25 FOLLOWING) AS sell_out_13_sum
            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN 52 FOLLOWING AND 77 FOLLOWING) AS sell_out_last_year_13_sum

            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN CURRENT ROW AND 25 FOLLOWING) AS sell_out_usd_13_sum
            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN 52 FOLLOWING AND 77 FOLLOWING) AS sell_out_usd_last_year_13_sum

            , LEAD(shipped_units, 51) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst1
            , LEAD(shipped_units, 50) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst2
            , LEAD(shipped_units, 49) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst3
            , LEAD(shipped_units, 48) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst4
            , LEAD(shipped_units, 47) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst5
            , LEAD(shipped_units, 46) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst6
            , LEAD(shipped_units, 45) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst7
            , LEAD(shipped_units, 44) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst8
            , LEAD(shipped_units, 43) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst9
            , LEAD(shipped_units, 42) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst10
            , LEAD(shipped_units, 41) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst11
            , LEAD(shipped_units, 40) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst12
            , LEAD(shipped_units, 39) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst13
            , LEAD(shipped_units, 38) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst14
            , LEAD(shipped_units, 37) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst15
            , LEAD(shipped_units, 36) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst16
            , LEAD(shipped_units, 35) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst17
            , LEAD(shipped_units, 34) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst18
            , LEAD(shipped_units, 33) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst19
            , LEAD(shipped_units, 32) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst20
            , LEAD(shipped_units, 31) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst21
            , LEAD(shipped_units, 30) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst22
            , LEAD(shipped_units, 29) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst23
            , LEAD(shipped_units, 28) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst24
            , LEAD(shipped_units, 27) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst25
            , LEAD(shipped_units, 26) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst26

            , LEAD(shipped_revenue, 51) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd1
            , LEAD(shipped_revenue, 50) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd2
            , LEAD(shipped_revenue, 49) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd3
            , LEAD(shipped_revenue, 48) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd4
            , LEAD(shipped_revenue, 47) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd5
            , LEAD(shipped_revenue, 46) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd6
            , LEAD(shipped_revenue, 45) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd7
            , LEAD(shipped_revenue, 44) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd8
            , LEAD(shipped_revenue, 43) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd9
            , LEAD(shipped_revenue, 42) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd10
            , LEAD(shipped_revenue, 41) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd11
            , LEAD(shipped_revenue, 40) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd12
            , LEAD(shipped_revenue, 39) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd13
            , LEAD(shipped_revenue, 38) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd14
            , LEAD(shipped_revenue, 37) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd15
            , LEAD(shipped_revenue, 36) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd16
            , LEAD(shipped_revenue, 35) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd17
            , LEAD(shipped_revenue, 34) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd18
            , LEAD(shipped_revenue, 33) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd19
            , LEAD(shipped_revenue, 32) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd20
            , LEAD(shipped_revenue, 31) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd21
            , LEAD(shipped_revenue, 30) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd22
            , LEAD(shipped_revenue, 29) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd23
            , LEAD(shipped_revenue, 28) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd24
            , LEAD(shipped_revenue, 27) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd25
            , LEAD(shipped_revenue, 26) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd26

            , p70.week0 as p70_week0, p70.week1 as p70_week1, p70.week2 as p70_week2, p70.week3 as p70_week3, p70.week4 as p70_week4, p70.week5 as p70_week5, p70.week6 as p70_week6, p70.week7 as p70_week7, p70.week8 as p70_week8, p70.week9 as p70_week9, p70.week10 as p70_week10, p70.week11 as p70_week11, p70.week12 as p70_week12, p70.week13 as p70_week13, p70.week14 as p70_week14, p70.week15 as p70_week15, p70.week16 as p70_week16, p70.week17 as p70_week17, p70.week18 as p70_week18, p70.week19 as p70_week19, p70.week20 as p70_week20, p70.week21 as p70_week21, p70.week22 as p70_week22, p70.week23 as p70_week23, p70.week24 as p70_week24, p70.week25 as p70_week25, p70.week26 as p70_week26, p70.week27 as p70_week27, p70.week28 as p70_week28, p70.week29 as p70_week29, p70.week30 as p70_week30, p70.week31 as p70_week31, p70.week32 as p70_week32, p70.week33 as p70_week33, p70.week34 as p70_week34, p70.week35 as p70_week35, p70.week36 as p70_week36, p70.week37 as p70_week37, p70.week38 as p70_week38, p70.week39 as p70_week39, p70.week40 as p70_week40, p70.week41 as p70_week41, p70.week42 as p70_week42, p70.week43 as p70_week43, p70.week44 as p70_week44, p70.week45 as p70_week45, p70.week46 as p70_week46, p70.week47 as p70_week47
            , p80.week0 as p80_week0, p80.week1 as p80_week1, p80.week2 as p80_week2, p80.week3 as p80_week3, p80.week4 as p80_week4, p80.week5 as p80_week5, p80.week6 as p80_week6, p80.week7 as p80_week7, p80.week8 as p80_week8, p80.week9 as p80_week9, p80.week10 as p80_week10, p80.week11 as p80_week11, p80.week12 as p80_week12, p80.week13 as p80_week13, p80.week14 as p80_week14, p80.week15 as p80_week15, p80.week16 as p80_week16, p80.week17 as p80_week17, p80.week18 as p80_week18, p80.week19 as p80_week19, p80.week20 as p80_week20, p80.week21 as p80_week21, p80.week22 as p80_week22, p80.week23 as p80_week23, p80.week24 as p80_week24, p80.week25 as p80_week25, p80.week26 as p80_week26, p80.week27 as p80_week27, p80.week28 as p80_week28, p80.week29 as p80_week29, p80.week30 as p80_week30, p80.week31 as p80_week31, p80.week32 as p80_week32, p80.week33 as p80_week33, p80.week34 as p80_week34, p80.week35 as p80_week35, p80.week36 as p80_week36, p80.week37 as p80_week37, p80.week38 as p80_week38, p80.week39 as p80_week39, p80.week40 as p80_week40, p80.week41 as p80_week41, p80.week42 as p80_week42, p80.week43 as p80_week43, p80.week44 as p80_week44, p80.week45 as p80_week45, p80.week46 as p80_week46, p80.week47 as p80_week47
            , p90.week0 as p90_week0, p90.week1 as p90_week1, p90.week2 as p90_week2, p90.week3 as p90_week3, p90.week4 as p90_week4, p90.week5 as p90_week5, p90.week6 as p90_week6, p90.week7 as p90_week7, p90.week8 as p90_week8, p90.week9 as p90_week9, p90.week10 as p90_week10, p90.week11 as p90_week11, p90.week12 as p90_week12, p90.week13 as p90_week13, p90.week14 as p90_week14, p90.week15 as p90_week15, p90.week16 as p90_week16, p90.week17 as p90_week17, p90.week18 as p90_week18, p90.week19 as p90_week19, p90.week20 as p90_week20, p90.week21 as p90_week21, p90.week22 as p90_week22, p90.week23 as p90_week23, p90.week24 as p90_week24, p90.week25 as p90_week25, p90.week26 as p90_week26, p90.week27 as p90_week27, p90.week28 as p90_week28, p90.week29 as p90_week29, p90.week30 as p90_week30, p90.week31 as p90_week31, p90.week32 as p90_week32, p90.week33 as p90_week33, p90.week34 as p90_week34, p90.week35 as p90_week35, p90.week36 as p90_week36, p90.week37 as p90_week37, p90.week38 as p90_week38, p90.week39 as p90_week39, p90.week40 as p90_week40, p90.week41 as p90_week41, p90.week42 as p90_week42, p90.week43 as p90_week43, p90.week44 as p90_week44, p90.week45 as p90_week45, p90.week46 as p90_week46, p90.week47 as p90_week47
            , mean.week0 as mean_week0, mean.week1 as mean_week1, mean.week2 as mean_week2, mean.week3 as mean_week3, mean.week4 as mean_week4, mean.week5 as mean_week5, mean.week6 as mean_week6, mean.week7 as mean_week7, mean.week8 as mean_week8, mean.week9 as mean_week9, mean.week10 as mean_week10, mean.week11 as mean_week11, mean.week12 as mean_week12, mean.week13 as mean_week13, mean.week14 as mean_week14, mean.week15 as mean_week15, mean.week16 as mean_week16, mean.week17 as mean_week17, mean.week18 as mean_week18, mean.week19 as mean_week19, mean.week20 as mean_week20, mean.week21 as mean_week21, mean.week22 as mean_week22, mean.week23 as mean_week23, mean.week24 as mean_week24, mean.week25 as mean_week25, mean.week26 as mean_week26, mean.week27 as mean_week27, mean.week28 as mean_week28, mean.week29 as mean_week29, mean.week30 as mean_week30, mean.week31 as mean_week31, mean.week32 as mean_week32, mean.week33 as mean_week33, mean.week34 as mean_week34, mean.week35 as mean_week35, mean.week36 as mean_week36, mean.week37 as mean_week37, mean.week38 as mean_week38, mean.week39 as mean_week39, mean.week40 as mean_week40, mean.week41 as mean_week41, mean.week42 as mean_week42, mean.week43 as mean_week43, mean.week44 as mean_week44, mean.week45 as mean_week45, mean.week46 as mean_week46, mean.week47 as mean_week47
        FROM
            cte_actual_collection_src a
        --                 LEFT JOIN cte_final_month_row_value c
        --                     ON a.country = c.country AND a.division = c.division AND a.category = c.category AND a.origin = c.origin
                LEFT JOIN cte_fcst p70
                    ON a.collection = p70.collection AND a.country = p70.country AND a.division = p70.division AND a.category = p70.category AND a.period_type = p70.period_type AND a.yr_month_or_week = p70.yr_month_or_week AND p70.type='p70'
                LEFT JOIN cte_fcst p80
                    ON a.collection = p80.collection AND a.country = p80.country AND a.division = p80.division AND a.category = p80.category AND a.period_type = p80.period_type AND a.yr_month_or_week = p80.yr_month_or_week AND p80.type='p80'
                LEFT JOIN cte_fcst p90
                    ON a.collection = p90.collection AND a.country = p90.country AND a.division = p90.division AND a.category = p90.category AND a.period_type = p90.period_type AND a.yr_month_or_week = p90.yr_month_or_week AND p90.type='p90'
                LEFT JOIN cte_fcst mean
                    ON a.collection = mean.collection AND a.country = mean.country AND a.division = mean.division AND a.category = mean.category AND a.period_type = mean.period_type AND a.yr_month_or_week = mean.yr_month_or_week AND mean.type = 'mean'
        WHERE
            a.period_type = 'WEEK'
        --         QUALIFY ROW_NUMBER() OVER (PARTITION BY a.country, a.division, a.category, a.origin, a.period_type ORDER BY a.yr_month_or_week DESC) = 1
        QUALIFY RANK() OVER (PARTITION BY a.country, a.period_type ORDER BY a.yr_month_or_week DESC) = 1
    )
    -- SELECT * FROM cte_final_week_row_value where country='MX';
    , cte_final_month_row_value AS (
        SELECT
            a.country
            , a.division
            , a.category
            , a.collection
            , a.period_type
            , a.yr_month_or_week

            , w.open_purchase_order_quantity
            , w.open_purchase_order_amount
            --             , a.open_purchase_order_quantity -- fcst received po
            --             , a.open_purchase_order_amount -- fcst received po usd

            , a.sellable_on_hand_units -- ending inv
            , a.sellable_on_hand_inventory -- ending inv usd

            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) AS sell_out_6_sum
            , SUM(shipped_units) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN 12 FOLLOWING AND 17 FOLLOWING) AS sell_out_last_year_6_sum

            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) AS sell_out_usd_6_sum
            , SUM(shipped_revenue) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC ROWS BETWEEN 12 FOLLOWING AND 17 FOLLOWING) AS sell_out_usd_last_year_6_sum

            , LEAD(shipped_units, 11) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst1
            , LEAD(shipped_units, 10) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst2
            , LEAD(shipped_units, 9) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst3
            , LEAD(shipped_units, 8) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst4
            , LEAD(shipped_units, 7) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst5
            , LEAD(shipped_units, 6) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst6
            , LEAD(shipped_units, 5) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst7
            , LEAD(shipped_units, 4) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst8
            , LEAD(shipped_units, 3) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst9
            , LEAD(shipped_units, 2) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst10
            , LEAD(shipped_units, 1) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst11
            , shipped_units AS sell_out_fcst12

            , LEAD(shipped_revenue, 11) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd1
            , LEAD(shipped_revenue, 10) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd2
            , LEAD(shipped_revenue, 9) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd3
            , LEAD(shipped_revenue, 8) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd4
            , LEAD(shipped_revenue, 7) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd5
            , LEAD(shipped_revenue, 6) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd6
            , LEAD(shipped_revenue, 5) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd7
            , LEAD(shipped_revenue, 4) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd8
            , LEAD(shipped_revenue, 3) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd9
            , LEAD(shipped_revenue, 2) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd10
            , LEAD(shipped_revenue, 1) OVER (PARTITION BY a.country, a.division, a.category, a.collection, a.period_type ORDER BY a.yr_month_or_week DESC) AS sell_out_fcst_usd11
            , shipped_revenue AS sell_out_fcst_usd12

            , w.p70_week0, w.p70_week1, w.p70_week2, w.p70_week3, w.p70_week4, w.p70_week5, w.p70_week6, w.p70_week7, w.p70_week8, w.p70_week9, w.p70_week10, w.p70_week11, w.p70_week12, w.p70_week13, w.p70_week14, w.p70_week15, w.p70_week16, w.p70_week17, w.p70_week18, w.p70_week19, w.p70_week20, w.p70_week21, w.p70_week22, w.p70_week23, w.p70_week24, w.p70_week25, w.p70_week26, w.p70_week27, w.p70_week28, w.p70_week29, w.p70_week30, w.p70_week31, w.p70_week32, w.p70_week33, w.p70_week34, w.p70_week35, w.p70_week36, w.p70_week37, w.p70_week38, w.p70_week39, w.p70_week40, w.p70_week41, w.p70_week42, w.p70_week43, w.p70_week44, w.p70_week45, w.p70_week46, w.p70_week47
            , w.p80_week0, w.p80_week1, w.p80_week2, w.p80_week3, w.p80_week4, w.p80_week5, w.p80_week6, w.p80_week7, w.p80_week8, w.p80_week9, w.p80_week10, w.p80_week11, w.p80_week12, w.p80_week13, w.p80_week14, w.p80_week15, w.p80_week16, w.p80_week17, w.p80_week18, w.p80_week19, w.p80_week20, w.p80_week21, w.p80_week22, w.p80_week23, w.p80_week24, w.p80_week25, w.p80_week26, w.p80_week27, w.p80_week28, w.p80_week29, w.p80_week30, w.p80_week31, w.p80_week32, w.p80_week33, w.p80_week34, w.p80_week35, w.p80_week36, w.p80_week37, w.p80_week38, w.p80_week39, w.p80_week40, w.p80_week41, w.p80_week42, w.p80_week43, w.p80_week44, w.p80_week45, w.p80_week46, w.p80_week47
            , w.p90_week0, w.p90_week1, w.p90_week2, w.p90_week3, w.p90_week4, w.p90_week5, w.p90_week6, w.p90_week7, w.p90_week8, w.p90_week9, w.p90_week10, w.p90_week11, w.p90_week12, w.p90_week13, w.p90_week14, w.p90_week15, w.p90_week16, w.p90_week17, w.p90_week18, w.p90_week19, w.p90_week20, w.p90_week21, w.p90_week22, w.p90_week23, w.p90_week24, w.p90_week25, w.p90_week26, w.p90_week27, w.p90_week28, w.p90_week29, w.p90_week30, w.p90_week31, w.p90_week32, w.p90_week33, w.p90_week34, w.p90_week35, w.p90_week36, w.p90_week37, w.p90_week38, w.p90_week39, w.p90_week40, w.p90_week41, w.p90_week42, w.p90_week43, w.p90_week44, w.p90_week45, w.p90_week46, w.p90_week47
            , w.mean_week0, w.mean_week1, w.mean_week2, w.mean_week3, w.mean_week4, w.mean_week5, w.mean_week6, w.mean_week7, w.mean_week8, w.mean_week9, w.mean_week10, w.mean_week11, w.mean_week12, w.mean_week13, w.mean_week14, w.mean_week15, w.mean_week16, w.mean_week17, w.mean_week18, w.mean_week19, w.mean_week20, w.mean_week21, w.mean_week22, w.mean_week23, w.mean_week24, w.mean_week25, w.mean_week26, w.mean_week27, w.mean_week28, w.mean_week29, w.mean_week30, w.mean_week31, w.mean_week32, w.mean_week33, w.mean_week34, w.mean_week35, w.mean_week36, w.mean_week37, w.mean_week38, w.mean_week39, w.mean_week40, w.mean_week41, w.mean_week42, w.mean_week43, w.mean_week44, w.mean_week45, w.mean_week46, w.mean_week47
        FROM
            cte_actual_collection_src a
                LEFT JOIN cte_final_week_row_value w
                    ON a.country = w.country AND a.division = w.division AND a.category = w.category AND a.collection = w.collection
        WHERE
            a.period_type = 'MONTH'
        --         QUALIFY ROW_NUMBER() OVER (PARTITION BY country, division, category, origin, period_type ORDER BY yr_month_or_week DESC) = 1
        QUALIFY RANK() OVER (PARTITION BY a.country, a.period_type ORDER BY a.yr_month_or_week DESC) = 1
    )
    --     SELECT sell_out_fcst1 * (sell_out_6_sum / sell_out_last_year_6_sum), * FROM cte_final_month_row_value;
    --    select * from cte_final_month_row_value;
    , cte_fcst_collection_src as (
        SELECT
            country
            , division
            , category
            , collection

            , FORMAT_DATE('%Y%m', DATE_ADD(PARSE_DATE('%Y%m', yr_month_or_week), INTERVAL arr MONTH)) AS yr_month_or_week
            , period_type

            , IF(arr <= 3, open_purchase_order_quantity / 3, 0) AS net_received_units
            , IF(arr <= 3, open_purchase_order_amount / 3, 0) AS net_received

            , sellable_on_hand_units
            , sellable_on_hand_inventory

            , CASE arr
                  WHEN 1 THEN sell_out_fcst1 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 2 THEN sell_out_fcst2 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 3 THEN sell_out_fcst3 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 4 THEN sell_out_fcst4 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 5 THEN sell_out_fcst5 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 6 THEN sell_out_fcst6 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 7 THEN sell_out_fcst7 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 8 THEN sell_out_fcst8 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 9 THEN sell_out_fcst9 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 10 THEN sell_out_fcst10 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 11 THEN sell_out_fcst11 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
                  WHEN 12 THEN sell_out_fcst12 * IF(sell_out_last_year_6_sum = 0 OR sell_out_last_year_6_sum IS NULL, 1, ( sell_out_6_sum / sell_out_last_year_6_sum ))
              END AS shipped_units
            , CASE arr
                  WHEN 1 THEN sell_out_fcst_usd1 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 2 THEN sell_out_fcst_usd2 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 3 THEN sell_out_fcst_usd3 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 4 THEN sell_out_fcst_usd4 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 5 THEN sell_out_fcst_usd5 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 6 THEN sell_out_fcst_usd6 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 7 THEN sell_out_fcst_usd7 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 8 THEN sell_out_fcst_usd8 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 9 THEN sell_out_fcst_usd9 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 10 THEN sell_out_fcst_usd10 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 11 THEN sell_out_fcst_usd11 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
                  WHEN 12 THEN sell_out_fcst_usd12 * IF(sell_out_usd_last_year_6_sum = 0 OR sell_out_usd_last_year_6_sum IS NULL, 1, ( sell_out_usd_6_sum / sell_out_usd_last_year_6_sum ))
              END AS shipped_revenue

            , CASE arr
                  WHEN 1 THEN sell_out_fcst1
                  WHEN 2 THEN sell_out_fcst2
                  WHEN 3 THEN sell_out_fcst3
                  WHEN 4 THEN sell_out_fcst4
                  WHEN 5 THEN sell_out_fcst5
                  WHEN 6 THEN sell_out_fcst6
                  WHEN 7 THEN sell_out_fcst7
                  WHEN 8 THEN sell_out_fcst8
                  WHEN 9 THEN sell_out_fcst9
                  WHEN 10 THEN sell_out_fcst10
                  WHEN 11 THEN sell_out_fcst11
                  WHEN 12 THEN sell_out_fcst12
              END AS sell_out_fcst_target

            , CASE arr
                  WHEN 1 THEN sell_out_fcst_usd1
                  WHEN 2 THEN sell_out_fcst_usd2
                  WHEN 3 THEN sell_out_fcst_usd3
                  WHEN 4 THEN sell_out_fcst_usd4
                  WHEN 5 THEN sell_out_fcst_usd5
                  WHEN 6 THEN sell_out_fcst_usd6
                  WHEN 7 THEN sell_out_fcst_usd7
                  WHEN 8 THEN sell_out_fcst_usd8
                  WHEN 9 THEN sell_out_fcst_usd9
                  WHEN 10 THEN sell_out_fcst_usd10
                  WHEN 11 THEN sell_out_fcst_usd11
                  WHEN 12 THEN sell_out_fcst_usd12
              END AS sell_out_fcst_target_usd

            , sell_out_6_sum as sell_out_sum
            , sell_out_last_year_6_sum as sell_out_last_year_sum
            , sell_out_usd_6_sum as sell_out_sum_usd
            , sell_out_usd_last_year_6_sum as sell_out_last_year_sum_usd

            , CASE arr
                  WHEN 1 THEN p70_week0+p70_week1+p70_week2+p70_week3
                  WHEN 2 THEN p70_week4+p70_week5+p70_week6+p70_week7
                  WHEN 3 THEN p70_week8+p70_week9+p70_week10+p70_week11
                  WHEN 4 THEN p70_week12+p70_week13+p70_week14+p70_week15
                  WHEN 5 THEN p70_week16+p70_week17+p70_week18+p70_week19
                  WHEN 6 THEN p70_week20+p70_week21+p70_week22+p70_week23
                  WHEN 7 THEN p70_week24+p70_week25+p70_week26+p70_week27
                  WHEN 8 THEN p70_week28+p70_week29+p70_week30+p70_week31
                  WHEN 9 THEN p70_week32+p70_week33+p70_week34+p70_week35
                  WHEN 10 THEN p70_week36+p70_week37+p70_week38+p70_week39
                  WHEN 11 THEN p70_week40+p70_week41+p70_week42+p70_week43
                  WHEN 12 THEN p70_week44+p70_week45+p70_week46+p70_week47
              END AS p70
            , CASE arr
                  WHEN 1 THEN p80_week0+p80_week1+p80_week2+p80_week3
                  WHEN 2 THEN p80_week4+p80_week5+p80_week6+p80_week7
                  WHEN 3 THEN p80_week8+p80_week9+p80_week10+p80_week11
                  WHEN 4 THEN p80_week12+p80_week13+p80_week14+p80_week15
                  WHEN 5 THEN p80_week16+p80_week17+p80_week18+p80_week19
                  WHEN 6 THEN p80_week20+p80_week21+p80_week22+p80_week23
                  WHEN 7 THEN p80_week24+p80_week25+p80_week26+p80_week27
                  WHEN 8 THEN p80_week28+p80_week29+p80_week30+p80_week31
                  WHEN 9 THEN p80_week32+p80_week33+p80_week34+p80_week35
                  WHEN 10 THEN p80_week36+p80_week37+p80_week38+p80_week39
                  WHEN 11 THEN p80_week40+p80_week41+p80_week42+p80_week43
                  WHEN 12 THEN p80_week44+p80_week45+p80_week46+p80_week47
              END AS p80
            , CASE arr
                  WHEN 1 THEN p90_week0+p90_week1+p90_week2+p90_week3
                  WHEN 2 THEN p90_week4+p90_week5+p90_week6+p90_week7
                  WHEN 3 THEN p90_week8+p90_week9+p90_week10+p90_week11
                  WHEN 4 THEN p90_week12+p90_week13+p90_week14+p90_week15
                  WHEN 5 THEN p90_week16+p90_week17+p90_week18+p90_week19
                  WHEN 6 THEN p90_week20+p90_week21+p90_week22+p90_week23
                  WHEN 7 THEN p90_week24+p90_week25+p90_week26+p90_week27
                  WHEN 8 THEN p90_week28+p90_week29+p90_week30+p90_week31
                  WHEN 9 THEN p90_week32+p90_week33+p90_week34+p90_week35
                  WHEN 10 THEN p90_week36+p90_week37+p90_week38+p90_week39
                  WHEN 11 THEN p90_week40+p90_week41+p90_week42+p90_week43
                  WHEN 12 THEN p90_week44+p90_week45+p90_week46+p90_week47
              END AS p90
            , CASE arr
                  WHEN 1 THEN mean_week0+mean_week1+mean_week2+mean_week3
                  WHEN 2 THEN mean_week4+mean_week5+mean_week6+mean_week7
                  WHEN 3 THEN mean_week8+mean_week9+mean_week10+mean_week11
                  WHEN 4 THEN mean_week12+mean_week13+mean_week14+mean_week15
                  WHEN 5 THEN mean_week16+mean_week17+mean_week18+mean_week19
                  WHEN 6 THEN mean_week20+mean_week21+mean_week22+mean_week23
                  WHEN 7 THEN mean_week24+mean_week25+mean_week26+mean_week27
                  WHEN 8 THEN mean_week28+mean_week29+mean_week30+mean_week31
                  WHEN 9 THEN mean_week32+mean_week33+mean_week34+mean_week35
                  WHEN 10 THEN mean_week36+mean_week37+mean_week38+mean_week39
                  WHEN 11 THEN mean_week40+mean_week41+mean_week42+mean_week43
                  WHEN 12 THEN mean_week44+mean_week45+mean_week46+mean_week47
              END AS mean

        FROM
            cte_final_month_row_value
                CROSS JOIN UNNEST(GENERATE_ARRAY(1, 12, 1)) AS arr

        UNION ALL

        SELECT
            country
            , division
            , category
            , collection
            -- , yr_month_or_week
            , FORMAT_DATE('%G%V', DATE_ADD(PARSE_DATE('%G-%V', concat(SUBSTRING(yr_month_or_week, 1, 4), '-', SUBSTRING(yr_month_or_week, 5, 2))), INTERVAL arr WEEK)) AS yr_month_or_week
            , period_type

            -- , open_purchase_order_quantity
            , IF(arr <= 13, open_purchase_order_quantity/13, 0) AS net_received_units
            , IF(arr <= 13, open_purchase_order_amount/13, 0) AS net_received

            , sellable_on_hand_units
            , sellable_on_hand_inventory

            , CASE arr
                  WHEN 1  THEN sell_out_fcst1 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 2  THEN sell_out_fcst2 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 3  THEN sell_out_fcst3 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 4  THEN sell_out_fcst4 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 5  THEN sell_out_fcst5 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 6  THEN sell_out_fcst6 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 7  THEN sell_out_fcst7 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 8  THEN sell_out_fcst8 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 9  THEN sell_out_fcst9 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 10 THEN sell_out_fcst10 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 11 THEN sell_out_fcst11 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 12 THEN sell_out_fcst12 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 13 THEN sell_out_fcst13 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 14 THEN sell_out_fcst14 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 15 THEN sell_out_fcst15 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 16 THEN sell_out_fcst16 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 17 THEN sell_out_fcst17 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 18 THEN sell_out_fcst18 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 19 THEN sell_out_fcst19 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 20 THEN sell_out_fcst20 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 21 THEN sell_out_fcst21 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 22 THEN sell_out_fcst22 * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 23 THEN sell_out_fcst23  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 24 THEN sell_out_fcst24  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 25 THEN sell_out_fcst25  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
                  WHEN 26 THEN sell_out_fcst26  * IF(sell_out_last_year_13_sum = 0 OR sell_out_last_year_13_sum IS NULL, 1, ( sell_out_13_sum / sell_out_last_year_13_sum ))
              END AS shipped_units
            , CASE arr
                  WHEN 1  THEN sell_out_fcst_usd1 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 2  THEN sell_out_fcst_usd2 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 3  THEN sell_out_fcst_usd3 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 4  THEN sell_out_fcst_usd4 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 5  THEN sell_out_fcst_usd5 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 6  THEN sell_out_fcst_usd6 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 7  THEN sell_out_fcst_usd7 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 8  THEN sell_out_fcst_usd8 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 9  THEN sell_out_fcst_usd9 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 10 THEN sell_out_fcst_usd10 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 11 THEN sell_out_fcst_usd11 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 12 THEN sell_out_fcst_usd12 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 13 THEN sell_out_fcst_usd13 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 14 THEN sell_out_fcst_usd14 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 15 THEN sell_out_fcst_usd15 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 16 THEN sell_out_fcst_usd16 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 17 THEN sell_out_fcst_usd17 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 18 THEN sell_out_fcst_usd18 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 19 THEN sell_out_fcst_usd19 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 20 THEN sell_out_fcst_usd20 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 21 THEN sell_out_fcst_usd21 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 22 THEN sell_out_fcst_usd22 * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 23 THEN sell_out_fcst_usd23  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 24 THEN sell_out_fcst_usd24  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 25 THEN sell_out_fcst_usd25  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
                  WHEN 26 THEN sell_out_fcst_usd26  * IF(sell_out_usd_last_year_13_sum = 0 OR sell_out_usd_last_year_13_sum IS NULL, 1, ( sell_out_usd_13_sum / sell_out_usd_last_year_13_sum ))
              END AS shipped_revenue

            , CASE arr
                  WHEN 1 THEN sell_out_fcst1
                  WHEN 2 THEN sell_out_fcst2
                  WHEN 3 THEN sell_out_fcst3
                  WHEN 4 THEN sell_out_fcst4
                  WHEN 5 THEN sell_out_fcst5
                  WHEN 6 THEN sell_out_fcst6
                  WHEN 7 THEN sell_out_fcst7
                  WHEN 8 THEN sell_out_fcst8
                  WHEN 9 THEN sell_out_fcst9
                  WHEN 10 THEN sell_out_fcst10
                  WHEN 11 THEN sell_out_fcst11
                  WHEN 12 THEN sell_out_fcst12
                  WHEN 13 THEN sell_out_fcst13
                  WHEN 14 THEN sell_out_fcst14
                  WHEN 15 THEN sell_out_fcst15
                  WHEN 16 THEN sell_out_fcst16
                  WHEN 17 THEN sell_out_fcst17
                  WHEN 18 THEN sell_out_fcst18
                  WHEN 19 THEN sell_out_fcst19
                  WHEN 20 THEN sell_out_fcst20
                  WHEN 21 THEN sell_out_fcst21
                  WHEN 22 THEN sell_out_fcst22
                  WHEN 23 THEN sell_out_fcst23
                  WHEN 24 THEN sell_out_fcst24
                  WHEN 25 THEN sell_out_fcst25
                  WHEN 26 THEN sell_out_fcst26
              END AS sell_out_fcst_target

            , CASE arr
                  WHEN 1 THEN sell_out_fcst_usd1
                  WHEN 2 THEN sell_out_fcst_usd2
                  WHEN 3 THEN sell_out_fcst_usd3
                  WHEN 4 THEN sell_out_fcst_usd4
                  WHEN 5 THEN sell_out_fcst_usd5
                  WHEN 6 THEN sell_out_fcst_usd6
                  WHEN 7 THEN sell_out_fcst_usd7
                  WHEN 8 THEN sell_out_fcst_usd8
                  WHEN 9 THEN sell_out_fcst_usd9
                  WHEN 10 THEN sell_out_fcst_usd10
                  WHEN 11 THEN sell_out_fcst_usd11
                  WHEN 12 THEN sell_out_fcst_usd12
                  WHEN 13 THEN sell_out_fcst_usd13
                  WHEN 14 THEN sell_out_fcst_usd14
                  WHEN 15 THEN sell_out_fcst_usd15
                  WHEN 16 THEN sell_out_fcst_usd16
                  WHEN 17 THEN sell_out_fcst_usd17
                  WHEN 18 THEN sell_out_fcst_usd18
                  WHEN 19 THEN sell_out_fcst_usd19
                  WHEN 20 THEN sell_out_fcst_usd20
                  WHEN 21 THEN sell_out_fcst_usd21
                  WHEN 22 THEN sell_out_fcst_usd22
                  WHEN 23 THEN sell_out_fcst_usd23
                  WHEN 24 THEN sell_out_fcst_usd24
                  WHEN 25 THEN sell_out_fcst_usd25
                  WHEN 26 THEN sell_out_fcst_usd26
              END AS sell_out_fcst_target_usd

            ,sell_out_13_sum
            , sell_out_last_year_13_sum
            , sell_out_usd_13_sum
            , sell_out_usd_last_year_13_sum


            , CASE arr
                  WHEN 1 THEN p70_week0
                  WHEN 2 THEN p70_week1
                  WHEN 3 THEN p70_week2
                  WHEN 4 THEN p70_week3
                  WHEN 5 THEN p70_week4
                  WHEN 6 THEN p70_week5
                  WHEN 7 THEN p70_week6
                  WHEN 8 THEN p70_week7
                  WHEN 9 THEN p70_week8
                  WHEN 10 THEN p70_week9
                  WHEN 11 THEN p70_week10
                  WHEN 12 THEN p70_week11
                  WHEN 13 THEN p70_week12
                  WHEN 14 THEN p70_week13
                  WHEN 15 THEN p70_week14
                  WHEN 16 THEN p70_week15
                  WHEN 17 THEN p70_week16
                  WHEN 18 THEN p70_week17
                  WHEN 19 THEN p70_week18
                  WHEN 20 THEN p70_week19
                  WHEN 21 THEN p70_week20
                  WHEN 22 THEN p70_week21
                  WHEN 23 THEN p70_week22
                  WHEN 24 THEN p70_week23
                  WHEN 25 THEN p70_week24
                  WHEN 26 THEN p70_week25
              END AS p70
            , CASE arr
                  WHEN 1 THEN p80_week0
                  WHEN 2 THEN p80_week1
                  WHEN 3 THEN p80_week2
                  WHEN 4 THEN p80_week3
                  WHEN 5 THEN p80_week4
                  WHEN 6 THEN p80_week5
                  WHEN 7 THEN p80_week6
                  WHEN 8 THEN p80_week7
                  WHEN 9 THEN p80_week8
                  WHEN 10 THEN p80_week9
                  WHEN 11 THEN p80_week10
                  WHEN 12 THEN p80_week11
                  WHEN 13 THEN p80_week12
                  WHEN 14 THEN p80_week13
                  WHEN 15 THEN p80_week14
                  WHEN 16 THEN p80_week15
                  WHEN 17 THEN p80_week16
                  WHEN 18 THEN p80_week17
                  WHEN 19 THEN p80_week18
                  WHEN 20 THEN p80_week19
                  WHEN 21 THEN p80_week20
                  WHEN 22 THEN p80_week21
                  WHEN 23 THEN p80_week22
                  WHEN 24 THEN p80_week23
                  WHEN 25 THEN p80_week24
                  WHEN 26 THEN p80_week25
              END AS p80
            , CASE arr
                  WHEN 1 THEN p90_week0
                  WHEN 2 THEN p90_week1
                  WHEN 3 THEN p90_week2
                  WHEN 4 THEN p90_week3
                  WHEN 5 THEN p90_week4
                  WHEN 6 THEN p90_week5
                  WHEN 7 THEN p90_week6
                  WHEN 8 THEN p90_week7
                  WHEN 9 THEN p90_week8
                  WHEN 10 THEN p90_week9
                  WHEN 11 THEN p90_week10
                  WHEN 12 THEN p90_week11
                  WHEN 13 THEN p90_week12
                  WHEN 14 THEN p90_week13
                  WHEN 15 THEN p90_week14
                  WHEN 16 THEN p90_week15
                  WHEN 17 THEN p90_week16
                  WHEN 18 THEN p90_week17
                  WHEN 19 THEN p90_week18
                  WHEN 20 THEN p90_week19
                  WHEN 21 THEN p90_week20
                  WHEN 22 THEN p90_week21
                  WHEN 23 THEN p90_week22
                  WHEN 24 THEN p90_week23
                  WHEN 25 THEN p90_week24
                  WHEN 26 THEN p90_week25
              END AS p90
            , CASE arr
                  WHEN 1  THEN mean_week0
                  WHEN 2  THEN mean_week1
                  WHEN 3  THEN mean_week2
                  WHEN 4  THEN mean_week3
                  WHEN 5  THEN mean_week4
                  WHEN 6  THEN mean_week5
                  WHEN 7  THEN mean_week6
                  WHEN 8  THEN mean_week7
                  WHEN 9  THEN mean_week8
                  WHEN 10 THEN mean_week9
                  WHEN 11 THEN mean_week10
                  WHEN 12 THEN mean_week11
                  WHEN 13 THEN mean_week12
                  WHEN 14 THEN mean_week13
                  WHEN 15 THEN mean_week14
                  WHEN 16 THEN mean_week15
                  WHEN 17 THEN mean_week16
                  WHEN 18 THEN mean_week17
                  WHEN 19 THEN mean_week18
                  WHEN 20 THEN mean_week19
                  WHEN 21 THEN mean_week20
                  WHEN 22 THEN mean_week21
                  WHEN 23 THEN mean_week22
                  WHEN 24 THEN mean_week23
                  WHEN 25 THEN mean_week24
                  WHEN 26 THEN mean_week25
              END AS mean
        FROM
            cte_final_week_row_value
                CROSS JOIN UNNEST(GENERATE_ARRAY(1, 26, 1)) AS arr
    )
    , cte_fcst_act_union as (
        SELECT
            country
            , division
            , category
            , collection
            , CAST(yr_month_or_week AS STRING) AS time
            , period_type as time_level

            , net_received_units
            , net_received

            --             , sellable_on_hand_units - shipped_units + net_received_units AS sellable_on_hand_units
            --             , sellable_on_hand_inventory - shipped_revenue + net_received AS sellable_on_hand_inventory

            -- 260108 / cube (GROUPING SETS) 단에서 계산
            , sellable_on_hand_units
            , sellable_on_hand_inventory

            -- 260108 / ratio 의 결과의 합 != 합 * ratio 문제 / cube (GROUPING SETS) 단에서 계산
            , NULL AS shipped_units
            , NULL AS shipped_revenue

            , sell_out_fcst_target
            , sell_out_fcst_target_usd

            , sell_out_sum
            , sell_out_last_year_sum

            , sell_out_sum_usd
            , sell_out_last_year_sum_usd

            , p70
            , p80
            , p90
            , mean

            , 'fcst' AS row_type
        FROM
            cte_fcst_collection_src

        UNION ALL

        SELECT
            country
            , division
            , category
            , collection
            , CAST(yr_month_or_week AS STRING) AS time
            , period_type as time_level

            , net_received_units
            , net_received

            , sellable_on_hand_units
            , sellable_on_hand_inventory

            , shipped_units
            , shipped_revenue

            , null
            , null

            , null
            , null

            , null
            , null

            , null as p70
            , null as p80
            , null as p90
            , null as mean

            , 'act' AS row_type
        FROM
            cte_actual_collection_src
    )
    , cte_sum_wos as (
        SELECT
            *
            , 'MONTH' as time_base_level
            -- 최소 단위가 collection 이고 collection 을 병합하여 계산하지 않기에 AVG 처리 가능
            , AVG(shipped_units) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout
            , AVG(shipped_revenue) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_usd
            , AVG(received_po) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po
            , AVG(received_po_usd) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po_usd

            , AVG(shipped_units_with_p70) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p70
            , AVG(shipped_units_with_p80) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p80
            , AVG(shipped_units_with_p90) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p90
            , AVG(shipped_units_with_mean) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_mean

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_revenue) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_sum_for_wos_usd

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p70) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p70_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p80) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p80_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p90) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p90_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_mean) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_mean_sum_for_wos
        FROM
            (
                SELECT
                    country
                    , IF(GROUPING(division) = 1, 'T', division) AS division_att
                    , IF(GROUPING(category) = 1, 'T', category) AS category_att
                    , collection

                    , time
                    , FORMAT_DATE('%Y%m', DATE_ADD(PARSE_DATE('%Y%m', time), INTERVAL 1 YEAR)) AS prev_ytd_time

                    --                     , SUM(shipped_units) AS shipped_units
                    --                     , SUM(shipped_revenue) AS shipped_revenue
                    , IF(row_type = 'fcst', SUM(sell_out_fcst_target) * (if(SUM(sell_out_last_year_sum) is null or SUM(sell_out_last_year_sum) = 0, 1, SUM(sell_out_sum) / SUM(sell_out_last_year_sum))), SUM(shipped_units)) AS shipped_units
                    , IF(row_type = 'fcst', SUM(sell_out_fcst_target_usd) * (if(SUM(sell_out_last_year_sum_usd) is null or SUM(sell_out_last_year_sum_usd) = 0, 1, SUM(sell_out_sum_usd) / SUM(sell_out_last_year_sum_usd))), SUM(shipped_revenue)) AS shipped_revenue

                    , IF(row_type = 'fcst', SUM(p70), SUM(shipped_units)) AS shipped_units_with_p70
                    , IF(row_type = 'fcst', SUM(p80), SUM(shipped_units)) AS shipped_units_with_p80
                    , IF(row_type = 'fcst', SUM(p90), SUM(shipped_units)) AS shipped_units_with_p90
                    , IF(row_type = 'fcst', SUM(mean), SUM(shipped_units)) AS shipped_units_with_mean

                    , SUM(net_received_units) AS received_po
                    , SUM(net_received) AS received_po_usd
                FROM
                    cte_fcst_act_union
                WHERE
                    time_level = 'MONTH'
                GROUP BY
--                     1, 2, 3, 4, 5, time_level, row_type
                    GROUPING SETS (
                        ( country, time, time_level, row_type, collection, division, category )
                        , ( country, time, time_level, row_type, collection, division )
                        , ( country, time, time_level, row_type, collection, category )
                        , ( country, time, time_level, row_type, collection )
                    )

            )

        UNION ALL

        SELECT
            *
            , 'WEEK' as time_base_level
            -- 최소 단위가 collection 이고 collection 을 병합하여 계산하지 않기에 AVG 처리 가능
            , AVG(shipped_units) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout
            , AVG(shipped_revenue) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_usd
            , AVG(received_po) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po
            , AVG(received_po_usd) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_received_po_usd

            , AVG(shipped_units_with_p70) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p70
            , AVG(shipped_units_with_p80) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p80
            , AVG(shipped_units_with_p90) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_p90
            , AVG(shipped_units_with_mean) OVER (PARTITION BY country, division_att, category_att, collection, SUBSTRING(time, 1, 4) ORDER BY time desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ytd_sellout_with_mean

            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING) = 13
                      THEN
                      SUM(shipped_units) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING)
              END AS sell_out_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING) = 13
                      THEN
                      SUM(shipped_revenue) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 13 FOLLOWING)
              END AS sell_out_sum_for_wos_usd
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p70) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p70_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p80) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p80_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_p90) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_p90_sum_for_wos
            , CASE
                  WHEN
                      COUNT(1) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) = 3
                      THEN
                      SUM(shipped_units_with_mean) OVER (PARTITION BY country, division_att, category_att, collection ORDER BY time ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
              END AS sell_out_with_mean_sum_for_wos
        FROM
            (
                SELECT
                    country
                    , IF(GROUPING(division) = 1, 'T', division) AS division_att
                    , IF(GROUPING(category) = 1, 'T', category) AS category_att
                    , collection

                    , time
                    , CAST(CAST(SUBSTRING(time,1,4) AS INT64) +1 AS STRING) || SUBSTRING(time, 5) AS prev_ytd_time

                    --                     , SUM(shipped_units) AS shipped_units
                    --                     , SUM(shipped_revenue) AS shipped_revenue
                    , IF(row_type = 'fcst', SUM(sell_out_fcst_target) * (if(SUM(sell_out_last_year_sum) is null or SUM(sell_out_last_year_sum) = 0, 1, SUM(sell_out_sum) / SUM(sell_out_last_year_sum))), SUM(shipped_units)) AS shipped_units
                    , IF(row_type = 'fcst', SUM(sell_out_fcst_target_usd) * (if(SUM(sell_out_last_year_sum_usd) is null or SUM(sell_out_last_year_sum_usd) = 0, 1, SUM(sell_out_sum_usd) / SUM(sell_out_last_year_sum_usd))), SUM(shipped_revenue)) AS shipped_revenue

                    , IF(row_type = 'fcst', SUM(p70), SUM(shipped_units)) AS shipped_units_with_p70
                    , IF(row_type = 'fcst', SUM(p80), SUM(shipped_units)) AS shipped_units_with_p80
                    , IF(row_type = 'fcst', SUM(p90), SUM(shipped_units)) AS shipped_units_with_p90
                    , IF(row_type = 'fcst', SUM(mean), SUM(shipped_units)) AS shipped_units_with_mean

                    , SUM(net_received_units) AS received_po
                    , SUM(net_received) AS received_po_usd
                FROM
                    cte_fcst_act_union
                WHERE
                    time_level = 'WEEK'
                GROUP BY
--                     1, 2, 3, 4, 5, time_level, row_type
                    GROUPING SETS (
                        ( country, time, time_level, row_type, collection, division, category )
                        , ( country, time, time_level, row_type, collection, division )
                        , ( country, time, time_level, row_type, collection, category )
                        , ( country, time, time_level, row_type, collection )
                    )
            )
    )
    , cte_cube as (
        SELECT
            country
            , IF(GROUPING(division) = 1, 'T', division) AS division_att
            , IF(GROUPING(category) = 1, 'T', category) AS category_att
            , collection

            , time_level
            , time

            , IF(row_type = 'fcst', SUM(sell_out_fcst_target) * (if(SUM(sell_out_last_year_sum) is null or SUM(sell_out_last_year_sum) = 0, 1, SUM(sell_out_sum) / SUM(sell_out_last_year_sum))), SUM(shipped_units)) AS sell_out
            , IF(row_type = 'fcst', SUM(sell_out_fcst_target_usd) * (if(SUM(sell_out_last_year_sum_usd) is null or SUM(sell_out_last_year_sum_usd) = 0, 1, SUM(sell_out_sum_usd) / SUM(sell_out_last_year_sum_usd))), SUM(shipped_revenue)) AS sell_out_usd
            --             , SUM(shipped_units) AS sell_out
            --             , SUM(shipped_revenue) AS sell_out_usd
            , IF(row_type = 'fcst'
                , SUM(p70)
                , SUM(shipped_units)
              ) AS sell_out_with_p70
            , IF(row_type = 'fcst'
                , SUM(p80)
                , SUM(shipped_units)
              ) AS sell_out_with_p80
            , IF(row_type = 'fcst'
                , SUM(p90)
                , SUM(shipped_units)
              ) AS sell_out_with_p90
            , IF(row_type = 'fcst'
                , SUM(mean)
                , SUM(shipped_units)
              ) AS sell_out_with_mean

            , SUM(net_received_units) AS received_po
            , SUM(sellable_on_hand_units) AS ending_inv

            , SUM(net_received) AS received_po_usd
            , SUM(sellable_on_hand_inventory) AS ending_inv_usd

            , row_type
        FROM
            cte_fcst_act_union
        GROUP BY
--             1, 2, 3, 4, 5, 6, 7, 8, row_type
            GROUPING SETS (
                (country, time, time_level, row_type, collection, division, category),
                (country, time, time_level, row_type, collection, division),
                (country, time, time_level, row_type, collection, category),
                (country, time, time_level, row_type, collection)
            )
    )
    , cte_semi_final as (
        SELECT
            f.* -- EXCEPT (ending_inv, ending_inv_usd)
--             , IF(f.row_type = 'act', 0, ROW_NUMBER() OVER (PARTITION BY f.country, f.division_att, f.category_att, f.collection, f.time_level, f.row_type ORDER BY f.time)) AS row_num
            , SUBSTRING(f.time, 1, 4) AS year
            , w.ytd_sellout
            , w.ytd_sellout_usd
            , w.ytd_received_po
            , w.ytd_received_po_usd
            , w.ytd_sellout_with_p70
            , w.ytd_sellout_with_p80
            , w.ytd_sellout_with_p90
            , w.ytd_sellout_with_mean
            --             , f.ending_inv
            --             , f.ending_inv_usd
            --     , if(f.row_type = 'fcst', f.ending_inv - f.sell_out + f.received_po , f.ending_inv) as ending_inv
            --     , if(f.row_type = 'fcst', f.ending_inv - f.sell_out_usd + f.received_po_usd , f.ending_inv_usd) as ending_inv_usd
            , w2.ytd_sellout AS prev_ytd_sellout
            , w2.ytd_sellout_usd AS prev_ytd_sellout_usd
            , w2.ytd_received_po AS prev_ytd_received_po
            , w2.ytd_received_po_usd AS prev_ytd_received_po_usd
            , w2.ytd_sellout_with_p70 AS prev_ytd_sellout_with_p70
            , w2.ytd_sellout_with_p80 AS prev_ytd_sellout_with_p80
            , w2.ytd_sellout_with_p90 AS prev_ytd_sellout_with_p90
            , w2.ytd_sellout_with_mean AS prev_ytd_sellout_with_mean

            , w.sell_out_sum_for_wos
            , w.sell_out_sum_for_wos_usd
            , w.sell_out_with_p70_sum_for_wos
            , w.sell_out_with_p80_sum_for_wos
            , w.sell_out_with_p90_sum_for_wos
            , w.sell_out_with_mean_sum_for_wos
        FROM
            cte_cube f
                LEFT JOIN cte_sum_wos w
                    ON f.country = w.country
                       AND f.division_att = w.division_att
                       AND f.category_att = w.category_att
                       AND f.collection = w.collection
                       AND f.time = w.time
                       AND IF(f.time_level = 'WEEK', 'WEEK', 'MONTH') = w.time_base_level
                LEFT JOIN cte_sum_wos w2
                    ON f.country = w2.country
                       AND f.division_att = w2.division_att
                       AND f.category_att = w2.category_att
                       AND f.collection = w2.collection
                       AND f.time = w2.prev_ytd_time
                       AND IF(f.time_level = 'WEEK', 'WEEK', 'MONTH') = w2.time_base_level
    )
    , cte_inventory_logic AS (
        SELECT
            *

            , LAST_VALUE(IF(row_type = 'act', ending_inv, NULL) IGNORE NULLS) OVER( PARTITION BY country, division_att, category_att, collection, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS start_inv_qty

            , SUM(IF(row_type = 'fcst', sell_out, 0)) OVER( PARTITION BY country, division_att, category_att, collection, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out
            , SUM(IF(row_type = 'fcst', received_po, 0)) OVER( PARTITION BY country, division_att, category_att, collection, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_po

            , SUM(IF(row_type = 'fcst', sell_out_with_p70, 0)) OVER( PARTITION BY country, division_att, category_att, collection, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_p70
            , SUM(IF(row_type = 'fcst', sell_out_with_p80, 0)) OVER( PARTITION BY country, division_att, category_att, collection, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_p80
            , SUM(IF(row_type = 'fcst', sell_out_with_p90, 0)) OVER( PARTITION BY country, division_att, category_att, collection, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_p90
            , SUM(IF(row_type = 'fcst', sell_out_with_mean, 0)) OVER( PARTITION BY country, division_att, category_att, collection, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_with_mean

            , LAST_VALUE(IF(row_type = 'act', ending_inv_usd, NULL) IGNORE NULLS) OVER(PARTITION BY country, division_att, category_att, collection, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS start_inv_amt
            , SUM(IF(row_type = 'fcst', sell_out_usd, 0)) OVER( PARTITION BY country, division_att, category_att, collection, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_sell_out_usd
            , SUM(IF(row_type = 'fcst', received_po_usd, 0)) OVER( PARTITION BY country, division_att, category_att, collection, time_level ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS cumulative_po_usd
        FROM
            cte_semi_final
    )
    , cte_month_n_week_final as (
        SELECT
            * EXCEPT (start_inv_qty, cumulative_sell_out, start_inv_amt, cumulative_sell_out_usd, ending_inv, ending_inv_usd, cumulative_po, cumulative_po_usd,  cumulative_sell_out_with_p70,  cumulative_sell_out_with_p80,  cumulative_sell_out_with_p90,  cumulative_sell_out_with_mean)
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out + cumulative_po, ending_inv) AS ending_inv
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_p70 + cumulative_po, ending_inv) AS ending_inv_with_p70
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_p80 + cumulative_po, ending_inv) AS ending_inv_with_p80
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_p90 + cumulative_po, ending_inv) AS ending_inv_with_p90
            , IF(row_type = 'fcst', start_inv_qty - cumulative_sell_out_with_mean + cumulative_po, ending_inv) AS ending_inv_with_mean
            , IF(row_type = 'fcst', start_inv_amt - cumulative_sell_out_usd + cumulative_po_usd, ending_inv_usd) AS ending_inv_usd
        FROM
            cte_inventory_logic
    )
    , cte_year_n_quarter_final as (
        SELECT
            country
            , division_att
            , category_att
            , collection
            , 'QUARTER' AS time_level
            , CONCAT(SUBSTRING(time, 1, 4), 'Q', CAST(CEIL(CAST(SUBSTRING(time, 5, 2) AS INT64) / 3) AS STRING)) AS time

            , SUM(sell_out) AS sell_out
            , SUM(sell_out_usd) AS sell_out_usd
            , SUM(sell_out_with_p70) AS sell_out_with_p70
            , SUM(sell_out_with_p80) AS sell_out_with_p80
            , SUM(sell_out_with_p90) AS sell_out_with_p90
            , SUM(sell_out_with_mean) AS sell_out_with_mean

            , SUM(received_po) AS received_po
            , SUM(received_po_usd) AS received_po_usd

            , ARRAY_AGG(row_type ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS row_type
            , SUBSTRING(MAX(time), 1, 4) AS year

            , ARRAY_AGG(ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout
            , ARRAY_AGG(ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_usd
            , ARRAY_AGG(ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po
            , ARRAY_AGG(ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po_usd
            , ARRAY_AGG(ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p70
            , ARRAY_AGG(ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p80
            , ARRAY_AGG(ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p90
            , ARRAY_AGG(ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_mean

            , ARRAY_AGG(prev_ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout
            , ARRAY_AGG(prev_ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_usd
            , ARRAY_AGG(prev_ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po
            , ARRAY_AGG(prev_ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po_usd
            , ARRAY_AGG(prev_ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p70
            , ARRAY_AGG(prev_ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p80
            , ARRAY_AGG(prev_ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p90
            , ARRAY_AGG(prev_ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_mean

            , ARRAY_AGG(sell_out_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos
            , ARRAY_AGG(sell_out_sum_for_wos_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos_usd
            , ARRAY_AGG(sell_out_with_p70_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p70_sum_for_wos
            , ARRAY_AGG(sell_out_with_p80_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p80_sum_for_wos
            , ARRAY_AGG(sell_out_with_p90_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p90_sum_for_wos
            , ARRAY_AGG(sell_out_with_mean_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_mean_sum_for_wos

            , ARRAY_AGG(ending_inv ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv
            , ARRAY_AGG(ending_inv_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p70
            , ARRAY_AGG(ending_inv_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p80
            , ARRAY_AGG(ending_inv_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p90
            , ARRAY_AGG(ending_inv_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_mean
            , ARRAY_AGG(ending_inv_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_usd
        FROM
            cte_month_n_week_final
        WHERE
            time_level = 'MONTH'
        GROUP BY 1, 2, 3, 4, 5, 6

        UNION ALL

        SELECT
            country
            , division_att
            , category_att
            , collection
            , 'YEAR' AS time_level
            , SUBSTRING(time, 1, 4) AS time

            , SUM(sell_out) AS sell_out
            , SUM(sell_out_usd) AS sell_out_usd
            , SUM(sell_out_with_p70) AS sell_out_with_p70
            , SUM(sell_out_with_p80) AS sell_out_with_p80
            , SUM(sell_out_with_p90) AS sell_out_with_p90
            , SUM(sell_out_with_mean) AS sell_out_with_mean

            , SUM(received_po) AS received_po
            , SUM(received_po_usd) AS received_po_usd

            , ARRAY_AGG(row_type ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS row_type
            , SUBSTRING(MAX(time), 1, 4) AS year

            , ARRAY_AGG(ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout
            , ARRAY_AGG(ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_usd
            , ARRAY_AGG(ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po
            , ARRAY_AGG(ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_received_po_usd
            , ARRAY_AGG(ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p70
            , ARRAY_AGG(ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p80
            , ARRAY_AGG(ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_p90
            , ARRAY_AGG(ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ytd_sellout_with_mean

            , ARRAY_AGG(prev_ytd_sellout ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout
            , ARRAY_AGG(prev_ytd_sellout_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_usd
            , ARRAY_AGG(prev_ytd_received_po ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po
            , ARRAY_AGG(prev_ytd_received_po_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_received_po_usd
            , ARRAY_AGG(prev_ytd_sellout_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p70
            , ARRAY_AGG(prev_ytd_sellout_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p80
            , ARRAY_AGG(prev_ytd_sellout_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_p90
            , ARRAY_AGG(prev_ytd_sellout_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS prev_ytd_sellout_with_mean

            , ARRAY_AGG(sell_out_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos
            , ARRAY_AGG(sell_out_sum_for_wos_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_sum_for_wos_usd
            , ARRAY_AGG(sell_out_with_p70_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p70_sum_for_wos
            , ARRAY_AGG(sell_out_with_p80_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p80_sum_for_wos
            , ARRAY_AGG(sell_out_with_p90_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_p90_sum_for_wos
            , ARRAY_AGG(sell_out_with_mean_sum_for_wos ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS sell_out_with_mean_sum_for_wos

            , ARRAY_AGG(ending_inv ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv
            , ARRAY_AGG(ending_inv_with_p70 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p70
            , ARRAY_AGG(ending_inv_with_p80 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p80
            , ARRAY_AGG(ending_inv_with_p90 ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_p90
            , ARRAY_AGG(ending_inv_with_mean ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_with_mean
            , ARRAY_AGG(ending_inv_usd ORDER BY time DESC LIMIT 1)[OFFSET(0)] AS ending_inv_usd
        FROM
            cte_month_n_week_final
        WHERE
            time_level = 'MONTH'
        GROUP BY 1, 2, 3, 4, 5, 6
    )
SELECT
    *
    , IF(row_type = 'fcst', ROW_NUMBER() OVER (PARTITION BY country, division_att, category_att, collection, time_level, row_type ORDER BY time), 0) AS row_num
FROM
    cte_month_n_week_final

UNION ALL

SELECT
    *
    , if(row_type = 'fcst', ROW_NUMBER() OVER (PARTITION BY country, division_att, category_att, collection, time_level, row_type ORDER BY time), 0) AS row_num
FROM
    cte_year_n_quarter_final
;


CREATE OR REPLACE TABLE mart.amz_di_global_psi_report_by_collection AS
WITH cte_add_last_time AS (
    SELECT
        *
        ,   CASE time_level
                WHEN 'YEAR' THEN
                    CAST(CAST(time AS INT64) - 1 AS STRING)

                WHEN 'QUARTER' THEN
                    CONCAT(
                            CAST(IF(SAFE_CAST(REGEXP_EXTRACT(time, r'Q([1-4])') AS INT64) = 1, SAFE_CAST(SUBSTR(time, 1, 4) AS INT64) - 1, SAFE_CAST(SUBSTR(time, 1, 4) AS INT64)) AS STRING),
                            'Q',
                            CAST(IF(SAFE_CAST(REGEXP_EXTRACT(time, r'Q([1-4])') AS INT64) = 1, 4, SAFE_CAST(REGEXP_EXTRACT(time, r'Q([1-4])') AS INT64) - 1) AS STRING)
                    )

                WHEN 'MONTH' THEN
                    FORMAT_DATE('%Y%m', DATE_SUB(PARSE_DATE('%Y%m', time), INTERVAL 1 MONTH))

                WHEN 'WEEK' THEN
                    FORMAT_DATE('%G%V', DATE_SUB(PARSE_DATE('%G-%V', SUBSTR(time, 1,4)||'-'||SUBSTR(time, 5,2)), INTERVAL 7 DAY))

                ELSE NULL
            END AS last_time
    FROM
        tmp1.amz_di_global_psi_report_by_collection
)
SELECT
    a.*
    , b.sell_out as last_sell_out
    , b.sell_out_usd as last_sell_out_usd
    , b.received_po as last_received_po
    , b.received_po_usd as last_received_po_usd

    , b.ending_inv as last_ending_inv
    , b.ending_inv_usd as last_ending_inv_usd

    , b.ending_inv_with_p70 as last_ending_inv_with_p70
    , b.ending_inv_with_p80 as last_ending_inv_with_p80
    , b.ending_inv_with_p90 as last_ending_inv_with_p90
    , b.ending_inv_with_mean as last_ending_inv_with_mean

    , b.sell_out_sum_for_wos as last_sell_out_sum_for_wos
    , b.sell_out_with_p70_sum_for_wos as last_sell_out_with_p70_sum_for_wos
    , b.sell_out_with_p80_sum_for_wos as last_sell_out_with_p80_sum_for_wos
    , b.sell_out_with_p90_sum_for_wos as last_sell_out_with_p90_sum_for_wos
    , b.sell_out_with_mean_sum_for_wos as last_sell_out_with_mean_sum_for_wos

FROM
    cte_add_last_time a
        LEFT JOIN cte_add_last_time b
            ON a.time_level = b.time_level AND a.last_time = b.time
                AND a.country = b.country
                AND a.division_att = b.division_att
                AND a.category_att = b.category_att
                AND a.collection = b.collection
;


-- [aging report] ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart.amz_di_global_aging_report AS
WITH
    cte_month_source AS (

        SELECT
            asin
--             , zinus_sku
            , COALESCE(zinus_sku, 'UNKNOWN') AS zinus_sku
            , COALESCE(country, 'UNKNOWN') AS country

--             , IF(COALESCE(financial_category, 'UNKNOWN') IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ), 'M', 'N') AS division
            , CASE
                  WHEN financial_category = '95.OTHERS' THEN 'Others'
                  WHEN financial_category IS NULL   THEN 'Unknown'
                  WHEN financial_category IN ( '10.FOAM MATTRESSES', '15.SPRING MATTRESS', '50.TOPPERS' ) THEN 'Mattress & Topper'
                  ELSE 'Non-Mattress'
              END AS division

            , COALESCE(financial_category, 'UNKNOWN') AS category
--             , single_cat_desc
            , COALESCE(coo, 'UNKNOWN') AS origin

            , CAST(yr_month_or_week AS STRING) AS time
            , CAST(yr_month_or_week AS STRING) AS end_time

            , period_type AS time_level

            , shipped_units
            , net_received_units
            , sellable_on_hand_units
            , shipped_revenue
            , net_received
            , sellable_on_hand_inventory
        FROM
            mart.amz_di_global
        WHERE
            period_type = 'MONTH'
            AND yr_month_or_week >= 202201
            AND is_closed = TRUE

        --         for test
        --             AND country = 'UK'
        --             AND asin = 'B0D4PPYLB1'

    )
    , cte_agg             AS (
        SELECT
            country
            , division
            , category
--             , single_cat_desc
            , origin
            , zinus_sku
--             , asin

            , time

            -- , PARSE_DATE('%Y%m', CAST(time AS STRING)) as month_date
            -- , DATE_DIFF(DATE_TRUNC(CURRENT_DATE(), MONTH), DATE_TRUNC(PARSE_DATE('%Y%m', CAST(time AS STRING)), MONTH), MONTH) AS month_diff
            -- , SUM(shipped_units) AS shipped_units

            , SUM(net_received_units) AS net_received_units
            , SUM(net_received) AS net_received
            , SUM(sellable_on_hand_units) AS sellable_on_hand_units
            , SUM(sellable_on_hand_inventory) AS sellable_on_hand_inventory
        FROM
            cte_month_source
--         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
--         GROUP BY 1, 2, 3, 4, 5, 6, 7
        GROUP BY 1, 2, 3, 4, 5, 6
        --         ORDER BY 8
    )
    , cte_sum            AS (
        with cte_tmp1 as (
            SELECT
                *
                --             , SUM(net_received) OVER (PARTITION BY country, asin ORDER BY time DESC ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS less3
                , SUM(net_received) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS less3
                , SUM(net_received) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN 3 FOLLOWING AND 5 FOLLOWING) AS less6

                , SUM(net_received) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN 6 FOLLOWING AND 8 FOLLOWING) AS less9
                , SUM(net_received) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN 9 FOLLOWING AND 11 FOLLOWING) AS less12

                , SUM(net_received) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN 12 FOLLOWING AND 17 FOLLOWING) AS less18
                , SUM(net_received) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN 18 FOLLOWING AND 23 FOLLOWING) AS less24

                --             , SUM(net_received) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN CURRENT ROW AND 23 FOLLOWING) AS over24
                --             , SUM(net_received) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS over24

                , SUM(net_received_units) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS less3_unit
                , SUM(net_received_units) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN 3 FOLLOWING AND 5 FOLLOWING) AS less6_unit

                , SUM(net_received_units) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN 6 FOLLOWING AND 8 FOLLOWING) AS less9_unit
                , SUM(net_received_units) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN 9 FOLLOWING AND 11 FOLLOWING) AS less12_unit

                , SUM(net_received_units) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN 12 FOLLOWING AND 17 FOLLOWING) AS less18_unit
                , SUM(net_received_units) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN 18 FOLLOWING AND 23 FOLLOWING) AS less24_unit

            --             , SUM(net_received_units) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN CURRENT ROW AND 23 FOLLOWING) AS over24_unit
            --             , SUM(net_received_units) OVER (PARTITION BY country, zinus_sku ORDER BY time DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS over24_unit
            FROM
                cte_agg
        )
        SELECT
            * EXCEPT (less3, less3_unit, less6, less6_unit, less9, less9_unit, less12, less12_unit, less18, less18_unit, less24, less24_unit)
            --              net received 가 0 보다 작게 되면 ending inventory 값과 aging 의 합이 달라짐 (aging 의 합이 더 커짐)
            , IF(less3 IS NULL OR less3 < 0, 0, less3) AS less3
            , IF(less6 IS NULL OR less6 < 0, 0, less6) AS less6

            , IF(less9 IS NULL OR less9 < 0, 0, less9) AS less9

            , IF(less12 IS NULL OR less12 < 0, 0, less12) AS less12
            , IF(less18 IS NULL OR less18 < 0, 0, less18) AS less18
            , IF(less24 IS NULL OR less24 < 0, 0, less24) AS less24
            , IF(less3_unit IS NULL OR less3_unit < 0, 0, less3_unit) AS less3_unit
            , IF(less6_unit IS NULL OR less6_unit < 0, 0, less6_unit) AS less6_unit

            , IF(less9_unit IS NULL OR less9_unit < 0, 0, less9_unit) AS less9_unit

            , IF(less12_unit IS NULL OR less12_unit < 0, 0, less12_unit) AS less12_unit
            , IF(less18_unit IS NULL OR less18_unit < 0, 0, less18_unit) AS less18_unit
            , IF(less24_unit IS NULL OR less24_unit < 0, 0, less24_unit) AS less24_unit
        FROM
            cte_tmp1
    )
    , cte_sum3           AS (
        SELECT
            * EXCEPT (less3, less3_unit)
            , IF(less3 > sellable_on_hand_inventory, sellable_on_hand_inventory, less3) AS less3
            , IF(less3_unit > sellable_on_hand_units, sellable_on_hand_units, less3_unit) AS less3_unit
        FROM cte_sum
    )
    , cte_sum6           AS (
        SELECT
            * EXCEPT (less6, less6_unit)
            , IF(less6 > sellable_on_hand_inventory - less3, sellable_on_hand_inventory - less3, less6) AS less6
            , IF(less6_unit > sellable_on_hand_units - less3_unit, sellable_on_hand_units - less3_unit, less6_unit) AS less6_unit
        FROM
            cte_sum3
    )
    , cte_sum9           AS (
        SELECT
            * EXCEPT (less9, less9_unit)
            , IF(less9 > sellable_on_hand_inventory - ( less3 + less6 ), sellable_on_hand_inventory - ( less3 + less6 ), less9) AS less9
            , IF(less9_unit > sellable_on_hand_units - ( less3_unit + less6_unit ), sellable_on_hand_units - ( less3_unit + less6_unit ), less9_unit) AS less9_unit
        FROM
            cte_sum6
    )
    , cte_sum12          AS (
        SELECT
            * EXCEPT (less12, less12_unit)
            , IF(less12 > sellable_on_hand_inventory - ( less3 + less6 + less9 ), sellable_on_hand_inventory - ( less3 + less6 + less9 ), less12) AS less12
            , IF(less12_unit > sellable_on_hand_units - ( less3_unit + less6_unit + less9_unit ), sellable_on_hand_units - ( less3_unit + less6_unit + less9_unit ), less12_unit) AS less12_unit
        FROM
            cte_sum9
    )
    , cte_sum18          AS (
        SELECT
            * EXCEPT (less18, less18_unit)
            , IF(less18 > sellable_on_hand_inventory - ( less3 + less6 + less9 + less12 ), sellable_on_hand_inventory - ( less3 + less6 + less9 + less12 ), less18) AS less18
            , IF(less18_unit > sellable_on_hand_units - ( less3_unit + less6_unit + less9_unit + less12_unit ), sellable_on_hand_units - ( less3_unit + less6_unit + less9_unit + less12_unit ), less18_unit) AS less18_unit
        FROM
            cte_sum12
    )
    , cte_sum24          AS (
        SELECT
            * EXCEPT (less24, less24_unit)
            , IF(less24 > sellable_on_hand_inventory - ( less3 + less6 + less9 + less12 + less18 ), sellable_on_hand_inventory - ( less3 + less6 + less9 + less12 + less18 ), less24) AS less24
            , IF(less24_unit > sellable_on_hand_units - ( less3_unit + less6_unit + less9_unit + less12_unit + less18_unit ), sellable_on_hand_units - ( less3_unit + less6_unit + less9_unit + less12_unit + less18_unit ), less24_unit) AS less24_unit
        FROM
            cte_sum18
    )
    , cte_over24          AS (
        SELECT
            * -- EXCEPT (over24, over24_unit)
            , sellable_on_hand_inventory - ( less3 + less6 + less9 + less12 + less18 + less24 ) AS over24
            , sellable_on_hand_units - ( less3_unit + less6_unit + less9_unit + less12_unit + less18_unit + less24_unit ) AS over24_unit
        FROM
            cte_sum24
    )
    , cte_month_aging as (
        SELECT
            country
            , CASE country
                  WHEN 'AU'     THEN 'Amazon AU'
                  WHEN 'CA'     THEN 'Amazon CA'
                  WHEN 'DE'     THEN 'Amazon DE'
                  WHEN 'US'     THEN 'Amazon DI'
                  WHEN 'ES'     THEN 'Amazon ES'
                  WHEN 'FR'     THEN 'Amazon FR'
                  WHEN 'IT'     THEN 'Amazon IT'
                  WHEN 'JP'     THEN 'Amazon JP'
                  WHEN 'UK'     THEN 'Amazon UK'
                  WHEN 'MX'     THEN 'Amazon MX'
                  WHEN 'MELLOW' THEN 'Amazon DI'
                  ELSE 'UNKNOWN'
              END AS company
            , division
            , category
--             , single_cat_desc
            , origin
            , zinus_sku
--             , asin
            , time AS yr_month
            , CAST(LEFT(time, 4) AS INT) AS year
            , CAST(RIGHT(time, 2) AS INT) AS month
            , CEIL(CAST(RIGHT(time, 2) AS INT) / 3) AS quarter
            , CONCAT(LEFT(time, 4), 'Q', CEIL(CAST(RIGHT(time, 2) AS INT) / 3.0)) AS year_quarter
            , net_received_units
            , net_received
            , sellable_on_hand_units
            , sellable_on_hand_inventory
            , COALESCE(less3, 0) AS less3
            , COALESCE(less6, 0) AS less6
            , COALESCE(less9, 0) AS less9
            , COALESCE(less12, 0) AS less12
            , COALESCE(less18, 0) AS less18
            , COALESCE(less24, 0) AS less24
            , COALESCE(over24, 0) AS over24

            , COALESCE(less3_unit, 0) AS less3_unit
            , COALESCE(less6_unit, 0) AS less6_unit
            , COALESCE(less9_unit, 0) AS less9_unit
            , COALESCE(less12_unit, 0) AS less12_unit
            , COALESCE(less18_unit, 0) AS less18_unit
            , COALESCE(less24_unit, 0) AS less24_unit
            , COALESCE(over24_unit, 0) AS over24_unit
        FROM
            cte_over24
    )
    , cte_quarter_aging as (
        SELECT *
        FROM cte_month_aging
        QUALIFY RANK() OVER (PARTITION BY year_quarter ORDER BY yr_month DESC) = 1
    )
    -- select * from cte_quarter_aging where country='UK' and zinus_sku='AK-MFMAA7ZC-08S'
    , cte_year_aging as (
        SELECT *
        FROM cte_month_aging
        QUALIFY RANK() OVER (PARTITION BY year ORDER BY yr_month DESC) = 1
    )
    , cte_union as (
        SELECT
            'MONTH' AS time_level
            , yr_month as time
            , *
        FROM
            cte_month_aging
        UNION ALL
        SELECT
            'QUARTER' AS time_level
            , year_quarter as time
            , *
        FROM
            cte_quarter_aging
        UNION ALL
        SELECT
            'YEAR' AS time_level
            , CAST(year AS STRING) AS time
            , *
        FROM
            cte_year_aging
    )
SELECT
    a.*
    , b.single_cat_desc
    , b.sell_out
    , b.sell_out_usd
    , b.received_po
    , b.received_po_usd
    , b.ending_inv
    , b.ending_inv_usd
    , b.sell_out_sum_for_wos
    , b.sell_out_sum_for_wos_usd
FROM
    cte_union a
        LEFT JOIN mart.amz_di_global_psi_report_detail b
            ON a.country = b.country

                AND a.category = b.category
                AND a.time_level = b.time_level
                AND a.time = b.time
                AND a.zinus_sku = b.zinus_sku
                -- sku level 이기 때문에 division group 은 주석 처리해도 무방
                -- AND a.division = b.division
WHERE
    a.sellable_on_hand_inventory IS NOT NULL
;

-- [aging summary] -----------------------------------------------------------------------------------------------------
-- select DISTINCT division from mart.amz_di_global_aging_summary;
-- Mattress & Topper
-- Non-Mattress
-- Others
-- Total
-- Unknown
CREATE OR REPLACE TABLE mart.amz_di_global_aging_summary AS
WITH
    cte_grp AS (
        SELECT
            IF(GROUPING ( country ) = 1, 'T', country) AS company
            --         , IF(GROUPING ( division ) = 1, 'T', division) AS product_group
            , division
            , time_level
            , IF(time_level = 'YEAR', CAST(year AS STRING), yr_month) AS time
            , SUM(less3) AS less3
            , SUM(less6) AS less6
            , SUM(less9) AS less9
            , SUM(less12) AS less12
            , SUM(less18) AS less18
            , SUM(less24) AS less24
            , SUM(over24) AS over24
            , SUM(less3 + less6 + less9 + less12 + less18 + less24 + over24) AS total
            , CASE
                  WHEN division = 'Mattress & Topper' THEN SUM(less18 + less24 + over24)
                  WHEN division = 'Non-Mattress' THEN SUM(less24 + over24)
                  WHEN division = 'Others' OR division = 'Unknown' THEN SUM(over24)
--                   ELSE SUM(less24 + over24)
              END AS sum_risk
        --     , SUM(less3) / (SUM(less3) + SUM(less6) + SUM(less12) + SUM(less18) + SUM(less24) + SUM(over24)) AS ratio_less3

            , SUM(less3_unit) AS less3_unit
            , SUM(less6_unit) AS less6_unit
            , SUM(less9_unit) AS less9_unit
            , SUM(less12_unit) AS less12_unit
            , SUM(less18_unit) AS less18_unit
            , SUM(less24_unit) AS less24_unit
            , SUM(over24_unit) AS over24_unit
            , SUM(less3_unit + less6_unit + less9_unit + less12_unit + less18_unit + less24_unit + over24_unit) AS total_unit
            , CASE
                  WHEN division = 'Mattress & Topper' THEN SUM(less18_unit + less24_unit + over24_unit)
                  WHEN division = 'Non-Mattress' THEN SUM(less24_unit + over24_unit)
                  WHEN division = 'Others' OR division = 'Unknown' THEN SUM(over24_unit)
--                   ELSE SUM(less24_unit + over24_unit)
              END AS sum_risk_unit
        FROM
            mart.amz_di_global_aging_report
--             (
--                 SELECT
--                     * EXCEPT (division)
--                     , CASE
--                           WHEN category = '95.OTHERS' THEN 'Others'
--                           WHEN category = 'UNKNOWN'   THEN 'Unknown'
--                           ELSE if(division = 'M', 'Mattress & Topper', 'Non-Mattress')
--                       END AS division
--
--                 FROM
--                     mart.amz_di_global_aging_report
--             )
        WHERE
            time_level IN ( 'YEAR', 'MONTH' )
        GROUP BY GROUPING SETS ( ( country, 2, 3, 4 ), ( 2, 3, 4 ) )
    ),
    cte_union as (
        with cte_add_risk as (
            SELECT
                company
                , division
                , time_level
                , time
                , less3
                , less6
                , less9
                , less12
                , less18
                , less24
                , over24
                , total
                --     , sum_risk

                --             , IF(total > 0, sum_risk / total, NULL) AS risk
                --             , IF(total > 0, less3 / total, NULL) AS risk_less3
                --             , IF(total > 0, less6 / total, NULL) AS risk_less6
                --             , IF(total > 0, less12 / total, NULL) AS risk_less12
                --             , IF(total > 0, less18 / total, NULL) AS risk_less18
                --             , IF(total > 0, less24 / total, NULL) AS risk_less24
                --             , IF(total > 0, over24 / total, NULL) AS risk_over24

                , sum_risk AS risk
                , NULL AS risk_less3
                , NULL AS risk_less6
                , NULL AS risk_less9
                , NULL AS risk_less12
                , IF(division = 'Mattress & Topper', less18, NULL) AS risk_less18
                , IF(division = 'Mattress & Topper' OR division = 'Non-Mattress', less24, NULL) AS risk_less24
                , over24 AS risk_over24


                , less3_unit
                , less6_unit
                , less9_unit
                , less12_unit
                , less18_unit
                , less24_unit
                , over24_unit
                , total_unit

                --             , IF(total_unit > 0, sum_risk_unit / total_unit, NULL) AS risk_unit
                --             , IF(total_unit > 0, less3_unit / total_unit, NULL) AS risk_less3_unit
                --             , IF(total_unit > 0, less6_unit / total_unit, NULL) AS risk_less6_unit
                --             , IF(total_unit > 0, less12_unit / total_unit, NULL) AS risk_less12_unit
                --             , IF(total_unit > 0, less18_unit / total_unit, NULL) AS risk_less18_unit
                --             , IF(total_unit > 0, less24_unit / total_unit, NULL) AS risk_less24_unit
                --             , IF(total_unit > 0, over24_unit / total_unit, NULL) AS risk_over24_unit

                , sum_risk_unit AS risk_unit
                , NULL AS risk_less3_unit
                , NULL AS risk_less6_unit
                , NULL AS risk_less9_unit
                , NULL AS risk_less12_unit
                , IF(division = 'Mattress & Topper', less18_unit, NULL) AS risk_less18_unit
                , IF(division = 'Mattress & Topper' OR division = 'Non-Mattress', less24_unit, NULL) AS risk_less24_unit
--                 , less24_unit AS risk_less24_unit
                , over24_unit AS risk_over24_unit

            FROM
                cte_grp
        )
        SELECT * FROM cte_add_risk

        UNION ALL

        SELECT
            company
            , 'Total' AS division
            , time_level
            , time
            , SUM(less3) AS less3
            , SUM(less6) AS less6
            , SUM(less9) AS less9
            , SUM(less12) AS less12
            , SUM(less18) AS less18
            , SUM(less24) AS less24
            , SUM(over24) AS over24
            , SUM(total) AS total

            , SUM(risk) AS risk
            , SUM(risk_less3) AS risk_less3
            , SUM(risk_less6) AS risk_less6
            , SUM(risk_less9) AS risk_less9
            , SUM(risk_less12) AS risk_less12
            , SUM(risk_less18) AS risk_less18
            , SUM(risk_less24) AS risk_less24
            , SUM(risk_over24) AS risk_over24

            , SUM(less3_unit) AS less3_unit
            , SUM(less6_unit) AS less6_unit
            , SUM(less9_unit) AS less9_unit
            , SUM(less12_unit) AS less12_unit
            , SUM(less18_unit) AS less18_unit
            , SUM(less24_unit) AS less24_unit
            , SUM(over24_unit) AS over24_unit
            , SUM(total_unit) AS total_unit

            , SUM(risk_unit) AS risk_unit
            , SUM(risk_less3_unit) AS risk_less3_unit
            , SUM(risk_less6_unit) AS risk_less6_unit
            , SUM(risk_less9_unit) AS risk_less9_unit
            , SUM(risk_less12_unit) AS risk_less12_unit
            , SUM(risk_less18_unit) AS risk_less18_unit
            , SUM(risk_less24_unit) AS risk_less24_unit
            , SUM(risk_over24_unit) AS risk_over24_unit
        FROM cte_add_risk
        GROUP BY company, time_level, time

    )
-- 25.12.01 - bi 요청 / if(company = 'T', null, ..)
SELECT
    company
    , division
    , time_level
    , time
    , IF(company = 'T', NULL, less3) AS less3
    , IF(company = 'T', NULL, less6) AS less6
    , IF(company = 'T', NULL, less9) AS less9
    , IF(company = 'T', NULL, less12) AS less12
    , IF(company = 'T', NULL, less18) AS less18
    , IF(company = 'T', NULL, less24) AS less24
    , IF(company = 'T', NULL, over24) AS over24
    , IF(company = 'T', NULL, total) AS total

    , IF(company = 'T', NULL, risk) AS risk
    , IF(company = 'T', NULL, risk_less3) AS risk_less3
    , IF(company = 'T', NULL, risk_less6) AS risk_less6
    , IF(company = 'T', NULL, risk_less9) AS risk_less9
    , IF(company = 'T', NULL, risk_less12) AS risk_less12
    , IF(company = 'T', NULL, risk_less18) AS risk_less18
    , IF(company = 'T', NULL, risk_less24) AS risk_less24
    , IF(company = 'T', NULL, risk_over24) AS risk_over24

    , IF(company = 'T', NULL, less3_unit) AS less3_unit
    , IF(company = 'T', NULL, less6_unit) AS less6_unit
    , IF(company = 'T', NULL, less9_unit) AS less9_unit
    , IF(company = 'T', NULL, less12_unit) AS less12_unit
    , IF(company = 'T', NULL, less18_unit) AS less18_unit
    , IF(company = 'T', NULL, less24_unit) AS less24_unit
    , IF(company = 'T', NULL, over24_unit) AS over24_unit
    , IF(company = 'T', NULL, total_unit) AS total_unit

    , IF(company = 'T', NULL, risk_unit) AS risk_unit
    , IF(company = 'T', NULL, risk_less3_unit) AS risk_less3_unit
    , IF(company = 'T', NULL, risk_less6_unit) AS risk_less6_unit
    , IF(company = 'T', NULL, risk_less9_unit) AS risk_less9_unit
    , IF(company = 'T', NULL, risk_less12_unit) AS risk_less12_unit
    , IF(company = 'T', NULL, risk_less18_unit) AS risk_less18_unit
    , IF(company = 'T', NULL, risk_less24_unit) AS risk_less24_unit
    , IF(company = 'T', NULL, risk_over24_unit) AS risk_over24_unit
FROM
    cte_union
;

CREATE OR REPLACE TABLE mart.amz_di_global_aging_summary_us AS
WITH
    cte_grp AS (
        SELECT
            IF(GROUPING ( country ) = 1, 'T', country) AS company
            --         , IF(GROUPING ( division ) = 1, 'T', division) AS product_group
            , division
            , time_level
            , IF(time_level = 'YEAR', CAST(year AS STRING), yr_month) AS time
            , SUM(less3) AS less3
            , SUM(less6) AS less6
            , SUM(less9) AS less9
            , SUM(less12) AS less12
            , SUM(less18) AS less18
            , SUM(less24) AS less24
            , SUM(over24) AS over24
            , SUM(less3 + less6 + less9 + less12 + less18 + less24 + over24) AS total
            , CASE
                  WHEN division = 'Mattress & Topper' THEN SUM(less18 + less24 + over24)
                  WHEN division = 'Non-Mattress' THEN SUM(less24 + over24)
                  WHEN division = 'Others' OR division = 'Unknown' THEN SUM(over24)
                --                   ELSE SUM(less24 + over24)
              END AS sum_risk
            --     , SUM(less3) / (SUM(less3) + SUM(less6) + SUM(less12) + SUM(less18) + SUM(less24) + SUM(over24)) AS ratio_less3

            , SUM(less3_unit) AS less3_unit
            , SUM(less6_unit) AS less6_unit
            , SUM(less9_unit) AS less9_unit
            , SUM(less12_unit) AS less12_unit
            , SUM(less18_unit) AS less18_unit
            , SUM(less24_unit) AS less24_unit
            , SUM(over24_unit) AS over24_unit
            , SUM(less3_unit + less6_unit + less9_unit + less12_unit + less18_unit + less24_unit + over24_unit) AS total_unit
            , CASE
                  WHEN division = 'Mattress & Topper' THEN SUM(less18_unit + less24_unit + over24_unit)
                  WHEN division = 'Non-Mattress' THEN SUM(less24_unit + over24_unit)
                  WHEN division = 'Others' OR division = 'Unknown' THEN SUM(over24_unit)
                --                   ELSE SUM(less24_unit + over24_unit)
              END AS sum_risk_unit
        FROM
            mart.amz_di_global_aging_report
        --             (
        --                 SELECT
        --                     * EXCEPT (division)
        --                     , CASE
        --                           WHEN category = '95.OTHERS' THEN 'Others'
        --                           WHEN category = 'UNKNOWN'   THEN 'Unknown'
        --                           ELSE if(division = 'M', 'Mattress & Topper', 'Non-Mattress')
        --                       END AS division
        --
        --                 FROM
        --                     mart.amz_di_global_aging_report
        --             )
        WHERE
            time_level IN ( 'YEAR', 'MONTH' )
            AND country='US'
        GROUP BY GROUPING SETS ( ( country, 2, 3, 4 ), ( 2, 3, 4 ) )
    ),
    cte_union as (
        with cte_add_risk as (
            SELECT
                company
                , division
                , time_level
                , time
                , less3
                , less6
                , less9
                , less12
                , less18
                , less24
                , over24
                , total
                --     , sum_risk

                --             , IF(total > 0, sum_risk / total, NULL) AS risk
                --             , IF(total > 0, less3 / total, NULL) AS risk_less3
                --             , IF(total > 0, less6 / total, NULL) AS risk_less6
                --             , IF(total > 0, less12 / total, NULL) AS risk_less12
                --             , IF(total > 0, less18 / total, NULL) AS risk_less18
                --             , IF(total > 0, less24 / total, NULL) AS risk_less24
                --             , IF(total > 0, over24 / total, NULL) AS risk_over24

                , sum_risk AS risk
                , NULL AS risk_less3
                , NULL AS risk_less6
                , NULL AS risk_less9
                , NULL AS risk_less12
                , IF(division = 'Mattress & Topper', less18, NULL) AS risk_less18
                , IF(division = 'Mattress & Topper' OR division = 'Non-Mattress', less24, NULL) AS risk_less24
                , over24 AS risk_over24


                , less3_unit
                , less6_unit
                , less9_unit
                , less12_unit
                , less18_unit
                , less24_unit
                , over24_unit
                , total_unit

                --             , IF(total_unit > 0, sum_risk_unit / total_unit, NULL) AS risk_unit
                --             , IF(total_unit > 0, less3_unit / total_unit, NULL) AS risk_less3_unit
                --             , IF(total_unit > 0, less6_unit / total_unit, NULL) AS risk_less6_unit
                --             , IF(total_unit > 0, less12_unit / total_unit, NULL) AS risk_less12_unit
                --             , IF(total_unit > 0, less18_unit / total_unit, NULL) AS risk_less18_unit
                --             , IF(total_unit > 0, less24_unit / total_unit, NULL) AS risk_less24_unit
                --             , IF(total_unit > 0, over24_unit / total_unit, NULL) AS risk_over24_unit

                , sum_risk_unit AS risk_unit
                , NULL AS risk_less3_unit
                , NULL AS risk_less6_unit
                , NULL AS risk_less9_unit
                , NULL AS risk_less12_unit
                , IF(division = 'Mattress & Topper', less18_unit, NULL) AS risk_less18_unit
                , IF(division = 'Mattress & Topper' OR division = 'Non-Mattress', less24_unit, NULL) AS risk_less24_unit
                --                 , less24_unit AS risk_less24_unit
                , over24_unit AS risk_over24_unit

            FROM
                cte_grp
        )
        SELECT * FROM cte_add_risk

        UNION ALL

        SELECT
            company
            , 'Total' AS division
            , time_level
            , time
            , SUM(less3) AS less3
            , SUM(less6) AS less6
            , SUM(less9) AS less9
            , SUM(less12) AS less12
            , SUM(less18) AS less18
            , SUM(less24) AS less24
            , SUM(over24) AS over24
            , SUM(total) AS total

            , SUM(risk) AS risk
            , SUM(risk_less3) AS risk_less3
            , SUM(risk_less6) AS risk_less6
            , SUM(risk_less9) AS risk_less9
            , SUM(risk_less12) AS risk_less12
            , SUM(risk_less18) AS risk_less18
            , SUM(risk_less24) AS risk_less24
            , SUM(risk_over24) AS risk_over24

            , SUM(less3_unit) AS less3_unit
            , SUM(less6_unit) AS less6_unit
            , SUM(less9_unit) AS less9_unit
            , SUM(less12_unit) AS less12_unit
            , SUM(less18_unit) AS less18_unit
            , SUM(less24_unit) AS less24_unit
            , SUM(over24_unit) AS over24_unit
            , SUM(total_unit) AS total_unit

            , SUM(risk_unit) AS risk_unit
            , SUM(risk_less3_unit) AS risk_less3_unit
            , SUM(risk_less6_unit) AS risk_less6_unit
            , SUM(risk_less9_unit) AS risk_less9_unit
            , SUM(risk_less12_unit) AS risk_less12_unit
            , SUM(risk_less18_unit) AS risk_less18_unit
            , SUM(risk_less24_unit) AS risk_less24_unit
            , SUM(risk_over24_unit) AS risk_over24_unit
        FROM cte_add_risk
        GROUP BY company, time_level, time

    )
-- 25.12.01 - bi 요청 / if(company = 'T', null, ..)
SELECT
    company
    , division
    , time_level
    , time
    , IF(company = 'T', NULL, less3) AS less3
    , IF(company = 'T', NULL, less6) AS less6
    , IF(company = 'T', NULL, less9) AS less9
    , IF(company = 'T', NULL, less12) AS less12
    , IF(company = 'T', NULL, less18) AS less18
    , IF(company = 'T', NULL, less24) AS less24
    , IF(company = 'T', NULL, over24) AS over24
    , IF(company = 'T', NULL, total) AS total

    , IF(company = 'T', NULL, risk) AS risk
    , IF(company = 'T', NULL, risk_less3) AS risk_less3
    , IF(company = 'T', NULL, risk_less6) AS risk_less6
    , IF(company = 'T', NULL, risk_less9) AS risk_less9
    , IF(company = 'T', NULL, risk_less12) AS risk_less12
    , IF(company = 'T', NULL, risk_less18) AS risk_less18
    , IF(company = 'T', NULL, risk_less24) AS risk_less24
    , IF(company = 'T', NULL, risk_over24) AS risk_over24

    , IF(company = 'T', NULL, less3_unit) AS less3_unit
    , IF(company = 'T', NULL, less6_unit) AS less6_unit
    , IF(company = 'T', NULL, less9_unit) AS less9_unit
    , IF(company = 'T', NULL, less12_unit) AS less12_unit
    , IF(company = 'T', NULL, less18_unit) AS less18_unit
    , IF(company = 'T', NULL, less24_unit) AS less24_unit
    , IF(company = 'T', NULL, over24_unit) AS over24_unit
    , IF(company = 'T', NULL, total_unit) AS total_unit

    , IF(company = 'T', NULL, risk_unit) AS risk_unit
    , IF(company = 'T', NULL, risk_less3_unit) AS risk_less3_unit
    , IF(company = 'T', NULL, risk_less6_unit) AS risk_less6_unit
    , IF(company = 'T', NULL, risk_less9_unit) AS risk_less9_unit
    , IF(company = 'T', NULL, risk_less12_unit) AS risk_less12_unit
    , IF(company = 'T', NULL, risk_less18_unit) AS risk_less18_unit
    , IF(company = 'T', NULL, risk_less24_unit) AS risk_less24_unit
    , IF(company = 'T', NULL, risk_over24_unit) AS risk_over24_unit
FROM
    cte_union
;

CREATE OR REPLACE TABLE mart.amz_di_global_aging_summary_non_us AS
WITH
    cte_grp AS (
        SELECT
            IF(GROUPING ( country ) = 1, 'T', country) AS company
            --         , IF(GROUPING ( division ) = 1, 'T', division) AS product_group
            , division
            , time_level
            , IF(time_level = 'YEAR', CAST(year AS STRING), yr_month) AS time
            , SUM(less3) AS less3
            , SUM(less6) AS less6
            , SUM(less9) AS less9
            , SUM(less12) AS less12
            , SUM(less18) AS less18
            , SUM(less24) AS less24
            , SUM(over24) AS over24
            , SUM(less3 + less6 + less9 + less12 + less18 + less24 + over24) AS total
            , CASE
                  WHEN division = 'Mattress & Topper' THEN SUM(less18 + less24 + over24)
                  WHEN division = 'Non-Mattress' THEN SUM(less24 + over24)
                  WHEN division = 'Others' OR division = 'Unknown' THEN SUM(over24)
                --                   ELSE SUM(less24 + over24)
              END AS sum_risk
            --     , SUM(less3) / (SUM(less3) + SUM(less6) + SUM(less12) + SUM(less18) + SUM(less24) + SUM(over24)) AS ratio_less3

            , SUM(less3_unit) AS less3_unit
            , SUM(less6_unit) AS less6_unit
            , SUM(less9_unit) AS less9_unit
            , SUM(less12_unit) AS less12_unit
            , SUM(less18_unit) AS less18_unit
            , SUM(less24_unit) AS less24_unit
            , SUM(over24_unit) AS over24_unit
            , SUM(less3_unit + less6_unit + less9_unit + less12_unit + less18_unit + less24_unit + over24_unit) AS total_unit
            , CASE
                  WHEN division = 'Mattress & Topper' THEN SUM(less18_unit + less24_unit + over24_unit)
                  WHEN division = 'Non-Mattress' THEN SUM(less24_unit + over24_unit)
                  WHEN division = 'Others' OR division = 'Unknown' THEN SUM(over24_unit)
                --                   ELSE SUM(less24_unit + over24_unit)
              END AS sum_risk_unit
        FROM
            mart.amz_di_global_aging_report
        --             (
        --                 SELECT
        --                     * EXCEPT (division)
        --                     , CASE
        --                           WHEN category = '95.OTHERS' THEN 'Others'
        --                           WHEN category = 'UNKNOWN'   THEN 'Unknown'
        --                           ELSE if(division = 'M', 'Mattress & Topper', 'Non-Mattress')
        --                       END AS division
        --
        --                 FROM
        --                     mart.amz_di_global_aging_report
        --             )
        WHERE
            time_level IN ( 'YEAR', 'MONTH' )
            AND country != 'US'
        GROUP BY GROUPING SETS ( ( country, 2, 3, 4 ), ( 2, 3, 4 ) )
    ),
    cte_union as (
        with cte_add_risk as (
            SELECT
                company
                , division
                , time_level
                , time
                , less3
                , less6
                , less9
                , less12
                , less18
                , less24
                , over24
                , total
                --     , sum_risk

                --             , IF(total > 0, sum_risk / total, NULL) AS risk
                --             , IF(total > 0, less3 / total, NULL) AS risk_less3
                --             , IF(total > 0, less6 / total, NULL) AS risk_less6
                --             , IF(total > 0, less12 / total, NULL) AS risk_less12
                --             , IF(total > 0, less18 / total, NULL) AS risk_less18
                --             , IF(total > 0, less24 / total, NULL) AS risk_less24
                --             , IF(total > 0, over24 / total, NULL) AS risk_over24

                , sum_risk AS risk
                , NULL AS risk_less3
                , NULL AS risk_less6
                , NULL AS risk_less9
                , NULL AS risk_less12
                , IF(division = 'Mattress & Topper', less18, NULL) AS risk_less18
                , IF(division = 'Mattress & Topper' OR division = 'Non-Mattress', less24, NULL) AS risk_less24
                , over24 AS risk_over24


                , less3_unit
                , less6_unit
                , less9_unit
                , less12_unit
                , less18_unit
                , less24_unit
                , over24_unit
                , total_unit

                --             , IF(total_unit > 0, sum_risk_unit / total_unit, NULL) AS risk_unit
                --             , IF(total_unit > 0, less3_unit / total_unit, NULL) AS risk_less3_unit
                --             , IF(total_unit > 0, less6_unit / total_unit, NULL) AS risk_less6_unit
                --             , IF(total_unit > 0, less12_unit / total_unit, NULL) AS risk_less12_unit
                --             , IF(total_unit > 0, less18_unit / total_unit, NULL) AS risk_less18_unit
                --             , IF(total_unit > 0, less24_unit / total_unit, NULL) AS risk_less24_unit
                --             , IF(total_unit > 0, over24_unit / total_unit, NULL) AS risk_over24_unit

                , sum_risk_unit AS risk_unit
                , NULL AS risk_less3_unit
                , NULL AS risk_less6_unit
                , NULL AS risk_less9_unit
                , NULL AS risk_less12_unit
                , IF(division = 'Mattress & Topper', less18_unit, NULL) AS risk_less18_unit
                , IF(division = 'Mattress & Topper' OR division = 'Non-Mattress', less24_unit, NULL) AS risk_less24_unit
                --                 , less24_unit AS risk_less24_unit
                , over24_unit AS risk_over24_unit

            FROM
                cte_grp
        )
        SELECT * FROM cte_add_risk

        UNION ALL

        SELECT
            company
            , 'Total' AS division
            , time_level
            , time
            , SUM(less3) AS less3
            , SUM(less6) AS less6
            , SUM(less9) AS less9
            , SUM(less12) AS less12
            , SUM(less18) AS less18
            , SUM(less24) AS less24
            , SUM(over24) AS over24
            , SUM(total) AS total

            , SUM(risk) AS risk
            , SUM(risk_less3) AS risk_less3
            , SUM(risk_less6) AS risk_less6
            , SUM(risk_less9) AS risk_less9
            , SUM(risk_less12) AS risk_less12
            , SUM(risk_less18) AS risk_less18
            , SUM(risk_less24) AS risk_less24
            , SUM(risk_over24) AS risk_over24

            , SUM(less3_unit) AS less3_unit
            , SUM(less6_unit) AS less6_unit
            , SUM(less9_unit) AS less9_unit
            , SUM(less12_unit) AS less12_unit
            , SUM(less18_unit) AS less18_unit
            , SUM(less24_unit) AS less24_unit
            , SUM(over24_unit) AS over24_unit
            , SUM(total_unit) AS total_unit

            , SUM(risk_unit) AS risk_unit
            , SUM(risk_less3_unit) AS risk_less3_unit
            , SUM(risk_less6_unit) AS risk_less6_unit
            , SUM(risk_less9_unit) AS risk_less9_unit
            , SUM(risk_less12_unit) AS risk_less12_unit
            , SUM(risk_less18_unit) AS risk_less18_unit
            , SUM(risk_less24_unit) AS risk_less24_unit
            , SUM(risk_over24_unit) AS risk_over24_unit
        FROM cte_add_risk
        GROUP BY company, time_level, time

    )
-- 25.12.01 - bi 요청 / if(company = 'T', null, ..)
SELECT
    company
    , division
    , time_level
    , time
    , IF(company = 'T', NULL, less3) AS less3
    , IF(company = 'T', NULL, less6) AS less6
    , IF(company = 'T', NULL, less9) AS less9
    , IF(company = 'T', NULL, less12) AS less12
    , IF(company = 'T', NULL, less18) AS less18
    , IF(company = 'T', NULL, less24) AS less24
    , IF(company = 'T', NULL, over24) AS over24
    , IF(company = 'T', NULL, total) AS total

    , IF(company = 'T', NULL, risk) AS risk
    , IF(company = 'T', NULL, risk_less3) AS risk_less3
    , IF(company = 'T', NULL, risk_less6) AS risk_less6
    , IF(company = 'T', NULL, risk_less9) AS risk_less9
    , IF(company = 'T', NULL, risk_less12) AS risk_less12
    , IF(company = 'T', NULL, risk_less18) AS risk_less18
    , IF(company = 'T', NULL, risk_less24) AS risk_less24
    , IF(company = 'T', NULL, risk_over24) AS risk_over24

    , IF(company = 'T', NULL, less3_unit) AS less3_unit
    , IF(company = 'T', NULL, less6_unit) AS less6_unit
    , IF(company = 'T', NULL, less9_unit) AS less9_unit
    , IF(company = 'T', NULL, less12_unit) AS less12_unit
    , IF(company = 'T', NULL, less18_unit) AS less18_unit
    , IF(company = 'T', NULL, less24_unit) AS less24_unit
    , IF(company = 'T', NULL, over24_unit) AS over24_unit
    , IF(company = 'T', NULL, total_unit) AS total_unit

    , IF(company = 'T', NULL, risk_unit) AS risk_unit
    , IF(company = 'T', NULL, risk_less3_unit) AS risk_less3_unit
    , IF(company = 'T', NULL, risk_less6_unit) AS risk_less6_unit
    , IF(company = 'T', NULL, risk_less9_unit) AS risk_less9_unit
    , IF(company = 'T', NULL, risk_less12_unit) AS risk_less12_unit
    , IF(company = 'T', NULL, risk_less18_unit) AS risk_less18_unit
    , IF(company = 'T', NULL, risk_less24_unit) AS risk_less24_unit
    , IF(company = 'T', NULL, risk_over24_unit) AS risk_over24_unit
FROM
    cte_union
;

-- [US & Non US ] ------------------------------------------------------------------------------------------------------
-- select distinct country FROM mart.amz_di_global_psi_report;
-- select distinct country FROM mart.amz_di_global_psi_report_detail;
-- select distinct country FROM mart.amz_di_global_psi_report_by_collection;
-- select distinct country FROM mart.amz_di_global_aging_report;

-- select distinct company FROM mart.amz_di_global_aging_summary;  -- has T


CREATE OR REPLACE TABLE mart.amz_di_global_psi_report_us AS
SELECT * FROM mart.amz_di_global_psi_report WHERE country = 'US';

CREATE OR REPLACE TABLE mart.amz_di_global_psi_report_non_us AS
SELECT * FROM mart.amz_di_global_psi_report WHERE country != 'US';

CREATE OR REPLACE TABLE mart.amz_di_global_psi_report_detail_us as
SELECT * FROM mart.amz_di_global_psi_report_detail WHERE country = 'US';

CREATE OR REPLACE TABLE mart.amz_di_global_psi_report_detail_non_us as
SELECT * FROM mart.amz_di_global_psi_report_detail WHERE country != 'US';

CREATE OR REPLACE TABLE mart.amz_di_global_psi_report_by_collection_us as
SELECT * FROM mart.amz_di_global_psi_report_by_collection WHERE country = 'US';

CREATE OR REPLACE TABLE mart.amz_di_global_psi_report_by_collection_non_us as
SELECT * FROM mart.amz_di_global_psi_report_by_collection WHERE country != 'US';

CREATE OR REPLACE TABLE mart.amz_di_global_aging_report_us as
SELECT * FROM mart.amz_di_global_aging_report WHERE country = 'US';

CREATE OR REPLACE TABLE mart.amz_di_global_aging_report_non_us as
SELECT * FROM mart.amz_di_global_aging_report WHERE country != 'US';

-- mart.amz_di_global_aging_summary 는 국가 전체(T) 집계가 있어 테이블 생성 쿼리를 전체, us, non us 세종류로 분리
-- CREATE OR REPLACE TABLE mart.amz_di_global_aging_summary_us as
-- CREATE OR REPLACE TABLE mart.amz_di_global_aging_summary_non_us as
