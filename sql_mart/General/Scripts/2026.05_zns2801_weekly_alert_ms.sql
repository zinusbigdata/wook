/*
 * ZNS-2801_Market Share Alert
 */

-- [ 01. Total Market ] ------------------------------------------------------------------
WITH weekly_market AS (
    SELECT
        yr_week,
        SUM(RetailSales) AS sales,
        SUM(UnitsSold)   AS units
    FROM vs1.stckln_amz_ms_trend
    WHERE bsr_ctgry_label = '01. Mattresses'
    GROUP BY yr_week
)
, compared AS (
    SELECT
        yr_week,
        sales,
        units,
        -- WoW (1주 전)
        LAG(sales, 1)  OVER (ORDER BY yr_week) AS sales_w1,
        LAG(units, 1)  OVER (ORDER BY yr_week) AS units_w1,
        -- YoY (52주 전)
        LAG(sales, 52) OVER (ORDER BY yr_week) AS sales_y1,
        LAG(units, 52) OVER (ORDER BY yr_week) AS units_y1,
        -- 4주 / 13주 이동평균 (노이즈 완화 & 추세 비교용)
        AVG(sales) OVER (
            ORDER BY yr_week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS sales_ma4,
        AVG(sales) OVER (
            ORDER BY yr_week
            ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
        ) AS sales_ma13,
        -- 평균 단가(ASP) = 매출 / 판매수량
        SAFE_DIVIDE(sales, units) AS asp
    FROM weekly_market
)
SELECT
    yr_week,
    ROUND(sales, 0)  AS sales,
    units,
    ROUND(asp, 2)    AS asp,

    -- WoW 변화
    ROUND((sales / NULLIF(sales_w1, 0) - 1) * 100, 1) AS sales_wow_pct,
    ROUND((units / NULLIF(units_w1, 0) - 1) * 100, 1) AS units_wow_pct,
    --ROUND(sales - sales_w1, 1)              AS sales_wow_pp,

    -- YoY 변화
    ROUND((sales / NULLIF(sales_y1, 0) - 1) * 100, 1) AS sales_yoy_pct,
    --ROUND(sales - sales_y1, 1)              AS sales_yoy_pp,
    ROUND((units / NULLIF(units_y1, 0) - 1) * 100, 1) AS units_yoy_pct,

    -- 추세선(이동평균)
    ROUND(sales_ma4,  0) AS sales_ma4,
    ROUND(sales_ma13, 0) AS sales_ma13,

    -- 이동평균 기준 YoY (단주 변동성 제거한 진짜 추세 비교)
    ROUND(
        (sales_ma4 / NULLIF(LAG(sales_ma4, 52) OVER (ORDER BY yr_week), 0) - 1) * 100,
        1
    ) AS sales_ma4_yoy_pct
FROM compared
ORDER BY yr_week DESC;




-- [ 02. Top 10 Brand ] -----------------------------------------------------------------
WITH weekly_brand AS (
    SELECT
        yr_week,
        Brand_raw,
        SUM(RetailSales) AS sales,
        SUM(UnitsSold)   AS units
    FROM vs1.stckln_amz_ms_trend
    WHERE bsr_ctgry_label = '01. Mattresses'
    GROUP BY yr_week, Brand_raw
)
, with_share AS (
    SELECT
        yr_week, Brand_raw, sales, units,
        SAFE_DIVIDE(sales, units) AS asp,
        sales / NULLIF(SUM(sales) OVER (PARTITION BY yr_week), 0) * 100 AS share_pct,
        RANK() OVER (PARTITION BY yr_week ORDER BY sales DESC)          AS sales_rank
    FROM weekly_brand
)
, with_ma AS (
    SELECT
        yr_week, Brand_raw, sales, units, asp, share_pct, sales_rank,
        AVG(share_pct) OVER (
            PARTITION BY Brand_raw ORDER BY yr_week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS share_ma4,
        -- ★ ASP의 4주 이동평균 (주간 노이즈 완화)
        AVG(asp) OVER (
            PARTITION BY Brand_raw ORDER BY yr_week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS asp_ma4
    FROM with_share
)
, compared AS (
    SELECT
        yr_week, Brand_raw, sales, units, asp, share_pct, sales_rank,
        share_ma4, asp_ma4,
        -- WoW (1주 전)
        LAG(sales,     1)  OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS sales_w1,
        LAG(units,     1)  OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS units_w1,
        LAG(asp,       1)  OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS asp_w1,
        LAG(share_pct, 1)  OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS share_w1,
        LAG(sales_rank,1)  OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS rank_w1,
        -- YoY (52주 전)
        LAG(sales,     52) OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS sales_y1,
        LAG(units,     52) OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS units_y1,
        LAG(asp,       52) OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS asp_y1,
        LAG(share_pct, 52) OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS share_y1,
        LAG(share_ma4, 52) OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS share_ma4_y1,
        -- ★ ASP 이동평균의 52주 전 값
        LAG(asp_ma4,   52) OVER (PARTITION BY Brand_raw ORDER BY yr_week) AS asp_ma4_y1
    FROM with_ma
)
SELECT
    yr_week,
    Brand_raw,
    ROUND(sales, 0)     AS sales,
    units,
    ROUND(asp, 2)       AS asp,
    ROUND(share_pct, 2) AS share_pct,
    sales_rank,
    -- WoW 변화
    --ROUND((sales / NULLIF(sales_w1, 0) - 1) * 100, 1) AS sales_wow_pct,
    --ROUND((units / NULLIF(units_w1, 0) - 1) * 100, 1) AS units_wow_pct,
    --ROUND((asp   / NULLIF(asp_w1,   0) - 1) * 100, 1) AS asp_wow_pct,
    ROUND(share_pct - share_w1, 2)                    AS share_wow_pp,
    rank_w1 - sales_rank                              AS rank_chg_wow,
    -- YoY 변화
    --ROUND((sales / NULLIF(sales_y1, 0) - 1) * 100, 1) AS sales_yoy_pct,
    --ROUND((units / NULLIF(units_y1, 0) - 1) * 100, 1) AS units_yoy_pct,
    --ROUND((asp   / NULLIF(asp_y1,   0) - 1) * 100, 1) AS asp_yoy_pct,
    ROUND(share_pct - share_y1, 2)                    AS share_yoy_pp,
    -- 이동평균 & YoY 추세
    ROUND(share_ma4, 2)                               AS share_ma4,
    ROUND(share_ma4 - share_ma4_y1, 2)                AS share_ma4_yoy_pp,
    -- ★ 추가: ASP 4주 이동평균 및 YoY
    ROUND(asp_ma4, 2)                                 AS asp_ma4,
    ROUND(asp_ma4 - asp_ma4_y1, 2)                    AS asp_ma4_yoy,       -- 절대값 차이 ($)
    ROUND((asp_ma4 / NULLIF(asp_ma4_y1, 0) - 1) * 100, 1) AS asp_ma4_yoy_pct -- 변화율 (%)
FROM compared
WHERE sales_rank <= 10
ORDER BY yr_week DESC, sales_rank
;

/* End */


--SELECT min(WeekEnding), max(WeekEnding) FROM stck.atlas_sales_all 


