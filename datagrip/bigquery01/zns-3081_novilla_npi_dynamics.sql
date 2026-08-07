/*
 * ZNS-3081 : NOVILLA가 Zinus의 전철을 밟고 있는지 분석.
 * 소스 테이블 : wook.stck_sales_analysis_of_target_brands
 */

--------------------------
-- Step 1 : 코호트 분석 --
--------------------------
WITH
-- 42-month observation calendar (2023-01 ~ 2026-06)
months AS (
  SELECT FORMAT_DATE('%Y-%m', d) AS ym, o
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2023-01-01', DATE '2026-06-01', INTERVAL 1 MONTH)) d
  WITH OFFSET o
),

-- Collapse source rows to one row per asin x month
raw_monthly AS (
  SELECT asin,
         ANY_VALUE(first_year) AS first_year,
         ANY_VALUE(first_date) AS first_date,
         ANY_VALUE(inch)       AS inch,
         ANY_VALUE(size)       AS size,
         yr_month,
         SUM(units) AS units,
         SUM(sales) AS sales
  FROM `market-analysis-project-91130.wook.stck_sales_analysis_of_target_brands`
  WHERE brand = 'NOVILLA'
    AND yr_month BETWEEN '2023-01' AND '2026-06'
  GROUP BY asin, yr_month
),

-- Launch year (impute from first selling month when first_year is NULL)
-- launch_idx = month index of first_date, anchor for the 6-month window
asin_master AS (
  SELECT asin,
         ANY_VALUE(inch) AS inch,
         ANY_VALUE(size) AS size,
         COALESCE(ANY_VALUE(first_year), SUBSTR(MIN(yr_month), 1, 4)) AS launch_year,
         ANY_VALUE(first_year) IS NULL                                AS is_imputed,
         GREATEST(0, DATE_DIFF(
           COALESCE(DATE_TRUNC(ANY_VALUE(first_date), MONTH),
                    PARSE_DATE('%Y-%m', MIN(yr_month))),
           DATE '2023-01-01', MONTH))                                 AS launch_idx
  FROM raw_monthly
  GROUP BY asin
),
cohort AS (SELECT * FROM asin_master WHERE launch_year IN ('2023','2024','2025')),

-- Dense grid: months with no sales become 0 (required for gap detection)
grid AS (
  SELECT c.asin, c.launch_year, c.is_imputed, c.inch, c.size, c.launch_idx, m.o,
         IFNULL(r.units, 0) AS units,
         IFNULL(r.sales, 0) AS sales
  FROM cohort c
  CROSS JOIN months m
  LEFT JOIN raw_monthly r ON r.asin = c.asin AND r.yr_month = m.ym
),

-- Compress monthly units into an array + 6-month launch window volume
series AS (
  SELECT asin,
         ANY_VALUE(launch_year) AS launch_year,
         ANY_VALUE(is_imputed)  AS is_imputed,
         ANY_VALUE(inch)        AS inch,
         ANY_VALUE(size)        AS size,
         ARRAY_AGG(units ORDER BY o) AS monthly_units,
         ROUND(SUM(sales)) AS total_sales,
         SUM(units)        AS total_units,
         SUM(IF(o BETWEEN launch_idx AND launch_idx + 5, units, 0)) AS units_first_6m
  FROM grid
  GROUP BY asin
),

-- First selling month = first month with units >= 2
first_sale AS (
  SELECT s.*,
         (SELECT MIN(i) FROM UNNEST(GENERATE_ARRAY(0,41)) i
          WHERE monthly_units[OFFSET(i)] >= 2) AS first_idx
  FROM series s
),

-- Churn point = first run of 3 consecutive months with units <= 1
-- Upper bound is 39 because i+2 must stay inside the 42-month array
churn AS (
  SELECT f.*,
         IF(f.first_idx IS NULL, NULL,
            (SELECT MIN(i) FROM UNNEST(GENERATE_ARRAY(0,39)) i
             WHERE i > f.first_idx
               AND monthly_units[OFFSET(i)]   <= 1
               AND monthly_units[OFFSET(i+1)] <= 1
               AND monthly_units[OFFSET(i+2)] <= 1)) AS churn_idx
  FROM first_sale f
),

flags AS (
  SELECT c.*,
         c.units_first_6m < 10   AS is_launch_fail,   -- excluded from survival rate
         c.churn_idx IS NOT NULL AS is_churned,
         IF(c.first_idx IS NULL, NULL,
            (SELECT MAX(i) FROM UNNEST(GENERATE_ARRAY(0,41)) i
             WHERE monthly_units[OFFSET(i)] >= 2
               AND (c.churn_idx IS NULL OR i < c.churn_idx)) - c.first_idx + 1) AS lifespan_months,
         IF(c.first_idx IS NULL, FALSE,
            (SELECT LOGICAL_OR(monthly_units[OFFSET(i)] >= 2)
             FROM UNNEST(GENERATE_ARRAY(0,41)) i
             WHERE c.churn_idx IS NOT NULL AND i > c.churn_idx)) AS has_resale
  FROM churn c
)

SELECT
  launch_year,
  COUNT(*)                    AS launched,
  COUNTIF(is_imputed)         AS imputed_year,  -- 출시년도를 매출 발생 년도도 대체한 경우
  COUNTIF(is_launch_fail)     AS launch_fail,
  COUNTIF(NOT is_launch_fail) AS eligible,
  COUNTIF(NOT is_launch_fail AND is_churned AND lifespan_months <= 3)              AS churn_le_3m,
  COUNTIF(NOT is_launch_fail AND is_churned AND lifespan_months BETWEEN 4 AND 6)   AS churn_4_6m,
  COUNTIF(NOT is_launch_fail AND is_churned AND lifespan_months BETWEEN 7 AND 9)   AS churn_7_9m,
  COUNTIF(NOT is_launch_fail AND is_churned AND lifespan_months BETWEEN 10 AND 12) AS churn_10_12m,
  COUNTIF(NOT is_launch_fail AND is_churned AND lifespan_months > 12)              AS churn_over_12m,
  COUNTIF(NOT is_launch_fail AND NOT is_churned)                                   AS surviving,
  ROUND(100 * COUNTIF(NOT is_launch_fail AND NOT is_churned)
            / NULLIF(COUNTIF(NOT is_launch_fail), 0), 1)  AS survival_rate_pct,
  CAST(SUM(IF(NOT is_launch_fail, total_sales, 0)) AS INT64) AS eligible_sales,
  CAST(SUM(IF(is_launch_fail, total_sales, 0))     AS INT64) AS fail_sales,
  COUNTIF(NOT is_launch_fail AND has_resale)                 AS resale_after_churn,
  ROUND(AVG(IF(NOT is_launch_fail AND is_churned, lifespan_months, NULL)), 1) AS avg_lifespan_churned
FROM flags
GROUP BY ROLLUP(launch_year)
ORDER BY launch_year IS NULL, launch_year;


-- =========================================================
-- NOVILLA 신제품 ASIN 단위 생존 분석 (v16 정의)
-- 관측: 2023-01 ~ 2026-06 (42개월, offset 0~41)
-- 기준점: 첫 매출월(s0). first_date 미사용
-- 결과: 747개 ASIN / 생존 453개
-- =========================================================
WITH months AS (
  SELECT FORMAT_DATE('%Y-%m', d) AS ym, o
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2023-01-01', DATE '2026-06-01', INTERVAL 1 MONTH)) d
  WITH OFFSET o
),

-- 1) ASIN × 월 집계 (seller_type 등으로 분할된 행 합산)
src AS (
  SELECT asin,
         ANY_VALUE(first_year) AS fy,
         ANY_VALUE(inch)       AS inch,
         ANY_VALUE(size)       AS size,
         yr_month, SUM(sales) AS s, SUM(units) AS u
  FROM `market-analysis-project-91130.wook.stck_sales_analysis_of_target_brands`
  WHERE brand = 'NOVILLA'
    AND yr_month BETWEEN '2023-01' AND '2026-06'
  GROUP BY asin, yr_month
),

-- 2) 출시연도 + 첫 매출월 인덱스(s0)  ★ 모든 시점 계산의 단일 기준점
--    ※ first_year 결측 ASIN은 보정하지 않고 제외 (747개 확정)
master AS (
  SELECT asin, ANY_VALUE(inch) AS inch, ANY_VALUE(size) AS size,
         ANY_VALUE(fy) AS launch_year,
         DATE_DIFF(PARSE_DATE('%Y-%m', MIN(yr_month)), DATE '2023-01-01', MONTH) AS s0
  FROM src GROUP BY asin
),
base AS (SELECT * FROM master WHERE launch_year IN ('2023','2024','2025')),

-- 3) 판매 없는 달을 0으로 채운 완전 격자 ★ 연속 무판매 판정의 전제
grid AS (
  SELECT b.asin, b.launch_year, b.inch, b.size, b.s0, m.o,
         IFNULL(s.u, 0) AS u, IFNULL(s.s, 0) AS s
  FROM base b
  CROSS JOIN months m
  LEFT JOIN src s ON s.asin = b.asin AND s.yr_month = m.ym
),

-- 4) 배열 압축 + s0 기준 6개월 누적 판매량 / 12개월 누적 매출
agg AS (
  SELECT asin, launch_year, inch, size, s0,
         ARRAY_AGG(u ORDER BY o) AS us,
         ROUND(SUM(s)) AS t_sales,
         SUM(u)        AS t_units,
         SUM(IF(o BETWEEN s0 AND s0 + 5,  u, 0))         AS units_6m,   -- 판매 미달 판정
         ROUND(SUM(IF(o BETWEEN s0 AND s0 + 11, s, 0)))  AS sales_12m   -- 첫 12개월 매출
  FROM grid GROUP BY asin, launch_year, inch, size, s0
),

-- 5) 참고용: 월 판매량 2개 이상인 최초 달 (수명 계산에는 미사용)
mark AS (
  SELECT a.*,
         (SELECT MIN(i) FROM UNNEST(GENERATE_ARRAY(0,41)) i WHERE us[OFFSET(i)] >= 2) AS fi
  FROM agg a
),

-- 6) 소멸 판정 ★ 기준점을 s0로 변경 (이전에는 fi)
--    1개 이하 3개월 연속 + 그 시점 이후 누적 20개 미만인 최초 지점
death AS (
  SELECT m.*,
    (SELECT MIN(i) FROM UNNEST(GENERATE_ARRAY(0,39)) i
     WHERE i > m.s0
       AND us[OFFSET(i)]   <= 1
       AND us[OFFSET(i+1)] <= 1
       AND us[OFFSET(i+2)] <= 1
       AND (SELECT SUM(us[OFFSET(j)]) FROM UNNEST(GENERATE_ARRAY(0,41)) j WHERE j >= i) < 20
    ) AS di
  FROM mark m
),

-- 7) 마지막 판매월 · 판매월수 · 소멸 후 재판매
calc AS (
  SELECT d.*,
    d.units_6m < 10 AS shortfall,
    (SELECT MAX(i) FROM UNNEST(GENERATE_ARRAY(0,41)) i
     WHERE us[OFFSET(i)] >= 2 AND (d.di IS NULL OR i < d.di))            AS la,
    (SELECT COUNTIF(us[OFFSET(i)] >= 2) FROM UNNEST(GENERATE_ARRAY(0,41)) i
     WHERE d.di IS NULL OR i < d.di)                                     AS sell_months,
    IF(d.di IS NULL, FALSE,
       (SELECT LOGICAL_OR(us[OFFSET(i)] >= 2) FROM UNNEST(GENERATE_ARRAY(0,41)) i
        WHERE i > d.di))                                                 AS resale,
    IF(d.di IS NULL, 0,
       (SELECT IFNULL(SUM(us[OFFSET(i)]), 0) FROM UNNEST(GENERATE_ARRAY(0,41)) i
        WHERE i > d.di))                                                 AS post_units
  FROM death d
),

-- 8) 수명 = 첫 매출월 ~ 소멸 직전 마지막 판매월 (양끝 포함)
final AS (
  SELECT c.*, IF(la IS NULL, 0, la - s0 + 1) AS span_months FROM calc c
)

SELECT
  asin, launch_year, inch, size,
  t_sales, t_units, units_6m, sales_12m,
  FORMAT_DATE('%Y-%m', DATE_ADD(DATE '2023-01-01', INTERVAL s0 MONTH))        AS anchor_month,
  IFNULL(FORMAT_DATE('%Y-%m', DATE_ADD(DATE '2023-01-01', INTERVAL fi MONTH)), '-') AS first_2plus_month,
  IFNULL(FORMAT_DATE('%Y-%m', DATE_ADD(DATE '2023-01-01', INTERVAL la MONTH)), '-') AS last_month,
  IFNULL(FORMAT_DATE('%Y-%m', DATE_ADD(DATE '2023-01-01', INTERVAL di MONTH)), '-') AS death_month,
  span_months,
  sell_months,
  span_months - sell_months  AS gap_months,
  IF(resale, 'Y', '')        AS resale_flag,
  post_units,
  CASE WHEN shortfall                      THEN 'launch_shortfall'
       WHEN di IS NULL OR span_months > 12 THEN 'surviving'      -- ★ 12개월 초과는 생존
       WHEN span_months <=  3              THEN 'churn_le_3m'
       WHEN span_months <=  6              THEN 'churn_4_6m'
       WHEN span_months <=  9              THEN 'churn_7_9m'
       ELSE                                     'churn_10_12m'
  END AS churn_bucket,
  IF(shortfall OR (di IS NOT NULL AND span_months BETWEEN 1 AND 6), 'Y', '') AS launch_fail_flag,
  us[OFFSET(0)]  AS m2023_01, us[OFFSET(1)]  AS m2023_02, us[OFFSET(2)]  AS m2023_03,
  us[OFFSET(3)]  AS m2023_04, us[OFFSET(4)]  AS m2023_05, us[OFFSET(5)]  AS m2023_06,
  us[OFFSET(6)]  AS m2023_07, us[OFFSET(7)]  AS m2023_08, us[OFFSET(8)]  AS m2023_09,
  us[OFFSET(9)]  AS m2023_10, us[OFFSET(10)] AS m2023_11, us[OFFSET(11)] AS m2023_12,
  us[OFFSET(12)] AS m2024_01, us[OFFSET(13)] AS m2024_02, us[OFFSET(14)] AS m2024_03,
  us[OFFSET(15)] AS m2024_04, us[OFFSET(16)] AS m2024_05, us[OFFSET(17)] AS m2024_06,
  us[OFFSET(18)] AS m2024_07, us[OFFSET(19)] AS m2024_08, us[OFFSET(20)] AS m2024_09,
  us[OFFSET(21)] AS m2024_10, us[OFFSET(22)] AS m2024_11, us[OFFSET(23)] AS m2024_12,
  us[OFFSET(24)] AS m2025_01, us[OFFSET(25)] AS m2025_02, us[OFFSET(26)] AS m2025_03,
  us[OFFSET(27)] AS m2025_04, us[OFFSET(28)] AS m2025_05, us[OFFSET(29)] AS m2025_06,
  us[OFFSET(30)] AS m2025_07, us[OFFSET(31)] AS m2025_08, us[OFFSET(32)] AS m2025_09,
  us[OFFSET(33)] AS m2025_10, us[OFFSET(34)] AS m2025_11, us[OFFSET(35)] AS m2025_12,
  us[OFFSET(36)] AS m2026_01, us[OFFSET(37)] AS m2026_02, us[OFFSET(38)] AS m2026_03,
  us[OFFSET(39)] AS m2026_04, us[OFFSET(40)] AS m2026_05, us[OFFSET(41)] AS m2026_06
FROM final
ORDER BY t_sales DESC;

-------------------
-- KM 생존율 분석 --
-------------------
WITH months AS (
  SELECT FORMAT_DATE('%Y-%m', d) AS ym, o
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2023-01-01', DATE '2026-06-01', INTERVAL 1 MONTH)) d WITH OFFSET o),
raw AS (
  SELECT asin, ANY_VALUE(first_year) AS fy, yr_month, SUM(units) AS u
  FROM `market-analysis-project-91130.wook.stck_sales_analysis_of_target_brands`
  WHERE brand = 'NOVILLA' AND yr_month BETWEEN '2023-01' AND '2026-06'
  GROUP BY asin, yr_month),
am AS (
  SELECT asin, COALESCE(ANY_VALUE(fy), SUBSTR(MIN(yr_month),1,4)) AS launch_year,
         DATE_DIFF(PARSE_DATE('%Y-%m', MIN(yr_month)), DATE '2023-01-01', MONTH) AS s0
  FROM raw GROUP BY asin),
cohort AS (SELECT * FROM am WHERE launch_year = '2025'),          -- ★ 코호트 선택
grid AS (
  SELECT c.asin, c.s0, m.o, IFNULL(r.u, 0) AS u
  FROM cohort c CROSS JOIN months m
  LEFT JOIN raw r ON r.asin = c.asin AND r.yr_month = m.ym),
agg0 AS (
  SELECT asin, ARRAY_AGG(u ORDER BY o) AS us,
         SUM(IF(o BETWEEN s0 AND s0 + 5, u, 0)) AS units_6m
  FROM grid GROUP BY asin),
mark AS (
  SELECT a.*, (SELECT MIN(i) FROM UNNEST(GENERATE_ARRAY(0,41)) i WHERE us[OFFSET(i)] >= 2) AS fi
  FROM agg0 a),
death AS (
  SELECT m.*, IF(m.fi IS NULL, NULL,
    (SELECT MIN(i) FROM UNNEST(GENERATE_ARRAY(0,39)) i
     WHERE i > m.fi AND us[OFFSET(i)] <= 1 AND us[OFFSET(i+1)] <= 1 AND us[OFFSET(i+2)] <= 1
       AND (SELECT SUM(us[OFFSET(j)]) FROM UNNEST(GENERATE_ARRAY(0,41)) j WHERE j >= i) < 20)) AS di
  FROM mark m),

-- ↓ 여기부터 A와 동일
subjects AS (
  SELECT (SELECT MAX(i) FROM UNNEST(GENERATE_ARRAY(0,41)) i
          WHERE us[OFFSET(i)] >= 2 AND (di IS NULL OR i < di)) - fi + 1 AS life,
         di IS NOT NULL AS dead
  FROM death
  WHERE units_6m >= 10                                            -- ★ 판매 미달 제외
),
agg AS (
  SELECT mm, COUNTIF(life >= mm) AS n_risk,
             COUNTIF(life = mm AND dead) AS n_death,
             COUNTIF(life = mm AND NOT dead) AS n_censor
  FROM UNNEST(GENERATE_ARRAY(1, 18)) mm CROSS JOIN subjects GROUP BY mm)
SELECT mm AS month, n_risk, n_death, n_censor,
  ROUND(1 - SAFE_DIVIDE(n_death, n_risk), 4) AS step,
  ROUND(100 * EXP(SUM(LN(1 - SAFE_DIVIDE(n_death, n_risk))) OVER (ORDER BY mm)), 1) AS survival_pct
FROM agg WHERE n_risk > 0 ORDER BY mm;



-- =============================================================================
-- 연도별 제품 운영 · 신제품 출시 · 매출 추이
--   대상: wook.stck_sales_analysis_of_target_brands
--   기간: 2022 ~ 2026년 상반기 (2026년은 1~6월만 집계)
--
-- 컬럼 정의
--   운영 제품 수        : 해당 연도에 매출이 발생한 ASIN 수
--   신제품 출시 수      : first_year가 해당 연도인 ASIN 수 (판매 여부 무관, 출시 기준)
--   매출                : 해당 연도 총매출
--   신제품 매출(1년 이내): 판매월이 그 ASIN의 출시월로부터 0~11개월 이내인 매출의 합
--   [참고] 당해 출시분   : first_year = 해당 연도인 ASIN이 그 해에 올린 매출 (이전 정의)
--
-- 주의
--   · '신제품 출시 수'는 기간 컷을 적용하지 않는다. 2026년 하반기에 첫 판매가
--     잡히는 ASIN도 출시 자체는 2026년이므로 포함해야 하기 때문.
--   · first_date가 NULL이거나 first_year가 '2021b'처럼 파싱 불가한 값이면
--     경과월을 계산할 수 없어 '신제품 매출(1년 이내)'에서 제외된다. 총매출에는 포함.
-- =============================================================================

WITH src AS (
  SELECT *
  FROM `market-analysis-project-91130.wook.stck_sales_analysis_of_target_brands`
  WHERE brand = 'NOVILLA'          -- ← 브랜드 변경 지점 (예: 'ZINUS')
),

-- 기간 컷: 2026년은 상반기(1~6월)까지만
period AS (
  SELECT *
  FROM src
  WHERE year <> '2026' OR yr_month <= '2026-06'
),

-- 연도별 실적
perf AS (
  SELECT
    year,
    COUNT(DISTINCT asin) AS operating_asin,
    SUM(sales)           AS sales,

    -- 판매 시점이 출시 후 12개월(0~11개월) 이내인 매출만 합산
    SUM(IF(first_date IS NOT NULL
           AND DATE_DIFF(PARSE_DATE('%Y-%m', yr_month),
                         DATE_TRUNC(first_date, MONTH),
                         MONTH) BETWEEN 0 AND 11,
           sales, 0))    AS new_sales_within_1y,

    -- [참고] 이전 정의: 그 해에 출시된 ASIN이 그 해에 올린 매출
    SUM(IF(first_year = year, sales, 0)) AS ref_launch_year_sales
  FROM period
  GROUP BY year
),

-- 출시 기준 집계 (기간 컷 미적용)
launch AS (
  SELECT
    first_year AS year,
    COUNT(DISTINCT asin) AS launched_asin
  FROM src
  WHERE SAFE_CAST(first_year AS INT64) IS NOT NULL   -- '2021b', NULL 제외
  GROUP BY first_year
)

SELECT
  IF(p.year = '2026', '2026 상반기', p.year) AS yr_label,
  p.operating_asin,
  IFNULL(l.launched_asin, 0)                AS launched_asin,
  ROUND(p.sales)                            AS sales,
  ROUND(p.new_sales_within_1y)              AS new_sales_within_1y,
  ROUND(p.ref_launch_year_sales)            AS ref_launch_year_sales
FROM perf p
LEFT JOIN launch l USING (year)
ORDER BY p.year;




--- test -------
WITH
-- 1) 54개월 캘린더 (2022-01 ~ 2026-06)
months AS (
  SELECT FORMAT_DATE('%Y-%m', d) AS ym, o
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2022-01-01', DATE '2026-06-01', INTERVAL 1 MONTH)) d
  WITH OFFSET o
),

-- 2) ASIN × 월 집계
raw_monthly AS (
  SELECT asin, ANY_VALUE(first_year) AS first_year,
         yr_month, SUM(units) AS units, SUM(sales) AS sales
  FROM `market-analysis-project-91130.wook.stck_sales_analysis_of_target_brands`
  WHERE brand = 'NOVILLA'
    AND yr_month BETWEEN '2022-01' AND '2026-06'
  GROUP BY asin, yr_month
),

-- 3) 출시연도 + 첫 매출월 인덱스(s0)
asin_master AS (
  SELECT asin,
         ANY_VALUE(first_year) AS launch_year,
         DATE_DIFF(PARSE_DATE('%Y-%m', MIN(yr_month)), DATE '2022-01-01', MONTH) AS s0
  FROM raw_monthly GROUP BY asin
),
cohort AS (SELECT * FROM asin_master WHERE launch_year IN ('2023','2024','2025')),

-- 4) 판매 없는 달을 0으로 채운 격자
grid AS (
  SELECT c.asin, c.launch_year, c.s0, m.o,
         IFNULL(r.units, 0) AS units, IFNULL(r.sales, 0) AS sales
  FROM cohort c CROSS JOIN months m
  LEFT JOIN raw_monthly r ON r.asin = c.asin AND r.yr_month = m.ym
),

-- 5) 첫 6개월 판매량(판매 미달 판정용) + 첫 12개월 매출·수량
series AS (
  SELECT asin, ANY_VALUE(launch_year) AS launch_year,
         ARRAY_AGG(units ORDER BY o) AS monthly_units,
         SUM(IF(o BETWEEN s0 AND s0 + 5,  units, 0)) AS units_6m,
         SUM(IF(o BETWEEN s0 AND s0 + 11, sales, 0)) AS sales_12m,
         SUM(IF(o BETWEEN s0 AND s0 + 11, units, 0)) AS units_12m
  FROM grid GROUP BY asin
),

-- 6) 첫 판매 시점 (월 2개 이상)
first_sale AS (
  SELECT s.*, (SELECT MIN(i) FROM UNNEST(GENERATE_ARRAY(0,53)) i
               WHERE monthly_units[OFFSET(i)] >= 2) AS fi
  FROM series s
),

-- 7) 소멸 판정 (월 1개 이하 3개월 연속 + 이후 누적 20개 미만)
churn AS (
  SELECT f.*, IF(f.fi IS NULL, NULL,
    (SELECT MIN(i) FROM UNNEST(GENERATE_ARRAY(0,51)) i
     WHERE i > f.fi
       AND monthly_units[OFFSET(i)]   <= 1
       AND monthly_units[OFFSET(i+1)] <= 1
       AND monthly_units[OFFSET(i+2)] <= 1
       AND (SELECT SUM(monthly_units[OFFSET(j)]) FROM UNNEST(GENERATE_ARRAY(0,53)) j
            WHERE j >= i) < 20)) AS di
  FROM first_sale f
),
flags AS (
  SELECT c.*, c.units_6m < 10 AS shortfall,
    IF(c.fi IS NULL, NULL,
       (SELECT MAX(i) FROM UNNEST(GENERATE_ARRAY(0,53)) i
        WHERE monthly_units[OFFSET(i)] >= 2 AND (c.di IS NULL OR i < c.di)) - c.fi + 1) AS lifespan
  FROM churn c
),

-- 8) ★ 세 그룹으로 배타 분류
seg AS (
  SELECT launch_year, sales_12m, units_12m,
    CASE WHEN shortfall                         THEN 'shortfall'            -- 판매 미달
         WHEN di IS NULL OR lifespan > 12       THEN 'surviving'            -- 생존(12개월 초과)
         ELSE                                        'churned_within_12m'   -- 12개월 내 소멸
    END AS segment
  FROM flags
)

SELECT
  launch_year,
  COUNT(*) AS launched,

  -- ① 판매가 시작된 제품 전체 (판매 미달 제외) ← 보고서 인용 기준
  COUNTIF(segment <> 'shortfall')                                                AS n_started,  -- 판매미달 외 제품
  CAST(ROUND(AVG(IF(segment <> 'shortfall', sales_12m, NULL))) AS INT64)         AS avg_sales_12m_started,
  CAST(ROUND(AVG(IF(segment <> 'shortfall', units_12m, NULL))) AS INT64)         AS avg_units_12m_started,

  -- ② 생존 제품만
  COUNTIF(segment = 'surviving')                                                 AS n_surviving,
  CAST(ROUND(AVG(IF(segment = 'surviving', sales_12m, NULL))) AS INT64)          AS avg_sales_12m_surviving,

  -- ③ 12개월 내 소멸 제품만
  COUNTIF(segment = 'churned_within_12m')                                        AS n_churned_12m,
  CAST(ROUND(AVG(IF(segment = 'churned_within_12m', sales_12m, NULL))) AS INT64) AS avg_sales_12m_churned,

  COUNTIF(segment = 'shortfall')                                                 AS n_shortfall   -- 판매 미달
FROM seg
GROUP BY ROLLUP(launch_year)
ORDER BY launch_year IS NULL, launch_year;



-----------
-- test ---
-----------
SELECT
  COALESCE(a.brand, b.brand)     AS brand,
  COALESCE(a.year, b.first_year) AS year,
  IFNULL(a.asin_cnt, 0)          AS asin_cnt,
  IFNULL(b.new_asin_cnt, 0)      AS new_asin_cnt
FROM (
  SELECT brand, year, COUNT(DISTINCT asin) AS asin_cnt
  FROM wook.stck_sales_analysis_of_target_brands
  GROUP BY 1,2
) a
FULL OUTER JOIN (
  SELECT brand, first_year, COUNT(DISTINCT asin) AS new_asin_cnt
  FROM wook.stck_sales_analysis_of_target_brands
  GROUP BY 1,2
) b
  ON a.brand = b.brand AND a.year = b.first_year
WHERE  COALESCE(a.year, b.first_year) >= '2022'
ORDER BY 1,2;




SELECT first_year FROM wook.stck_sales_analysis_of_target_brands;


---
SELECT --year,
       COUNT(DISTINCT asin)
FROM wook.stck_sales_analysis_of_target_brands
WHERE brand='NOVILLA' and first_date is NULL
--GROUP BY 1 ORDER BY 1;

SELECT FORMAT_DATE('%Y', WeekEnding) AS year,
       count(DISTINCT RetailerSku)
FROM stck.atlas_sales_all
WHERE SubCategory='Mattresses' AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]','') = 'NOVILLA'
GROUP BY 1
ORDER BY 1;

-- NOVILLA 제품(asin) 매출 순위
SELECT asin, max(title) as title,
       round(sum(sales),1) as sales,
        sum(units) as units,
        min(first_date)
FROM wook.stck_sales_analysis_of_target_brands
WHERE brand = 'NOVILLA' --and year='2026'
GROUP BY 1 ORDER BY 4 DESC;

SELECT count(DISTINCT asin)
FROM wook.stck_sales_analysis_of_target_brands
WHERE brand='NOVILLA'