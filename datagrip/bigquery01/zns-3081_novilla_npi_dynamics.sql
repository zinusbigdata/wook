/*
 * ZNS-3081 : NOVILLA가 Zinus의 전철을 밟고 있는지 분석.
 * 소스 테이블 : wook.stck_sales_analysis_of_target_brands
 */

WITH params AS (
  SELECT 2 AS min_units,     -- 판매 인정 최소 수량
         3 AS dead_streak    -- 소멸 판정 연속 개월 (아래 로직에 반영)
),

-- 1) 18개월 캘린더 (2025-01 ~ 2026-06)
months AS (
  SELECT FORMAT_DATE('%Y-%m', d) AS ym, o
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2025-01-01', DATE '2026-06-01', INTERVAL 1 MONTH)) d
  WITH OFFSET o
),

-- 2) ASIN × 월 집계 (seller_type 등으로 분할된 행 합산)
src AS (
  SELECT asin, ANY_VALUE(inch) AS inch, ANY_VALUE(size) AS size,
         yr_month, SUM(sales) AS s, SUM(units) AS u
  FROM `market-analysis-project-91130.wook.stck_sales_analysis_of_target_brands`
  WHERE first_year = '2025'
    AND brand = 'NOVILLA'
    AND yr_month BETWEEN '2025-01' AND '2026-06'
  GROUP BY asin, yr_month
),

-- 3) 판매 없는 달을 0으로 채운 완전 격자 ★ 연속 무판매 판정의 전제
grid AS (
  SELECT b.asin, b.inch, b.size, m.o,
         IFNULL(s.u, 0) AS u,
         IFNULL(s.s, 0) AS s
  FROM (SELECT DISTINCT asin, inch, size FROM src) b
  CROSS JOIN months m
  LEFT JOIN src s ON s.asin = b.asin AND s.yr_month = m.ym
),

-- 4) 월별 판매량을 배열로 압축 + 총매출/총판매량(인정 기준 적용)
agg AS (
  SELECT asin, ANY_VALUE(inch) AS inch, ANY_VALUE(size) AS size,
         ARRAY_AGG(u ORDER BY o) AS us,
         ROUND(SUM(IF(u >= (SELECT min_units FROM params), s, 0))) AS t_sales,
         SUM(IF(u >= (SELECT min_units FROM params), u, 0))        AS t_units
  FROM grid GROUP BY asin
),

-- 5) 첫 판매 시점 (판매량 2개 이상인 최초 달)
mark AS (
  SELECT a.*,
         (SELECT MIN(i) FROM UNNEST(GENERATE_ARRAY(0,17)) i
          WHERE us[OFFSET(i)] >= 2) AS fi
  FROM agg a
),

-- 6) 소멸 판정: 1개 이하가 3개월 연속되는 최초 시점
--    i, i+1, i+2 를 동시에 보므로 i 는 15까지만 (16,17은 뒤가 부족)
death AS (
  SELECT m.*,
         (SELECT MIN(i) FROM UNNEST(GENERATE_ARRAY(0,15)) i
          WHERE i > m.fi
            AND us[OFFSET(i)]   <= 1
            AND us[OFFSET(i+1)] <= 1
            AND us[OFFSET(i+2)] <= 1) AS di
  FROM mark m
  WHERE m.fi IS NOT NULL          -- 전 기간 1개 이하인 ASIN 제외
),

-- 7) 마지막 판매월 · 판매월수 · 소멸 후 재판매
calc AS (
  SELECT d.*,
    (SELECT MAX(i) FROM UNNEST(GENERATE_ARRAY(0,17)) i
     WHERE us[OFFSET(i)] >= 2 AND (d.di IS NULL OR i < d.di))            AS la,
    (SELECT COUNTIF(us[OFFSET(i)] >= 2) FROM UNNEST(GENERATE_ARRAY(0,17)) i
     WHERE d.di IS NULL OR i < d.di)                                     AS sell_months,
    (SELECT LOGICAL_OR(us[OFFSET(i)] >= 2) FROM UNNEST(GENERATE_ARRAY(0,17)) i
     WHERE d.di IS NOT NULL AND i > d.di)                                AS resale
  FROM death d
)

SELECT
  asin, inch, size, t_sales, t_units,
  FORMAT_DATE('%Y-%m', DATE_ADD(DATE '2025-01-01', INTERVAL fi MONTH)) AS first_month,
  FORMAT_DATE('%Y-%m', DATE_ADD(DATE '2025-01-01', INTERVAL la MONTH)) AS last_month,
  IFNULL(FORMAT_DATE('%Y-%m', DATE_ADD(DATE '2025-01-01', INTERVAL di MONTH)), '-') AS death_month,
  sell_months,
  la - fi + 1                AS span_months,
  la - fi + 1 - sell_months  AS gap_months,
  IF(resale, 'Y', '')        AS resale_flag,
  CASE WHEN di IS NULL      THEN '생존'
       WHEN la-fi+1 <= 3    THEN '3개월 이내'
       WHEN la-fi+1 <= 6    THEN '6개월 이내'
       WHEN la-fi+1 <= 9    THEN '9개월 이내'
       WHEN la-fi+1 <= 12   THEN '12개월 이내'
       ELSE                      '1년 이상'
  END AS churn_bucket,
  us[OFFSET(0)]  AS m2025_01, us[OFFSET(1)]  AS m2025_02, us[OFFSET(2)]  AS m2025_03,
  us[OFFSET(3)]  AS m2025_04, us[OFFSET(4)]  AS m2025_05, us[OFFSET(5)]  AS m2025_06,
  us[OFFSET(6)]  AS m2025_07, us[OFFSET(7)]  AS m2025_08, us[OFFSET(8)]  AS m2025_09,
  us[OFFSET(9)]  AS m2025_10, us[OFFSET(10)] AS m2025_11, us[OFFSET(11)] AS m2025_12,
  us[OFFSET(12)] AS m2026_01, us[OFFSET(13)] AS m2026_02, us[OFFSET(14)] AS m2026_03,
  us[OFFSET(15)] AS m2026_04, us[OFFSET(16)] AS m2026_05, us[OFFSET(17)] AS m2026_06
FROM calc
ORDER BY t_sales DESC;


-- test ---
SELECT first_year,
       COUNT(DISTINCT asin)
FROM wook.stck_sales_analysis_of_target_brands
WHERE brand='NOVILLA'
GROUP BY 1
ORDER BY 1;


