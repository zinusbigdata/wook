/*
 * ZNS-3073 : 경쟁사 신제품이 차지하는 MS, 지누스에 특정 인치/사이즈를 잠식했나?
 */

-- 인치, 사이즈별 Amazon Mattress 시장 구하기
CREATE OR REPLACE TABLE wook.stck_mattress_brands_with_meta AS
WITH cte_meta AS (            -- 업로드 코드의 cte_meta 그대로 (pi + GPT 마스터)
    SELECT * FROM (
        SELECT DISTINCT TRIM(a.asin) AS asin,
          IF(a.asin='B0B6FQZMJ4','5',
             REGEXP_EXTRACT(LOWER(TRIM(inch_color)), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)')) AS inch,
          TRIM(size) AS size,
          IF(a.financial_category='Foam Mattresses','Foam Mattress',a.financial_category) AS category, 1 AS ord
        FROM meta.amz_zinus_master_pdt_pi_enriched a
    WHERE a.financial_category IN ('Foam Mattresses','Spring Mattresses')
    UNION ALL
    SELECT DISTINCT a.asin, profile AS inch, size_adj AS size, a.category, 2 AS ord
    FROM meta.amazon_mattress_master a
  ) QUALIFY ROW_NUMBER() OVER (PARTITION BY asin ORDER BY category IS NULL, ord)=1
),
cte_base AS (                 -- 브랜드 필터 제거 = 전체 시장, category 있는 것만 (=매트리스)
    SELECT a.RetailerSku AS asin, FORMAT_DATE('%Y', a.WeekEnding) AS year,
        MAX(REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','')) as brand,
        SUM(a.RetailSales) AS sales, SUM(a.UnitsSold) AS units,
        ANY_VALUE(b.size) AS size, ANY_VALUE(b.inch) AS inch,
        LOWER(REGEXP_REPLACE(NORMALIZE(ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)], NFKC),
              r'[^[:print:]]',' ')) AS title
      FROM stck.atlas_sales_all a
      JOIN cte_meta b ON a.RetailerSku = b.asin
      WHERE --a.SubCategory = 'Mattresses' AND
          b.category IS NOT NULL AND FORMAT_DATE('%Y', a.WeekEnding) IN ('2025','2026')
      GROUP BY 1,2
),
cte_clean AS (                -- 인치 title 폴백 + 사이즈 정규화 (업로드 코드 로직)
    SELECT year, asin, brand, sales, units,
        COALESCE(NULLIF(inch,'OTHERS'),
            REGEXP_EXTRACT(LOWER(title), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)'),'OTHERS') AS inch_raw,
      CASE
          -- 1) 개별 ASIN 예외 (데이터 오류 패치)
          WHEN asin IN ('B00X6L6DCO','B00X6LCL3O')                              THEN 'Twin'
          WHEN asin = 'B0765C7YPX'                                              THEN 'King'

          -- 2) size 컬럼 매칭 (세부 사이즈를 상위 사이즈보다 먼저)
          WHEN LOWER(TRIM(size)) IN ('ck','california king')                    THEN 'Cal King'
          WHEN LOWER(TRIM(size)) = 'sq'                                         THEN 'Short Queen'
          WHEN LOWER(TRIM(size)) = 'nt'                                         THEN 'Narrow Twin'
          WHEN LOWER(TRIM(size)) IN ('txl','twin-xl','twin xl')                 THEN 'Twin XL'
          WHEN LOWER(TRIM(size)) = '12 inch queen medium firm'                  THEN 'Queen'
          WHEN LOWER(TRIM(size)) IN ('q','queen (u.s. standard)','queen')       THEN 'Queen'
          WHEN LOWER(TRIM(size)) IN ('k','king (u.s. standard)','king')         THEN 'King'
          WHEN LOWER(TRIM(size)) IN ('f','full','full (75*54)')                 THEN 'Full'
          WHEN LOWER(TRIM(size)) IN ('t','twin','twin (75*38)')                 THEN 'Twin'
          WHEN LOWER(TRIM(size)) IN ('s','single')                              THEN 'Single'

          -- 3) title 폴백 (LOWER 적용 — 원본은 대소문자 때문에 거의 작동 안 했음)
          WHEN REGEXP_CONTAINS(LOWER(title), r'\bcal(?:ifornia)?\s*king\b')     THEN 'Cal King'
          WHEN REGEXP_CONTAINS(LOWER(title), r'\bshort\s*queen\b')              THEN 'Short Queen'
          WHEN REGEXP_CONTAINS(LOWER(title), r'\bnarrow\s*twin\b')              THEN 'Narrow Twin'
          WHEN REGEXP_CONTAINS(LOWER(title), r'\btwin[\s-]*xl\b')               THEN 'Twin XL'
          WHEN REGEXP_CONTAINS(LOWER(title), r'\bq(?:u)?een\b')                 THEN 'Queen'
          WHEN REGEXP_CONTAINS(LOWER(title), r'\bking\b')                       THEN 'King'
          WHEN REGEXP_CONTAINS(LOWER(title), r'\bfull\b')                       THEN 'Full'
          WHEN REGEXP_CONTAINS(LOWER(title), r'\btwin\b')                       THEN 'Twin'
          WHEN REGEXP_CONTAINS(LOWER(title), r'\bsingle\b')                     THEN 'Single'

          ELSE 'Other'
        END AS size_c
    FROM cte_base
)
SELECT * FROM cte_clean;

-- test
SELECT * FROM wook.stck_mattress_brands_with_meta;
SELECT DISTINCT size_c FROM wook.stck_mattress_brands_with_meta;
SELECT DISTINCT asin FROM wook.stck_mattress_brands_with_meta;
SELECT DISTINCT category FROM wook.stck_mattress_brands_with_meta;
SELECT DISTINCT RetailerSku FROM stck.atlas_sales_all WHERE SubCategory='Mattresses' and WeekEnding >= '2025-01-01';

-- 아마존 메트리스 시장에서 인치/사이즈별 마켓 크기 집계
SELECT year,
  IF(inch_raw IN ('6','8','10','12','14'), inch_raw, 'Other') AS inch,
  IF(size_c IN ('Twin','Twin XL','Full','Queen','Short Queen','King','Cal King'), size_c, 'Other') AS size,
  CAST(SUM(sales) AS NUMERIC) AS market_sales,
  COUNT(DISTINCT asin) AS asin_cnt
FROM --cte_clean
    wook.stck_mattress_brands_with_meta
GROUP BY 1,2,3
ORDER BY 1,2,3;



-- 브랜드별 · 연도별 신제품 개수, 총매출, 신제품 매출 및 비중
-- 신제품 = 2025년 이후 출시(first_year >= '2025')
-- 신제품 수 = 해당 연도에 매출이 발생한 신제품 ASIN 수
SELECT
    brand
    , year
    , COUNT(DISTINCT IF(first_year >= '2025', asin, NULL))        AS new_asin_cnt      -- 신제품 개수
    , CAST(SUM(sales) AS NUMERIC)                                 AS total_sales       -- 총매출
    , CAST(SUM(IF(first_year >= '2025', sales, 0)) AS NUMERIC)    AS new_sales         -- 신제품 매출
    , ROUND(SUM(IF(first_year >= '2025', sales, 0)) / SUM(sales) * 100, 1)
                                                                  AS new_share_pct     -- 신제품 비중(%)
FROM
    wook.stck_sales_analysis_of_target_brands
GROUP BY
    brand, year
ORDER BY
    brand, year;





