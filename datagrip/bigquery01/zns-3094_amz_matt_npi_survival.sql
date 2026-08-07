/*
 * ZNS-3094 : 아마존 매트리스 시장에서 NPI의 생존율 분석하기
 */

-- 아마존 메트리스 인치/사이즈, brand, first_date 붙이기
CREATE OR REPLACE TABLE wook.stck_mattress_brands_with_meta_first AS
WITH cte_mm AS (              -- 주 소스: amazon_mattress_master (inch/size/category/first_date)
    SELECT TRIM(asin) AS asin, profile AS inch, size_adj AS size, category, first_date
    FROM meta.amazon_mattress_master
    WHERE asin IS NOT NULL AND TRIM(asin) != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(asin)
             ORDER BY category IS NULL, first_date IS NULL, first_date) = 1   -- 중복 asin 1건 제거
),
cte_pi AS (                   -- 보조 소스: pi_enriched (master에 없거나 OTHERS일 때만)
    SELECT TRIM(a.asin) AS asin,
           IF(a.asin='B0B6FQZMJ4','5',
              REGEXP_EXTRACT(LOWER(TRIM(inch_color)),
                             r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)')) AS inch,
           TRIM(size) AS size,
           a.financial_category AS category
    FROM meta.amz_zinus_master_pdt_pi_enriched a
    WHERE a.financial_category IN ('Foam Mattresses','Spring Mattresses')
      AND a.asin IS NOT NULL AND TRIM(a.asin) != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(a.asin) ORDER BY size IS NULL) = 1
),
cte_meta AS (                 -- 컬럼 단위 COALESCE (master 우선, 빈 값만 pi 보충)
    SELECT COALESCE(m.asin, p.asin) AS asin,
           COALESCE(NULLIF(m.inch,'OTHERS'), p.inch) AS inch,
           COALESCE(NULLIF(m.size,'OTHERS'), NULLIF(p.size,'-'), m.size) AS size,
           REGEXP_REPLACE(COALESCE(m.category, p.category), r'Mattresses$', 'Mattress') AS category,
           m.first_date AS meta_first_date
    FROM cte_mm m FULL JOIN cte_pi p USING (asin)
),
cte_first AS (                -- 판매 기반 폴백 (실판매 주 → 노출 주)
    SELECT RetailerSku AS asin,
           MIN(IF(COALESCE(RetailSales,0) > 0 OR COALESCE(UnitsSold,0) > 0, WeekEnding, NULL)) AS first_sold_week,
           MIN(WeekEnding) AS first_seen_week
    FROM stck.atlas_sales_all
    GROUP BY 1
),
cte_base AS (
    SELECT a.RetailerSku                                            AS asin,
           FORMAT_DATE('%Y', a.WeekEnding)                          AS year,
           MAX(REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]', '')) AS brand,
           SUM(a.RetailSales)                                       AS sales,
           SUM(a.UnitsSold)                                         AS units,
           ANY_VALUE(b.size)                                        AS size,
           ANY_VALUE(b.inch)                                        AS inch,
           ANY_VALUE(b.category)                                    AS category,
           COALESCE(ANY_VALUE(b.meta_first_date),                   -- 1순위: master
                    ANY_VALUE(c.first_sold_week),                   -- 2순위: 최초 판매주
                    ANY_VALUE(c.first_seen_week))                   AS first_date,   -- 3순위: 최초 노출주
           CASE WHEN ANY_VALUE(b.meta_first_date) IS NOT NULL THEN 'master'
                WHEN ANY_VALUE(c.first_sold_week) IS NOT NULL THEN 'sales'
                ELSE 'seen' END                                     AS first_date_src,
           LOWER(REGEXP_REPLACE(
                   NORMALIZE(ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)], NFKC),
                   r'[^[:print:]]', ' '))                           AS title
    FROM stck.atlas_sales_all a
         JOIN cte_meta b ON a.RetailerSku = b.asin
         LEFT JOIN cte_first c ON a.RetailerSku = c.asin
    WHERE b.category IS NOT NULL
      AND a.WeekEnding >= DATE '2021-01-01'
    GROUP BY 1, 2
)
SELECT year, asin, brand, sales, units, category, first_date, first_date_src,
       COALESCE(NULLIF(inch,'OTHERS'),
                REGEXP_EXTRACT(LOWER(title), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)'),
                'OTHERS') AS inch_raw,
       CASE
           -- 0) Short Queen만 title 우선 (master size_adj에 해당 값이 아예 없음)
           WHEN REGEXP_CONTAINS(LOWER(title), r'\bshort\s*queen\b')          THEN 'Short Queen'

           -- 1) meta(size_adj) / pi(코드값) 매칭 — 세부 사이즈 우선
           WHEN LOWER(TRIM(size)) IN ('cal king','california king','ck')     THEN 'Cal King'
           WHEN LOWER(TRIM(size)) IN ('narrow twin','nt')                    THEN 'Narrow Twin'
           WHEN LOWER(TRIM(size)) IN ('short queen','sq')                    THEN 'Short Queen'
           WHEN LOWER(TRIM(size)) IN ('twin xl','twin-xl','txl')             THEN 'Twin XL'
           WHEN LOWER(TRIM(size)) = 'small single'                           THEN 'Single'
           WHEN LOWER(TRIM(size)) = '12 inch queen medium firm'              THEN 'Queen'
           WHEN LOWER(TRIM(size)) IN ('q','queen (u.s. standard)','queen')   THEN 'Queen'
           WHEN LOWER(TRIM(size)) IN ('k','king (u.s. standard)','king')     THEN 'King'
           WHEN LOWER(TRIM(size)) IN ('f','full','full (75*54)')             THEN 'Full'
           WHEN LOWER(TRIM(size)) IN ('t','twin','twin (75*38)')             THEN 'Twin'
           WHEN LOWER(TRIM(size)) IN ('s','single')                          THEN 'Single'

           -- 2) title 폴백 (size가 OTHERS/NULL인 경우)
           WHEN REGEXP_CONTAINS(LOWER(title), r'\bcal(?:ifornia)?\s*king\b') THEN 'Cal King'
           WHEN REGEXP_CONTAINS(LOWER(title), r'\bnarrow\s*twin\b')          THEN 'Narrow Twin'
           WHEN REGEXP_CONTAINS(LOWER(title), r'\btwin[\s-]*xl\b')           THEN 'Twin XL'
           WHEN REGEXP_CONTAINS(LOWER(title), r'\bq(?:u)?een\b')             THEN 'Queen'
           WHEN REGEXP_CONTAINS(LOWER(title), r'\bking\b')                   THEN 'King'
           WHEN REGEXP_CONTAINS(LOWER(title), r'\bfull\b')                   THEN 'Full'
           WHEN REGEXP_CONTAINS(LOWER(title), r'\btwin\b')                   THEN 'Twin'
           WHEN REGEXP_CONTAINS(LOWER(title), r'\bsingle\b')                 THEN 'Single'
           ELSE 'Other'
       END AS size_c
FROM cte_base;




--------------------------------------
----- test  ------------------------
--------------------------------------


SELECT DISTINCT asin, first_date FROM wook.stck_mattress_brands_with_meta_first
WHERE brand='ZINUS' AND first_date >= '2025-01-01';

SELECT COUNT(*)
FROM wook.stck_mattress_brands_with_meta_first
WHERE --brand='ZINUS' AND first_date >= '2025-01-01';
      year IN ('2025', '2026');

SELECT DISTINCT asin, first_date
FROM wook.stck_sales_analysis_of_target_brands
WHERE brand='ZINUS' and  first_year='2026';

SELECT year,
       sum(sales) as total_sales,
       sum(if(brand='ZINUS', sales, 0)) as zinus_sales,
       count(DISTINCT asin) as total_asins,
       count(DISTINCT if(brand='ZINUS', asin, NULL)) as zinus_asin
FROM wook.stck_mattress_brands_with_meta_first
GROUP BY 1 ORDER BY 1;

SELECT DISTINCT asin, brand,




SELECT * FROM meta.amazon_mattress_master
WHERE asin in ('B0FG6P1R5X','B0FL21DDT9');
--    brand = 'ZINUS'

SELECT FORMAT_DATE('%Y', first_date) as yr,
       count(DISTINCT asin)
FROM meta.amazon_mattress_master
GROUP BY 1 ORDER BY 1;

select FORMAT_DATE('%Y', WeekEnding) as yr
    , count(DISTINCT RetailerSku)
FROM stck.atlas_sales_all WHERE SubCategory = 'Mattresses'
and COALESCE(RetailSales,0) > 0
GROUP BY 1 ORDER BY 1;

-- GPT 매트리스 분류 미처리 ASIN 추출 (Stackline 2023+ 매트리스 기준)
-- 결과: 2,349 ASIN / 2023년 이후 매출 $376M
WITH cte_stackline AS (
    SELECT
        RetailerSku                                           AS asin
        , ANY_VALUE(Brand)                                    AS brand
        , ANY_VALUE(Title)                                    AS title
        , MIN(WeekEnding)                                     AS stck_first_date
        , MAX(WeekEnding)                                     AS stck_last_date
        , COUNTIF(WeekEnding >= '2023-01-01')                 AS weeks_23plus
        , SUM(IF(WeekEnding >= '2023-01-01', RetailSales, 0)) AS sales_23plus
        , SUM(IF(WeekEnding >= '2023-01-01', UnitsSold,   0)) AS units_23plus
    FROM
        stck.atlas_sales_all
    WHERE
        SubCategory = 'Mattresses'
    GROUP BY 1
    HAVING
        COUNTIF(WeekEnding >= '2023-01-01') > 0   -- 2023년 이후 판매 이력 있는 ASIN만
)
-- 기존 GPT 분류 배치에 한 번이라도 투입된 ASIN (카테고리 결과와 무관하게 '처리됨')
, cte_gpt_done AS (
             SELECT asin FROM tmp.amz_stck_cate_by_gpt_250715
    UNION DISTINCT SELECT asin FROM tmp.amz_stck_cate_by_gpt_250716
    UNION DISTINCT SELECT asin FROM tmp.amz_stck_cate_by_gpt_250717
    UNION DISTINCT SELECT asin FROM tmp.amz_stck_cate_matt_by_gpt_250801
    UNION DISTINCT SELECT asin FROM tmp.amz_stck_cate_nrr_matt_by_gpt_250730
    UNION DISTINCT SELECT asin FROM tmp.amz_stck_cate_matt_by_gpt_260312
    UNION DISTINCT SELECT asin FROM tmp.amz_stck_cate_matt_by_gpt_260406
    UNION DISTINCT SELECT asin FROM tmp.amz_stck_cate_matt_by_gpt_260518
    UNION DISTINCT SELECT asin FROM tmp.amz_stck_cate_matt_by_gpt_260721
)
SELECT
    s.asin
    , s.brand
    , s.title
    , s.stck_first_date
    , s.stck_last_date
    , s.weeks_23plus
    , ROUND(s.sales_23plus, 0) AS sales_23plus
    , s.units_23plus
FROM
    cte_stackline s
        LEFT JOIN cte_gpt_done g
            ON s.asin = g.asin
WHERE
    g.asin IS NULL
ORDER BY
    s.sales_23plus DESC