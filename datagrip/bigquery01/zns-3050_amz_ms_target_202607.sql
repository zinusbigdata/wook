/*
 * ZNS-3050: 아마존 Market Share 타겟 설정을 위한 기초 데이터 추출
 *
 *  I.  Mattress 메타 매핑 temp 테이블 생성
 *  II. 전 카테고리(Foam / Spring / Bed Frames / Beds / Box Springs) 브랜드 MS 통합 출력
 */


--- I. Mattress 메타 매핑 Temp 테이블 -------------------------------------------------------------------

CREATE OR REPLACE TABLE wook.tmp_stck_matt_with_meta AS
WITH with_meta AS (
    SELECT
        *
    FROM
        (
            SELECT DISTINCT
                TRIM(a.asin) AS asin
                , TRIM(a.zinus_sku) AS zinus_sku
                , IF(a.asin = 'B0B6FQZMJ4', '5', REGEXP_EXTRACT(LOWER(TRIM(inch_color)), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)')) AS inch
                , TRIM(size) AS size
                , CASE a.financial_category
                      WHEN 'Foam Mattresses'   THEN 'Foam Mattress'
                      WHEN 'Spring Mattresses' THEN 'Spring Mattress'
                      ELSE a.financial_category
                  END AS category
                , 1 AS ord
            FROM
                meta.amz_zinus_master_pdt_pi_enriched a
            WHERE
                a.financial_category IN ('Foam Mattresses', 'Spring Mattresses')

            UNION ALL

            SELECT DISTINCT
                a.asin
                , CAST(NULL AS STRING) AS zinus_sku
                , profile AS inch
                , size_adj AS size
                , a.category
                , 2 AS ord
            FROM
                meta.amazon_mattress_master a  -- 경쟁사
        )
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY asin ORDER BY category IS NULL, ord) = 1
)
, stck_with_meta AS (
    SELECT
        a.RetailerSku AS asin
        , FORMAT_DATE('%Y-%m', a.WeekEnding) AS yr_month
        , ANY_VALUE(b.zinus_sku) AS zinus_sku
        , MAX(REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','')) AS brand
        , SUM(a.RetailSales) AS sales
        , SUM(a.UnitsSold) AS units
        , AVG(RetailPrice) AS avg_retail_price
        , ANY_VALUE(b.category) AS category
        , ANY_VALUE(b.size) AS size
        , ANY_VALUE(b.inch) AS inch
        , LOWER(
                REGEXP_REPLACE(
                        NORMALIZE(ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)],
                                  NFKC),
                        r'[^[:print:]]',
                        ' '
                )
          ) AS title
    FROM
        stck.atlas_sales_all a
        JOIN with_meta b ON a.RetailerSku = b.asin
    WHERE
        SubCategory = 'Mattresses' AND a.WeekEnding >= '2025-01-01'
    GROUP BY 1, 2
)
SELECT
    yr_month, category, brand
    , ROUND(SUM(sales), 0) AS sales
    , SUM(units) AS units
    , ROUND(SAFE_DIVIDE(SUM(sales), SUM(units)), 1) AS asp
FROM stck_with_meta
GROUP BY 1, 2, 3;


--- II. 전 카테고리 브랜드 MS 통합 출력 -----------------------------------------------------------------
/*
 * 대상 브랜드
 *   Foam Mattress   : ZINUS, NOVILLA, EGOHOME, MLILY, NECTAR + OTHERS
 *   Spring Mattress : ZINUS, NOVILLA, BEDSTORY, NECTAR, SIGNATURE DESIGN BY ASHLEY + OTHERS
 *   Bed Frames      : NEW JETO
 *   Beds            : ALLEWIE, SHA CERLIN
 *   Box Springs     : AMAZONBASICS
 *
 * share_pct = 해당 브랜드 매출 / 그 달 그 카테고리 전체 매출
 *   → 브랜드 필터는 최종 WHERE 절에만 적용하여 분모(전체 매출)를 보존
 */

WITH matt_grouped AS (
    SELECT
        category
        , yr_month
        , CASE
            WHEN category = 'Foam Mattress'
                 AND brand IN ('ZINUS','NOVILLA','EGOHOME','MLILY','NECTAR') THEN brand
            WHEN category = 'Spring Mattress'
                 AND brand IN ('ZINUS','NOVILLA','BEDSTORY','NECTAR','SIGNATURE DESIGN BY ASHLEY') THEN brand
            ELSE 'OTHERS'
          END AS brand
        , SUM(sales) AS sales
        , SUM(units) AS units
    FROM wook.tmp_stck_matt_with_meta
    WHERE category IN ('Foam Mattress', 'Spring Mattress')
    GROUP BY 1, 2, 3
)
, nonmatt_grouped AS (
    SELECT
        SubCategory AS category
        , FORMAT_DATE('%Y-%m', WeekEnding) AS yr_month
        , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]','') AS brand
        , SUM(RetailSales) AS sales
        , SUM(UnitsSold) AS units
    FROM stck.atlas_sales_all
    WHERE SubCategory IN ('Bed Frames', 'Beds', 'Box Springs')
      AND WeekEnding >= '2025-01-01'
    GROUP BY 1, 2, 3
)
, combined AS (
    SELECT category, yr_month, brand, sales, units FROM matt_grouped
    UNION ALL
    SELECT category, yr_month, brand, sales, units FROM nonmatt_grouped
)
, with_share AS (
    SELECT *
        , ROUND(sales / NULLIF(SUM(sales) OVER (PARTITION BY category, yr_month), 0) * 100, 1) AS share_pct
        -- 정렬용 보조 컬럼 (출력에서는 제외)
        , CASE category
            WHEN 'Foam Mattress' THEN 1 WHEN 'Spring Mattress' THEN 2
            WHEN 'Bed Frames'    THEN 3 WHEN 'Beds'            THEN 4
            ELSE 5 END AS cat_ord
        , CASE
            WHEN category = 'Foam Mattress' THEN
                CASE brand WHEN 'ZINUS' THEN 1 WHEN 'NOVILLA' THEN 2 WHEN 'EGOHOME' THEN 3
                           WHEN 'MLILY' THEN 4 WHEN 'NECTAR'  THEN 5 ELSE 6 END
            WHEN category = 'Spring Mattress' THEN
                CASE brand WHEN 'ZINUS' THEN 1 WHEN 'NOVILLA' THEN 2 WHEN 'BEDSTORY' THEN 3
                           WHEN 'NECTAR' THEN 4 WHEN 'SIGNATURE DESIGN BY ASHLEY' THEN 5 ELSE 6 END
            ELSE 1
          END AS brand_ord
    FROM combined
)
SELECT
    category
    , yr_month
    , brand
    , ROUND(sales, 0) AS total_sales
    , units AS total_units
    , ROUND(SAFE_DIVIDE(sales, units), 1) AS asp
    , share_pct
FROM with_share
WHERE category IN ('Foam Mattress', 'Spring Mattress')
   OR (category = 'Bed Frames'  AND brand = 'NEW JETO')
   OR (category = 'Beds'        AND brand IN ('ALLEWIE', 'SHA CERLIN'))
   OR (category = 'Box Springs' AND brand = 'AMAZONBASICS')
ORDER BY cat_ord, yr_month, brand_ord, brand;