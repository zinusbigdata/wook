/*
 *   ZNS-2972 : Walmart 주요 브랜드에 대한 매출 차트에서 Foam, Spring 구분하기
 */

WITH wmt_brand_yr AS (
    SELECT * EXCEPT (Brand)
        , EXTRACT(YEAR FROM WeekEnding)                      AS yr
        , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')  AS brand
       --, ROUND(SUM(RetailSales), 0)                         AS sales
    FROM stck.wmt_atlas_sales_all
    WHERE SubCategory = 'Mattresses' AND RetailSales > 0
)
, wmt_brand_family AS (
    SELECT byr.*
        , COALESCE(fm.FAMILY_NAME, byr.brand) AS brand_family
    FROM wmt_brand_yr AS byr
    LEFT JOIN `meta.brand_family_mapping` AS fm
         ON byr.brand = REGEXP_REPLACE(UPPER(fm.BRAND_UPPER), r'[^[:print:]]', '')
)
--select * from wmt_brand_family ;
, wmt_brand_fam_subtype AS (
    SELECT
        *
       , CASE
    -- 0) 매트리스가 아닌 제품 제외 (의자/프레임/푸톤 등)
        WHEN REGEXP_CONTAINS(LOWER(Title)
       , r'\b(chair|folding|frame|futon|topper|protector|pad|cover|sheet|pillow set)\b')
        AND NOT REGEXP_CONTAINS(LOWER(Title)
       , r'\bmattress\b')
        THEN 'Non-Mattress'

    -- 1) Hybrid 최우선 (coil + foam 복합)
        WHEN REGEXP_CONTAINS(LOWER(Title)
       , r'\bhybrid\b')
        THEN 'Hybrid'

    -- 2) Spring (innerspring / coil)
        WHEN REGEXP_CONTAINS(LOWER(Title)
       , r'\b(innerspring|inner spring|spring|coil|pocketed coil|bonnell)\b')
        THEN 'Spring'

    -- 3) Foam (memory foam / gel foam / 단순 foam)
        WHEN REGEXP_CONTAINS(LOWER(Title)
       , r'\b(memory foam|gel foam|foam)\b')
        THEN 'Foam'

    -- 4) Title만으로 판별 불가
        ELSE
            'Unknown'
        END
        AS matt_sub_type
    FROM wmt_brand_family
    WHERE brand_family in ('ZINUS', 'NOVILLA FAMILY', 'MLILY FAMILY','FDW DIRECT')
   -- WHERE SubCategory = 'Mattresses' AND RetailSales > 0
)
SELECT *
FROM wmt_brand_fam_subtype;


-- [ test ]
SELECT brand_family, matt_sub_type
    , count(*), count(distinct RetailerSku)
FROM wmt_brand_fam_subtype
GROUP BY 1,2 ORDER BY 1,2
;





