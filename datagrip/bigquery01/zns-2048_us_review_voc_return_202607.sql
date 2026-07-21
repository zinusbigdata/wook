/*
    ZNS-3048 : 미국 Review, CS Claim, Return 통합 분석
 */

--- i) Amazon return 데이터 마트 생성
CREATE OR REPLACE TABLE wook.amz_sales_return AS
SELECT
    a.*
    , FORMAT_DATE('%Y-%m', a.date) AS yr_month
    , c.yr_wk
    --, b.* EXCEPT (asin)
    , d.sku
    , d.zinus_sku
    , d.new_collection as collection
    , d.material
    , d.size
    , d.inch_color
FROM
    vs_pb.amz_sitewalk_sales_trend a
        LEFT JOIN meta.wk_calendar_new c ON a.date BETWEEN c.start_date AND c.end_date
        --LEFT JOIN vs_pb.amz_site_content_monitor b ON a.asin = b.asin
        LEFT JOIN meta.amz_zinus_master_pdt_pi_enriched d ON a.asin = d.asin
;

--- ii) CS Claim data
WITH amz_voc as (
    SELECT *
    FROM dw.cs_voc_cf
    WHERE channel = 'AMAZON' AND date >= '2025-01-01'
)
SELECT;










--- v) test
--- v.1) cf1별로
SELECT cf1
     --, cf2
    , count(*) cnt
    , SUM(CASE WHEN voc_text IS NULL THEN 1 ELSE 0 END) AS NULL_cnt
    , SUM(CASE WHEN voc_text IS NOT NULL THEN 1 ELSE 0 END) AS FULL_cnt
FROM dw.cs_voc_cf
WHERE date >= '2025-01-01'
GROUP BY 1
ORDER BY 4 DESC;

-- v.2) channel 별로
SELECT channel
     --, cf2
    , count(*) cnt
    , SUM(CASE WHEN voc_text IS NULL THEN 1 ELSE 0 END) AS no_cnt
    , SUM(CASE WHEN voc_text IS NOT NULL THEN 1 ELSE 0 END) AS text_cnt
FROM dw.cs_voc_cf
WHERE date >= '2025-01-01'
GROUP BY 1
ORDER BY 4 DESC;



