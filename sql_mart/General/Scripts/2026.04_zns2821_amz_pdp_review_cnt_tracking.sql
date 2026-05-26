/*
 * ZNS-2821: PDP review count tracking
 */

-- [ 01. 특정 asin에 대한 review rating, cnt 추적 ] ---------------------------------
SELECT
    DATE(crawlTime_utc) as crawl_date
    , rating
    , ratings_total
    , rating_breakdown_five_star_count as stars_5_cnt
    , rating_breakdown_four_star_count as stars_4_cnt
    , rating_breakdown_three_star_count as stars_3_cnt
    , rating_breakdown_two_star_count as stars_2_cnt
    , rating_breakdown_one_star_count as stars_1_cnt
    , rating_breakdown_five_star_percentage
    , rating_breakdown_four_star_percentage
    , rating_breakdown_three_star_percentage
    , rating_breakdown_two_star_percentage
    , rating_breakdown_one_star_percentage
FROM
    dw.rf_amz_pdt_zns_comp_daily
WHERE
    asin = 'B0CLC8HQTJ'
ORDER BY 1 desc
;

-- [ 02. Zinus의 pdp rating 추출하여 분석 마트 생성 ] -------------------------------------

CREATE OR REPLACE TABLE wook.amz_zns_pdp_review_rating_small AS
WITH
    cte_union AS (
        SELECT
            p.asin
            , p.parent_asin
            , p.crawlTime_utc
            , DATE(p.crawlTime_utc) AS pdp_date
            , FORMAT_DATE("%Y%m", DATE(p.crawlTime_utc)) AS yr_month
            , FORMAT_DATE('%Y Q%Q', DATE(p.crawlTime_utc) ) as yr_quarter
            , FORMAT_DATE('%Y', DATE(p.crawlTime_utc) ) as yr
            , p.rating
            , p.ratings_total
            , p.rating_breakdown_five_star_count AS cnt_5
            , p.rating_breakdown_four_star_count AS cnt_4
            , p.rating_breakdown_three_star_count AS cnt_3
            , p.rating_breakdown_two_star_count AS cnt_2
            , p.rating_breakdown_one_star_count AS cnt_1
            , p.rating_breakdown_five_star_percentage AS rat_5
            , p.rating_breakdown_four_star_percentage AS rat_4
            , p.rating_breakdown_three_star_percentage AS rat_3
            , p.rating_breakdown_two_star_percentage AS rat_2
            , p.rating_breakdown_one_star_percentage AS rat_1
            , m.* EXCEPT (asin)
        FROM
            dw.rf_amz_pdt_zns_comp_daily p
                INNER JOIN meta.amz_zinus_master_pdt_pi_add_new_col m
                    ON REGEXP_REPLACE(TRIM(p.asin), r'[^[:print:]]|&lrm;', '') = m.asin

        UNION ALL

        SELECT
            p.request_asin AS asin
            , p.parent_asin
            , p.crawlTime_utc
            , DATE(p.crawlTime_utc) AS pdp_date
            , FORMAT_DATE("%Y%m", DATE(p.crawlTime_utc)) AS yr_month
            , FORMAT_DATE('%Y Q%Q', DATE(p.crawlTime_utc) ) as yr_quarter
            , FORMAT_DATE('%Y', DATE(p.crawlTime_utc) ) as yr
            , p.rating
            , p.ratings_total
            , p.rating_breakdown_five_star_count AS cnt_5
            , p.rating_breakdown_four_star_count AS cnt_4
            , p.rating_breakdown_three_star_count AS cnt_3
            , p.rating_breakdown_two_star_count AS cnt_2
            , p.rating_breakdown_one_star_count AS cnt_1
            , p.rating_breakdown_five_star_percentage AS rat_5
            , p.rating_breakdown_four_star_percentage AS rat_4
            , p.rating_breakdown_three_star_percentage AS rat_3
            , p.rating_breakdown_two_star_percentage AS rat_2
            , p.rating_breakdown_one_star_percentage AS rat_1
            , m.* EXCEPT (asin)
        FROM
            dw.rf_amz_pdt_zns_comp_daily p
                --INNER JOIN meta.amz_zns_cat_col_mst m
            INNER JOIN meta.amz_zinus_master_pdt_pi_add_new_col m
                    ON REGEXP_REPLACE(TRIM(p.request_asin), r'[^[:print:]]|&lrm;', '') = m.asin
    )

SELECT
    * EXCEPT (crawlTime_utc)
FROM
    cte_union
WHERE
	CONCAT(LOWER(collection), LOWER(product_description), LOWER(abbre)) LIKE '%wonder%'
QUALIFY
    ROW_NUMBER() OVER ( PARTITION BY asin, pdp_date ORDER BY crawlTime_utc DESC ) = 1
;

-- [03. samll box asin 만 추적하기 - 최근 평점 변화] --------------------------------------------
 
WITH ranked AS (
    SELECT
        asin, parent_asin, pdp_date, rating, ratings_total,
        financial_category, new_collection, 
        LAG(rating) OVER (PARTITION BY asin ORDER BY pdp_date) AS prev_rating
    FROM wook.amz_zns_pdp_review_rating_small
)
SELECT
    asin,
    financial_category,
    new_collection,
    pdp_date AS last_change_date,
    prev_rating,
    rating AS new_rating,
    ROUND(rating - prev_rating, 2) AS rating_diff,
    ratings_total
FROM ranked
WHERE prev_rating IS NOT NULL
  AND rating != prev_rating
QUALIFY ROW_NUMBER() OVER (PARTITION BY asin ORDER BY pdp_date DESC) = 1
ORDER BY last_change_date DESC
;

-- [04. 문제되는 컬렉션의 매출 구하기]-------------------------------------------------------------------
SELECT min(date), MAX(date), sum(shipped_revenue) , sum(shipped_units) 
FROM vc.amz_vc_sales_daily_all 
WHERE asin IN ('B0FV37VBR4', 'B0FV37VJ4Z', 'B0FV3BT8F2', 'B0FV37L4DH', 'B0FV38GRVZ')
;


