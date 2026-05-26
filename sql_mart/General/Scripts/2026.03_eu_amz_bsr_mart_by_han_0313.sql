CREATE OR REPLACE TABLE tmp1.amz_eu_bsr_brand_list2 AS
WITH TMP1 AS (
    SELECT
        country
        , bsr_ctgry_label
        , brand
        , COUNT(asin) AS cnt
    FROM tmp1.amz_eu_bsr_shr_daily_acc
    WHERE
        bsr_date >= '2023-01-01'
        AND bsr_rank_range = 'Top 50'
    GROUP BY 1, 2, 3
)
SELECT
    country
    , bsr_ctgry_label
    , brand
FROM
    TMP1
QUALIFY
    RANK() OVER (PARTITION BY country, bsr_ctgry_label ORDER BY cnt DESC ) <= 10
;


CREATE OR REPLACE TABLE wook.amz_eu_bsr_shr_daily_acc2 AS
WITH
    cte_legend AS (
        SELECT
            brand
            , ROW_NUMBER() OVER (ORDER BY brand_asin_cnt DESC) AS brand_legend_ord
        FROM
            (
                SELECT
                    brand
                    , COUNT(DISTINCT asin) AS brand_asin_cnt
                FROM
                    tmp1.amz_eu_bsr_shr_daily_acc
                WHERE
                    UPPER(brand) NOT LIKE '%ZINUS%'
                    AND UPPER(brand) NOT LIKE '%MELLOW%'
                GROUP BY 1
            )
    )
SELECT
    A.country
    , bsr_ctgry
    , asin
    , rank
    , bsr_date
    --             , brand
    , CASE
          WHEN UPPER(A.brand) LIKE '%ZINUS%'  THEN 'ZINUS'
          WHEN UPPER(A.brand) LIKE '%MELLOW%' THEN 'MELLOW'
          WHEN A.brand = 'No Brand Info'      THEN A.brand
          ELSE COALESCE(TRIM(B.brand), 'Others')
      END AS brand
    , CASE
          WHEN UPPER(A.brand) LIKE '%ZINUS%'  THEN 'ZINUS'
          WHEN UPPER(A.brand) LIKE '%MELLOW%' THEN 'MELLOW'
          ELSE A.brand
      END AS brand_raw
    , title
    , image_url
    , A.bsr_ctgry_label
    , pdt_url

    , substring(cast(t_cal.yr_month as string), 3, 2) || '-' || substring(cast(t_cal.yr_month as string), -2, 2) as yr_month -- 23-05
    , 'Y' || substring(cast(t_cal.yr_wk as string), 3, 2) || ' W' || substring(cast(t_cal.yr_wk as string), -2, 2) as yr_week -- Y23 W20

    , is_maxdt
    , is_maxdt_range
    , bsr_ord
    , brand_prod_num
    , brand_ord
    , date_str
    , bsr_rank_range
    , CASE
          WHEN UPPER(A.brand) LIKE '%ZINUS%'  THEN 0
          WHEN UPPER(A.brand) LIKE '%MELLOW%' THEN 1
          WHEN A.brand = 'No Brand Info'      THEN 999999
          WHEN B.brand IS NULL                THEN 1000000
          ELSE C.brand_legend_ord + 1
      END AS brand_legend_ord
FROM
    tmp1.amz_eu_bsr_shr_daily_acc A
        LEFT JOIN meta.wk_calendar t_cal
            ON A.bsr_date BETWEEN t_cal.start_date AND t_cal.end_date
        LEFT OUTER JOIN tmp1.amz_eu_bsr_brand_list2 B
            ON A.brand = B.brand AND A.bsr_ctgry_label = B.bsr_ctgry_label and A.country = B.country
        LEFT OUTER JOIN cte_legend C
            ON A.brand = C.brand
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY country, bsr_ctgry, asin, bsr_date, t.brand, bsr_rank_range ORDER BY rank) = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY A.country, bsr_ctgry, asin, bsr_date, bsr_rank_range ORDER BY rank) = 1
;

 

