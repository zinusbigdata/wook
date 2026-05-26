-- ##### BSR Share Report for EU #### ------------------------------------------------------------------------------------------------------------------------
-- 08/29/22, last modified by Wook
-- 05/30/23, modified by Sangtae, adding 'Chairs' category for every country
-- 06/19/23, modified by Sangtae, adding BSR for 'NL', 'SE', 'BE' countries

-- I. UK 
-- 1.1) Declare date variable to mark max date
DECLARE BSR_MAX_DT DATE
;

SET BSR_MAX_DT = (
    SELECT
        MAX(CAST(SUBSTR(initialTime, 1, 10) AS DATE))
    FROM
        dw.rf_amzuk_bsr_all
)
;

--select BSR_MAX_DT

-- 1.2) BSR raw data - zinus bsr asins
DROP TABLE IF EXISTS tmp1.amzuk_bsr_shr_rawdata
;

CREATE TABLE tmp1.amzuk_bsr_shr_rawdata AS
SELECT
    CASE
        WHEN CONTAINS_SUBSTR(LOWER(fullCategory), 'patio') THEN 'Patio ' || TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
        ELSE TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
    END AS bsr_ctgry
    , bestsellers_asin AS asin
    , bestsellers_rank AS rank
    , CAST(SUBSTR(initialTime, 1, 10) AS DATE) AS initialTime
FROM
    dw.rf_amzuk_bsr_all
WHERE
    CAST(SUBSTR(initialTime, 1, 10) AS DATE) >= '2022-08-19' --AND rank<=50
;

-- 1.3) PDT raw data - select pdt info for BSR asins
DROP TABLE IF EXISTS tmp1.amzuk_bsr_shr_pdt_tmp1
;

CREATE TABLE tmp1.amzuk_bsr_shr_pdt_tmp1 AS
WITH
    TMP1 AS (
        SELECT asin
        FROM tmp1.amzuk_bsr_shr_rawdata
        GROUP BY 1
    )
SELECT
    A.asin
    , crawlTime
    , brand
    , title
    , imageLink
FROM
    dw.amzuk_pdt_all A
        INNER JOIN TMP1 B
            ON A.asin = B.asin
WHERE
    brand IS NOT NULL --AND title IS NOT NULL
GROUP BY
    1, 2, 3, 4, 5
;

-- 1.4) PDT raw data - select most recent pdt brand/title/url info for BSR asins
DROP TABLE IF EXISTS tmp1.amzuk_bsr_shr_pdt_tmp2
;

CREATE TABLE tmp1.amzuk_bsr_shr_pdt_tmp2 AS
WITH
    TMP1 AS (
        SELECT
            asin
            , crawlTime
            , brand
            , title
            , imageLink
            , ROW_NUMBER() OVER (PARTITION BY asin ORDER BY crawlTime DESC) AS filter
        FROM
            tmp1.amzuk_bsr_shr_pdt_tmp1 A
    )
SELECT
    asin
    , brand
    , title
    , imageLink AS image_url
FROM
    TMP1
WHERE
    filter = 1
;

-- 1.5) Map pdt info into BSR raw data -->> final table 1
--DECLARE BSR_MAX_DT DATE;
--SET BSR_MAX_DT = (select max(CAST(SUBSTR(initialTime,1,10) AS DATE)) from dw.rf_amzuk_bsr_all);

DROP TABLE IF EXISTS tmp1.amzuk_bsr_shr_daily
;

CREATE TABLE tmp1.amzuk_bsr_shr_daily AS
WITH
    TMP1 AS (
        SELECT
            A.bsr_ctgry
            , A.asin
            , A.rank
            , A.initialTime AS bsr_date
            --B.brand,
            , TRIM(REPLACE(COALESCE(UPPER(B.brand), 'No Brand Info'), ".", "")) AS brand
            , B.title
            , B.image_url
            , ( CASE
                    WHEN bsr_ctgry = 'Mattresses'            THEN '01. Mattresses'
                    WHEN bsr_ctgry = 'Beds, Frames & Bases'  THEN '02. Beds, Frames & Bases'
                    WHEN bsr_ctgry = 'Bed Frames'            THEN '03. Bed Frames'
                    WHEN bsr_ctgry = 'Bed & Mattress Sets'   THEN '04. Bed & Mattress Sets'
                    WHEN bsr_ctgry = 'Mattress Toppers'      THEN '05. Mattress Toppers'
                    WHEN bsr_ctgry = 'Sofas & Couches'       THEN '06. Sofas & Couches'
                    WHEN bsr_ctgry = 'Dining Tables'         THEN '07. Dining Tables'
                --WHEN bsr_ctgry='Tables' THEN '06. Tables'
                    WHEN bsr_ctgry = 'Desks'
                                                             THEN '08. Desks' --WHEN bsr_ctgry='End Tables' THEN '09. End Tables'
                --WHEN bsr_ctgry='Benches' THEN '10. Benches'
                    WHEN bsr_ctgry = 'Computer Workstations' THEN '09. Computer Workstations'
                    WHEN bsr_ctgry = 'Chairs'                THEN '10. Chairs'
                END ) AS bsr_ctgry_label
            , 'https://www.amazon.co.uk/dp/' || A.asin AS pdt_url

            -- , SUBSTR(CAST(initialTime AS STRING), 3, 5) AS yr_month

            -- , "Y" || SUBSTR(CAST(initialTime AS STRING), 3, 2) || " W" || CASE
            --                                                                   WHEN EXTRACT(WEEK FROM initialTime) = 0
            --                                                                       THEN EXTRACT(WEEK FROM initialTime - 7) + 1
            --                                                                   ELSE EXTRACT(WEEK FROM initialTime)
            --                                                               END AS yr_week

            , CASE
                  WHEN A.initialTime = BSR_MAX_DT THEN 1
                  ELSE 0
              END AS is_maxdt
            , CASE
                  WHEN A.initialTime <= BSR_MAX_DT AND A.initialTime >= BSR_MAX_DT - 3 THEN 1
                  ELSE 0
              END AS is_maxdt_range
            , ROW_NUMBER() OVER (PARTITION BY brand, initialTime, bsr_ctgry, ( CASE
                                                                                   WHEN rank <= 10               THEN 1
                                                                                   WHEN rank > 10 AND rank <= 20 THEN 2
                                                                                   ELSE 3
                                                                               END ) ORDER BY rank) AS bsr_ord
            , COUNT(A.asin) OVER (PARTITION BY B.brand, A.initialTime, A.bsr_ctgry) AS brand_prod_num

        FROM
            tmp1.amzuk_bsr_shr_rawdata A
                LEFT OUTER JOIN tmp1.amzuk_bsr_shr_pdt_tmp2 B
                    ON A.asin = B.asin
    )
SELECT
    *
    , CASE
          WHEN brand = 'Zinus' THEN 0
          ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry, bsr_date ORDER BY brand_prod_num DESC)
      END AS brand_ord
    --CASE WHEN bsr_date=BSR_MAX_DT THEN 'Today' ELSE
    , CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) AS date_str
FROM
    TMP1
;

-- 1.6) Mark Top 10/20/50/100 raws for Power BI 
DROP TABLE IF EXISTS tmp1.amzuk_bsr_shr_daily_acc
;

CREATE TABLE tmp1.amzuk_bsr_shr_daily_acc AS
(
    SELECT *, 'Top 10' AS bsr_rank_range FROM tmp1.amzuk_bsr_shr_daily WHERE rank <= 10
)
UNION ALL
(
    SELECT *, 'Top 20' AS bsr_rank_range FROM tmp1.amzuk_bsr_shr_daily WHERE rank <= 20
)
UNION ALL
(
    SELECT *, 'Top 50' AS bsr_rank_range FROM tmp1.amzuk_bsr_shr_daily WHERE rank <= 50
)
UNION ALL
(
    SELECT *, 'Top 100' AS bsr_rank_range FROM tmp1.amzuk_bsr_shr_daily WHERE rank <= 100
)
;

/*
-- 1.7)  make the brand_legend_order field
DROP TABLE IF EXISTS tmp1.amzuk_bsr_shr_daily_acc_brand;
CREATE TABLE tmp1.amzuk_bsr_shr_daily_acc_brand AS
WITH TMP1 AS (
  SELECT brand, COUNT(DISTINCT asin) AS brand_legend_cnt
  FROM tmp1.amzuk_bsr_shr_daily_acc
  GROUP BY 1 order by 1 desc
  )
SELECT brand,TMP1.brand_legend_cnt,
  CASE WHEN brand = 'Zinus' OR brand = 'ZINUS' THEN 0
       WHEN brand = 'No Brand Info' THEN 999999
       ELSE ROW_NUMBER() OVER (ORDER BY brand_legend_cnt DESC) END AS brand_legend_ord
from TMP1
;

 -- 1.8)  final table 
DROP TABLE IF EXISTS vs1.amzuk_bsr_shr_daily_acc;
CREATE TABLE vs1.amzuk_bsr_shr_daily_acc AS
SELECT A.*, B.brand_legend_ord
FROM tmp1.amzuk_bsr_shr_daily_acc A
LEFT OUTER JOIN tmp1.amzuk_bsr_shr_daily_acc_brand B ON A.brand=B.brand
*/

-- II. Germany 
-- 2.1) Declare date variable to mark max date
--DECLARE BSR_MAX_DT DATE;
--SET BSR_MAX_DT = (select max(CAST(SUBSTR(initialTime,1,10) AS DATE)) from dw.rf_amzde_bsr_all);
--select BSR_MAX_DT

-- 2.2) BSR raw data - zinus bsr asins
DROP TABLE IF EXISTS tmp1.amzde_bsr_shr_rawdata
;

CREATE TABLE tmp1.amzde_bsr_shr_rawdata AS
SELECT
    CASE
        WHEN CONTAINS_SUBSTR(LOWER(fullCategory), 'patio') THEN 'Patio ' || TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
        ELSE TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
    END AS bsr_ctgry
    , bestsellers_asin AS asin
    , bestsellers_rank AS rank
    , CAST(SUBSTR(initialTime, 1, 10) AS DATE) AS initialTime
FROM
    dw.rf_amzde_bsr_all
WHERE
    CAST(SUBSTR(initialTime, 1, 10) AS DATE) >= '2022-08-19' --AND rank<=50
;

-- 2.3) PDT raw data - select pdt info for BSR asins
DROP TABLE IF EXISTS tmp1.amzde_bsr_shr_pdt_tmp1
;

CREATE TABLE tmp1.amzde_bsr_shr_pdt_tmp1 AS
WITH
    TMP1 AS (
        SELECT asin
        FROM tmp1.amzde_bsr_shr_rawdata
        GROUP BY 1
    )
SELECT
    A.asin
    , crawlTime
    , brand
    , title
    , imageLink
FROM
    dw.amzde_pdt_all A
        INNER JOIN TMP1 B
            ON A.asin = B.asin
WHERE
    brand IS NOT NULL --AND title IS NOT NULL
GROUP BY
    1, 2, 3, 4, 5
;

-- 2.4) PDT raw data - select most recent pdt brand/title/url info for BSR asins
DROP TABLE IF EXISTS tmp1.amzde_bsr_shr_pdt_tmp2
;

CREATE TABLE tmp1.amzde_bsr_shr_pdt_tmp2 AS
WITH
    TMP1 AS (
        SELECT
            asin
            , crawlTime
            , brand
            , title
            , imageLink
            , ROW_NUMBER() OVER (PARTITION BY asin ORDER BY crawlTime DESC) AS filter
        FROM
            tmp1.amzde_bsr_shr_pdt_tmp1 A
    )
SELECT
    asin
    , brand
    , title
    , imageLink AS image_url
FROM
    TMP1
WHERE
    filter = 1
;

-- 2.5) Map pdt info into BSR raw data -->> final table 1
--DECLARE BSR_MAX_DT DATE;
--SET BSR_MAX_DT = (select max(CAST(SUBSTR(initialTime,1,10) AS DATE)) from dw.rf_amzde_bsr_all);

DROP TABLE IF EXISTS tmp1.amzde_bsr_shr_daily
;

CREATE TABLE tmp1.amzde_bsr_shr_daily AS
WITH
    TMP1 AS (
        SELECT
            A.bsr_ctgry
            , A.asin
            , A.rank
            , A.initialTime AS bsr_date
            --B.brand,
            , TRIM(REPLACE(COALESCE(UPPER(B.brand), 'No Brand Info'), ".", "")) AS brand
            , B.title
            , B.image_url
            , ( CASE
                    WHEN bsr_ctgry = 'Mattresses'                        THEN '01. Mattresses'
                    WHEN bsr_ctgry = 'Beds, Frames & Bases'              THEN '02. Beds, Frames & Bases'
                    WHEN bsr_ctgry = 'Bed Frames'                        THEN '03. Bed Frames'
                    WHEN bsr_ctgry = 'Wood Beds'                         THEN '04. Wood Beds'
                    WHEN bsr_ctgry = 'Metal Beds'                        THEN '05. Metal Beds'
                    WHEN bsr_ctgry = 'Padded Beds'                       THEN '06. Padded Beds'
                    WHEN bsr_ctgry = 'Slatted Divan Bases & Foundations' THEN '07. Slatted Divan Bases & Foundations'
                    WHEN bsr_ctgry = 'Mattress Toppers'                  THEN '08. Mattress Toppers'
                    WHEN bsr_ctgry = 'Sofas & Couches'                   THEN '09. Sofas & Couches'
                    WHEN bsr_ctgry = 'Dining Tables'                     THEN '10. Dining Tables'
                    WHEN bsr_ctgry = 'Desks'                             THEN '11. Desks'
                    WHEN bsr_ctgry = 'Computer Workstations'             THEN '12. Computer Workstations'
                    WHEN bsr_ctgry = 'Chairs'
                                                                         THEN '13. Chairs' --WHEN bsr_ctgry='Overlays' THEN '12. Overlays'
                --WHEN bsr_ctgry='Bookcases' THEN '13. Bookcases'
                --WHEN bsr_ctgry='Headboards' THEN '14. Headboards'
                END ) AS bsr_ctgry_label
            , 'https://www.amazon.de/dp/' || A.asin AS pdt_url
            -- , SUBSTR(CAST(initialTime AS STRING), 3, 5) AS yr_month
            -- , "Y" || SUBSTR(CAST(initialTime AS STRING), 3, 2) || " W" || CASE
            --                                                                   WHEN EXTRACT(WEEK FROM initialTime) = 0
            --                                                                       THEN EXTRACT(WEEK FROM initialTime - 7) + 1
            --                                                                   ELSE EXTRACT(WEEK FROM initialTime)
            --                                                               END AS yr_week
            , CASE
                  WHEN A.initialTime = BSR_MAX_DT THEN 1
                  ELSE 0
              END AS is_maxdt
            , CASE
                  WHEN A.initialTime <= BSR_MAX_DT AND A.initialTime >= BSR_MAX_DT - 3 THEN 1
                  ELSE 0
              END AS is_maxdt_range
            , ROW_NUMBER() OVER (PARTITION BY brand, initialTime, bsr_ctgry, ( CASE
                                                                                   WHEN rank <= 10               THEN 1
                                                                                   WHEN rank > 10 AND rank <= 20 THEN 2
                                                                                   ELSE 3
                                                                               END ) ORDER BY rank) AS bsr_ord
            , COUNT(A.asin) OVER (PARTITION BY B.brand, A.initialTime, A.bsr_ctgry) AS brand_prod_num

        FROM
            tmp1.amzde_bsr_shr_rawdata A
                LEFT OUTER JOIN tmp1.amzde_bsr_shr_pdt_tmp2 B
                    ON A.asin = B.asin
    )
SELECT
    *
    , CASE
          WHEN brand = 'Zinus' THEN 0
          ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry, bsr_date ORDER BY brand_prod_num DESC)
      END AS brand_ord
    --CASE WHEN bsr_date=BSR_MAX_DT THEN 'Today' ELSE
    , CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) AS date_str
FROM
    TMP1
;

-- 2.6) Mark Top 10/20/50/100 raws for Power BI 
DROP TABLE IF EXISTS tmp1.amzde_bsr_shr_daily_acc
;

CREATE TABLE tmp1.amzde_bsr_shr_daily_acc AS
(
    SELECT *, 'Top 10' AS bsr_rank_range FROM tmp1.amzde_bsr_shr_daily WHERE rank <= 10
)
UNION ALL
(
    SELECT *, 'Top 20' AS bsr_rank_range FROM tmp1.amzde_bsr_shr_daily WHERE rank <= 20
)
UNION ALL
(
    SELECT *, 'Top 50' AS bsr_rank_range FROM tmp1.amzde_bsr_shr_daily WHERE rank <= 50
)
UNION ALL
(
    SELECT *, 'Top 100' AS bsr_rank_range FROM tmp1.amzde_bsr_shr_daily WHERE rank <= 100
)
;

-- III. France / Space / Italy
-- 3.1) Declare date variable to mark max date
--DECLARE BSR_MAX_DT DATE;
--SET BSR_MAX_DT = (select max(CAST(SUBSTR(initialTime,1,10) AS DATE)) from dw.rf_amzfr_bsr_all);
--select BSR_MAX_DT

-- 3.2) BSR raw data - zinus bsr asins
DROP TABLE IF EXISTS tmp1.amzfr_bsr_shr_rawdata
;

CREATE TABLE tmp1.amzfr_bsr_shr_rawdata AS
SELECT
    CASE
        WHEN CONTAINS_SUBSTR(LOWER(fullCategory), 'patio') THEN 'Patio ' || TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
        ELSE TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
    END AS bsr_ctgry
    , bestsellers_asin AS asin
    , bestsellers_rank AS rank
    , CAST(SUBSTR(initialTime, 1, 10) AS DATE) AS initialTime
FROM
    dw.rf_amzfr_bsr_all
WHERE
    CAST(SUBSTR(initialTime, 1, 10) AS DATE) >= '2022-08-19' --AND rank<=50
;

DROP TABLE IF EXISTS tmp1.amzes_bsr_shr_rawdata
;

CREATE TABLE tmp1.amzes_bsr_shr_rawdata AS
SELECT
    CASE
        WHEN CONTAINS_SUBSTR(LOWER(fullCategory), 'patio') THEN 'Patio ' || TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
        ELSE TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
    END AS bsr_ctgry
    , bestsellers_asin AS asin
    , bestsellers_rank AS rank
    , CAST(SUBSTR(initialTime, 1, 10) AS DATE) AS initialTime
FROM
    dw.rf_amzes_bsr_all
WHERE
    CAST(SUBSTR(initialTime, 1, 10) AS DATE) >= '2022-08-19' --AND rank<=50
;

DROP TABLE IF EXISTS tmp1.amzit_bsr_shr_rawdata
;

CREATE TABLE tmp1.amzit_bsr_shr_rawdata AS
WITH
    TMP1 AS (
        SELECT
            *
        FROM
            dw.rf_amzit_bsr_all
        WHERE
            fullCategory != 'All categories > Home and kitchen > Furniture > Study > Desks'
            AND CAST(SUBSTR(initialTime, 1, 10) AS DATE) >= '2022-08-19'
    )
SELECT
    fullCategory
    , TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) AS bsr_ctgry
    , bestsellers_asin AS asin
    , bestsellers_rank AS rank
    , CAST(SUBSTR(initialTime, 1, 10) AS DATE) AS initialTime
FROM
    TMP1 -- dw.rf_amzit_bsr_all WHERE CAST(SUBSTR(initialTime,1,10) AS DATE) >='2022-08-19'
;


DROP TABLE IF EXISTS tmp1.amznl_bsr_shr_rawdata
;

CREATE TABLE tmp1.amznl_bsr_shr_rawdata AS
WITH
    TMP1 AS (
        SELECT
            *
        FROM
            dw.rf_amznl_bsr_all
        WHERE
            1 = 1
            --AND fullCategory != 'All categories > Home and kitchen > Furniture > Study > Desks'
            AND CAST(SUBSTR(initialTime, 1, 10) AS DATE) >= '2022-08-19'
    )
SELECT
    fullCategory
    , TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) AS bsr_ctgry
    , bestsellers_asin AS asin
    , bestsellers_rank AS rank
    , CAST(SUBSTR(initialTime, 1, 10) AS DATE) AS initialTime
FROM
    TMP1 -- dw.rf_amzit_bsr_all WHERE CAST(SUBSTR(initialTime,1,10) AS DATE) >='2022-08-19'
;

DROP TABLE IF EXISTS tmp1.amzse_bsr_shr_rawdata
;

CREATE TABLE tmp1.amzse_bsr_shr_rawdata AS
WITH
    TMP1 AS (
        SELECT
            *
        FROM
            dw.rf_amzse_bsr_all
        WHERE
            1 = 1
            --AND fullCategory != 'All categories > Home and kitchen > Furniture > Study > Desks'
            AND CAST(SUBSTR(initialTime, 1, 10) AS DATE) >= '2022-08-19'
    )
SELECT
    fullCategory
    , TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) AS bsr_ctgry
    , bestsellers_asin AS asin
    , bestsellers_rank AS rank
    , CAST(SUBSTR(initialTime, 1, 10) AS DATE) AS initialTime
FROM
    TMP1 -- dw.rf_amzit_bsr_all WHERE CAST(SUBSTR(initialTime,1,10) AS DATE) >='2022-08-19'
;


DROP TABLE IF EXISTS tmp1.amzbe_bsr_shr_rawdata
;

CREATE TABLE tmp1.amzbe_bsr_shr_rawdata AS
WITH
    TMP1 AS (
        SELECT
            *
        FROM
            dw.rf_amzbe_bsr_all
        WHERE
            1 = 1
            --AND fullCategory != 'All categories > Home and kitchen > Furniture > Study > Desks'
            AND CAST(SUBSTR(initialTime, 1, 10) AS DATE) >= '2022-08-19'
    )
SELECT
    fullCategory
    , TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) AS bsr_ctgry
    , bestsellers_asin AS asin
    , bestsellers_rank AS rank
    , CAST(SUBSTR(initialTime, 1, 10) AS DATE) AS initialTime
FROM
    TMP1 -- dw.rf_amzit_bsr_all WHERE CAST(SUBSTR(initialTime,1,10) AS DATE) >='2022-08-19'
;


-- 3.3) PDT raw data - select pdt info for BSR asins
DROP TABLE IF EXISTS tmp1.amzfr_bsr_shr_pdt_tmp1
;

CREATE TABLE tmp1.amzfr_bsr_shr_pdt_tmp1 AS
WITH
    TMP1 AS (
        SELECT asin
        FROM tmp1.amzfr_bsr_shr_rawdata
        GROUP BY 1
    )
SELECT
    A.asin
    , crawlTime
    , brand
    , title
    , imageLink
FROM
    dw.amzfr_pdt_all A
        INNER JOIN TMP1 B
            ON A.asin = B.asin
WHERE
    brand IS NOT NULL --AND title IS NOT NULL
GROUP BY
    1, 2, 3, 4, 5
;

DROP TABLE IF EXISTS tmp1.amzes_bsr_shr_pdt_tmp1
;

CREATE TABLE tmp1.amzes_bsr_shr_pdt_tmp1 AS
WITH
    TMP1 AS (
        SELECT asin
        FROM tmp1.amzes_bsr_shr_rawdata
        GROUP BY 1
    )
SELECT
    A.asin
    , crawlTime
    , brand
    , title
    , imageLink
FROM
    dw.amzes_pdt_all A
        INNER JOIN TMP1 B
            ON A.asin = B.asin
WHERE
    brand IS NOT NULL --AND title IS NOT NULL
GROUP BY
    1, 2, 3, 4, 5
;

DROP TABLE IF EXISTS tmp1.amzit_bsr_shr_pdt_tmp1
;

CREATE TABLE tmp1.amzit_bsr_shr_pdt_tmp1 AS
WITH
    TMP1 AS (
        SELECT asin
        FROM tmp1.amzit_bsr_shr_rawdata
        GROUP BY 1
    )
SELECT
    A.asin
    , crawlTime
    , brand
    , title
    , imageLink
FROM
    dw.amzit_pdt_all A
        INNER JOIN TMP1 B
            ON A.asin = B.asin
WHERE
    brand IS NOT NULL --AND title IS NOT NULL
GROUP BY
    1, 2, 3, 4, 5
;

DROP TABLE IF EXISTS tmp1.amznl_bsr_shr_pdt_tmp1
;

CREATE TABLE tmp1.amznl_bsr_shr_pdt_tmp1 AS
WITH
    TMP1 AS (
        SELECT asin
        FROM tmp1.amznl_bsr_shr_rawdata
        GROUP BY 1
    )
SELECT
    A.asin
    , crawlTime_utc AS crawlTime
    , brand
    , title
    , main_image_link AS imageLink
FROM
    dw.rf_amznl_pdt_daily A
        INNER JOIN TMP1 B
            ON A.asin = B.asin
WHERE
    brand IS NOT NULL --AND title IS NOT NULL
GROUP BY
    1, 2, 3, 4, 5
;

DROP TABLE IF EXISTS tmp1.amzse_bsr_shr_pdt_tmp1
;

CREATE TABLE tmp1.amzse_bsr_shr_pdt_tmp1 AS
WITH
    TMP1 AS (
        SELECT asin
        FROM tmp1.amzse_bsr_shr_rawdata
        GROUP BY 1
    )
SELECT
    A.asin
    , crawlTime_utc AS crawlTime
    , brand
    , title
    , main_image_link AS imageLink
FROM
    dw.rf_amzse_pdt_daily A
        INNER JOIN TMP1 B
            ON A.asin = B.asin
WHERE
    brand IS NOT NULL --AND title IS NOT NULL
GROUP BY
    1, 2, 3, 4, 5
;

DROP TABLE IF EXISTS tmp1.amzbe_bsr_shr_pdt_tmp1
;

CREATE TABLE tmp1.amzbe_bsr_shr_pdt_tmp1 AS
WITH
    TMP1 AS (
        SELECT asin
        FROM tmp1.amzbe_bsr_shr_rawdata
        GROUP BY 1
    )
SELECT
    A.asin
    , crawlTime_utc AS crawlTime
    , brand
    , title
    , main_image_link AS imageLink
FROM
    dw.rf_amzbe_pdt_daily A
        INNER JOIN TMP1 B
            ON A.asin = B.asin
WHERE
    brand IS NOT NULL --AND title IS NOT NULL
GROUP BY
    1, 2, 3, 4, 5
;

-- 3.4) PDT raw data - select most recent pdt brand/title/url info for BSR asins
DROP TABLE IF EXISTS tmp1.amzfr_bsr_shr_pdt_tmp2
;

CREATE TABLE tmp1.amzfr_bsr_shr_pdt_tmp2 AS
WITH
    TMP1 AS (
        SELECT
            asin
            , crawlTime
            , brand
            , title
            , imageLink
            , ROW_NUMBER() OVER (PARTITION BY asin ORDER BY crawlTime DESC) AS filter
        FROM
            tmp1.amzfr_bsr_shr_pdt_tmp1 A
    )
SELECT
    asin
    , brand
    , title
    , imageLink AS image_url
FROM
    TMP1
WHERE
    filter = 1
;

DROP TABLE IF EXISTS tmp1.amzes_bsr_shr_pdt_tmp2
;

CREATE TABLE tmp1.amzes_bsr_shr_pdt_tmp2 AS
WITH
    TMP1 AS (
        SELECT
            asin
            , crawlTime
            , brand
            , title
            , imageLink
            , ROW_NUMBER() OVER (PARTITION BY asin ORDER BY crawlTime DESC) AS filter
        FROM
            tmp1.amzes_bsr_shr_pdt_tmp1 A
    )
SELECT
    asin
    , brand
    , title
    , imageLink AS image_url
FROM
    TMP1
WHERE
    filter = 1
;

DROP TABLE IF EXISTS tmp1.amzit_bsr_shr_pdt_tmp2
;

CREATE TABLE tmp1.amzit_bsr_shr_pdt_tmp2 AS
WITH
    TMP1 AS (
        SELECT
            asin
            , crawlTime
            , brand
            , title
            , imageLink
            , ROW_NUMBER() OVER (PARTITION BY asin ORDER BY crawlTime DESC) AS filter
        FROM
            tmp1.amzit_bsr_shr_pdt_tmp1 A
    )
SELECT
    asin
    , brand
    , title
    , imageLink AS image_url
FROM
    TMP1
WHERE
    filter = 1
;

DROP TABLE IF EXISTS tmp1.amznl_bsr_shr_pdt_tmp2
;

CREATE TABLE tmp1.amznl_bsr_shr_pdt_tmp2 AS
WITH
    TMP1 AS (
        SELECT
            asin
            , crawlTime
            , brand
            , title
            , imageLink
            , ROW_NUMBER() OVER (PARTITION BY asin ORDER BY crawlTime DESC) AS filter
        FROM
            tmp1.amznl_bsr_shr_pdt_tmp1 A
    )
SELECT
    asin
    , brand
    , title
    , imageLink AS image_url
FROM
    TMP1
WHERE
    filter = 1
;

DROP TABLE IF EXISTS tmp1.amzse_bsr_shr_pdt_tmp2
;

CREATE TABLE tmp1.amzse_bsr_shr_pdt_tmp2 AS
WITH
    TMP1 AS (
        SELECT
            asin
            , crawlTime
            , brand
            , title
            , imageLink
            , ROW_NUMBER() OVER (PARTITION BY asin ORDER BY crawlTime DESC) AS filter
        FROM
            tmp1.amzse_bsr_shr_pdt_tmp1 A
    )
SELECT
    asin
    , brand
    , title
    , imageLink AS image_url
FROM
    TMP1
WHERE
    filter = 1
;

DROP TABLE IF EXISTS tmp1.amzbe_bsr_shr_pdt_tmp2
;

CREATE TABLE tmp1.amzbe_bsr_shr_pdt_tmp2 AS
WITH
    TMP1 AS (
        SELECT
            asin
            , crawlTime
            , brand
            , title
            , imageLink
            , ROW_NUMBER() OVER (PARTITION BY asin ORDER BY crawlTime DESC) AS filter
        FROM
            tmp1.amzbe_bsr_shr_pdt_tmp1 A
    )
SELECT
    asin
    , brand
    , title
    , imageLink AS image_url
FROM
    TMP1
WHERE
    filter = 1
;

-- 3.5) Map pdt info into BSR raw data -->> final table 1
--DECLARE BSR_MAX_DT DATE;
--SET BSR_MAX_DT = (select max(CAST(SUBSTR(initialTime,1,10) AS DATE)) from dw.rf_amzfr_bsr_all);
--select BSR_MAX_DT

DROP TABLE IF EXISTS tmp1.amzfr_bsr_shr_daily
;

CREATE TABLE tmp1.amzfr_bsr_shr_daily AS
WITH
    TMP1 AS (
        SELECT
            A.bsr_ctgry
            , A.asin
            , A.rank
            , A.initialTime AS bsr_date
            --B.brand,
            , TRIM(REPLACE(COALESCE(UPPER(B.brand), 'No Brand Info'), ".", "")) AS brand
            , B.title
            , B.image_url
            , ( CASE
                    WHEN bsr_ctgry = 'Mattress'             THEN '01. Mattresses'
                    WHEN bsr_ctgry = 'Beds and bed frames'  THEN '02. Beds and Bed Frames'
                    WHEN bsr_ctgry = 'Beds'                 THEN '03. Beds'
                    WHEN bsr_ctgry = 'bed frames'           THEN '04. Bed Frames'
                --WHEN bsr_ctgry='Beds' THEN '03. Beds'
                    WHEN bsr_ctgry = 'Mattress topper'      THEN '05. Mattress Toppper'
                    WHEN bsr_ctgry = 'Sofas and divans'     THEN '06. Sofas and Divans'
                    WHEN bsr_ctgry = 'Tables'               THEN '07. Tables'
                    WHEN bsr_ctgry = 'Offices'              THEN '08. Offices'
                    WHEN bsr_ctgry = 'Armchairs and chairs' THEN '09. Armchairs and chairs'
                END ) AS bsr_ctgry_label
            , 'https://www.amazon.fr/dp/' || A.asin AS pdt_url
            -- , SUBSTR(CAST(initialTime AS STRING), 3, 5) AS yr_month
            -- , "Y" || SUBSTR(CAST(initialTime AS STRING), 3, 2) || " W" || CASE
            --                                                                   WHEN EXTRACT(WEEK FROM initialTime) = 0
            --                                                                       THEN EXTRACT(WEEK FROM initialTime - 7) + 1
            --                                                                   ELSE EXTRACT(WEEK FROM initialTime)
            --                                                               END AS yr_week
            , CASE
                  WHEN A.initialTime = BSR_MAX_DT THEN 1
                  ELSE 0
              END AS is_maxdt
            , CASE
                  WHEN A.initialTime <= BSR_MAX_DT AND A.initialTime >= BSR_MAX_DT - 3 THEN 1
                  ELSE 0
              END AS is_maxdt_range
            , ROW_NUMBER() OVER (PARTITION BY brand, initialTime, bsr_ctgry, ( CASE
                                                                                   WHEN rank <= 10               THEN 1
                                                                                   WHEN rank > 10 AND rank <= 20 THEN 2
                                                                                   ELSE 3
                                                                               END ) ORDER BY rank) AS bsr_ord
            , COUNT(A.asin) OVER (PARTITION BY B.brand, A.initialTime, A.bsr_ctgry) AS brand_prod_num

        FROM
            tmp1.amzfr_bsr_shr_rawdata A
                LEFT OUTER JOIN tmp1.amzfr_bsr_shr_pdt_tmp2 B
                    ON A.asin = B.asin
    )
SELECT
    *
    , CASE
          WHEN brand = 'Zinus' THEN 0
          ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry, bsr_date ORDER BY brand_prod_num DESC)
      END AS brand_ord
    --CASE WHEN bsr_date=BSR_MAX_DT THEN 'Today' ELSE
    , CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) AS date_str
FROM
    TMP1
;

--DECLARE BSR_MAX_DT DATE;
--SET BSR_MAX_DT = (select max(CAST(SUBSTR(initialTime,1,10) AS DATE)) from dw.rf_amzes_bsr_all);
--select BSR_MAX_DT

DROP TABLE IF EXISTS tmp1.amzes_bsr_shr_daily
;

CREATE TABLE tmp1.amzes_bsr_shr_daily AS
WITH
    TMP1 AS (
        SELECT
            A.bsr_ctgry
            , A.asin
            , A.rank
            , A.initialTime AS bsr_date
            --B.brand,
            , TRIM(REPLACE(COALESCE(UPPER(B.brand), 'No Brand Info'), ".", "")) AS brand
            , B.title
            , B.image_url
            , ( CASE
                    WHEN bsr_ctgry = 'mattresses'                 THEN '01. Mattresses'
                    WHEN bsr_ctgry = 'Beds, structures and bases' THEN '02. Beds, Structures and Bases'
                    WHEN bsr_ctgry = 'Beds'                       THEN '03. Beds'
                    WHEN bsr_ctgry = 'frames'                     THEN '04. Frames'
                    WHEN bsr_ctgry = 'Beds, frames and bases'     THEN '05. Beds, Frames and Bases'
                    WHEN bsr_ctgry = 'bed bases'                  THEN '06. Bed Bases'
                    WHEN bsr_ctgry = 'slatted bed bases'          THEN '07. Slatted Bed Bases'
                    WHEN bsr_ctgry = 'sofas'                      THEN '08. Sofas'
                    WHEN bsr_ctgry = 'tables'                     THEN '09. Tables'
                    WHEN bsr_ctgry = 'desks'                      THEN '10. Desks'
                    WHEN bsr_ctgry = 'Chairs'
                                                                  THEN '11. Chairs' --WHEN bsr_ctgry='furniture sets' THEN '10. Funiture Sets'
                --WHEN bsr_ctgry='headboards' THEN '10. Headboards'
                --WHEN bsr_ctgry='Banks' THEN '11. Banks'
                END ) AS bsr_ctgry_label
            , 'https://www.amazon.es/dp/' || A.asin AS pdt_url
            -- , SUBSTR(CAST(initialTime AS STRING), 3, 5) AS yr_month
            -- , "Y" || SUBSTR(CAST(initialTime AS STRING), 3, 2) || " W" || CASE
            --                                                                   WHEN EXTRACT(WEEK FROM initialTime) = 0
            --                                                                       THEN EXTRACT(WEEK FROM initialTime - 7) + 1
            --                                                                   ELSE EXTRACT(WEEK FROM initialTime)
            --                                                               END AS yr_week
            , CASE
                  WHEN A.initialTime = BSR_MAX_DT THEN 1
                  ELSE 0
              END AS is_maxdt
            , CASE
                  WHEN A.initialTime <= BSR_MAX_DT AND A.initialTime >= BSR_MAX_DT - 3 THEN 1
                  ELSE 0
              END AS is_maxdt_range
            , ROW_NUMBER() OVER (PARTITION BY brand, initialTime, bsr_ctgry, ( CASE
                                                                                   WHEN rank <= 10               THEN 1
                                                                                   WHEN rank > 10 AND rank <= 20 THEN 2
                                                                                   ELSE 3
                                                                               END ) ORDER BY rank) AS bsr_ord
            , COUNT(A.asin) OVER (PARTITION BY B.brand, A.initialTime, A.bsr_ctgry) AS brand_prod_num

        FROM
            tmp1.amzes_bsr_shr_rawdata A
                LEFT OUTER JOIN tmp1.amzes_bsr_shr_pdt_tmp2 B
                    ON A.asin = B.asin
    )
SELECT
    *
    , CASE
          WHEN brand = 'Zinus' THEN 0
          ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry, bsr_date ORDER BY brand_prod_num DESC)
      END AS brand_ord
    --CASE WHEN bsr_date=BSR_MAX_DT THEN 'Today' ELSE
    , CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) AS date_str
FROM
    TMP1
;

--DECLARE BSR_MAX_DT DATE;
--SET BSR_MAX_DT = (select max(CAST(SUBSTR(initialTime,1,10) AS DATE)) from dw.rf_amzit_bsr_all);
--select BSR_MAX_DT

DROP TABLE IF EXISTS tmp1.amzit_bsr_shr_daily
;

CREATE TABLE tmp1.amzit_bsr_shr_daily AS
WITH
    TMP1 AS (
        SELECT
            A.bsr_ctgry
            , A.asin
            , A.rank
            , A.initialTime AS bsr_date
            --B.brand,
            , TRIM(REPLACE(COALESCE(UPPER(B.brand), 'No Brand Info'), ".", "")) AS brand
            , B.title
            , B.image_url
            , ( CASE
                    WHEN bsr_ctgry = 'Mattresses'                  THEN '01. Mattresses'
                    WHEN bsr_ctgry = 'Beds, frames and bases'      THEN '02. Beds, Frames and Bases'
                    WHEN bsr_ctgry = 'Beds'                        THEN '03. Beds'
                    WHEN bsr_ctgry = 'Bed structures'              THEN '04. Beds Structures'
                --WHEN bsr_ctgry='Mattress covers' THEN '04. Mattress Covers'
                    WHEN bsr_ctgry = 'Mattress covers and toppers'
                                                                   THEN '05. Mattress Covers and Toppers' -- WHEN bsr_ctgry='Mattress covers and protections' THEN '06. Mattress Covers and Protections'
                -- WHEN bsr_ctgry='Headboards' THEN '07. Headboards'
                    WHEN bsr_ctgry = 'Sofas'                       THEN '06. Sofas'
                    WHEN bsr_ctgry = 'Dining room tables'          THEN '07. Dining Room Tables'
                --WHEN bsr_ctgry='Low tables' THEN '13. Low Tables'
                    WHEN bsr_ctgry = 'Dining room set'             THEN '08. Dining Room Set'
                --WHEN bsr_ctgry='Desks' THEN '09. Desks'
                    WHEN bsr_ctgry = 'Computer workstations'       THEN '09. Computer Workstations'
                    WHEN bsr_ctgry = 'Seats'                       THEN '10. Seats'
                -- WHEN bsr_ctgry='Benches' THEN '11. Benches'

                END ) AS bsr_ctgry_label
            , 'https://www.amazon.it/dp/' || A.asin AS pdt_url
            -- , SUBSTR(CAST(initialTime AS STRING), 3, 5) AS yr_month
            -- , "Y" || SUBSTR(CAST(initialTime AS STRING), 3, 2) || " W" || CASE
            --                                                                   WHEN EXTRACT(WEEK FROM initialTime) = 0
            --                                                                       THEN EXTRACT(WEEK FROM initialTime - 7) + 1
            --                                                                   ELSE EXTRACT(WEEK FROM initialTime)
            --                                                               END AS yr_week
            , CASE
                  WHEN A.initialTime = BSR_MAX_DT THEN 1
                  ELSE 0
              END AS is_maxdt
            , CASE
                  WHEN A.initialTime <= BSR_MAX_DT AND A.initialTime >= BSR_MAX_DT - 3 THEN 1
                  ELSE 0
              END AS is_maxdt_range
            , ROW_NUMBER() OVER (PARTITION BY brand, initialTime, bsr_ctgry, ( CASE
                                                                                   WHEN rank <= 10               THEN 1
                                                                                   WHEN rank > 10 AND rank <= 20 THEN 2
                                                                                   ELSE 3
                                                                               END ) ORDER BY rank) AS bsr_ord
            , COUNT(A.asin) OVER (PARTITION BY B.brand, A.initialTime, A.bsr_ctgry) AS brand_prod_num

        FROM
            tmp1.amzit_bsr_shr_rawdata A
                LEFT OUTER JOIN tmp1.amzit_bsr_shr_pdt_tmp2 B
                    ON A.asin = B.asin
    )
SELECT
    *
    , CASE
          WHEN brand = 'Zinus' THEN 0
          ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry, bsr_date ORDER BY brand_prod_num DESC)
      END AS brand_ord
    --CASE WHEN bsr_date=BSR_MAX_DT THEN 'Today' ELSE
    , CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) AS date_str
FROM
    TMP1
;

DROP TABLE IF EXISTS tmp1.amznl_bsr_shr_daily
;

CREATE TABLE tmp1.amznl_bsr_shr_daily AS
WITH
    TMP1 AS (
        SELECT
            A.bsr_ctgry
            , A.asin
            , A.rank
            , A.initialTime AS bsr_date
            --B.brand,
            , TRIM(REPLACE(COALESCE(UPPER(B.brand), 'No Brand Info'), ".", "")) AS brand
            , B.title
            , B.image_url
            , ( CASE
                    WHEN bsr_ctgry = 'Mattresses'           THEN '01. Mattresses'
                    WHEN bsr_ctgry = 'Bed Frames'           THEN '02. Bed Frames'
                    WHEN bsr_ctgry = 'Beds, Frames & Bases' THEN '03. Beds'
                    WHEN bsr_ctgry = 'Sofas & Couches'      THEN '04. Sofas'
                    WHEN bsr_ctgry = 'Chairs'               THEN '05. Chairs'
                    WHEN bsr_ctgry = 'Desks'                THEN '06. Desks'
                END ) AS bsr_ctgry_label
            , 'https://www.amazon.nl/dp/' || A.asin AS pdt_url
            -- , SUBSTR(CAST(initialTime AS STRING), 3, 5) AS yr_month
            -- , "Y" || SUBSTR(CAST(initialTime AS STRING), 3, 2) || " W" || CASE
            --                                                                   WHEN EXTRACT(WEEK FROM initialTime) = 0
            --                                                                       THEN EXTRACT(WEEK FROM initialTime - 7) + 1
            --                                                                   ELSE EXTRACT(WEEK FROM initialTime)
            --                                                               END AS yr_week
            , CASE
                  WHEN A.initialTime = BSR_MAX_DT THEN 1
                  ELSE 0
              END AS is_maxdt
            , CASE
                  WHEN A.initialTime <= BSR_MAX_DT AND A.initialTime >= BSR_MAX_DT - 3 THEN 1
                  ELSE 0
              END AS is_maxdt_range
            , ROW_NUMBER() OVER (PARTITION BY brand, initialTime, bsr_ctgry, ( CASE
                                                                                   WHEN rank <= 10               THEN 1
                                                                                   WHEN rank > 10 AND rank <= 20 THEN 2
                                                                                   ELSE 3
                                                                               END ) ORDER BY rank) AS bsr_ord
            , COUNT(A.asin) OVER (PARTITION BY B.brand, A.initialTime, A.bsr_ctgry) AS brand_prod_num

        FROM
            tmp1.amznl_bsr_shr_rawdata A
                LEFT OUTER JOIN tmp1.amznl_bsr_shr_pdt_tmp2 B
                    ON A.asin = B.asin
    )
SELECT
    *
    , CASE
          WHEN brand = 'Zinus' THEN 0
          ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry, bsr_date ORDER BY brand_prod_num DESC)
      END AS brand_ord
    --CASE WHEN bsr_date=BSR_MAX_DT THEN 'Today' ELSE
    , CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) AS date_str
FROM
    TMP1
;

DROP TABLE IF EXISTS tmp1.amzse_bsr_shr_daily
;

CREATE TABLE tmp1.amzse_bsr_shr_daily AS
WITH
    TMP1 AS (
        SELECT
            A.bsr_ctgry
            , A.asin
            , A.rank
            , A.initialTime AS bsr_date
            --B.brand,
            , TRIM(REPLACE(COALESCE(UPPER(B.brand), 'No Brand Info'), ".", "")) AS brand
            , B.title
            , B.image_url
            , ( CASE
                    WHEN bsr_ctgry = 'Mattresses'           THEN '01. Mattresses'
                    WHEN bsr_ctgry = 'Bed Frames'           THEN '02. Bed Frames'
                    WHEN bsr_ctgry = 'Beds, Frames & Bases' THEN '03. Beds'
                    WHEN bsr_ctgry = 'Sofas & Divans'       THEN '04. Sofas'
                    WHEN bsr_ctgry = 'Chairs'               THEN '05. Chairs'
                    WHEN bsr_ctgry = 'Desks'                THEN '06. Desks'
                END ) AS bsr_ctgry_label
            , 'https://www.amazon.se/dp/' || A.asin AS pdt_url
            -- , SUBSTR(CAST(initialTime AS STRING), 3, 5) AS yr_month
            -- , "Y" || SUBSTR(CAST(initialTime AS STRING), 3, 2) || " W" || CASE
            --                                                                   WHEN EXTRACT(WEEK FROM initialTime) = 0
            --                                                                       THEN EXTRACT(WEEK FROM initialTime - 7) + 1
            --                                                                   ELSE EXTRACT(WEEK FROM initialTime)
            --                                                               END AS yr_week
            , CASE
                  WHEN A.initialTime = BSR_MAX_DT THEN 1
                  ELSE 0
              END AS is_maxdt
            , CASE
                  WHEN A.initialTime <= BSR_MAX_DT AND A.initialTime >= BSR_MAX_DT - 3 THEN 1
                  ELSE 0
              END AS is_maxdt_range
            , ROW_NUMBER() OVER (PARTITION BY brand, initialTime, bsr_ctgry, ( CASE
                                                                                   WHEN rank <= 10               THEN 1
                                                                                   WHEN rank > 10 AND rank <= 20 THEN 2
                                                                                   ELSE 3
                                                                               END ) ORDER BY rank) AS bsr_ord
            , COUNT(A.asin) OVER (PARTITION BY B.brand, A.initialTime, A.bsr_ctgry) AS brand_prod_num

        FROM
            tmp1.amzse_bsr_shr_rawdata A
                LEFT OUTER JOIN tmp1.amzse_bsr_shr_pdt_tmp2 B
                    ON A.asin = B.asin
    )
SELECT
    *
    , CASE
          WHEN brand = 'Zinus' THEN 0
          ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry, bsr_date ORDER BY brand_prod_num DESC)
      END AS brand_ord
    --CASE WHEN bsr_date=BSR_MAX_DT THEN 'Today' ELSE
    , CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) AS date_str
FROM
    TMP1
;

DROP TABLE IF EXISTS tmp1.amzbe_bsr_shr_daily
;

CREATE TABLE tmp1.amzbe_bsr_shr_daily AS
WITH
    TMP1 AS (
        SELECT
            A.bsr_ctgry
            , A.asin
            , A.rank
            , A.initialTime AS bsr_date
            --B.brand,
            , TRIM(REPLACE(COALESCE(UPPER(B.brand), 'No Brand Info'), ".", "")) AS brand
            , B.title
            , B.image_url
            , ( CASE
                    WHEN bsr_ctgry = 'Mattresses'           THEN '01. Mattresses'
                    WHEN bsr_ctgry = 'Bed Frames'           THEN '02. Bed Frames'
                    WHEN bsr_ctgry = 'Beds, Frames & Bases' THEN '03. Beds'
                    WHEN bsr_ctgry = 'Sofas & Couches'      THEN '04. Sofas'
                    WHEN bsr_ctgry = 'Chairs'               THEN '05. Chairs'
                    WHEN bsr_ctgry = 'Desks'                THEN '06. Desks'
                END ) AS bsr_ctgry_label
            , 'https://www.amazon.com.be/dp/' || A.asin AS pdt_url
            -- , SUBSTR(CAST(initialTime AS STRING), 3, 5) AS yr_month
            -- , "Y" || SUBSTR(CAST(initialTime AS STRING), 3, 2) || " W" || CASE
            --                                                                   WHEN EXTRACT(WEEK FROM initialTime) = 0
            --                                                                       THEN EXTRACT(WEEK FROM initialTime - 7) + 1
            --                                                                   ELSE EXTRACT(WEEK FROM initialTime)
            --                                                               END AS yr_week
            , CASE
                  WHEN A.initialTime = BSR_MAX_DT THEN 1
                  ELSE 0
              END AS is_maxdt
            , CASE
                  WHEN A.initialTime <= BSR_MAX_DT AND A.initialTime >= BSR_MAX_DT - 3 THEN 1
                  ELSE 0
              END AS is_maxdt_range
            , ROW_NUMBER() OVER (PARTITION BY brand, initialTime, bsr_ctgry, ( CASE
                                                                                   WHEN rank <= 10               THEN 1
                                                                                   WHEN rank > 10 AND rank <= 20 THEN 2
                                                                                   ELSE 3
                                                                               END ) ORDER BY rank) AS bsr_ord
            , COUNT(A.asin) OVER (PARTITION BY B.brand, A.initialTime, A.bsr_ctgry) AS brand_prod_num

        FROM
            tmp1.amzbe_bsr_shr_rawdata A
                LEFT OUTER JOIN tmp1.amzbe_bsr_shr_pdt_tmp2 B
                    ON A.asin = B.asin
    )
SELECT
    *
    , CASE
          WHEN brand = 'Zinus' THEN 0
          ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry, bsr_date ORDER BY brand_prod_num DESC)
      END AS brand_ord
    --CASE WHEN bsr_date=BSR_MAX_DT THEN 'Today' ELSE
    , CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) AS date_str
FROM
    TMP1
;


-- 3.6) Mark Top 10/20/50/100 raws for Power BI 
DROP TABLE IF EXISTS tmp1.amzfr_bsr_shr_daily_acc
;

CREATE TABLE tmp1.amzfr_bsr_shr_daily_acc AS
(
    SELECT *, 'Top 10' AS bsr_rank_range FROM tmp1.amzfr_bsr_shr_daily WHERE rank <= 10
)
UNION ALL
(
    SELECT *, 'Top 20' AS bsr_rank_range FROM tmp1.amzfr_bsr_shr_daily WHERE rank <= 20
)
UNION ALL
(
    SELECT *, 'Top 50' AS bsr_rank_range FROM tmp1.amzfr_bsr_shr_daily WHERE rank <= 50
)
UNION ALL
(
    SELECT *, 'Top 100' AS bsr_rank_range FROM tmp1.amzfr_bsr_shr_daily WHERE rank <= 100
)
;

DROP TABLE IF EXISTS tmp1.amzes_bsr_shr_daily_acc
;

CREATE TABLE tmp1.amzes_bsr_shr_daily_acc AS
(
    SELECT *, 'Top 10' AS bsr_rank_range FROM tmp1.amzes_bsr_shr_daily WHERE rank <= 10
)
UNION ALL
(
    SELECT *, 'Top 20' AS bsr_rank_range FROM tmp1.amzes_bsr_shr_daily WHERE rank <= 20
)
UNION ALL
(
    SELECT *, 'Top 50' AS bsr_rank_range FROM tmp1.amzes_bsr_shr_daily WHERE rank <= 50
)
UNION ALL
(
    SELECT *, 'Top 100' AS bsr_rank_range FROM tmp1.amzes_bsr_shr_daily WHERE rank <= 100
)
;

DROP TABLE IF EXISTS tmp1.amzit_bsr_shr_daily_acc
;

CREATE TABLE tmp1.amzit_bsr_shr_daily_acc AS
(
    SELECT *, 'Top 10' AS bsr_rank_range FROM tmp1.amzit_bsr_shr_daily WHERE rank <= 10
)
UNION ALL
(
    SELECT *, 'Top 20' AS bsr_rank_range FROM tmp1.amzit_bsr_shr_daily WHERE rank <= 20
)
UNION ALL
(
    SELECT *, 'Top 50' AS bsr_rank_range FROM tmp1.amzit_bsr_shr_daily WHERE rank <= 50
)
UNION ALL
(
    SELECT *, 'Top 100' AS bsr_rank_range FROM tmp1.amzit_bsr_shr_daily WHERE rank <= 100
)
;

DROP TABLE IF EXISTS tmp1.amznl_bsr_shr_daily_acc
;

CREATE TABLE tmp1.amznl_bsr_shr_daily_acc AS
(
    SELECT *, 'Top 10' AS bsr_rank_range FROM tmp1.amznl_bsr_shr_daily WHERE rank <= 10
)
UNION ALL
(
    SELECT *, 'Top 20' AS bsr_rank_range FROM tmp1.amznl_bsr_shr_daily WHERE rank <= 20
)
UNION ALL
(
    SELECT *, 'Top 50' AS bsr_rank_range FROM tmp1.amznl_bsr_shr_daily WHERE rank <= 50
)
UNION ALL
(
    SELECT *, 'Top 100' AS bsr_rank_range FROM tmp1.amznl_bsr_shr_daily WHERE rank <= 100
)
;

DROP TABLE IF EXISTS tmp1.amzse_bsr_shr_daily_acc
;

CREATE TABLE tmp1.amzse_bsr_shr_daily_acc AS
(
    SELECT *, 'Top 10' AS bsr_rank_range FROM tmp1.amzse_bsr_shr_daily WHERE rank <= 10
)
UNION ALL
(
    SELECT *, 'Top 20' AS bsr_rank_range FROM tmp1.amzse_bsr_shr_daily WHERE rank <= 20
)
UNION ALL
(
    SELECT *, 'Top 50' AS bsr_rank_range FROM tmp1.amzse_bsr_shr_daily WHERE rank <= 50
)
UNION ALL
(
    SELECT *, 'Top 100' AS bsr_rank_range FROM tmp1.amzse_bsr_shr_daily WHERE rank <= 100
)
;

DROP TABLE IF EXISTS tmp1.amzbe_bsr_shr_daily_acc
;

CREATE TABLE tmp1.amzbe_bsr_shr_daily_acc AS
(
    SELECT *, 'Top 10' AS bsr_rank_range FROM tmp1.amzbe_bsr_shr_daily WHERE rank <= 10
)
UNION ALL
(
    SELECT *, 'Top 20' AS bsr_rank_range FROM tmp1.amzbe_bsr_shr_daily WHERE rank <= 20
)
UNION ALL
(
    SELECT *, 'Top 50' AS bsr_rank_range FROM tmp1.amzbe_bsr_shr_daily WHERE rank <= 50
)
UNION ALL
(
    SELECT *, 'Top 100' AS bsr_rank_range FROM tmp1.amzbe_bsr_shr_daily WHERE rank <= 100
)
;


-- VI. consolidate 5 Country Data 

-- 6.1) union 5 tables
-- DROP TABLE IF EXISTS tmp1.amz_eu_bsr_shr_daily_acc
-- ;

CREATE OR REPLACE TABLE tmp1.amz_eu_bsr_shr_daily_acc AS
(
    SELECT
        'UK' AS country
        , * EXCEPT (brand)
        , REGEXP_REPLACE(TRIM(brand), r'\p{C}[^\p{Han}]', '') AS brand -- 제어 문자 (control 문자) 삭제
    FROM
        tmp1.amzuk_bsr_shr_daily_acc
)
UNION ALL
(
    SELECT 'DE' AS country, * EXCEPT (brand), REGEXP_REPLACE(TRIM(brand), r'\p{C}[^\p{Han}]', '') as brand
    FROM tmp1.amzde_bsr_shr_daily_acc
)
UNION ALL
(
    SELECT 'FR' AS country, * EXCEPT (brand), REGEXP_REPLACE(TRIM(brand), r'\p{C}[^\p{Han}]', '') as brand
    FROM tmp1.amzfr_bsr_shr_daily_acc
)
UNION ALL
(
    SELECT 'ES' AS country, * EXCEPT (brand), REGEXP_REPLACE(TRIM(brand), r'\p{C}[^\p{Han}]', '') as brand
    FROM tmp1.amzes_bsr_shr_daily_acc
)
UNION ALL
(
    SELECT 'IT' AS country, * EXCEPT (brand), REGEXP_REPLACE(TRIM(brand), r'\p{C}[^\p{Han}]', '') as brand
    FROM tmp1.amzit_bsr_shr_daily_acc
)
UNION ALL
(
    SELECT 'NL' AS country, * EXCEPT (brand), REGEXP_REPLACE(TRIM(brand), r'\p{C}[^\p{Han}]', '') as brand
    FROM tmp1.amznl_bsr_shr_daily_acc
)
UNION ALL
(
    SELECT 'SE' AS country, * EXCEPT (brand), REGEXP_REPLACE(TRIM(brand), r'\p{C}[^\p{Han}]', '') as brand
    FROM tmp1.amzse_bsr_shr_daily_acc
)
UNION ALL
(
    SELECT 'BE' AS country, * EXCEPT (brand), REGEXP_REPLACE(TRIM(brand), r'\p{C}[^\p{Han}]', '') as brand
    FROM tmp1.amzbe_bsr_shr_daily_acc
)

;


CREATE OR REPLACE TABLE tmp1.amz_eu_bsr_brand_list AS
WITH TMP1 AS (
    SELECT
        country
        , bsr_ctgry_label
        , brand
        , count(asin) AS cnt
    FROM tmp1.amz_eu_bsr_shr_daily_acc
    WHERE
        bsr_date >= (
            SELECT DATE_SUB(MAX(bsr_date), INTERVAL 3 MONTH) FROM tmp1.amz_eu_bsr_shr_daily_acc
        )
    GROUP BY 1, 2, 3
)
SELECT
    country
    , bsr_ctgry_label
    , brand
FROM
    TMP1
QUALIFY
    RANK() OVER (PARTITION BY country, bsr_ctgry_label ORDER BY cnt DESC ) <= 20
;

-- select DISTINCT brand from tmp1.amz_eu_bsr_shr_daily_acc where lower(brand) like '%zinus%' or lower(brand) like '%mellow%';

-- 6.2)  make the brand_legend_order field
-- DROP TABLE IF EXISTS tmp1.amz_eu_bsr_shr_daily_acc_brand
-- ;

-- CREATE OR REPLACE TABLE tmp1.amz_eu_bsr_shr_daily_acc_brand AS
-- WITH
--     TMP1 AS (
--         SELECT brand, COUNT(DISTINCT asin) AS brand_legend_cnt
--         FROM tmp1.amz_eu_bsr_shr_daily_acc
--         GROUP BY 1
--         ORDER BY 1 DESC
--     )
-- SELECT
--     brand
--     , TMP1.brand_legend_cnt
--     , CASE
--           WHEN UPPER(brand) LIKE '%ZINUS%' THEN 0
--           WHEN UPPER(brand) LIKE '%MELLOW%' THEN 1
--           WHEN brand IS NULL THEN 2
--           WHEN brand = 'No Brand Info'            THEN 999999
--           ELSE ROW_NUMBER() OVER (ORDER BY brand_legend_cnt DESC) + 2
--       END AS brand_legend_ord
-- FROM
--     TMP1
-- ;

-- 6.3)  final tables
-- DROP TABLE IF EXISTS vs1.amz_eu_bsr_shr_daily_acc;

CREATE OR REPLACE TABLE vs1.amz_eu_bsr_shr_daily_acc AS
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
--     , cte AS (
--         SELECT DISTINCT
--             A.*
--             , B.brand_legend_ord
--         FROM
--             tmp1.amz_eu_bsr_shr_daily_acc A
--                 LEFT OUTER JOIN tmp1.amz_eu_bsr_shr_daily_acc_brand B
--                     ON A.brand = B.brand
--         -- where asin = 'B002JY7M38'
--     )
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
        LEFT OUTER JOIN tmp1.amz_eu_bsr_brand_list B
            ON A.brand = B.brand AND A.bsr_ctgry_label = B.bsr_ctgry_label and A.country = B.country
        LEFT OUTER JOIN cte_legend C
            ON A.brand = C.brand
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY country, bsr_ctgry, asin, bsr_date, t.brand, bsr_rank_range ORDER BY rank) = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY A.country, bsr_ctgry, asin, bsr_date, bsr_rank_range ORDER BY rank) = 1
;

-- select DISTINCT country, brand, bsr_ctgry_label from vs1.amz_eu_bsr_shr_daily_acc ORDER BY 3, 1, 2;
-- select * from vs1.amz_eu_bsr_shr_daily_acc;

--DECLARE BSR_MAX_DT DATE;
--SET BSR_MAX_DT = (select max(CAST(SUBSTR(initialTime,1,10) AS DATE)) from dw.rf_amzes_bsr_all);
--select BSR_MAX_DT
-- 6.4) Summary
-- DROP TABLE IF EXISTS vs1.amz_eu_bsr_shr_daily_summary
-- ;

CREATE OR REPLACE TABLE vs1.amz_eu_bsr_shr_daily_summary AS
    --WITH TMP1 AS (
SELECT
    bsr_date
    , country
    , bsr_ctgry_label
    , bsr_rank_range
    , SUM(IF(brand = 'ZINUS' AND bsr_rank_range = 'Top 10', 1, 0)) AS top10_cnt
    , SUM(IF(brand = 'ZINUS' AND bsr_rank_range = 'Top 20', 1, 0)) AS top20_cnt
    , SUM(IF(brand = 'ZINUS' AND bsr_rank_range = 'Top 50', 1, 0)) AS top50_cnt
    , SUM(IF(brand = 'ZINUS' AND bsr_rank_range = 'Top 100', 1, 0)) AS top100_cnt
FROM
    `vs1.amz_eu_bsr_shr_daily_acc`
GROUP BY
    1, 2, 3, 4
/* )   -- order by 1,2,3,4 )
SELECT
  *, top10_cnt/10 as top10_rate,
  top20_cnt/20 as top20_rate,
  top50_cnt/50 as top50_rate,
  top100_cnt/100 as top100_rate
FROM TMP1 */
;
