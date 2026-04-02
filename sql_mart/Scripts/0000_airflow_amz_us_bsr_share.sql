-- ##### BSR Share Report #### ------------------------------------------------------------------------------------------------------------------------
-- 08/15/22, last modified by Kyungjin Lee

-- Source data (BigQuery):
---- crwl.amz_bsr_all_rf: daily amazon bsr rank, asin by bsr category from rainforest API
---- crwl.amz_pdt_all: amazon product information from PDP, every day / all major asins including competitors

-- Output:
---- vs.amz_bsr_shr_daily: for Executive Summary / Daily Status pages, incl. daily rank by ctgry with brand/title/image... etc.
---- vs.amz_bsr_shr_daily_acc: for BSR Share Trends, including dup raws to mark Top10/Top20/Top50 in Power BI
-------------------------------------------------------------------------------------------------------------------------------------------------------

-- 1) Declare date variable to mark max date
DECLARE BSR_MAX_DT DATE;
DECLARE BSR_COMP_START_DT DATE;

SET BSR_MAX_DT = (select max(DATE(DATETIME(timestamp(initialTime), "America/Los_Angeles"))) from rfapi.rf_amz_bsr_hourly);
SET BSR_COMP_START_DT = (select BSR_MAX_DT-29);


-- 2) BSR raw data - zinus bsr asins

--- 2.1)  SNAPSHOT CRAWLING (Static)
--  tmp.amz_bsr_shr_rawdata_tmp1
/*
DROP TABLE IF EXISTS tmp.amz_bsr_shr_rawdata_tmp1;
CREATE TABLE tmp.amz_bsr_shr_rawdata_tmp1 AS
WITH TMP2 AS (
    WITH TMP1 AS (
      SELECT
        CASE WHEN CONTAINS_SUBSTR(lower(fullCategory),'patio') THEN 'Patio '||TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
            ELSE TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) END AS bsr_ctgry,
        asin,
        rank,
        DATETIME(timestamp(initialTime), "America/Los_Angeles") AS initialTime_PT,
        DATE(DATETIME(timestamp(initialTime), "America/Los_Angeles")) AS initialDate_PT
      FROM crwl.amz_bsr_all_rf
      WHERE SUBSTR(initialTime,1,10)>='2021-01-01' AND SUBSTR(initialTime,1,10)<='2022-07-31' AND rank<=50)
    SELECT
     bsr_ctgry,
     asin,
     rank,
     initialTime_PT,
     initialDate_PT,
     ROW_NUMBER() OVER (PARTITION BY bsr_ctgry, initialDate_PT, rank ORDER BY initialTime_PT DESC) AS filter_rnk
    FROM TMP1
    )
SELECT
  bsr_ctgry,
  asin,
  rank,
  initialTime_PT,
  initialDate_PT
FROM TMP2
WHERE filter_rnk=1
;
*/

-- 2.2) Hourly BSR data
DROP TABLE IF EXISTS tmp.amz_bsr_shr_rawdata_tmp2;
CREATE TABLE tmp.amz_bsr_shr_rawdata_tmp2 AS
WITH TMP2 AS (
    WITH TMP1 AS (
        SELECT
            CASE
                WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Dining Room Furniture > Tables' THEN 'Dining Tables'
                WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Living Room Furniture > Chairs' THEN 'Living Room Chairs'
                WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Living Room Furniture > Television Stands & Entertainment Centers' THEN 'TV Stands & Entertainment Centers'
                WHEN CONTAINS_SUBSTR(lower(fullCategory),'patio') THEN 'Patio '||TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
                ELSE TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) END AS bsr_ctgry,
            bestsellers_asin,
            bestsellers_title,
            bestsellers_image,
            bestsellers_rank,
            DATETIME(timestamp(initialTime), "America/Los_Angeles") AS initialTime_PT,
            DATE(DATETIME(timestamp(initialTime), "America/Los_Angeles")) AS initialDate_PT
        FROM rfapi.rf_amz_bsr_hourly
        WHERE bestsellers_rank<=50)
    SELECT
        bsr_ctgry,
        bestsellers_asin,
        bestsellers_rank,
        bestsellers_title,
        bestsellers_image,
        initialTime_PT,
        initialDate_PT,
        ROW_NUMBER() OVER (PARTITION BY bsr_ctgry, initialDate_PT, bestsellers_rank ORDER BY initialTime_PT DESC) AS filter_rnk
    FROM TMP1
)
SELECT
    bsr_ctgry,
    bestsellers_asin,
    bestsellers_rank,
    bestsellers_title,
    bestsellers_image,
    initialTime_PT,
    initialDate_PT
FROM TMP2
WHERE filter_rnk=1
;

-- 2.3) Aggregate 2.2 & 2.3
DROP TABLE IF EXISTS tmp.amz_bsr_shr_rawdata;
CREATE TABLE tmp.amz_bsr_shr_rawdata AS
(SELECT
     bsr_ctgry,
     asin,
     rank,
     initialTime_PT,
     initialDate_PT,
     NULL AS title,
     NULL AS image
 FROM tmp.amz_bsr_shr_rawdata_tmp1)
UNION ALL
(SELECT
     bsr_ctgry,
     bestsellers_asin,
     bestsellers_rank,
     initialTime_PT,
     initialDate_PT,
     bestsellers_title,
     bestsellers_image
 FROM tmp.amz_bsr_shr_rawdata_tmp2)
;

-- 3.1) PDT raw data - select pdt info for BSR asins
DROP TABLE IF EXISTS tmp.amz_bsr_shr_pdt_tmp1;
CREATE TABLE tmp.amz_bsr_shr_pdt_tmp1 AS
WITH TMP1 AS (SELECT asin FROM tmp.amz_bsr_shr_rawdata GROUP BY 1)
SELECT A.asin, initialTime, UPPER(brand) AS brand, title, imageUrl
FROM crwl.amz_pdt_all A
         INNER JOIN TMP1 B ON A.asin=B.asin
WHERE brand IS NOT NULL AND UPPER(brand)<>'NONE' --AND title IS NOT NULL
GROUP BY 1,2,3,4,5
;

-- 3.2) PDT raw data - select most recent pdt brand/title/url info for BSR asins
DROP TABLE IF EXISTS tmp.amz_bsr_shr_pdt_tmp2;
CREATE TABLE tmp.amz_bsr_shr_pdt_tmp2 AS
WITH TMP1 AS (
    SELECT asin, initialTime, brand, title, imageUrl, ROW_NUMBER() OVER (PARTITION BY asin ORDER BY initialTime DESC) as filter
    FROM tmp.amz_bsr_shr_pdt_tmp1 A)
SELECT
    A.asin
    ,COALESCE(B.FAMILY_NAME,UPPER(TRIM(A.brand))) AS brand
    ,UPPER(TRIM(A.brand)) AS brand_raw
    ,A.title
    ,A.imageUrl AS image_url
FROM TMP1 A
         LEFT OUTER JOIN meta.brand_family_mapping B ON UPPER(TRIM(A.brand))=B.BRAND_UPPER
WHERE filter=1
;

DROP TABLE IF EXISTS tmp.amz_asin_brand_fill_family;
CREATE TABLE tmp.amz_asin_brand_fill_family AS
WITH TMP1 AS (
    SELECT * FROM meta.amz_asin_brand_fill
    UNION ALL
    SELECT
        RetailerSku,
        Brand
    FROM stck.atlas_sales_all
    WHERE RetailerSku NOT IN (SELECT asin FROM tmp.amz_bsr_shr_pdt_tmp2 GROUP BY 1)
    group by 1,2
)
SELECT
    A.asin
    ,MAX(COALESCE(B.FAMILY_NAME,UPPER(TRIM(A.brand)))) AS brand
    ,MAX(UPPER(TRIM(A.brand))) AS brand_raw
FROM TMP1 A
         LEFT OUTER JOIN meta.brand_family_mapping B ON UPPER(TRIM(A.brand))=B.BRAND_UPPER
GROUP BY 1
;


-- 4) Map pdt info into BSR raw data -->> final table 1
DROP TABLE IF EXISTS vs.amz_bsr_shr_daily;
CREATE TABLE vs.amz_bsr_shr_daily AS
WITH TMP1 AS (
    SELECT
        A.bsr_ctgry,
        A.asin,
        A.rank,
        A.initialDate_PT as bsr_date,
        TRIM(REPLACE(COALESCE(UPPER(B.brand), UPPER(C.brand), 'No Brand Info'),",","")) AS brand,
        TRIM(REPLACE(COALESCE(UPPER(B.brand_raw), UPPER(C.brand_raw), 'No Brand Info'),",","")) AS brand_raw,
        COALESCE(B.title, A.title) AS title,
        COALESCE(B.image_url, A.image) AS image_url,

        (CASE WHEN bsr_ctgry='Mattresses' THEN '01. Mattresses'
              WHEN bsr_ctgry='Box Springs' THEN '02. Box Springs'
              WHEN bsr_ctgry='Bed Frames' THEN '03. Bed Frames'
              WHEN bsr_ctgry='Beds' THEN '04. Beds'
              WHEN bsr_ctgry='Mattress Toppers' THEN '05. Mattress Toppers'
              WHEN bsr_ctgry='Sofas & Couches' THEN '06. Sofas & Couches'
              WHEN bsr_ctgry='Living Room Chairs' THEN '07. Living Room Chairs'
              WHEN bsr_ctgry='TV Stands & Entertainment Centers' THEN '08. TV Stands & Entertainment Centers'
              WHEN bsr_ctgry='Dining Tables' THEN '09. Dining Tables'
              WHEN bsr_ctgry='Living Room Tables' THEN '10. Living Room Tables'
              WHEN bsr_ctgry='Living Room Table Sets' THEN '11. Living Room Table Sets'
              WHEN bsr_ctgry='Home Office Desks' THEN '12. Home Office Desks'
              WHEN bsr_ctgry='Patio Conversation Sets' THEN '13. Patio Conversation Sets'
              WHEN bsr_ctgry='Adjustable Bases' THEN '14. Adjustable Bases'
              ELSE bsr_ctgry
         END) AS bsr_ctgry_label,

            'https://www.amazon.com/dp/'||A.asin AS pdt_url,

        SUBSTR(CAST(initialDate_PT AS STRING),3,5) AS yr_month,

        CONCAT('Y', SUBSTR(CAST(D.yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(D.yr_wk AS STRING), 5, 2)) AS yr_week,

--                 LPAD(CAST(CASE WHEN EXTRACT(WEEK FROM initialDate_PT)=0 THEN EXTRACT(WEEK FROM initialDate_PT-7)+1 ELSE EXTRACT(WEEK FROM initialDate_PT) END AS STRING),2,'0')  AS yr_week,
        CASE WHEN A.initialDate_PT=BSR_MAX_DT THEN 1 ELSE 0 END AS is_maxdt,
        CASE WHEN A.initialDate_PT<=BSR_MAX_DT AND A.initialDate_PT>=BSR_MAX_DT-3 THEN 1 ELSE 0 END AS is_maxdt_range,


        ROW_NUMBER() OVER (PARTITION BY TRIM(REPLACE(COALESCE(UPPER(B.brand), UPPER(C.brand), 'No Brand Info'),",","")), initialDate_PT, bsr_ctgry,
            (CASE WHEN rank<=10 THEN 1 WHEN rank>10 AND rank<=20 THEN 2 ELSE 3 END)
            ORDER BY rank) as bsr_ord,

        COUNT(A.asin) OVER (PARTITION BY TRIM(REPLACE(COALESCE(UPPER(B.brand), UPPER(C.brand), 'No Brand Info'),",","")), A.initialDate_PT, A.bsr_ctgry) AS brand_prod_num,

        COUNT(DISTINCT A.asin) OVER (PARTITION BY TRIM(REPLACE(COALESCE(UPPER(B.brand), UPPER(C.brand), 'No Brand Info'),",","")), A.initialDate_PT) AS brand_ttl_cnt_num,

        COUNT(DISTINCT A.asin) OVER (PARTITION BY TRIM(REPLACE(COALESCE(UPPER(B.brand), UPPER(C.brand), 'No Brand Info'),",",""))) AS brand_legend_num,

        initialTime_PT

    FROM tmp.amz_bsr_shr_rawdata A
             LEFT OUTER JOIN tmp.amz_bsr_shr_pdt_tmp2 B ON A.asin=B.asin
             LEFT OUTER JOIN tmp.amz_asin_brand_fill_family C ON A.asin=C.asin
             LEFT OUTER JOIN meta.wk_calendar D ON A.initialDate_PT BETWEEN D.start_date AND D.end_date
)
SELECT
    * ,
    CASE WHEN brand='ZINUS' THEN 0
         ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry, bsr_date ORDER BY brand_prod_num DESC) END AS brand_ord,

    CASE WHEN bsr_date=BSR_MAX_DT THEN 'Today(Pacific Time)' ELSE CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) END AS date_str,

    CASE WHEN brand='ZINUS' THEN 0
         WHEN brand='No Brand Info' THEN 99999999
         ELSE DENSE_RANK() OVER (PARTITION BY bsr_date ORDER BY brand_ttl_cnt_num DESC) END AS brand_ttl_cnt_ord,

    CASE WHEN brand='ZINUS' THEN 0
         WHEN brand='No Brand Info' THEN 99999999
         ELSE DENSE_RANK() OVER (ORDER BY brand_legend_num DESC) END AS brand_legend_ord,


--   CASE WHEN brand_raw='ZINUS' THEN 0
--        ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry, bsr_date ORDER BY brand_raw_prod_num DESC) END AS brand_raw_ord,
--
--   CASE WHEN brand_raw='ZINUS' THEN 0
--        WHEN brand_raw='No Brand Info' THEN 99999999
--        ELSE DENSE_RANK() OVER (PARTITION BY bsr_date ORDER BY brand_raw_ttl_cnt_num DESC) END AS brand_raw_ttl_cnt_ord,
--
--    CASE WHEN brand_raw='ZINUS' THEN 0
--         WHEN brand_raw='No Brand Info' THEN 99999999
--         ELSE DENSE_RANK() OVER (ORDER BY brand_raw_legend_num DESC) END AS brand_raw_legend_ord

FROM TMP1
;


DROP TABLE IF EXISTS tmp.amz_bsr_shr_daily_brand_list;
CREATE TABLE tmp.amz_bsr_shr_daily_brand_list AS
WITH TMP2 AS (
    WITH TMP1 AS (
        SELECT
            bsr_ctgry_label,
            brand,
            count(asin) AS cnt
        FROM vs.amz_bsr_shr_daily
        WHERE bsr_date>=BSR_MAX_DT-29
        group by 1,2)
    SELECT
        bsr_ctgry_label,
        brand,
        DENSE_RANK() OVER (PARTITiON BY bsr_ctgry_label order by cnt desc ) as rnk
    FROM TMP1)
SELECT * FROM TMP2 WHERE (rnk<=20 or LOWER(brand) like "%zinus%" OR LOWER(brand) like "%mellow%")
;


DROP TABLE IF EXISTS tmp.amz_bsr_shr_daily_brand_raw_list;
CREATE TABLE tmp.amz_bsr_shr_daily_brand_raw_list AS
WITH TMP2 AS (
    WITH TMP1 AS (
        SELECT
            bsr_ctgry_label,
            brand_raw,
            count(asin) AS cnt
        FROM vs.amz_bsr_shr_daily
        WHERE bsr_date>=BSR_MAX_DT-29
        group by 1,2)
    SELECT
        bsr_ctgry_label,
        brand_raw,
        DENSE_RANK() OVER (PARTITiON BY bsr_ctgry_label order by cnt desc ) as rnk
    FROM TMP1)
SELECT * FROM TMP2 WHERE (rnk<=20 or LOWER(brand_raw) like "%zinus%" OR LOWER(brand_raw) like "%mellow%" OR LOWER(brand_raw) LIKE "%best price mattress%")
;


DROP TABLE IF EXISTS tmp.amz_bsr_shr_daily_brand_raw_oth;
CREATE TABLE tmp.amz_bsr_shr_daily_brand_raw_oth AS
SELECT
    A.bsr_ctgry,
    A.asin,
    A.rank,
    A.bsr_date,
    CASE WHEN A.brand IN ('ZINUS','MELLOW BPM') THEN  A.brand ELSE COALESCE(B.brand,'Others') END AS brand,
    CASE WHEN A.brand_raw IN ('ZINUS','MELLOW','BEST PRICE MATTRESS') THEN A.brand_raw ELSE COALESCE(C.brand_raw,'Others') END AS brand_raw,
    A.brand_raw AS brand_raw_org,
    A.title,
    A.image_url,
    A.bsr_ctgry_label,
    A.pdt_url,
    A.yr_month,
    A.yr_week,
    A.is_maxdt,
    A.is_maxdt_range,
    A.bsr_ord,
    A.brand_prod_num,
    A.initialTime_PT,
    A.brand_ord,
    A.date_str

FROM vs.amz_bsr_shr_daily A
         LEFT OUTER JOIN  tmp.amz_bsr_shr_daily_brand_list B ON A.bsr_ctgry_label=B.bsr_ctgry_label AND A.brand=B.brand
         LEFT OUTER JOIN  tmp.amz_bsr_shr_daily_brand_raw_list C ON A.bsr_ctgry_label=C.bsr_ctgry_label AND A.brand_raw=C.brand_raw
;


-- 5.1) Mark Top 10/20/50 raws for Power BI
DROP TABLE IF EXISTS tmp.amz_bsr_shr_daily_acc_raw;
CREATE TABLE tmp.amz_bsr_shr_daily_acc_raw AS
(SELECT *, 'Top 10' AS bsr_rank_range FROM tmp.amz_bsr_shr_daily_brand_raw_oth WHERE rank<=10)
UNION ALL
(SELECT *, 'Top 20' AS bsr_rank_range FROM tmp.amz_bsr_shr_daily_brand_raw_oth WHERE rank<=20)
UNION ALL
(SELECT *, 'Top 50'AS bsr_rank_range  FROM tmp.amz_bsr_shr_daily_brand_raw_oth WHERE rank<=50)
;

-- 5.2) Calcuate missing date list for cagegory x brand (to calculate share diff in Power BI)
DROP TABLE IF EXISTS tmp.amz_bsr_shr_acc_date;
CREATE TABLE tmp.amz_bsr_shr_acc_date AS
WITH TMP1 AS (
        WITH
            BSR_DATA AS (
                SELECT
                    bsr_rank_range, bsr_ctgry_label, brand, brand_raw
                FROM tmp.amz_bsr_shr_daily_acc_raw
                WHERE brand is not null
                GROUP BY 1,2,3,4),
            DATE_LIST AS (
                SELECT
                    bsr_date
                FROM tmp.amz_bsr_shr_daily_acc_raw
                GROUP BY 1)
        SELECT
            A.bsr_rank_range,
            A.bsr_ctgry_label,
            A.brand,
            A.brand_raw,
            B.bsr_date
        FROM BSR_DATA A
                 CROSS JOIN DATE_LIST B
    ),
    MIN_DATE AS (
        SELECT
            bsr_ctgry_label, bsr_rank_range, min(bsr_date) AS min_bsr_date
        FROM tmp.amz_bsr_shr_daily_acc_raw
        WHERE brand is not null AND brand_raw is not null
        GROUP BY 1,2),
    ORG_DATA AS (
        SELECT
            bsr_ctgry_label, bsr_rank_range, brand, brand_raw, bsr_date
        FROM tmp.amz_bsr_shr_daily_acc_raw
        WHERE brand is not null AND brand_raw is not null
        GROUP BY 1,2,3,4,5)
SELECT
    A.bsr_rank_range,
    A.bsr_ctgry_label,
    A.brand,
    A.brand_raw,
    A.bsr_date
FROM TMP1 A
         LEFT OUTER JOIN MIN_DATE B ON A.bsr_ctgry_label=B.bsr_ctgry_label AND A.bsr_rank_range=B.bsr_rank_range
         LEFT OUTER JOIN ORG_DATA C ON A.bsr_ctgry_label=C.bsr_ctgry_label AND A.bsr_rank_range=C.bsr_rank_range AND A.brand=C.brand AND A.bsr_date=C.bsr_date AND  A.brand_raw=C.brand_raw
WHERE A.bsr_date>=B.min_bsr_date AND C.bsr_date IS NULL
;


-- 5.3) Create fill-in table with missing date dataset 5.2
DROP TABLE IF EXISTS tmp.amz_bsr_shr_acc_fill;
CREATE TABLE tmp.amz_bsr_shr_acc_fill AS
SELECT

    CAST(NULL AS STRING) AS bsr_ctgry,
    CAST(NULL AS STRING) AS asin,
    CAST(NULL AS INT64) AS rank,
    A.bsr_date,
    A.brand,
    A.brand_raw,
    CAST(NULL AS STRING) AS brand_raw_org,
    CAST(NULL AS STRING) AS title,
    CAST(NULL AS STRING) AS image_url,
    A.bsr_ctgry_label,
    CAST(NULL AS STRING) AS pdt_url,

    SUBSTR(CAST(A.bsr_date AS STRING),3,5) AS yr_month,

    CONCAT('Y', SUBSTR(CAST(D.yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(D.yr_wk AS STRING), 5, 2)) AS yr_week,
    CASE WHEN A.bsr_date=BSR_MAX_DT THEN 1 ELSE 0 END AS is_maxdt,
    CASE WHEN A.bsr_date<=BSR_MAX_DT AND A.bsr_date>=BSR_MAX_DT-3 THEN 1 ELSE 0 END AS is_maxdt_range,

    CAST(NULL AS INT64) as bsr_ord,

    CAST(NULL AS INT64) as brand_prod_num,

    CAST(NULL AS DATETIME) AS initialTime_PT,
    CAST(NULL AS INT64) AS brand_ord,
    CASE WHEN A.bsr_date=BSR_MAX_DT THEN 'Today(Pacific Time)' ELSE CAST(FORMAT_DATE('%m/%d/%y',  A.bsr_date) AS STRING) END AS date_str,
    A.bsr_rank_range
FROM tmp.amz_bsr_shr_acc_date A
LEFT OUTER JOIN meta.wk_calendar D
            ON A.bsr_date BETWEEN D.start_date AND D.end_date    
;

-- 5.4) Insert 5.3 fill-in data into 5.1 dataset --> final table 2
DROP TABLE IF EXISTS tmp.amz_bsr_shr_daily_acc_f;
CREATE TABLE tmp.amz_bsr_shr_daily_acc_f AS
WITH TMP2 AS (
    WITH TMP1 AS (
        SELECT * FROM tmp.amz_bsr_shr_daily_acc_raw
        UNION ALL
        SELECT * FROM tmp.amz_bsr_shr_acc_fill
    )
    SELECT
        A.bsr_ctgry,
        A.asin,
        A.rank,
        A.bsr_date,
        A.brand,
        A.brand_raw,
        A.brand_raw_org,
        A.title,
        A.image_url,
        A.bsr_ctgry_label,
        A.pdt_url,
        A.yr_month,
        A.yr_week,
        A.is_maxdt,
        A.is_maxdt_range,

        ROW_NUMBER() OVER (PARTITION BY A.brand, A.bsr_date, A.bsr_ctgry_label,
            (CASE WHEN rank<=10 THEN 1
                  WHEN rank>10 AND rank<=20 THEN 2
                  WHEN rank>20 AND rank<=50 THEN 3
                  ELSE 4 END)
            ORDER BY rank) as bsr_ord,

        COUNT(A.asin) OVER (PARTITION BY A.brand, A.bsr_date, A.bsr_ctgry_label) AS brand_prod_num,

        A.initialTime_PT,
        bsr_rank_range
    FROM TMP1 A)
SELECT
    A.*,

    CASE WHEN brand='ZINUS' THEN 0
         ELSE DENSE_RANK() OVER (PARTITION BY bsr_ctgry_label, bsr_date ORDER BY brand_prod_num DESC) END AS brand_ord,

    CASE WHEN bsr_date=BSR_MAX_DT THEN 'Today(Pacific Time)' ELSE CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) END AS date_str,

    COUNT(asin) OVER (PARTITION BY bsr_date, bsr_ctgry_label, brand, bsr_rank_range) AS asin_cnt_brand_dt,

    CASE WHEN ROW_NUMBER() OVER (PARTITION BY bsr_date, bsr_ctgry_label, brand, bsr_rank_range ORDER BY rank)=1 THEN 1 ELSE 0 END AS asin_cnt_brand_dt_rank,

    COUNT(asin) OVER (PARTITION BY bsr_date, bsr_ctgry_label, bsr_rank_range) AS asin_cnt_ttl_dt,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY bsr_date, bsr_ctgry_label, bsr_rank_range ORDER BY rank)=1 THEN 1 ELSE 0 END AS asin_cnt_ttl_dt_rank

FROM TMP2 A
;

DROP TABLE IF EXISTS tmp.amz_bsr_shr_daily_acc_brand_ord;
CREATE TABLE tmp.amz_bsr_shr_daily_acc_brand_ord AS
WITH TMP1 AS (
    SELECT
        brand,
        COUNT(DISTINCT asin) AS brand_legend_ord
    FROM tmp.amz_bsr_shr_daily_acc_f
    GROUP BY 1)
SELECT
    brand,
    CASE WHEN brand='ZINUS' THEN 0
         WHEN brand='No Brand Info' THEN 9999999
         WHEN brand='Others' THEN 99999999
         ELSE ROW_NUMBER() OVER (ORDER BY brand_legend_ord DESC) END AS brand_legend_ord
FROM TMP1
;

DROP TABLE IF EXISTS vs.amz_bsr_shr_daily_acc;
CREATE TABLE vs.amz_bsr_shr_daily_acc AS
SELECT A.*, B.brand_legend_ord
FROM tmp.amz_bsr_shr_daily_acc_f A
         LEFT OUTER JOIN tmp.amz_bsr_shr_daily_acc_brand_ord B ON A.brand=B.brand
;



-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 6)Competitor Analysis

--- 6.1) Hourly BSR data
---- SELECT HOURLY DATA FOR RECENT 30 DAYS
DROP TABLE IF EXISTS tmp.amz_bsr_shr_rawdata_hourly;
CREATE TABLE tmp.amz_bsr_shr_rawdata_hourly AS
SELECT
    CASE
        WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Dining Room Furniture > Tables' THEN 'Dining Tables'
        WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Living Room Furniture > Chairs' THEN 'Living Room Chairs'
        WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Living Room Furniture > Television Stands & Entertainment Centers' THEN 'TV Stands & Entertainment Centers'
        WHEN CONTAINS_SUBSTR(lower(fullCategory),'patio') THEN 'Patio '||TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
        ELSE TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) END AS bsr_ctgry,

    bestsellers_asin AS asin,
    bestsellers_title AS title,
    bestsellers_image AS image_url,
    bestsellers_rank AS rank,
    bestsellers_price_value AS price,
    bestsellers_rating AS rating,

    DATETIME(timestamp(initialTime), "America/Los_Angeles") AS initialTime_PT,
    DATE(DATETIME(timestamp(initialTime), "America/Los_Angeles")) AS initialDate_PT
FROM rfapi.rf_amz_bsr_hourly
WHERE bestsellers_rank<=50 AND
        DATE(DATETIME(timestamp(initialTime), "America/Los_Angeles")) >= BSR_COMP_START_DT AND
        DATE(DATETIME(timestamp(initialTime), "America/Los_Angeles")) <= BSR_MAX_DT
;

--- 6.2) ADD & FILTER BRAND INFO
DROP TABLE IF EXISTS vs.amz_bsr_shr_hourly;
CREATE TABLE vs.amz_bsr_shr_hourly AS
WITH TMP1 AS (
    SELECT
        A.bsr_ctgry,

        (CASE WHEN A.bsr_ctgry='Mattresses' THEN '01. Mattresses'
              WHEN A.bsr_ctgry='Box Springs' THEN '02. Box Springs'
              WHEN A.bsr_ctgry='Bed Frames' THEN '03. Bed Frames'
              WHEN A.bsr_ctgry='Beds' THEN '04. Beds'
              WHEN A.bsr_ctgry='Mattress Toppers' THEN '05. Mattress Toppers'
              WHEN A.bsr_ctgry='Sofas & Couches' THEN '06. Sofas & Couches'
              WHEN A.bsr_ctgry='Living Room Chairs' THEN '07. Living Room Chairs'
              WHEN A.bsr_ctgry='TV Stands & Entertainment Centers' THEN '08. TV Stands & Entertainment Centers'
              WHEN A.bsr_ctgry='Dining Tables' THEN '09. Dining Tables'
              WHEN A.bsr_ctgry='Living Room Tables' THEN '10. Living Room Tables'
              WHEN A.bsr_ctgry='Living Room Table Sets' THEN '11. Living Room Table Sets'
              WHEN A.bsr_ctgry='Home Office Desks' THEN '12. Home Office Desks'
              WHEN A.bsr_ctgry='Patio Conversation Sets' THEN '13. Patio Conversation Sets'
         END) AS bsr_ctgry_label,

            'https://www.amazon.com/dp/'||A.asin AS pdt_url,

        A.asin,
        A.title,
        TRIM(REPLACE(COALESCE(UPPER(B.brand), UPPER(C.brand), 'No Brand Info'),",","")) AS brand,
        TRIM(REPLACE(COALESCE(UPPER(B.brand_raw), UPPER(C.brand_raw), 'No Brand Info'),",","")) AS brand_raw,
        A.image_url,
        A.rank,
        A.price,
        A.rating,
        A.initialTime_PT,
        A.initialDate_PT AS bsr_date,

        COUNT(DISTINCT A.asin) OVER (PARTITION BY TRIM(REPLACE(COALESCE(UPPER(B.brand), UPPER(C.brand), 'No Brand Info'),",",""))) AS brand_legend_num,

    FROM  tmp.amz_bsr_shr_rawdata_hourly A
              LEFT OUTER JOIN tmp.amz_bsr_shr_pdt_tmp2 B ON A.asin=B.asin
              LEFT OUTER JOIN tmp.amz_asin_brand_fill_family C ON A.asin=C.asin)
SELECT
    A.bsr_ctgry,
    A.bsr_ctgry_label,
    A.pdt_url,
    A.asin,
    A.title,
    A.brand,
    A.brand_raw,
    COALESCE(C.image_url, A.image_url) AS image_url,
    A.rank,
    A.price,
    A.rating,
    A.initialTime_PT,
    A.bsr_date,

    FORMAT_TIME("%R", TIME(initialTime_PT)) as time_str,

    SUBSTR(CAST(A.bsr_date AS STRING),3,5) AS yr_month,

   CONCAT('Y', SUBSTR(CAST(D.yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(D.yr_wk AS STRING), 5, 2))  AS yr_week,

    CASE WHEN A.bsr_date=BSR_MAX_DT THEN 'Today(Pacific Time)' ELSE CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING) END AS date_str,

    CASE WHEN A.brand='ZINUS' THEN 0
         WHEN A.brand='No Brand Info' THEN 99999999
         ELSE DENSE_RANK() OVER (ORDER BY A.brand_legend_num DESC) END AS brand_legend_ord

FROM TMP1 A
         INNER JOIN tmp.amz_bsr_shr_daily_brand_list B ON A.bsr_ctgry_label=B.bsr_ctgry_label AND A.brand=B.brand
         LEFT OUTER JOIN tmp.amz_bsr_shr_pdt_tmp2 C ON A.asin=C.asin
         LEFT OUTER JOIN meta.wk_calendar D ON A.bsr_date BETWEEN D.start_date AND D.end_date
WHERE A.brand NOT IN ('Others', 'No Brand Info')
;


-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 7) Top 100
DROP TABLE IF EXISTS tmp.amz_bsr_shr_top100_tmp1;
CREATE TABLE tmp.amz_bsr_shr_top100_tmp1 AS
WITH TMP2 AS (
    WITH TMP1 AS (
        SELECT
            CASE
                WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Dining Room Furniture > Tables' THEN 'Dining Tables'
                WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Living Room Furniture > Chairs' THEN 'Living Room Chairs'
                WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Living Room Furniture > Television Stands & Entertainment Centers' THEN 'TV Stands & Entertainment Centers'
                WHEN CONTAINS_SUBSTR(lower(fullCategory),'patio') THEN 'Patio '||TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
                ELSE TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) END AS bsr_ctgry,

            bestsellers_asin AS asin,
            bestsellers_title AS title,
            bestsellers_image AS image_url,
            bestsellers_rank AS rank,
            bestsellers_price_value AS price,
            bestsellers_rating AS rating,
            DATETIME(timestamp(initialTime), "America/Los_Angeles") AS initialTime_PT,
            DATE(DATETIME(timestamp(initialTime), "America/Los_Angeles")) AS initialDate_PT
        FROM rfapi.rf_amz_bsr_hourly
    )
    SELECT
        bsr_ctgry,
        asin,
        title,
        image_url,
        rank,
        price,
        rating,
        initialTime_PT,
        initialDate_PT,
        ROW_NUMBER() OVER (PARTITION BY bsr_ctgry, initialDate_PT, rank ORDER BY initialTime_PT DESC) AS filter_rnk
    FROM TMP1
)
SELECT
    bsr_ctgry,
    asin,
    title,
    image_url,
    rank,
    price,
    rating,
    initialTime_PT,
    initialDate_PT AS bsr_date
FROM TMP2
WHERE filter_rnk=1
;


-- 7.1) PDT raw data - select pdt info for BSR asins
DROP TABLE IF EXISTS tmp.amz_bsr_top100_pdt_tmp1;
CREATE TABLE tmp.amz_bsr_top100_pdt_tmp1 AS
WITH TMP1 AS (SELECT asin FROM tmp.amz_bsr_shr_top100_tmp1 GROUP BY 1)
SELECT A.asin, initialTime, UPPER(brand) AS brand, title, imageUrl
FROM crwl.amz_pdt_all A
         INNER JOIN TMP1 B ON A.asin=B.asin
WHERE brand IS NOT NULL AND UPPER(brand)<>'NONE' --AND title IS NOT NULL
GROUP BY 1,2,3,4,5
;

-- 7.2) PDT raw data - select most recent pdt brand/title/url info for BSR asins
DROP TABLE IF EXISTS tmp.amz_bsr_top100_pdt_tmp2;
CREATE TABLE tmp.amz_bsr_top100_pdt_tmp2 AS
WITH TMP1 AS (
    SELECT asin, initialTime, brand, title, imageUrl, ROW_NUMBER() OVER (PARTITION BY asin ORDER BY initialTime DESC) as filter
    FROM tmp.amz_bsr_top100_pdt_tmp1 A)
SELECT
    A.asin
    ,COALESCE(B.FAMILY_NAME,UPPER(TRIM(A.brand))) AS brand
    ,UPPER(TRIM(A.brand)) AS brand_raw
    ,A.title
    ,A.imageUrl AS image_url
FROM TMP1 A
         LEFT OUTER JOIN meta.brand_family_mapping B ON UPPER(TRIM(A.brand))=B.BRAND_UPPER
WHERE filter=1
;


DROP TABLE IF EXISTS tmp.amz_bsr_shr_top100_tmp2
;

CREATE TABLE tmp.amz_bsr_shr_top100_tmp2 AS
SELECT
    A.bsr_ctgry
    , ( CASE
            WHEN A.bsr_ctgry = 'Mattresses'                        THEN '01. Mattresses'
            WHEN A.bsr_ctgry = 'Box Springs'                       THEN '02. Box Springs'
            WHEN A.bsr_ctgry = 'Bed Frames'                        THEN '03. Bed Frames'
            WHEN A.bsr_ctgry = 'Beds'                              THEN '04. Beds'
            WHEN A.bsr_ctgry = 'Mattress Toppers'                  THEN '05. Mattress Toppers'
            WHEN A.bsr_ctgry = 'Sofas & Couches'                   THEN '06. Sofas & Couches'
            WHEN A.bsr_ctgry = 'Living Room Chairs'                THEN '07. Living Room Chairs'
            WHEN A.bsr_ctgry = 'TV Stands & Entertainment Centers' THEN '08. TV Stands & Entertainment Centers'
            WHEN A.bsr_ctgry = 'Dining Tables'                     THEN '09. Dining Tables'
            WHEN A.bsr_ctgry = 'Living Room Tables'                THEN '10. Living Room Tables'
            WHEN A.bsr_ctgry = 'Living Room Table Sets'            THEN '11. Living Room Table Sets'
            WHEN A.bsr_ctgry = 'Home Office Desks'                 THEN '12. Home Office Desks'
            WHEN A.bsr_ctgry = 'Patio Conversation Sets'           THEN '13. Patio Conversation Sets'
        END ) AS bsr_ctgry_label
    , 'https://www.amazon.com/dp/' || A.asin AS pdt_url
    , A.asin
    , A.title
    , CASE
          WHEN UPPER(B.brand) = 'NONE' THEN 'No Brand Info'
          ELSE TRIM(REPLACE(COALESCE(UPPER(B.brand), UPPER(C.brand), 'No Brand Info'), ",", ""))
      END AS brand
    , CASE
          WHEN UPPER(B.brand_raw) = 'NONE' THEN 'No Brand Info'
          ELSE TRIM(REPLACE(COALESCE(UPPER(B.brand_raw), UPPER(C.brand_raw), 'No Brand Info'), ",", ""))
      END AS brand_raw
    , COALESCE(B.image_url, A.image_url) AS image_url
    , A.rank
    , A.price
    , A.rating
    , A.initialTime_PT
    , A.bsr_date
    , FORMAT_TIME("%R", TIME(initialTime_PT)) AS time_str
    , SUBSTR(CAST(A.bsr_date AS STRING), 3, 5) AS yr_month
    , CONCAT('Y', SUBSTR(CAST(D.yr_wk AS STRING), 3, 2), ' W', SUBSTR(CAST(D.yr_wk AS STRING), 5, 2)) AS yr_week

    , CASE
          WHEN A.bsr_date = BSR_MAX_DT THEN 'Today(Pacific Time)'
          ELSE CAST(FORMAT_DATE('%m/%d/%y', bsr_date) AS STRING)
      END AS date_str


FROM
    tmp.amz_bsr_shr_top100_tmp1 A
        LEFT OUTER JOIN tmp.amz_bsr_top100_pdt_tmp2 B
            ON A.asin = B.asin
        LEFT OUTER JOIN tmp.amz_asin_brand_fill_family C
            ON A.asin = C.asin
        LEFT OUTER JOIN meta.wk_calendar D
            ON A.bsr_date BETWEEN D.start_date AND D.end_date
-- where D.yr_wk = 202401
;

CREATE OR REPLACE TABLE tmp.brand_mapping_upper AS
SELECT
    UPPER(brand) AS  brand,
    UPPER(brand_adj) AS brand_adj
FROM meta.brand_mapping
GROUP BY 1,2
;

DROP TABLE IF EXISTS vs.amz_bsr_shr_top100;
CREATE TABLE vs.amz_bsr_shr_top100 AS
WITH TMP1 AS (
    SELECT
        A.*,
        COALESCE(B.brand_adj,A.brand) AS brand_adj,
        COUNT(distinct asin) OVER (PARTITION BY COALESCE(B.brand_adj,A.brand)) AS brand_asin_cnt
    FROM tmp.amz_bsr_shr_top100_tmp2 A
             LEFT OUTER JOIN tmp.brand_mapping_upper B ON A.brand=B.brand)
SELECT
    A.*,

    DENSE_RANK() OVER (ORDER BY bsr_date DESC) AS bsr_date_ord,

    DENSE_RANK() OVER (ORDER BY yr_month DESC) AS yr_month_ord,

    DENSE_RANK() OVER (ORDER BY yr_week DESC) AS yr_week_ord,

    CASE WHEN brand_adj='ZINUS' THEN 0
         WHEN brand_adj='No Brand Info' THEN 99999999
         ELSE DENSE_RANK() OVER (ORDER BY brand_asin_cnt DESC) END AS brand_legend_num
FROM TMP1 A
;




------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
--- ASIN LIST: for TODAY'S BSR Top50 SKUs
DROP TABLE IF EXISTS tmp.bsr_asp_pdt_list;
CREATE TABLE tmp.bsr_asp_pdt_list AS
SELECT
    bsr_ctgry,
    asin,
    rank,
    bsr_date,
    brand,
    brand_raw,
    title,
    image_url,
    bsr_ctgry_label,
    pdt_url
FROM vs.amz_bsr_shr_daily A
WHERE A.bsr_date=BSR_MAX_DT
;


-- DAILY BSR RANK
/*
DROP TABLE IF EXISTS tmp.bsr_asp_rank_raw;
CREATE TABLE tmp.bsr_asp_rank_raw AS
SELECT
  bsr_ctgry,
  asin,
  rank,
  bsr_date
FROM vs.amz_bsr_shr_daily A
;
*/

/*
DROP TABLE IF EXISTS tmp.bsr_asp_rank_raw_tmp1;
CREATE TABLE tmp.bsr_asp_rank_raw_tmp1 AS
WITH TMP2 AS (
    WITH TMP1 AS (
      SELECT
        CASE WHEN CONTAINS_SUBSTR(lower(fullCategory),'patio') THEN 'Patio '||TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
            ELSE TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) END AS bsr_ctgry,
        asin,
        rank,
        DATETIME(timestamp(initialTime), "America/Los_Angeles") AS initialTime_PT,
        DATE(DATETIME(timestamp(initialTime), "America/Los_Angeles")) AS initialDate_PT
      FROM crwl.amz_bsr_all_rf
      WHERE SUBSTR(initialTime,1,10)>='2021-01-01' AND SUBSTR(initialTime,1,10)<='2022-07-31' AND rank<=100)
    SELECT
     bsr_ctgry,
     asin,
     rank,
     initialTime_PT,
     initialDate_PT,
     ROW_NUMBER() OVER (PARTITION BY bsr_ctgry, initialDate_PT, rank ORDER BY initialTime_PT DESC) AS filter_rnk
    FROM TMP1
    )
SELECT
  bsr_ctgry,
  asin,
  rank,
  initialTime_PT,
  initialDate_PT
FROM TMP2
WHERE filter_rnk=1
;
*/

-- 2.2) Hourly BSR data
DROP TABLE IF EXISTS tmp.bsr_asp_rank_raw_tmp2;
CREATE TABLE tmp.bsr_asp_rank_raw_tmp2 AS
WITH TMP2 AS (
    WITH TMP1 AS (
        SELECT
            CASE
                WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Dining Room Furniture > Tables' THEN 'Dining Tables'
                WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Living Room Furniture > Chairs' THEN 'Living Room Chairs'
                WHEN fullCategory='Any Department > Home & Kitchen > Furniture > Living Room Furniture > Television Stands & Entertainment Centers' THEN 'TV Stands & Entertainment Centers'
                WHEN CONTAINS_SUBSTR(lower(fullCategory),'patio') THEN 'Patio '||TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$'))
                ELSE TRIM(REGEXP_SUBSTR(fullCategory, '[^>]*$')) END AS bsr_ctgry,

            bestsellers_asin,
            bestsellers_title,
            bestsellers_image,
            bestsellers_rank,
            bestsellers_price_value,
            DATETIME(timestamp(initialTime), "America/Los_Angeles") AS initialTime_PT,
            DATE(DATETIME(timestamp(initialTime), "America/Los_Angeles")) AS initialDate_PT
        FROM rfapi.rf_amz_bsr_hourly
        WHERE bestsellers_rank<=100)
    SELECT
        bsr_ctgry,
        bestsellers_asin,
        bestsellers_rank,
        bestsellers_title,
        bestsellers_image,
        bestsellers_price_value,
        initialTime_PT,
        initialDate_PT,
        ROW_NUMBER() OVER (PARTITION BY bsr_ctgry, initialDate_PT, bestsellers_rank ORDER BY initialTime_PT DESC) AS filter_rnk
    FROM TMP1
)
SELECT
    bsr_ctgry,
    bestsellers_asin,
    bestsellers_rank,
    bestsellers_title,
    bestsellers_image,
    bestsellers_price_value,
    initialTime_PT,
    initialDate_PT
FROM TMP2
WHERE filter_rnk=1
;

-- 2.3) Aggregate 2.2 & 2.3
DROP TABLE IF EXISTS tmp.bsr_asp_rank_raw_tmp3;
CREATE TABLE tmp.bsr_asp_rank_raw_tmp3 AS
WITH TMP1 AS (
    (SELECT
         bsr_ctgry,
         asin,
         rank,
         DATE(initialDate_PT) AS bsr_date
     FROM tmp.bsr_asp_rank_raw_tmp1)
    UNION ALL
    (SELECT
         bsr_ctgry,
         bestsellers_asin AS asin,
         bestsellers_rank AS rank,
         DATE(initialDate_PT) AS bsr_date
     FROM tmp.bsr_asp_rank_raw_tmp2))
SELECT
    bsr_ctgry,
    asin,
    bsr_date,
    MIN(rank) AS rank
FROM TMP1
GROUP BY 1,2,3
;


--------------------------------------------------------------------------------------------------
-- LIST PRICE
DROP TABLE IF EXISTS tmp.bsr_asp_list_price_raw_tmp0;
CREATE TABLE tmp.bsr_asp_list_price_raw_tmp0 AS
WITH
    TMP1 AS (
        SELECT asin FROM tmp.bsr_asp_pdt_list GROUP BY 1
    )
SELECT
    CAST('2021-01-01T00:00:01' AS DATETIME) AS LISTPRICE_time,
    NULL AS LISTPRICE,
    asin
FROM TMP1
;


DROP TABLE IF EXISTS tmp.bsr_asp_list_price_raw_tmp1;
CREATE TABLE tmp.bsr_asp_list_price_raw_tmp1 AS
WITH
    TMP1 AS (
        SELECT
            LISTPRICE_time,
            LISTPRICE,
            asin
        FROM keepa.list_price_all_bsr_fill
        UNION ALL
        SELECT
            LISTPRICE_time,
            LISTPRICE,
            asin
        FROM keepa.list_price_all_bsr
        UNION ALL
        SELECT
            LISTPRICE_time,
            LISTPRICE,
            asin
        FROM keepa.zinus_amz_list_price
        UNION ALL
        SELECT
            LISTPRICE_time,
            LISTPRICE,
            asin
        FROM tmp.bsr_asp_list_price_raw_tmp0),
    TMP2 AS (
        SELECT asin FROM tmp.bsr_asp_pdt_list GROUP BY 1
    )
SELECT
    A.LISTPRICE_time,
    A.LISTPRICE,
    A.asin
FROM TMP1 A
         INNER JOIN TMP2 B ON A.asin=B.asin
GROUP BY 1,2,3
;

DROP TABLE IF EXISTS tmp.bsr_asp_list_price_raw_tmp2;
CREATE TABLE tmp.bsr_asp_list_price_raw_tmp2 AS
WITH TMP1 AS (
    SELECT
        LISTPRICE_time,
        LISTPRICE,
        asin,
        COUNT(LISTPRICE) OVER (PARTITION BY asin ORDER BY LISTPRICE_time) AS LISTPRICE_grp
    FROM tmp.bsr_asp_list_price_raw_tmp1)
SELECT
    LISTPRICE_time,
    FIRST_VALUE(LISTPRICE) OVER (PARTITION BY asin, LISTPRICE_grp ORDER BY LISTPRICE_time) AS LISTPRICE,
    asin
FROM TMP1
;

DROP TABLE IF EXISTS tmp.bsr_asp_list_price_raw_tmp3;
CREATE TABLE tmp.bsr_asp_list_price_raw_tmp3 AS
WITH TMP1 AS (
    SELECT
        DATE(LISTPRICE_time) AS bsr_date,
        LISTPRICE,
        asin,
        ROW_NUMBER() OVER (PARTITION BY asin, CAST(LISTPRICE_time AS DATE) ORDER BY LISTPRICE_time DESC) AS dt_filter
    FROM tmp.bsr_asp_list_price_raw_tmp2)
SELECT
    bsr_date,
    LISTPRICE,
    asin
FROM TMP1 WHERE dt_filter=1
;


--------------------------------------------------------------------------------------------------
-- BUYBOX PRICE
DROP TABLE IF EXISTS tmp.bsr_asp_bb_price_raw_tmp1;
CREATE TABLE tmp.bsr_asp_bb_price_raw_tmp1 AS
WITH
    TMP1 AS (
        SELECT
            BUY_BOX_SHIPPING_time,
            BUY_BOX_SHIPPING,
            asin
        FROM keepa.buybox_ship_price_all_bsr_fill
        UNION ALL
        SELECT
            BUY_BOX_SHIPPING_time,
            BUY_BOX_SHIPPING,
            asin
        FROM keepa.buybox_ship_price_all_bsr
        UNION ALL
        SELECT
            BUY_BOX_SHIPPING_time,
            BUY_BOX_SHIPPING,
            asin
        FROM keepa.zinus_amz_bb_ship_price
        UNION ALL
        SELECT
            LISTPRICE_time AS BUY_BOX_SHIPPING_time,
            NULL AS BUY_BOX_SHIPPING,
            asin
        FROM tmp.bsr_asp_list_price_raw_tmp0),
    TMP2 AS (
        SELECT asin FROM tmp.bsr_asp_pdt_list GROUP BY 1
    )
SELECT
    A.BUY_BOX_SHIPPING_time,
    A.BUY_BOX_SHIPPING,
    A.asin
FROM TMP1 A
         INNER JOIN TMP2 B ON A.asin=B.asin
GROUP BY 1,2,3
;

DROP TABLE IF EXISTS tmp.bsr_asp_bb_price_raw_tmp2;
CREATE TABLE tmp.bsr_asp_bb_price_raw_tmp2 AS
WITH TMP1 AS (
    SELECT
        BUY_BOX_SHIPPING_time,
        BUY_BOX_SHIPPING,
        asin,
        COUNT(BUY_BOX_SHIPPING) OVER (PARTITION BY asin ORDER BY BUY_BOX_SHIPPING_time) AS BB_grp
    FROM tmp.bsr_asp_bb_price_raw_tmp1)
SELECT
    BUY_BOX_SHIPPING_time,
    FIRST_VALUE(BUY_BOX_SHIPPING) OVER (PARTITION BY asin, BB_grp ORDER BY BUY_BOX_SHIPPING_time) AS BUY_BOX_SHIPPING,
    asin
FROM TMP1
;

DROP TABLE IF EXISTS tmp.bsr_asp_bb_price_raw_tmp3;
CREATE TABLE tmp.bsr_asp_bb_price_raw_tmp3 AS
WITH TMP1 AS (
    SELECT
        DATE(BUY_BOX_SHIPPING_time) AS bsr_date,
        BUY_BOX_SHIPPING,
        asin,
        ROW_NUMBER() OVER (PARTITION BY asin, CAST(BUY_BOX_SHIPPING_time AS DATE) ORDER BY BUY_BOX_SHIPPING_time DESC) AS dt_filter
    FROM tmp.bsr_asp_bb_price_raw_tmp2)
SELECT
    bsr_date,
    BUY_BOX_SHIPPING,
    asin
FROM TMP1 WHERE dt_filter=1
;



--------------------------------------------------------------------------------------------------
-- mkt spend
DROP TABLE IF EXISTS  tmp.bsr_asp_mkt_spend;
CREATE TABLE tmp.bsr_asp_mkt_spend AS
WITH TMP1 AS (
    SELECT
        Date,
        Product_ID,
        Cost
    FROM skai.daily_ad_spend_15days
    WHERE Product_ID is not null
    union all
    SELECT
        date,
        item,
        spend
    FROM mkt_ti.ti_raw_amz_2022
    where date<(select min(Date) from skai.daily_ad_spend_15days) AND item IS NOT NULL
)
SELECT
    Date AS date,
    Product_ID AS asin,
    sum(Cost) AS ad_spend
FROM TMP1
GROUP BY 1,2
;



-----
DROP TABLE IF EXISTS tmp.bsr_asp_pdt_date;
CREATE TABLE tmp.bsr_asp_pdt_date AS
WITH DATE_LIST AS (
    SELECT *
    FROM UNNEST(GENERATE_DATE_ARRAY('2022-01-01',BSR_MAX_DT)) AS bsr_date
)
SELECT
    A.bsr_ctgry
    ,A.asin
    ,A.brand
    ,A.brand_raw
    ,A.title
    ,A.image_url
    ,A.bsr_ctgry_label
    ,A.pdt_url
    ,A.rank
    ,B.bsr_date
FROM tmp.bsr_asp_pdt_list A
         CROSS JOIN DATE_LIST B
;


DROP TABLE IF EXISTS tmp.bsr_asp_agg_tmp1;
CREATE TABLE tmp.bsr_asp_agg_tmp1 AS
SELECT
    A.bsr_ctgry
    ,A.asin
    ,A.brand
    ,A.brand_raw
    ,A.title
    ,A.image_url
    ,A.bsr_ctgry_label
    ,A.pdt_url
    ,A.bsr_date

    ,COALESCE(B.sku,C.SKU) AS zinus_sku
    ,COALESCE(B.collection,C.Collection) AS zinus_collection

    ,B.target_bsr_rnk
    ,C.SRP AS zinus_srp
    ,CASE WHEN A.bsr_date=BSR_MAX_DT THEN A.rank ELSE D.rank END AS rank
    ,E.LISTPRICE
    ,F.BUY_BOX_SHIPPING
    ,G.ad_spend

FROM tmp.bsr_asp_pdt_date  A
         LEFT OUTER JOIN meta.bsr_target_rnk B ON A.asin=B.asin
         LEFT OUTER JOIN meta.list_price_srp C ON A.asin=c.asin
         LEFT OUTER JOIN tmp.bsr_asp_rank_raw_tmp3 D ON A.asin=D.asin AND A.bsr_ctgry=D.bsr_ctgry AND A.bsr_date=D.bsr_date
         LEFT OUTER JOIN tmp.bsr_asp_list_price_raw_tmp3 E ON A.asin=E.asin AND A.bsr_date=E.bsr_date
         LEFT OUTER JOIN tmp.bsr_asp_bb_price_raw_tmp3 F ON A.asin=F.asin AND A.bsr_date=F.bsr_date
         LEFT OUTER JOIN tmp.bsr_asp_mkt_spend G ON A.asin=G.asin AND A.bsr_date=G.date
;



DROP TABLE IF EXISTS tmp.bsr_asp_agg_tmp2;
CREATE TABLE tmp.bsr_asp_agg_tmp2 AS
WITH TMP1 AS (
    SELECT
        bsr_ctgry
        ,asin
        ,brand
        ,brand_raw
        ,title
        ,image_url
        ,bsr_ctgry_label
        ,pdt_url
        ,bsr_date

        ,zinus_sku
        ,zinus_collection

        ,target_bsr_rnk
        ,zinus_srp
        ,rank
        ,LISTPRICE
        ,BUY_BOX_SHIPPING
        ,ad_spend

        ,COUNT(LISTPRICE) OVER (PARTITION BY asin ORDER BY bsr_date) AS LISTPRICE_grp
        ,COUNT(BUY_BOX_SHIPPING) OVER (PARTITION BY asin ORDER BY bsr_date) AS BB_grp

    FROM  tmp.bsr_asp_agg_tmp1
)
SELECT
    bsr_ctgry
    ,asin
    ,brand
    ,brand_raw
    ,title
    ,image_url
    ,bsr_ctgry_label
    ,pdt_url
    ,bsr_date

    ,zinus_sku
    ,zinus_collection

    ,target_bsr_rnk
    ,zinus_srp
    ,rank
    ,ad_spend

    ,FIRST_VALUE(LISTPRICE) OVER (PARTITION BY asin, LISTPRICE_grp ORDER BY bsr_date) AS list_price
    ,FIRST_VALUE(BUY_BOX_SHIPPING) OVER (PARTITION BY asin, BB_grp ORDER BY bsr_date) AS bb_price

FROM TMP1
;


DROP TABLE IF EXISTS vs_pb.bsr_asp_agg;
CREATE TABLE vs_pb.bsr_asp_agg AS
WITH TMP1 AS (
    SELECT
        bestsellers_asin
        ,initialDate_PT
        ,bsr_ctgry
        ,MIN(bestsellers_price_value) AS bestsellers_price_value
    FROM tmp.bsr_asp_rank_raw_tmp2
    GROUP BY 1,2,3
)
SELECT
    A.bsr_ctgry
    ,A.asin
    ,A.brand
    ,A.brand_raw
    ,A.title
    ,A.image_url
    ,A.bsr_ctgry_label
    ,A.pdt_url
    ,A.bsr_date

    ,A.zinus_sku
    ,A.zinus_collection

    ,A.target_bsr_rnk
    ,A.zinus_srp
    ,A.rank
    ,A.ad_spend
    ,A.list_price
    ,COALESCE(B.bestsellers_price_value, A.bb_price) AS bb_price
    ,CASE WHEN A.bsr_date=BSR_MAX_DT THEN 1 ELSE 0 END AS max_date_ind
    ,CASE WHEN A.bsr_date>=DATE_SUB(BSR_MAX_DT, INTERVAL 7 DAY) THEN 1 ELSE 0 END AS last7days_ind
FROM tmp.bsr_asp_agg_tmp2 A
         LEFT OUTER JOIN TMP1 B ON A.asin=B.bestsellers_asin AND A.bsr_date=B.initialDate_PT AND A.bsr_ctgry=B.bsr_ctgry
;



DROP TABLE IF EXISTS vs_pb.bsr_asp_calendar;
CREATE TABLE vs_pb.bsr_asp_calendar AS
SELECT bsr_date
FROM vs_pb.bsr_asp_agg
GROUP BY 1
;

DROP TABLE IF EXISTS vs_pb.bsr_asp_calendar_2;
CREATE TABLE vs_pb.bsr_asp_calendar_2 AS
SELECT bsr_date
FROM vs_pb.bsr_asp_agg
GROUP BY 1
;

-----------------------
-- covered products :  vs.amz_bsr_shr_daily
-- meta.bsr_target_rnk + top 50 competitors
-- meta.list_price_srp



-- End of Document ====================================================================================================================================
-------------------------------------------------------------------------------------------------------------------------------------------------------
