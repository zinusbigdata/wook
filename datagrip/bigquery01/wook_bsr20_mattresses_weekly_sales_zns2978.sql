/* -------------------------------------------------------------------------------------------------
   File purpose:
     Mattress BSR Top 20 ASIN을 주 단위로 선정하고,
     해당 ASIN의 본품 + variant ASIN 판매 실적을 주 단위로 집계한다.

   Main output:
     asin, collection, yr_wk 기준
       - BSR Top20 진입일수
       - 평균/최고/최저 rank
       - rank 가중 점수
       - 주간 weighted rank
       - variant 포함 판매 수량/매출/평균 가격

   Important assumptions:
     1. tmp.rf_amz_bsr_all_mattress_top20_ranked는 yr_wk + asin 기준 유일하다.
     2. dw.rf_amz_pdt_zns_comp_daily는 asin + crawlTime_utc 기준 유일하거나,
        같은 조합 내 variant_asins_flat이 동일하다고 가정한다.
     3. variant_expanded 단계에서 asin + variant_asin + yr_wk를 유일 grain으로 사용한다.
     4. ISO week 기준은 FORMAT_DATE('%G-%V', date)로 통일한다.
------------------------------------------------------------------------------------------------- */


/* -------------------------------------------------------------------------------------------------
   1. Mattress 카테고리 주간 Top 20 ASIN 산정
   -------------------------------------------------------------------------------------------------
   목적:
     - 일별 BSR 데이터에서 Mattress 카테고리의 Top 20 진입 ASIN을 주 단위로 평가한다.
     - 단순 평균 순위가 아니라, Top 20 안에서의 순위 가중 점수까지 반영한다.

   기준:
     - rank 1위 = 20점, rank 20위 = 1점
     - 주간 점수(rank_weight_score)가 높은 순으로 주간 Top 20 ASIN 선정
     - 동점 또는 유사 점수일 경우:
         1) Top 20 진입일수 많은 순
         2) 평균 rank 낮은 순
         3) 최고 rank 낮은 순
------------------------------------------------------------------------------------------------- */

CREATE OR REPLACE TABLE wook.tmp_rf_amz_bsr_all_mattress_top20_ranked AS
WITH daily AS (
        /* ---------------------------------------------------------------------------------------------
           일별 ASIN별 최고 순위 산정

           - 원천 데이터가 일 단위보다 더 자주 쌓일 수 있으므로, 같은 일자/ASIN 안에서는 MIN(rank)를 사용한다.
           - rank <= 20 조건으로 Top 20 안에 진입한 기록만 사용한다.
           - yr_wk는 ISO week 기준('%G-%V')으로 생성한다.
             예: 2026-27
        --------------------------------------------------------------------------------------------- */
        SELECT
            DATE(initialTime) AS dt,
            FORMAT_DATE('%G-%V', DATE(initialTime)) AS yr_wk,
            asin,
            MIN(rank) AS daily_best_rank
        FROM
            rfapi.rf_amz_bsr_all
        WHERE
            fullCategory = 'Any Department > Home & Kitchen > Furniture > Bedroom Furniture > Mattresses & Box Springs > Mattresses'
            AND rank <= 20
            AND initialTime >= '2026-01-01'
        GROUP BY
            1, 2, 3
    ),
    weekly AS (
        /* ---------------------------------------------------------------------------------------------
           주간 ASIN별 Top 20 성과 집계

           top20_days:
             - 해당 주에 Top 20 안에 들어온 일수

           avg_rank:
             - Top 20에 들어온 날짜 기준 평균 순위

           rank_weight_score:
             - 순위 가중 점수
             - 1위는 20점, 20위는 1점
             - SUM(21 - daily_best_rank)

           avg_rank_weight_score:
             - Top 20 진입일 기준 평균 가중 점수
        --------------------------------------------------------------------------------------------- */
        SELECT
            yr_wk
            , asin

            , COUNT(*) AS top20_days

            , AVG(daily_best_rank) AS avg_rank
            , MIN(daily_best_rank) AS best_rank
            , MAX(daily_best_rank) AS worst_rank

            , SUM(21 - daily_best_rank) AS rank_weight_score
            , AVG(21 - daily_best_rank) AS avg_rank_weight_score
        FROM
            daily
        GROUP BY
            1, 2
    ),
    ranked AS (
        /* ---------------------------------------------------------------------------------------------
           주차별 ASIN 랭킹 부여

           weekly_weight_rank:
             - 주차별 최종 랭킹
             - rank_weight_score를 최우선으로 사용
             - 이후 top20_days, avg_rank, best_rank 순으로 tie-break
        --------------------------------------------------------------------------------------------- */
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY yr_wk
                ORDER BY
                    /*
                     1. 가중치 rank (1위 20점, 20위 1점) 가 가장 높고
                     2. top 20 위에 진입한 횟수가 높은 순서
                     3. 평균 랭크가 가장 낮은 순서
                     4. 최고 랭크가 가장 낮은 순서로 정렬
                     */
                    rank_weight_score DESC,
                    top20_days DESC,
                    avg_rank,
                    best_rank
                ) AS weekly_weight_rank
        FROM
            weekly
    )
SELECT
    yr_wk
    , asin
    , top20_days
    , avg_rank
    , best_rank
    , worst_rank
    , rank_weight_score
    , avg_rank_weight_score
    , weekly_weight_rank
FROM
    ranked
WHERE
    weekly_weight_rank <= 20
ORDER BY
    yr_wk DESC,
    weekly_weight_rank
;

-- SELECT
--     FORMAT_DATE('%G-%V', DATE(initialTime)) AS yr_wk
--     , asin
--     , AVG(rank) AS avg_rank
--     , SUM(rank/20) AS avg_weight_rank
-- FROM
--     rfapi.rf_amz_bsr_all
-- WHERE
--     fullCategory = 'Any Department > Home & Kitchen > Furniture > Bedroom Furniture > Mattresses & Box Springs > Mattresses'
--     AND rank <= 20
-- GROUP BY
--     1, 2
-- ;

/* -------------------------------------------------------------------------------------------------
   2. 주간 Top 20 ASIN 기준 variant 판매 실적 집계
   -------------------------------------------------------------------------------------------------
   목적:
     - 위에서 선정한 Mattress 주간 Top 20 ASIN을 기준으로,
       해당 ASIN의 variant ASIN까지 포함하여 주간 판매 실적을 집계한다.

   핵심 grain:
     - 중간 variant_expanded 단계에서는 아래 조합을 유일 key처럼 본다.
         asin + variant_asin + yr_wk

   처리 방식:
     1) Top 20에 선정된 본품 ASIN을 상품/variant 수집 테이블과 매칭
     2) variant_asins_flat 콤마 구분 문자열을 UNNEST로 행 분해
     3) 본품 asin도 판매 조회 대상에 포함하기 위해 ARRAY_CONCAT([asin], variant list) 사용
     4) asin + variant_asin + yr_wk 기준으로 중복 제거
     5) stck.atlas_sales_all과 variant_asin = RetailerSku 기준으로 주차 매칭
     6) 최종적으로 본품 asin + yr_wk 기준 판매 실적 집계
------------------------------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE wook.tmp_bsr20_mattresses_weekly_product_sales AS
WITH
    base               AS (
        /* -----------------------------------------------------------------------------------------
           Top 20 ASIN과 상품/variant 수집 테이블 매칭

           - tmp.rf_amz_bsr_all_mattress_top20_ranked:
               yr_wk + asin 기준으로 주간 Top 20 ASIN만 존재한다고 가정한다.

           - dw.rf_amz_pdt_zns_comp_daily:
               ASIN별 수집 시점의 variant_asins_flat 정보를 보유한다.

           - 조인 조건:
               같은 ISO week + 같은 asin

           - QUALIFY:
               a.asin + a.crawlTime_utc 조합이 유일하다는 전제
        ----------------------------------------------------------------------------------------- */
        SELECT DISTINCT
            a.request_asin
            , a.asin
            , FORMAT_DATE('%G-%V', DATE(SUBSTRING(a.crawlTime_utc, 1, 10))) AS yr_wk
            , a.title
            , a.variant_asins_flat
            , b.* EXCEPT (yr_wk, asin)
        FROM
            dw.rf_amz_pdt_zns_comp_daily a
                JOIN wook.tmp_rf_amz_bsr_all_mattress_top20_ranked b
                    ON FORMAT_DATE('%G-%V', DATE(SUBSTRING(a.crawlTime_utc, 1, 10))) = b.yr_wk
                       AND a.asin = b.asin
                       -- ( a.asin = b.asin OR a.request_asin = b.asin )
        -- QUALIFY ROW_NUMBER() OVER (PARTITION BY a.request_asin, a.asin, a.crawlTime_utc) = 1
        QUALIFY
            ROW_NUMBER() OVER (PARTITION BY a.asin, a.crawlTime_utc) = 1
    )
    , variant_expanded AS (
        /* -----------------------------------------------------------------------------------------
           variant_asins_flat 분해 및 본품 asin 포함

           목적:
             - variant_asins_flat은 콤마 구분 문자열이므로 SPLIT + UNNEST로 행 분해한다.
             - 판매 실적 조회 시 variant뿐 아니라 본품 asin도 포함해야 하므로,
               ARRAY_CONCAT([asin], SPLIT(...))을 사용한다.

           예:
             asin = A
             variant_asins_flat = 'B,C,D'
             결과 variant_asin:
               A
               B
               C
               D

           variant_asins_flat이 NULL 또는 빈 문자열인 경우:
             ARRAY_CONCAT([asin], SPLIT('', ',')) 결과에 빈 문자열이 섞일 수 있다.
             NULLIF(TRIM(variant_asin), '')로 빈 문자열을 NULL 처리하고,
             이후 variant_lookup에서 v.variant_asin IS NOT NULL 조건으로 제거한다.

           QUALIFY:
             최종적으로 asin + variant_asin + yr_wk 조합을 유일 key처럼 사용한다.
        ----------------------------------------------------------------------------------------- */
        SELECT
            base.* EXCEPT (variant_asins_flat)
            , NULLIF(TRIM(variant_asin), '') AS variant_asin
        FROM
            base
                LEFT JOIN UNNEST(ARRAY_CONCAT([asin], SPLIT(IFNULL(variant_asins_flat, ''), ','))) AS variant_asin
        QUALIFY ROW_NUMBER() OVER (PARTITION BY base.asin, NULLIF(TRIM(variant_asin), ''), yr_wk) = 1
    )
    , variant_lookup   AS (
        /* -----------------------------------------------------------------------------------------
           variant ASIN 기준 판매 테이블 매칭

           - variant_asin을 stck.atlas_sales_all.RetailerSku와 매칭한다.
           - 주차는 ISO week('%G-%V') 기준으로 맞춘다.
           - Brand는 공백/비출력 문자 제거 후 대문자로 정규화한다.

           주의:
             - stck.atlas_sales_all에 RetailerSku + WeekEnding 기준 여러 row가 있으면
               최종 SUM(UnitsSold), SUM(RetailSales)는 그 row들을 모두 합산한다.
        ----------------------------------------------------------------------------------------- */
        SELECT
            v.*

            , UPPER(REGEXP_REPLACE(TRIM(stck.Brand), r'[^[:print:]]', '')) AS brand
            , stck.WeekEnding
            , stck.RetailSales
            , stck.UnitsSold
            , stck.RetailPrice
        FROM
            variant_expanded v
                LEFT JOIN stck.atlas_sales_all stck
                    ON v.variant_asin = stck.RetailerSku
                        AND v.yr_wk = FORMAT_DATE('%G-%V', DATE(stck.WeekEnding))
        WHERE
            v.variant_asin IS NOT NULL
    )
    , weekly_sales AS (

/* -------------------------------------------------------------------------------------------------
   최종 주간 판매 집계

   grain:
     - 본품 asin + collection + yr_wk + 주간 BSR 지표

   집계값:
     - brand:
         현재는 MAX(brand)로 대표 brand 1개만 사용
         variant별 brand가 다를 수 있으면 STRING_AGG(DISTINCT brand) 검토 가능

     - avg_retail_price:
         RetailPrice 주간 평균

     - weekly_units_sold:
         variant_asin 기준 매칭된 모든 주간 판매 수량 합산

     - weekly_retail_sales:
         variant_asin 기준 매칭된 모든 주간 판매 금액 합산
------------------------------------------------------------------------------------------------- */
        SELECT
            v.asin
            , v.title
            , COALESCE(z.new_collection, '-') AS collection
            , v.yr_wk

            -- , v.v.request_asin
            -- , v.yr_wk
            -- , v.asin
            , v.top20_days
            , v.avg_rank
            , v.best_rank
            , v.worst_rank
            , v.rank_weight_score
            , v.avg_rank_weight_score
            , v.weekly_weight_rank
            -- , v.variant_asin
            -- , v.brand
            -- , v.WeekEnding
            -- , v.RetailSales
            -- , v.UnitsSold
            -- , v.RetailPrice
            , MAX(v.brand) AS brand

            , STRING_AGG(DISTINCT v.variant_asin, ',' ORDER BY v.variant_asin) AS variant_asins
            -- , COUNT(DISTINCT v.variant_asin) AS variant_cnt
            , AVG(v.RetailPrice) AS avg_retail_price
            , SUM(v.UnitsSold) AS weekly_units_sold
            , SUM(v.RetailSales) AS weekly_retail_sales
        FROM
            variant_lookup v
                LEFT JOIN meta.amz_zinus_master_pdt_pi_enriched z
                    ON v.asin = z.asin
        GROUP BY
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    )

/* -------------------------------------------------------------------------------------------------
   최종 결과

   YTD 기준:
     - yr_wk의 ISO year 기준으로 asin별 누적한다.
     - 현재 원천 BSR 필터가 2026-01-01 이후이므로 2026년 이후 주차만 집계된다.
------------------------------------------------------------------------------------------------- */
SELECT
    *
    , SUM(weekly_units_sold) OVER (
        PARTITION BY asin, SUBSTR(yr_wk, 1, 4)
        ORDER BY yr_wk
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS ytd_units_sold
    , SUM(weekly_retail_sales) OVER (
        PARTITION BY asin, SUBSTR(yr_wk, 1, 4)
        ORDER BY yr_wk
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS ytd_retail_sales
FROM
    weekly_sales
WHERE yr_wk <= (select max(yr_wk) from weekly_sales where avg_retail_price IS NOT NULL)
ORDER BY
    yr_wk DESC, weekly_weight_rank
;



-- SELECT
--     request_asin
--     , asin
--     , FORMAT_DATE('%G-%V', DATE(SUBSTRING(crawlTime_utc, 1, 10))) AS yr_wk
--     -- , crawlTime_utc
--     , ARRAY_AGG(ifnull(variant_asins_flat, '')) as sss
-- FROM
--     dw.rf_amz_pdt_zns_comp_daily
-- GROUP BY 1,2,3;
