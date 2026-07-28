/*
 *   ZNS-3048 : 미국 Review, CS Claim, Return 통합 분석
 */

-- =================================================================
-- I. 카테고리 기준 3개 채널 특성 비교
-- =================================================================

--WITH params AS (SELECT DATE '2025-01-01' AS start_date),

-- ① 카테고리 매핑 사전: sku·zinus_sku 양쪽을 키로 사용
WITH mp AS (
  SELECT key_sku, ANY_VALUE(financial_category) AS fin_cat FROM (
    SELECT sku AS key_sku, financial_category
    FROM meta.amz_zinus_master_pdt_pi_enriched
    WHERE sku IS NOT NULL AND financial_category IS NOT NULL
    UNION ALL
    SELECT zinus_sku, financial_category
    FROM meta.amz_zinus_master_pdt_pi_enriched
    WHERE zinus_sku IS NOT NULL AND financial_category IS NOT NULL
  ) GROUP BY key_sku
),

-- ② VOC (date = TIMESTAMP)
voc AS (
  SELECT sku, COUNT(*) AS voc_cnt
  FROM dw.cs_voc_cf
  WHERE channel='AMAZON' AND date >= TIMESTAMP('2025-01-01')
  GROUP BY sku
),

-- ③ 부정리뷰 (customer_material = SKU, date = TIMESTAMP)
rvw AS (
  SELECT customer_material AS sku, COUNT(*) AS rvw_cnt
  FROM dw.amz_rvw_cmpl_pi_all
  WHERE date >= TIMESTAMP('2025-01-01')
  GROUP BY customer_material
),

-- ④ 판매/반품 (date = DATE) + 데이터 가드
ret AS (
  SELECT sku, SUM(customer_returns) AS returns, SUM(ordered_units) AS units
  FROM wook.amz_sales_return
  WHERE date >= '2025-01-01' AND sku IS NOT NULL
    AND ordered_units >= 0 AND customer_returns >= 0
  GROUP BY sku
  HAVING SUM(ordered_units) > 0
     AND SUM(customer_returns) <= SUM(ordered_units)
),

-- ⑤ SKU 단위 결합 (판매 기준 LEFT JOIN)
base AS (
  SELECT mp.fin_cat AS category, r.sku, r.returns, r.units,
         COALESCE(v.voc_cnt,0) AS voc_cnt,
         COALESCE(w.rvw_cnt,0) AS rvw_cnt
  FROM ret r
  LEFT JOIN mp  ON mp.key_sku = r.sku
  LEFT JOIN voc v USING (sku)
  LEFT JOIN rvw w USING (sku)
  WHERE mp.fin_cat IS NOT NULL
),

-- ⑥ 카테고리 집계
agg AS (
  SELECT category,
    COUNT(DISTINCT sku) AS sku_cnt,
    CAST(SUM(units) AS INT64) AS units,
    SUM(voc_cnt) AS voc_cnt,
    ROUND(100 *SUM(returns)/NULLIF(SUM(units),0),1) AS return_rate,
    ROUND(1000*SUM(voc_cnt)/NULLIF(SUM(units),0),1) AS voc_per_1k,
    ROUND(1000*SUM(rvw_cnt)/NULLIF(SUM(units),0),1) AS rvw_per_1k
  FROM base GROUP BY category
)
SELECT * FROM agg;




-- ==== 추가 테이블 버전 ====================================================
SELECT
  category    AS `카테고리`,
  sku_cnt     AS `SKU`,
  FORMAT('%\'d', units) AS `판매수`,
  return_rate AS `반품률`,
  voc_per_1k  AS `VOC_1k`,
  rvw_per_1k  AS `리뷰_1k`,
  CASE
    WHEN voc_cnt = 0                                         THEN '기타'
    WHEN return_rate >= 6.0 AND rvw_per_1k >= 1.5*voc_per_1k  THEN '반품+리뷰형'
    WHEN return_rate >= 6.0                                   THEN '반품형'
    WHEN voc_per_1k  >= 1.5*rvw_per_1k                        THEN 'VOC형'
    WHEN rvw_per_1k  >= 1.5*voc_per_1k                        THEN '리뷰형'
    ELSE '혼재형'
  END AS `대응형태`
FROM agg
ORDER BY CASE WHEN voc_cnt = 0 THEN 1 ELSE 0 END, return_rate DESC;









-- ================================================================
-- [1] 3채널 통합 + 상관분석
-- ================================================================
/*WITH params AS (
  SELECT DATE '2025-01-01' AS start_date,   -- 분석 시작일
         2000               AS min_units    -- 최소 판매량(노이즈 억제)
)*/

-- ① VOC : 고객센터 접수 = '말한 불만(사적)'
WITH voc AS (
  SELECT sku, COUNT(*) AS voc_cnt
  FROM dw.cs_voc_cf
  WHERE channel = 'AMAZON'
    AND date >= TIMESTAMP('2025-01-01')
  GROUP BY sku
)
--SELECT * FROM voc ORDER BY 2 DESC ;
-- ② 부정리뷰 : 공개 리뷰 불만 = '말한 불만(공개)'
--    ※ 이 테이블은 별점 2점 이하 complaint 만 수록(평균 1.27)
, rvw AS (
  SELECT customer_material AS sku, COUNT(*) AS rvw_cnt
  FROM dw.amz_rvw_cmpl_pi_all
  WHERE date >= TIMESTAMP('2025-01-01')
  GROUP BY customer_material
 -- ORDER BY 2 DESC
)
-- ③ 판매/반품 : 빈품률의 분자·분모. 컬렉션 마스터 역할도 겸함
, ret AS (
  SELECT sku,
         ANY_VALUE(collection) AS collection,
         SUM(customer_returns) AS returns,
         SUM(ordered_units)    AS units
  FROM wook.amz_sales_return
  WHERE date >= '2025-01-01'
  GROUP BY sku
)

-- ④ SKU 조인 → 컬렉션 집계 → 3개 지표 산출
--    ※ ret 를 기준(LEFT JOIN)으로 둬야 판매는 있는데 VOC/리뷰가 0인 건도 보존됨
, coll AS (
  SELECT
    r.collection,
    SUM(COALESCE(v.voc_cnt,0)) AS voc_cnt,
    SUM(COALESCE(w.rvw_cnt,0)) AS rvw_cnt,
    SUM(r.returns)             AS returns,
    SUM(r.units)               AS units,
    -- 지표1: 빈품률(%)  = 반품 ÷ 판매
    100  * SUM(r.returns)             / NULLIF(SUM(r.units),0) AS return_rate,
    -- 지표2: 판매 1천개당 VOC   (※ 건수 아닌 '밀도'로 봐야 비교 가능)
    1000 * SUM(COALESCE(v.voc_cnt,0)) / NULLIF(SUM(r.units),0) AS voc_per_1k,
    -- 지표3: 판매 1천개당 부정리뷰
    1000 * SUM(COALESCE(w.rvw_cnt,0)) / NULLIF(SUM(r.units),0) AS rvw_per_1k
  FROM ret r
  LEFT JOIN voc v USING (sku)
  LEFT JOIN rvw w USING (sku)
  GROUP BY r.collection
  HAVING units > 1000
     AND (voc_cnt > 0 OR rvw_cnt > 0)
)
--SELECT * FROM coll;
-- ⑤ 상관분석 (컬렉션 단위 피어슨 상관)
SELECT
  COUNT(*)                                AS n_collections,
  ROUND(CORR(rvw_per_1k, voc_per_1k ),2)  AS corr_rvw_voc,        -- 리뷰↔VOC       : 가장 강함
  ROUND(CORR(voc_per_1k, return_rate),2)  AS corr_voc_retrate,    -- VOC밀도↔빈품률  : 약함
  ROUND(CORR(rvw_per_1k, return_rate),2)  AS corr_rvw_retrate,    -- 리뷰↔빈품률     : 거의 무관
  ROUND(CORR(voc_cnt,    return_rate),2)  AS corr_voccnt_retrate  -- VOC'건수'↔빈품률 : 무관(분모효과)
FROM coll;
--  결과: n=78 / 0.42 / 0.27 / 0.10 / -0.04
--  해석: '말하는 두 채널(리뷰·VOC)'끼리는 붙지만, '행동한 불만(반품)'과는 따로 논다.
--        VOC 를 건수로 쓰면 상관이 사라짐 → 반드시 판매량으로 정규화할 것.


-- ================================================================
-- [2] 컬렉션별 상세 (통합 결과를 그대로 보고 싶을 때)
--     → 위 CTE 에서 마지막 SELECT 만 아래로 교체
-- ================================================================
-- SELECT collection, voc_cnt, rvw_cnt, returns, units,
--        ROUND(return_rate,2) AS return_rate,
--        ROUND(voc_per_1k ,2) AS voc_per_1k,
--        ROUND(rvw_per_1k ,2) AS rvw_per_1k
-- FROM coll
-- ORDER BY return_rate DESC;


-- ================================================================
-- [3] 카테고리 내부 상관 (SKU 단위)
--     "제품군을 통제해도 상관이 올라가는가?" 검증용
--     → 결과 0.18~0.48 : 통제해도 약함 = 지표가 근본적으로 다른 것을 측정
-- ================================================================
WITH voc AS (
  SELECT sku, category, COUNT(*) AS voc_cnt
  FROM dw.cs_voc_cf
  WHERE channel='AMAZON' AND date >= TIMESTAMP('2025-01-01')
  GROUP BY sku, category
),
ret AS (
  SELECT sku, SUM(customer_returns) AS returns, SUM(ordered_units) AS units
  FROM wook.amz_sales_return
  WHERE date >= DATE '2025-01-01'
  GROUP BY sku
),
sku_j AS (
  SELECT v.category, v.sku,
         100  * r.returns / NULLIF(r.units,0) AS return_rate,
         1000 * v.voc_cnt / NULLIF(r.units,0) AS voc_per_1k
  FROM voc v JOIN ret r USING (sku)
  WHERE r.units >= 300          -- SKU 단위는 표본이 작아 최소 판매량 필터 필수
)
SELECT category,
       COUNT(*)                               AS n_sku,
       ROUND(CORR(voc_per_1k, return_rate),2) AS corr_within_category
FROM sku_j
GROUP BY category
HAVING n_sku >= 15
ORDER BY n_sku DESC;


-- ================================================================
-- [4] (응용) 리스크 스코어 + Alert 등급
--     3지표를 백분위 정규화 → 가중합 → 등급 분류
-- ================================================================
WITH params AS (SELECT DATE '2025-01-01' AS start_date, 2000 AS min_units),
voc AS (
  SELECT sku, COUNT(*) AS voc_cnt FROM dw.cs_voc_cf, params
  WHERE channel='AMAZON' AND date >= TIMESTAMP(start_date) GROUP BY sku
),
rvw AS (
  SELECT customer_material AS sku, COUNT(*) AS rvw_cnt FROM dw.amz_rvw_cmpl_pi_all, params
  WHERE date >= TIMESTAMP(start_date) GROUP BY customer_material
),
ret AS (
  SELECT sku, ANY_VALUE(collection) AS collection,
         SUM(customer_returns) AS returns, SUM(ordered_units) AS units
  FROM wook.amz_sales_return, params
  WHERE date >= start_date GROUP BY sku
),
coll AS (
  SELECT r.collection,
         100  * SUM(r.returns)             / NULLIF(SUM(r.units),0) AS return_rate,
         1000 * SUM(COALESCE(v.voc_cnt,0)) / NULLIF(SUM(r.units),0) AS voc_per_1k,
         1000 * SUM(COALESCE(w.rvw_cnt,0)) / NULLIF(SUM(r.units),0) AS rvw_per_1k,
         SUM(r.units) AS units
  FROM ret r LEFT JOIN voc v USING (sku) LEFT JOIN rvw w USING (sku)
  GROUP BY r.collection
  HAVING units > (SELECT min_units FROM params)
),
scored AS (
  SELECT *,
    -- 백분위(0~100) : 지표 스케일이 달라 정규화 필수
    100*PERCENT_RANK() OVER (ORDER BY return_rate) AS pr_ret,
    100*PERCENT_RANK() OVER (ORDER BY voc_per_1k ) AS pr_voc,
    100*PERCENT_RANK() OVER (ORDER BY rvw_per_1k ) AS pr_rvw
  FROM coll
),
final AS (
  SELECT collection, units,
    ROUND(return_rate,2) AS return_rate,
    ROUND(voc_per_1k ,2) AS voc_per_1k,
    ROUND(rvw_per_1k ,2) AS rvw_per_1k,
    ROUND(pr_ret) AS pr_ret, ROUND(pr_voc) AS pr_voc, ROUND(pr_rvw) AS pr_rvw,
    -- 리스크 = 0.4*반품 + 0.3*VOC + 0.3*리뷰  (반품=금전손실이라 최상 가중)
    ROUND(0.4*pr_ret + 0.3*pr_voc + 0.3*pr_rvw, 1) AS risk_score,
    -- 초과채널수 : 75%ile 이상인 지표 개수 (다채널 동시초과 = 진짜 불량 신호)
    (CASE WHEN pr_ret>=75 THEN 1 ELSE 0 END
   + CASE WHEN pr_voc>=75 THEN 1 ELSE 0 END
   + CASE WHEN pr_rvw>=75 THEN 1 ELSE 0 END) AS breadth
  FROM scored
)
SELECT *,
  CASE
    WHEN breadth>=2 AND risk_score>=70 THEN 'Critical'
    WHEN breadth>=2 OR  risk_score>=75
      OR (GREATEST(pr_ret,pr_voc,pr_rvw)>=90 AND breadth>=1) THEN 'Warning'
    WHEN breadth=1 OR risk_score>=50 THEN 'Watch'
    ELSE 'Normal'
  END AS alert_tier,
  -- 단일채널 라우팅 : 가장 튀는 채널로 담당 배정
  CASE GREATEST(pr_ret,pr_voc,pr_rvw)
    WHEN pr_ret THEN '반품(사이즈·기대) → MD/상세페이지'
    WHEN pr_voc THEN 'VOC(부품·물류·조립) → SCM/QA'
    ELSE            '리뷰(사용경험·편안함) → 제품기획/QA'
  END AS routing
FROM final
ORDER BY risk_score DESC;






---- test -----------------------------------------------------------------

WITH
voc AS (SELECT sku, COUNT(*) c FROM dw.cs_voc_cf
        WHERE channel='AMAZON' AND date>=TIMESTAMP('2025-01-01') GROUP BY sku),
rvw AS (SELECT customer_material sku, COUNT(*) c FROM dw.amz_rvw_cmpl_pi_all
        WHERE date>=TIMESTAMP('2025-01-01') GROUP BY customer_material),
ret AS (SELECT sku, ANY_VALUE(collection) collection,
               SUM(customer_returns) returns, SUM(ordered_units) units
        FROM wook.amz_sales_return WHERE date>=DATE '2025-01-01' GROUP BY sku),
joined AS (SELECT r.sku, r.units, v.c voc_c, w.c rvw_c
           FROM ret r LEFT JOIN voc v USING(sku) LEFT JOIN rvw w USING(sku))

SELECT '1. ret 행수' AS check_item, CAST(COUNT(*) AS STRING) AS value,
       'BASE' AS expect FROM ret
UNION ALL SELECT '2. ret sku 유니크', CAST(COUNT(DISTINCT sku) AS STRING),
       '1과 같아야 정상(다르면 NULL sku 존재)' FROM ret
UNION ALL SELECT '3. joined 행수', CAST(COUNT(*) AS STRING),
       '1과 같아야 정상(크면 fan-out=중복조인)' FROM joined
UNION ALL SELECT '4. units 합계 조인전', CAST(CAST(SUM(units) AS INT64) AS STRING),
       'BASE' FROM ret
UNION ALL SELECT '5. units 합계 조인후', CAST(CAST(SUM(units) AS INT64) AS STRING),
       '4와 같아야 정상(줄면 INNER JOIN 누락)' FROM joined
;


SELECT cf1 FROM dw.cs_voc_cf
WHERE channel='AMAZON' AND date>=TIMESTAMP('2025-01-01');

SELECT Complaining_factor_Part_1_right FROM dw.amz_rvw_cmpl_pi_all
WHERE date>=TIMESTAMP('2025-01-01');

SELECT COUNT(DISTINCT sku)
FROM wook.amz_sales_return WHERE date>=DATE '2025-01-01' AND customer_returns > 0;

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




