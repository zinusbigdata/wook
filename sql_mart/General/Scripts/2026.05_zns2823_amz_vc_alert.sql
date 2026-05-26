/*
 * ZNS-2823 : Retail Sales Alert 
 */

-- [01. Amazon VC data monitoring mart] ----------------------------------------

CREATE OR REPLACE TABLE wook.amz_vc_weekly_alert_asin AS

WITH weekly_agg AS (
    SELECT
        DATE_TRUNC(a.date, WEEK(SUNDAY))             AS week_start,
        FORMAT_DATE('%Y-%U', a.date)                 AS yr_wk,
        a.asin,
        ANY_VALUE(b.financial_category)              AS financial_category,
        ANY_VALUE(b.new_collection)                  AS new_collection,
        -- 필요시 master에서 product name 등 추가
        -- ANY_VALUE(b.product_name)                 AS product_name,
        ROUND(SUM(a.shipped_revenue), 0)             AS revenue,
        SUM(a.shipped_units)                         AS units,
        ROUND(
            SAFE_DIVIDE(SUM(a.shipped_revenue), SUM(a.shipped_units))
        , 1)                                         AS asp
    FROM vc.amz_vc_sales_daily_all a
        INNER JOIN meta.amz_zinus_master_pdt_pi_add_new_col b
            ON a.asin = b.asin
    GROUP BY 1, 2, 3
),
with_lags_and_ma AS (
    SELECT
        *,
        -- ========== Lags ==========
        LAG(revenue, 1)  OVER w AS revenue_lw,
        LAG(units,   1)  OVER w AS units_lw,
        LAG(asp,     1)  OVER w AS asp_lw,
        LAG(revenue, 52) OVER w AS revenue_ly,
        LAG(units,   52) OVER w AS units_ly,
        LAG(asp,     52) OVER w AS asp_ly,

        -- ========== Moving Averages (current 주 제외) ==========
        AVG(revenue) OVER w4  AS revenue_ma4,
        AVG(units)   OVER w4  AS units_ma4,
        AVG(asp)     OVER w4  AS asp_ma4,

        AVG(revenue) OVER w13 AS revenue_ma13,
        AVG(units)   OVER w13 AS units_ma13,
        AVG(asp)     OVER w13 AS asp_ma13,

        STDDEV(revenue) OVER w13 AS revenue_std13,
        STDDEV(units)   OVER w13 AS units_std13,

        -- ========== ASIN 활성 기간 추적 ==========
        COUNT(*) OVER (
            PARTITION BY asin ORDER BY week_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS weeks_since_first_sale,
        
        -- log 변환
        LN(NULLIF(units,0)) AS ln_units,
        LN(NULLIF(asp,0))	AS ln_asp

    FROM weekly_agg
    WINDOW
        w   AS (PARTITION BY asin ORDER BY week_start),
        w4  AS (PARTITION BY asin ORDER BY week_start
                ROWS BETWEEN 4  PRECEDING AND 1 PRECEDING),
        w13 AS (PARTITION BY asin ORDER BY week_start
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING)
),
with_ma_yoy AS (
    SELECT
        *,
        LAG(revenue_ma4, 52)  OVER w AS revenue_ma4_ly,
        LAG(units_ma4,   52)  OVER w AS units_ma4_ly,
        LAG(asp_ma4,     52)  OVER w AS asp_ma4_ly,
        LAG(revenue_ma13, 52) OVER w AS revenue_ma13_ly,
        LAG(units_ma13,   52) OVER w AS units_ma13_ly,
        LAG(asp_ma13,     52) OVER w AS asp_ma13_ly
    FROM with_lags_and_ma
    WINDOW w AS (PARTITION BY asin ORDER BY week_start)
),
calc AS (
    SELECT
        yr_wk,
        week_start,
        asin,
        financial_category,
        new_collection,
        weeks_since_first_sale,

        revenue, units, asp,
        ln_units, ln_asp,

        -- WoW 차이
        ROUND(revenue - revenue_lw, 1) AS revenue_wow_diff,
        ROUND(units   - units_lw,   1) AS units_wow_diff,
        ROUND(asp     - asp_lw,     1) AS asp_wow_diff,

        -- YoY 차이
        ROUND(revenue - revenue_ly, 1) AS revenue_yoy_diff,
        ROUND(units   - units_ly,   1) AS units_yoy_diff,
        ROUND(asp     - asp_ly,     1) AS asp_yoy_diff,

        -- WoW %
        ROUND(SAFE_DIVIDE(revenue - revenue_lw, revenue_lw) * 100, 1) AS revenue_wow_pct,
        ROUND(SAFE_DIVIDE(units   - units_lw,   units_lw)   * 100, 1) AS units_wow_pct,
        ROUND(SAFE_DIVIDE(asp     - asp_lw,     asp_lw)     * 100, 1) AS asp_wow_pct,

        -- YoY %
        ROUND(SAFE_DIVIDE(revenue - revenue_ly, revenue_ly) * 100, 1) AS revenue_yoy_pct,
        ROUND(SAFE_DIVIDE(units   - units_ly,   units_ly)   * 100, 1) AS units_yoy_pct,
        ROUND(SAFE_DIVIDE(asp     - asp_ly,     asp_ly)     * 100, 1) AS asp_yoy_pct,

        -- Moving Averages
        ROUND(revenue_ma4,  0) AS revenue_ma4,
        ROUND(units_ma4,    0) AS units_ma4,
        ROUND(asp_ma4,      1) AS asp_ma4,
        ROUND(revenue_ma13, 0) AS revenue_ma13,
        ROUND(units_ma13,   0) AS units_ma13,
        ROUND(asp_ma13,     1) AS asp_ma13,

        -- MA YoY
        ROUND(SAFE_DIVIDE(revenue_ma4 - revenue_ma4_ly, revenue_ma4_ly) * 100, 1) AS revenue_ma4_yoy_pct,
        ROUND(SAFE_DIVIDE(units_ma4   - units_ma4_ly,   units_ma4_ly)   * 100, 1) AS units_ma4_yoy_pct,
        ROUND(SAFE_DIVIDE(asp_ma4     - asp_ma4_ly,     asp_ma4_ly)     * 100, 1) AS asp_ma4_yoy_pct,

        ROUND(SAFE_DIVIDE(revenue_ma13 - revenue_ma13_ly, revenue_ma13_ly) * 100, 1) AS revenue_ma13_yoy_pct,
        ROUND(SAFE_DIVIDE(units_ma13   - units_ma13_ly,   units_ma13_ly)   * 100, 1) AS units_ma13_yoy_pct,
        ROUND(SAFE_DIVIDE(asp_ma13     - asp_ma13_ly,     asp_ma13_ly)     * 100, 1) AS asp_ma13_yoy_pct,

        -- vs MA13 baseline
        ROUND(SAFE_DIVIDE(revenue - revenue_ma13, revenue_ma13) * 100, 1) AS revenue_vs_ma13_pct,
        ROUND(SAFE_DIVIDE(units   - units_ma13,   units_ma13)   * 100, 1) AS units_vs_ma13_pct,
        ROUND(SAFE_DIVIDE(asp     - asp_ma13,     asp_ma13)     * 100, 1) AS asp_vs_ma13_pct,

        -- Momentum
        ROUND(SAFE_DIVIDE(revenue_ma4 - revenue_ma13, revenue_ma13) * 100, 1) AS revenue_momentum_pct,
        ROUND(SAFE_DIVIDE(units_ma4   - units_ma13,   units_ma13)   * 100, 1) AS units_momentum_pct,

        -- Z-score
        ROUND(SAFE_DIVIDE(revenue - revenue_ma13, NULLIF(revenue_std13, 0)), 2) AS revenue_zscore,
        ROUND(SAFE_DIVIDE(units   - units_ma13,   NULLIF(units_std13,   0)), 2) AS units_zscore,

        -- 임계값 판정용 원본 컬럼 유지
        --revenue_ma13 AS _revenue_ma13_raw,
        
        
        -- Price Elasticity : 회귀방식, YoY Delta 방식 
		-- 1) Rolling 13W (장기 smoothed - 현재 추세)
		ROUND(
		    SAFE_DIVIDE(
		        COVAR_POP(ln_units, ln_asp) OVER (
		            PARTITION BY asin
		            ORDER BY week_start
		            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
		        ),
		        NULLIF(VAR_POP(ln_asp) OVER (
		            PARTITION BY asin
		            ORDER BY week_start
		            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
		        ), 0)
		    )
		, 2) AS elasticity_rolling_reg_13w,
		
		-- 2) Rolling 4W (단기 reactive - 최근 동학) 
        ROUND(
            SAFE_DIVIDE(
                COVAR_POP(ln_units, ln_asp) OVER (
                    PARTITION BY asin
                    ORDER BY week_start
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ),
                NULLIF(VAR_POP(ln_asp) OVER (
                    PARTITION BY asin
                    ORDER BY week_start
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ), 0)
            )
        , 2) AS elasticity_rolling_reg_4w,	
        
		ROUND(SAFE_DIVIDE(
		    LN(NULLIF(SAFE_DIVIDE(units_ma13, units_ma13_ly), 0)),
		    LN(NULLIF(SAFE_DIVIDE(asp_ma13,   asp_ma13_ly),   0))
		), 2) AS elasticity_ma13_yoy_log,
		
		ROUND(SAFE_DIVIDE(
		    LN(NULLIF(SAFE_DIVIDE(units_ma4, units_ma4_ly), 0)),
		    LN(NULLIF(SAFE_DIVIDE(asp_ma4,   asp_ma4_ly),   0))
		), 2) AS elasticity_ma4_yoy_log,
		
    FROM with_ma_yoy
)
SELECT * FROM calc
;





-- [2026년 기준 매출 기여 상위 80% asin만 추출] --------------------------------------------

WITH asin_2026 AS (
    SELECT
        asin,
        round(SUM(shipped_revenue),0) AS rev_2026,
        SUM(shipped_units)   AS units_2026
    FROM vc.amz_vc_sales_daily_all
    WHERE EXTRACT(YEAR FROM date) = 2026
    GROUP BY asin
)
, ranked AS (
    SELECT
        asin, 
        rev_2026, units_2026,
        SUM(rev_2026) OVER (ORDER BY rev_2026 DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_rev,
        SUM(rev_2026) OVER () AS total_rev
    FROM asin_2026
)
--SELECT * FROM ranked;
, ranked_calc AS (
	SELECT
	    ROW_NUMBER() OVER (ORDER BY rev_2026 DESC) AS rank,
	    asin, 
	    --financial_category, new_collection,
	    ROUND(rev_2026, 0) AS rev_2026,
	    units_2026,
	    ROUND(rev_2026 / total_rev * 100, 2) AS share_pct,
	    ROUND(cum_rev  / total_rev * 100, 2) AS cum_share_pct
	FROM ranked
	QUALIFY COALESCE(LAG(cum_rev / total_rev) OVER (ORDER BY rev_2026 DESC), 0) < 0.9
)
--SELECT * FROM ranked_calc
, ranked_asins AS (
	SELECT a.* 
	FROM wook.amz_vc_weekly_alert_asin a 
	JOIN ranked_calc b ON a.asin=b.asin 
)
SELECT DISTINCT asin FROM ranked_asins 
;








--------------------------------------------------------------------------------
-- [02-1. Alert 기준: 가격탄력성 분석]--------------------------------------------------------------
--------------------------------------------------------------------------------
WITH base AS (
    SELECT
        *,
        -- ========== 핵심 진단 지표 ==========
        
        -- 1) 비탄력 신호 카운트 (0~4점, 4점일수록 모든 시간대에서 비탄력)
        (CASE WHEN COALESCE(elasticity_rolling_reg_4w, 0)  > -1.0 THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(elasticity_rolling_reg_13w, 0) > -1.0 THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(elasticity_ma4_yoy_log, 0)     > -0.5 THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(elasticity_ma13_yoy_log, 0)    > -0.5 THEN 1 ELSE 0 END
        ) AS inelastic_signal_count,
        
        -- 2) 가격 인하 효과성 비율 (Units YoY / ASP YoY)
        --    > 1   : 정상 탄력 (가격 인하분보다 수량 증가가 큼)
        --    0~1   : 효과 미흡 (가격은 내렸지만 수량 증가 약함)
        --    < 0   : 역효과 (가격↓ + 수량↓ = 수요 곡선 이동)
        ROUND(
            SAFE_DIVIDE(units_ma13_yoy_pct, NULLIF(asp_ma13_yoy_pct, 0))
        , 2) AS price_cut_effectiveness_ratio,
        
        -- 3) Rolling 가속도 (4W - 13W: 양수 = 최근 더 비탄력화)
        ROUND(
            COALESCE(elasticity_rolling_reg_4w, 0) - COALESCE(elasticity_rolling_reg_13w, 0)
        , 2) AS elasticity_acceleration
        
    FROM wook.amz_vc_weekly_alert_asin
    WHERE new_collection = 'GTFM'
      AND revenue > 0
      AND yr_wk >= '2026-01'
)
SELECT
    yr_wk, week_start, asin, weeks_since_first_sale,
    revenue, units, asp,
    
    -- 가격 인하 정도
    asp_vs_ma13_pct      AS asp_vs_ma13,
    asp_ma13_yoy_pct     AS asp_yoy_long,
    asp_ma4_yoy_pct      AS asp_yoy_recent,
    
    -- 수요 반응 (없어야 비탄력 증거)
    units_vs_ma13_pct    AS units_vs_ma13,
    units_ma13_yoy_pct   AS units_yoy_long,
    units_ma4_yoy_pct    AS units_yoy_recent,
    revenue_ma13_yoy_pct AS revenue_yoy_long,
    revenue_momentum_pct,
    
    -- 탄력성 4종 원본
    elasticity_rolling_reg_4w,
    elasticity_rolling_reg_13w,
    elasticity_ma4_yoy_log,
    elasticity_ma13_yoy_log,
    
    -- 파생 지표
    inelastic_signal_count,
    price_cut_effectiveness_ratio,
    elasticity_acceleration,
    
    -- ========== 심각도 분류 ==========
    CASE
        -- 🔴🔴 LEVEL 1 : CRITICAL
        -- 큰 폭 가격 인하 + 큰 폭 수량 감소 + 강한 비탄력
        WHEN asp_ma13_yoy_pct <= -10
             AND units_ma13_yoy_pct <= -15
             AND COALESCE(elasticity_ma13_yoy_log, 0) > -0.5
             AND inelastic_signal_count >= 3
             AND weeks_since_first_sale >= 65
        THEN '1_CRITICAL'

        -- 🔴 LEVEL 2 : SEVERE (수요 곡선 이동)
        -- 가격 인하 + 수량 감소 + YoY 양의 탄력성 (가격↓수량↓의 명백한 신호)
        WHEN asp_ma13_yoy_pct <= -5
             AND units_ma13_yoy_pct <= -5
             AND COALESCE(elasticity_ma13_yoy_log, 0) > 0
             AND inelastic_signal_count >= 2
             AND weeks_since_first_sale >= 65
        THEN '2_SEVERE_DEMAND_SHIFT'

        -- 🟠 LEVEL 3 : MODERATE (가격 인하 후 정체)
        -- 가격 인하 + 수량 정체 + Rolling 비탄력
        WHEN asp_vs_ma13_pct <= -5
             AND units_vs_ma13_pct < 5
             AND COALESCE(elasticity_rolling_reg_13w, 0) > -1.0
             AND COALESCE(elasticity_rolling_reg_4w,  0) > -1.0
             AND weeks_since_first_sale >= 26
        THEN '3_MODERATE_NO_RESPONSE'

        -- 🟡 LEVEL 4 : MILD (단기 신호)
        -- 단기 가격 인하 무반응 조기 감지
        WHEN asp_vs_ma13_pct <= -3
             AND units_vs_ma13_pct < 0
             AND COALESCE(elasticity_rolling_reg_4w, 0) > -0.5
             AND weeks_since_first_sale >= 13
        THEN '4_MILD_EARLY_SIGNAL'

        ELSE NULL  -- 분석 대상 아님
    END AS price_inelastic_severity,
    
    -- ========== 진단 플래그 (왜 가격 인하가 실패했는가) ==========
    
    -- 장기 수요 자체가 붕괴
    (units_ma13_yoy_pct <= -15) AS flag_long_demand_collapse,
    
    -- 최근에도 수요 감소
    (units_ma4_yoy_pct <= -10) AS flag_recent_demand_decline,
    
    -- 이미 가격을 충분히 내렸음 (마진 여력 없음)
    (asp_ma13_yoy_pct <= -10) AS flag_significant_price_cut_done,
    
    -- 수요 곡선이 명백히 이동 (Rolling 탄력 + YoY 양수)
    (COALESCE(elasticity_rolling_reg_13w, 0) < -1.0
      AND COALESCE(elasticity_ma13_yoy_log, 0) > 0) AS flag_demand_curve_shift,
    
    -- 모멘텀 붕괴
    (revenue_momentum_pct <= -15) AS flag_momentum_breakdown,
    
    -- 최근 더 비탄력화 (가속도 양수)
    (elasticity_acceleration >= 2.0) AS flag_increasing_inelasticity,
    
    -- 모든 시간대에서 비탄력 (가장 강한 신호)
    (inelastic_signal_count = 4) AS flag_fully_inelastic
    
FROM base

WHERE 
    -- 분석 대상 필터: 비탄력 ASIN 중 가격 인하가 있었던 경우
    inelastic_signal_count >= 2                        -- 최소 2개 지표에서 비탄력
    AND (
        asp_vs_ma13_pct  <= -3                         -- 단기 가격 인하 OR
        OR asp_ma13_yoy_pct <= -3                      -- 장기 가격 인하 OR
        OR asp_ma4_yoy_pct  <= -3                      -- 최근 가격 인하
    )

ORDER BY 
    price_inelastic_severity NULLS LAST,               -- 심각도 순
    revenue DESC,                                       -- 매출 규모 순
    yr_wk DESC
;














--------------------------------------------------------------------------------
-- [02-2. Alert 기준: 가격탄력성 지표 추가]--------------------------------------------------------------
--------------------------------------------------------------------------------
SELECT
    * EXCEPT (_revenue_ma13_raw),

    -- ========== 파생 탄력성 신호 (진단 보조) ==========

    -- 탄력성 체제 전환: MA4_YoY - MA13_YoY (음수 확대 = 최근 가격 민감도 급증)
    ROUND(
        COALESCE(elasticity_ma4_yoy_log, 0) - COALESCE(elasticity_ma13_yoy_log, 0)
    , 2) AS elasticity_shift,

    -- 3개 신호 일치도
    CASE
        WHEN COALESCE(elasticity_rolling_reg_13w, 0) < -1
             AND COALESCE(elasticity_ma13_yoy_log,    0) < -1
             AND COALESCE(elasticity_ma4_yoy_log,     0) < -1
        THEN 'HIGH_ELASTIC_CONFIRMED'       -- 3개 일치: 가격 인하 효과 있음

        WHEN COALESCE(elasticity_rolling_reg_13w, 0) > -1
             AND COALESCE(elasticity_ma13_yoy_log,    0) > -1
             AND COALESCE(elasticity_ma4_yoy_log,     0) > -1
        THEN 'HIGH_INELASTIC_CONFIRMED'     -- 3개 일치: 가격 인하 효과 없음

        ELSE 'MIXED_SIGNAL'                 -- 불일치: 체제 전환 중, 단독 판단 주의
    END AS elasticity_confidence,

    -- ========== ALERT PRIORITY ==========
    CASE
        -- 🔴🔴 P1 : STRUCTURAL DECLINE
        -- elasticity_ma13_yoy_log 활용: 장기 비탄력(>-1) → 가격 인하로도 수요 미회복, 구조적 붕괴
        -- ※ P1/P2는 최고 심각도이므로 elasticity를 hard filter 대신 보조 조건으로만 추가
        WHEN revenue_ma13_yoy_pct <= -15
             AND revenue_ma4_yoy_pct <= revenue_ma13_yoy_pct + 3
             AND units_ma13_yoy_pct  <= -10
             AND weeks_since_first_sale >= 65
             AND COALESCE(elasticity_ma13_yoy_log, 0) > -1.0   -- 장기 비탄력 → 순수 수요 붕괴
        THEN 'P1_STRUCTURAL_DECLINE'

        -- 🔴🔴 P1b : STRUCTURAL DECLINE (가격 인상 동반형)
        -- 탄력적인데도 매출·수량 장기 하락 → 가격 경쟁력 상실 또는 프리미엄 전략 실패
        WHEN revenue_ma13_yoy_pct <= -15
             AND revenue_ma4_yoy_pct <= revenue_ma13_yoy_pct + 3
             AND units_ma13_yoy_pct  <= -10
             AND weeks_since_first_sale >= 65
             AND COALESCE(elasticity_ma13_yoy_log, 0) <= -1.0  -- 탄력적인데 수량도 하락
        THEN 'P1B_STRUCTURAL_PRICE_ISSUE'

        -- 🔴🔴 P2 : SUDDEN CLIFF (탄력성 무관 — 긴급 이상값)
        WHEN ABS(revenue_zscore) >= 3
             AND revenue_wow_pct    <= -30
             AND weeks_since_first_sale >= 13
        THEN 'P2_SUDDEN_CLIFF'

        -- 🔴 P3 : REAL DEMAND PROBLEM
        -- elasticity_rolling_reg_13w 활용: 현재 비탄력(>-1) → 가격 내렸는데도 수요 무반응
        WHEN units_vs_ma13_pct     <= -15
             AND asp_vs_ma13_pct   <= -5
             AND units_ma4_yoy_pct  < -5
             AND revenue_momentum_pct <= -15
             AND weeks_since_first_sale >= 56
             AND COALESCE(elasticity_rolling_reg_13w, 0) > -1.0  -- 비탄력 확인
        THEN 'P3_REAL_DEMAND_PROBLEM'

        -- 🟠 P4 : MARGIN EROSION
        -- elasticity_rolling_reg_13w 활용: 현재 탄력적(<-1.5) → 가격 인하가 수량을 끌어올린 것 확인
        WHEN revenue_wow_pct       >= -3
             AND asp_vs_ma13_pct   <= -8
             AND units_vs_ma13_pct >= 10
             AND weeks_since_first_sale >= 13
             AND COALESCE(elasticity_rolling_reg_13w, 0) < -1.5  -- 가격 의존 성장 확인
        THEN 'P4_MARGIN_EROSION'

        -- 🟠 P4B : PRICE CUT INEFFECTIVE  (신규)
        -- elasticity_rolling_reg_13w + elasticity_ma4_yoy_log 활용:
        -- 가격 내렸으나 수량 무반응 → 가격 인하 정책 실효성 없음
        WHEN asp_vs_ma13_pct       <= -8
             AND units_vs_ma13_pct  < 10
             AND COALESCE(elasticity_rolling_reg_13w, 0) > -0.5  -- 현재 거의 비탄력
             AND COALESCE(elasticity_ma4_yoy_log,     0) > -0.5  -- 최근 4주도 무반응
             AND weeks_since_first_sale >= 26
        THEN 'P4B_PRICE_CUT_INEFFECTIVE'

        -- 🟡 P5 : WATCH
        -- elasticity_shift 활용: MA4_YoY - MA13_YoY 급감 → 최근 가격 민감도 체제 전환 조기 감지
        WHEN ABS(revenue_zscore)   >= 2.5
             OR revenue_momentum_pct <= -15
             OR revenue_vs_ma13_pct  <= -20
             OR (revenue_ma13_yoy_pct <= -8 AND weeks_since_first_sale >= 65)
             OR (                                                   -- 탄력성 체제 전환 신호
                    COALESCE(elasticity_ma4_yoy_log,  0)
                  - COALESCE(elasticity_ma13_yoy_log, 0) < -1.0    -- 최근 민감도 급증
                    AND weeks_since_first_sale >= 56
                )
        THEN 'P5_WATCH'

        -- 🟢 P6 : POSITIVE
        -- elasticity_ma13_yoy_log 활용: 장기 비탄력(>-0.8) → 가격 올려도 수요 유지(가격 경쟁력 보유)
        WHEN revenue_ma13_yoy_pct  >= 20
             AND revenue_momentum_pct >= 15
             AND units_ma13_yoy_pct >= 15
             AND weeks_since_first_sale >= 65
             AND COALESCE(elasticity_ma13_yoy_log, 0) > -0.8      -- 가격 인상 여지 있음
        THEN 'P6_POSITIVE'

        -- 🆕 NEW LAUNCH
        WHEN weeks_since_first_sale < 13
        THEN 'NEW_LAUNCH'

        ELSE 'P0_NORMAL'
    END AS alert_priority,

    -- ========== 4-Quadrant 진단 ==========
    -- elasticity_rolling_reg_13w 활용: Bought Growth를 가격 의존 여부로 세분화
    CASE
        WHEN units_vs_ma13_pct < 0 AND asp_vs_ma13_pct < 0
            THEN 'Real Demand Problem'

        WHEN units_vs_ma13_pct > 0 AND asp_vs_ma13_pct <= -5
             AND COALESCE(elasticity_rolling_reg_13w, 0) < -1.5
            THEN 'Bought Growth (Price-Driven)'     -- 가격 인하가 수량을 끌어올린 것 확인

        WHEN units_vs_ma13_pct > 0 AND asp_vs_ma13_pct <= -5
             AND COALESCE(elasticity_rolling_reg_13w, 0) >= -1.5
            THEN 'Bought Growth (Factor Unclear)'   -- 가격 외 요인 가능성, 추가 분석 필요

        WHEN units_vs_ma13_pct <= -5 AND asp_vs_ma13_pct > 0
            THEN 'Premiumization'

        WHEN units_vs_ma13_pct > 0 AND asp_vs_ma13_pct > 0
            THEN 'Healthy Growth'

        ELSE 'Stable'
    END AS alert_type

FROM wook.amz_vc_weekly_alert_asin
WHERE new_collection = 'GTFM' AND revenue > 0 AND yr_wk >= '2026-01'
--_revenue_ma13_raw >= 2000
ORDER BY yr_wk DESC, revenue DESC
;










-------------------------------------------------------------------------------------
-- [03. Collection level ] ----------------------------------------------------------
-------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE wook.amz_vc_weekly_alert AS
WITH weekly_agg AS (
    SELECT
        DATE_TRUNC(a.date, WEEK(SUNDAY))             AS week_start,
        FORMAT_DATE('%Y-%U', a.date)                 AS yr_wk,
        b.financial_category,
        b.new_collection,
        ROUND(SUM(a.shipped_revenue), 0)             AS revenue,
        SUM(a.shipped_units)                         AS units,
        ROUND(
            SAFE_DIVIDE(SUM(a.shipped_revenue), SUM(a.shipped_units))
        , 1)                                         AS asp
    FROM vc.amz_vc_sales_daily_all a
        INNER JOIN meta.amz_zinus_master_pdt_pi_add_new_col b
            ON a.asin = b.asin
    GROUP BY 1, 2, 3, 4
),
with_lags_and_ma AS (
    SELECT
        *,
        -- ========== Lags ==========
        LAG(revenue, 1)  OVER w AS revenue_lw,
        LAG(units,   1)  OVER w AS units_lw,
        LAG(asp,     1)  OVER w AS asp_lw,
        LAG(revenue, 52) OVER w AS revenue_ly,
        LAG(units,   52) OVER w AS units_ly,
        LAG(asp,     52) OVER w AS asp_ly,

        -- ========== Moving Averages (current 주 제외) ==========
        AVG(revenue) OVER w4  AS revenue_ma4,
        AVG(units)   OVER w4  AS units_ma4,
        AVG(asp)     OVER w4  AS asp_ma4,

        AVG(revenue) OVER w13 AS revenue_ma13,
        AVG(units)   OVER w13 AS units_ma13,
        AVG(asp)     OVER w13 AS asp_ma13,

        STDDEV(revenue) OVER w13 AS revenue_std13,
        STDDEV(units)   OVER w13 AS units_std13

    FROM weekly_agg
    WINDOW
        w   AS (PARTITION BY financial_category, new_collection
                ORDER BY week_start),
        w4  AS (PARTITION BY financial_category, new_collection
                ORDER BY week_start
                ROWS BETWEEN 4  PRECEDING AND 1 PRECEDING),
        w13 AS (PARTITION BY financial_category, new_collection
                ORDER BY week_start
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING)
),
-- ⭐ NEW : MA 값에 대한 52주 전 LAG (YoY 비교용)
with_ma_yoy AS (
    SELECT
        *,
        -- MA4 의 52주 전 값
        LAG(revenue_ma4, 52) OVER w AS revenue_ma4_ly,
        LAG(units_ma4,   52) OVER w AS units_ma4_ly,
        LAG(asp_ma4,     52) OVER w AS asp_ma4_ly,

        -- MA13 도 같이 (보너스 — 더 부드러운 YoY 비교)
        LAG(revenue_ma13, 52) OVER w AS revenue_ma13_ly,
        LAG(units_ma13,   52) OVER w AS units_ma13_ly,
        LAG(asp_ma13,     52) OVER w AS asp_ma13_ly

    FROM with_lags_and_ma
    WINDOW w AS (
        PARTITION BY financial_category, new_collection
        ORDER BY week_start
    )
)
SELECT
    yr_wk,
    financial_category,
    new_collection,

    revenue,
    units,
    asp,

    -- 기존 WoW / YoY 차이
    ROUND(revenue - revenue_lw, 1) AS revenue_wow_diff,
    ROUND(units   - units_lw,   1) AS units_wow_diff,
    ROUND(asp     - asp_lw,     1) AS asp_wow_diff,
    ROUND(revenue - revenue_ly, 1) AS revenue_yoy_diff,
    ROUND(units   - units_ly,   1) AS units_yoy_diff,
    ROUND(asp     - asp_ly,     1) AS asp_yoy_diff,

    -- 기존 WoW / YoY 변화율
    ROUND(SAFE_DIVIDE(revenue - revenue_lw, revenue_lw) * 100, 1) AS revenue_wow_pct,
    ROUND(SAFE_DIVIDE(units   - units_lw,   units_lw)   * 100, 1) AS units_wow_pct,
    ROUND(SAFE_DIVIDE(asp     - asp_lw,     asp_lw)     * 100, 1) AS asp_wow_pct,
    ROUND(SAFE_DIVIDE(revenue - revenue_ly, revenue_ly) * 100, 1) AS revenue_yoy_pct,
    ROUND(SAFE_DIVIDE(units   - units_ly,   units_ly)   * 100, 1) AS units_yoy_pct,
    ROUND(SAFE_DIVIDE(asp     - asp_ly,     asp_ly)     * 100, 1) AS asp_yoy_pct,

    -- ============================================
    -- Moving Averages
    -- ============================================
    ROUND(revenue_ma4,  0) AS revenue_ma4,
    ROUND(units_ma4,    0) AS units_ma4,
    ROUND(asp_ma4,      1) AS asp_ma4,

    ROUND(revenue_ma13, 0) AS revenue_ma13,
    ROUND(units_ma13,   0) AS units_ma13,
    ROUND(asp_ma13,     1) AS asp_ma13,

    -- ============================================
    -- ⭐ NEW : MA4 YoY (4주 smoothed YoY)
    -- ============================================
    ROUND(revenue_ma4 - revenue_ma4_ly, 1) AS revenue_ma4_yoy_diff,
    ROUND(units_ma4   - units_ma4_ly,   1) AS units_ma4_yoy_diff,
    ROUND(asp_ma4     - asp_ma4_ly,     1) AS asp_ma4_yoy_diff,

    ROUND(SAFE_DIVIDE(revenue_ma4 - revenue_ma4_ly, revenue_ma4_ly) * 100, 1) AS revenue_ma4_yoy_pct,
    ROUND(SAFE_DIVIDE(units_ma4   - units_ma4_ly,   units_ma4_ly)   * 100, 1) AS units_ma4_yoy_pct,
    ROUND(SAFE_DIVIDE(asp_ma4     - asp_ma4_ly,     asp_ma4_ly)     * 100, 1) AS asp_ma4_yoy_pct,

    -- ============================================
    -- ⭐ 보너스 : MA13 YoY (분기 smoothed YoY — 가장 robust)
    -- ============================================
    ROUND(SAFE_DIVIDE(revenue_ma13 - revenue_ma13_ly, revenue_ma13_ly) * 100, 1) AS revenue_ma13_yoy_pct,
    ROUND(SAFE_DIVIDE(units_ma13   - units_ma13_ly,   units_ma13_ly)   * 100, 1) AS units_ma13_yoy_pct,
    ROUND(SAFE_DIVIDE(asp_ma13     - asp_ma13_ly,     asp_ma13_ly)     * 100, 1) AS asp_ma13_yoy_pct,

    -- 현재 vs MA13 (baseline 대비)
    ROUND(SAFE_DIVIDE(revenue - revenue_ma13, revenue_ma13) * 100, 1) AS revenue_vs_ma13_pct,
    ROUND(SAFE_DIVIDE(units   - units_ma13,   units_ma13)   * 100, 1) AS units_vs_ma13_pct,
    ROUND(SAFE_DIVIDE(asp     - asp_ma13,     asp_ma13)     * 100, 1) AS asp_vs_ma13_pct,

    -- Momentum
    ROUND(SAFE_DIVIDE(revenue_ma4 - revenue_ma13, revenue_ma13) * 100, 1) AS revenue_momentum_pct,
    ROUND(SAFE_DIVIDE(units_ma4   - units_ma13,   units_ma13)   * 100, 1) AS units_momentum_pct,

    -- Z-score
    ROUND(SAFE_DIVIDE(revenue - revenue_ma13, revenue_std13), 2) AS revenue_zscore,
    ROUND(SAFE_DIVIDE(units   - units_ma13,   units_std13),   2) AS units_zscore

FROM with_ma_yoy
ORDER BY yr_wk DESC, revenue DESC
;