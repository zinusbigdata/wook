/*
 * Amazon Mattress Price 분석
 * table: wook.amz_zns30_promo_with_comp <-- tmp.amz_zns_promo_with_comp2, 
 * 		dw.amz_bsr_matt_top10_price
 */


select *
from wook.amz_zns30_promo_with_comp
where date >= '2026-01-01'


-- 2026.03.19 : inch, size별 통계

select inch, size,  
	--asin,collection, target_srp,   
	--count(*),
	count(distinct asin) as cnt,
	
	-- zinus
	avg(nullif(buybox_price,0)) as bb_price, 
	avg(nullif(list_price,0)) as list_price,
	avg(nullif(buybox_price,0)) - avg(nullif(list_price,0)) as discount,
	(avg(nullif(buybox_price,0)) - avg(nullif(list_price,0))) / avg(nullif(list_price,0)) as discount_ratio,
	
	-- comp
	avg(nullif(comp_buybox_price,0)) as comp_bb_price,
	avg(nullif(comp_list_price,0)) as comp_list_price,
	avg(nullif(comp_buybox_price,0)) - avg(nullif(comp_list_price,0)) as comp_discount,
	(avg(nullif(comp_buybox_price,0)) - avg(nullif(comp_list_price,0))) / avg(nullif(comp_list_price,0)) as comp_discount_ratio,
	
	-- diff
	avg(nullif(buybox_price,0)) - avg(nullif(comp_buybox_price,0)) as diff_bb_price, 
	avg(nullif(list_price,0)) - avg(nullif(comp_list_price,0)) as diff_list_price, 
	
	(avg(nullif(buybox_price,0)) - avg(nullif(list_price,0))) / avg(nullif(list_price,0))
		- (avg(nullif(comp_buybox_price,0)) - avg(nullif(comp_list_price,0))) / avg(nullif(comp_list_price,0)) 
	as diff_discount_ratio,
		
	avg(nullif(buybox_price,0)) / avg(nullif(comp_buybox_price,0)) as price_index
	
from wook.amz_zns30_promo_with_comp
where date >= '2026-01-01'
group by 1,2 
order by 1,2
;




-- 2026.03.19 : 컬럼 순서 조정

select inch, size, asin, collection, target_srp,   
	--count(*),
	count(distinct asin) as cnt,
	
	-- zinus
	avg(nullif(buybox_price,0)) as bb_price, 
	avg(nullif(list_price,0)) as list_price,
	avg(nullif(buybox_price,0)) - avg(nullif(list_price,0)) as discount,
	(avg(nullif(buybox_price,0)) - avg(nullif(list_price,0))) / avg(nullif(list_price,0)) as discount_ratio,
	
	-- comp
	avg(nullif(comp_buybox_price,0)) as comp_bb_price,
	avg(nullif(comp_list_price,0)) as comp_list_price,
	avg(nullif(comp_buybox_price,0)) - avg(nullif(comp_list_price,0)) as comp_discount,
	(avg(nullif(comp_buybox_price,0)) - avg(nullif(comp_list_price,0))) / avg(nullif(comp_list_price,0)) as comp_discount_ratio,
	
	-- diff
	avg(nullif(buybox_price,0)) - avg(nullif(comp_buybox_price,0)) as diff_bb_price, 
	avg(nullif(list_price,0)) - avg(nullif(comp_list_price,0)) as diff_list_price, 
	
	(avg(nullif(buybox_price,0)) - avg(nullif(list_price,0))) / avg(nullif(list_price,0))
		- (avg(nullif(comp_buybox_price,0)) - avg(nullif(comp_list_price,0))) / avg(nullif(comp_list_price,0)) 
	as diff_discount_ratio,
		
	avg(nullif(buybox_price,0)) / avg(nullif(comp_buybox_price,0)) as price_index
	
from wook.amz_zns30_promo_with_comp
where date >= '2026-01-01'
group by 1,2,3,4,5 
order by 1,2,3,4,5
;


select avg(comp_buybox_price) 
from wook.amz_zns30_promo_with_comp
where date >= '2026-01-01' and asin='B0CKYZHV5D'








-- 1.01 zinus 30개 스큐의 기초 통계 
select inch, size,
	--count(*),
	count(asin) as zinus_cnt,
	count(distinct asin) as zinus_asins,
	count(comp_asin) as comp_cnt,
	count(distinct comp_asin) as comp_asin,
	
	avg(nullif(buybox_price,0)) as bb_price, 
	avg(nullif(comp_buybox_price,0)) as comp_bb_price,
	avg(nullif(buybox_price,0)) - avg(nullif(comp_buybox_price,0)) as diff_bb_price, 
	
	avg(nullif(list_price,0)) as list_price,
	avg(nullif(comp_list_price,0)) as comp_list_price,
	avg(nullif(list_price,0)) - avg(nullif(comp_list_price,0)) as diff_list_price, 
	
	(avg(nullif(buybox_price,0)) - avg(nullif(list_price,0))) / avg(nullif(list_price,0)) as discount_ratio,
	(avg(nullif(comp_buybox_price,0)) - avg(nullif(comp_list_price,0))) / avg(nullif(comp_list_price,0)) as comp_discount_ratio
from wook.amz_zns30_promo_with_comp
where date >= '2026-01-01'
group by 1,2
order by 1,2
;




select min(date), max(date)
from wook.amz_zns30_promo_with_comp

-- 1.02 zinus 30개 스큐의 기초 통계 

select inch, size, collection, asin,  
	--count(*),
	count(distinct asin) as cnt,
	
	avg(nullif(buybox_price,0)) as bb_price, 
	avg(nullif(comp_buybox_price,0)) as comp_bb_price,
	avg(nullif(buybox_price,0)) - avg(nullif(comp_buybox_price,0)) as diff_bb_price, 
	
	avg(nullif(list_price,0)) as list_price,
	avg(nullif(comp_list_price,0)) as comp_list_price,
	avg(nullif(list_price,0)) - avg(nullif(comp_list_price,0)) as diff_list_price, 
	
	(avg(nullif(buybox_price,0)) - avg(nullif(list_price,0))) / avg(nullif(list_price,0)) as discount_ratio,
	(avg(nullif(comp_buybox_price,0)) - avg(nullif(comp_list_price,0))) / avg(nullif(comp_list_price,0)) as comp_discount_ratio
	
	
from wook.amz_zns30_promo_with_comp
where date >= '2026-01-01'
group by 1,2,3,4 
order by 1,2,3,4
;


-- test


select min(bsr_date), max(bsr_date) from dw.amz_bsr_matt_top10_price
union all
select min(date), max(date) from tmp.amz_zns_promo_with_comp

select distinct asin from tmp.amz_zns_promo_with_comp


select distinct collection from tmp.amz_zns_promo_with_comp









select inch, size,
	count(*),
	count(distinct asin),
	avg(buybox_price), 
	avg(list_price)
from tmp.amz_zns_promo_with_comp 
where is_zinus is false
group by 1,2 
order by 1,2








