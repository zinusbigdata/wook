/* 
 * BSR Share Analysis
 * Last : 1/12/2026
 * Data Source : wook.amz_us_bsr_add_seller_price_rating
 * 			   : stck.atlas_sales_all 
 */

select distinct bw_ff_type  from dw.rf_amz_pdt_zns_comp_daily

select asin, seller_type, count(*) from wook.amz_us_bsr_add_seller_price_rating
where bsr_ctgry='Bed Frames' and brand='NEW JETO' and bsr_date >= '2025-01-01'
group by 1,2 

select count(*), max(WeekId) from stck.atlas_sales_all 
where SubCategory in ('Mattresses','Beds','Bed Frames') and WeekId >= 202501;


select count(*) from wook.amz_us_bsr_add_seller_price_rating
where bsr_ctgry in ('Mattresses','Beds','Bed Frames') and bsr_date <='2025-12-31' and rank <= 20


select max(rank), max(bsr_date) from wook.amz_us_bsr_add_seller_price_rating

with base as (
select * from wook.amz_us_bsr_add_seller_price_rating
where bsr_ctgry in ('Mattresses','Beds','Bed Frames') and rank <= 20
)
select bsr_date, count(*) from base
where bsr_ctgry='Bed Frames' and yr_month='21-01'
group by 1 order by 1


select distinct * from wook.amz_us_bsr_add_seller_price_rating_202601
where bsr_ctgry='Bed Frames' and bsr_date='2025-12-01' and rank <= 20
order by rank 

with tmp1 as (
select distinct * from wook.amz_us_bsr_add_seller_price_rating_202601
where bsr_ctgry in ('Mattresses','Beds','Bed Frames') and rank <= 20 and bsr_date <= '2025-12-31'
)
select count(*) from tmp1 

with zinus2025 as (
select * from wook.amz_us_bsr_add_seller_price_rating_202601
where bsr_ctgry='Bed Frames' and 
	bsr_date between '2025-01-01' and '2025-12-31' and
	rank <= 20 and brand_raw_org='ZINUS'
)
select seller_type, count(*) asin from zinus2025 
group by 1;  

select * from wook.amz_us_bsr_add_seller_price_rating_202601
where bsr_ctgry='Bed Frames' and 
	bsr_date between '2025-01-01' and '2025-12-31' and
	rank <= 20 and brand_raw_org='ZINUS' and seller_type='3P'


with zinus2025 as (
select * from wook.amz_us_bsr_add_seller_price_rating_202601
where bsr_ctgry='Mattresses' and 
	bsr_date between '2025-01-01' and '2025-12-31' and
	rank <= 20 and brand_raw_org='GAESTE'
)
select max(retail_price), min(retail_price), avg(retail_price)  from zinus2025 
  

