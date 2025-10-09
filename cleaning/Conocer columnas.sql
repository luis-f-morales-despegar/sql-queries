
--- PnL

select *
from analytics.bi_pnlop_fact_current_model pnl
where date_reservation_year_month > '2021-01'
and transaction_code = 64322282400
limit 100

--pnL.margin_net_usd as RM
--pnL.margin_usd as NCRM
--pnL.margin_variable_net_usd as margen_variable
---------------------------------------------------------
----------------------------------------------------------

select *
from analytics.bi_sales_fact_sales_recognition fv
where fv.partition_period > '2021-01'
--and transaction_code = 64322282400
limit 100

----------------------------------------------------------

select *
from data.analytics.bi_transactional_fact_products fpr 
where fpr.reservation_year_month >= date '2021-07-01' 
and product_type = 'Hoteles'
limit 100


----------------------------------------------------------

--- DASH POWER BI TRÁFICO API

select *
from data.analytics.b2b_fact_look_to_book
where hsm_date >= date'2024-10-01'
limit 100


---- TRÁFICO API POR HOTEL (MUY PESADA, SOLO ULTIMA SEMANA)

select * 
from analytics.b2b_fact_hotel_requests
where request_date is not null
limit 100



select * 
from analytics.b2b_fact_hotel_requests
where request_date is not null
and pos = 'US'
limit 100

select distinct channel
from analytics.b2b_fact_hotel_requests
where request_date is not null

------------------------------------------------


select * --distinct(type)
from analytics.bi_transactional_fact_collections
where reservation_year_month >= date'2023-01-01'
limit 100