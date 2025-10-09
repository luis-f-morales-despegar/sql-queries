------- Dash Arge

select 
date_format(fh.gestion_date, '%Y-%m') AS mes_reserva
--,pr.checkin_date as checkin
--,pr.checkout_date as checkout
,pr.hotel_despegar_id as ho_hoteldespegarid
,pr.hotel_name as ho_hotelnombre
,pr.hotel_chain_name
,pr.product_type as producto
,fh.site
,fh.trip_type_code as tipoviaje
,pr.destination_city as destino
,pr.destination_country_code as destino_codigopais
,fh.buy_type_code as productooriginal
,fh.brand
--,fh.channel
,fh.parent_channel
,fh.partner_id
,pr.effective_rate as tarifaefectiva
,pr.provider_code  as gateway
,fh.product_status
,sum(fh.gestion_gb * fh.confirmation_gradient) as gb_cgx
,sum(fh.gestion_gb) as gb_sin_gradiente
,count(distinct fh.transaction_code) as orders
,sum(case when fh.country_code = 'BR' and fh.product not in ('Vuelos') then (pnl.net_revenues_usd-pnl.affiliates_usd) else pnl.net_revenues_usd  end) as net_revenues_usd   ---- *Pendiente: Comprobar ajuste (20250213)
,sum(pnl.net_revenues_usd) as net_revenues_usd_s_ajuste
,sum(pnl.npv_net_usd) as npv
,sum(pnl.fee_net_usd) as fee_neto
,sum(pnl.commission_net_usd) as comision_neta
,-sum(pnl.discounts_net_usd) as descuentos_neto
,sum(c.agency_fee_total) as fee_agencia
,-sum(case when fh.buy_type_code='Carrito' then pnl.affiliates_usd else c.agency_fee_total end) as comision_agencia
,sum(pnl.backend_air_usd) as backend_air
,sum(pnl.backend_non_air_usd) as backend_nonair
,sum(pnl.other_incentives_air_usd) as other_incentives_air
,sum(pnl.other_incentives_non_air_usd) as other_incentives_nonair   				
,sum(pnl.breakage_revenue_usd) as breakage_revenue
,+sum(pnl.media_revenue_usd ) as media_revenue    				
,+sum(pnl.discounts_mkt_funds_usd) as mkt_discounts
,-sum(pnl.ccp_usd) as ccp
,-sum(pnl.coi_usd) as coi
,sum(pnl.coi_interest_usd) as interes_coi
,sum(pnl.customer_service_usd) as customer_service
,sum(pnl.errors_usd) as errors 
,-sum(pnl.affiliates_usd) as afiliadas
,-sum(pnl.frauds_usd) as frauds				
,-sum(pnl.loyalty_usd) as loyalty
,-sum(pnl.ott_usd) as ott
,-sum(pnl.revenue_taxes_usd) as revenue_tax
,-sum(pnl.cancellations_usd) as cancelaciones
,-sum(pnl.customer_claims_usd) as customer_claims				
,-sum(pnl.revenue_sharing_usd) as revenue_sharing  			
,-sum(pnl.vendor_commission_usd) as vendor_commission 
,-sum(pnl.mkt_cost_net_usd) as mkt_cost
,sum(pnl.margin_variable_net_usd) as margin_variable
,sum(pnl.margin_net_usd) as margin_net
,sum(pnl.fee_income_mkt_cmr_usd) as fee_income_mkt_cmr_usd
,sum(pnl.dif_fx_usd + pnl.dif_fx_air_usd) as dif_fx
,sum(pnl.currency_hedge_usd + pnl.currency_hedge_air_usd) as hedge
,sum(pnl.financial_result_usd) as financial_result
,sum(pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd + pnl.dif_fx_usd + pnl.dif_fx_air_usd + pnl.currency_hedge_usd + pnl.currency_hedge_air_usd) as npv_calc
from analytics.bi_sales_fact_sales_recognition fh
left join analytics.bi_transactional_fact_charges c on fh.product_id = c.product_id
left join analytics.bi_transactional_fact_products pr on cast(pr.product_id as varchar)=cast(fh.product_id as varchar) and pr.reservation_year_month >= date('2024-01-01')
left join analytics.bi_pnlop_fact_current_model pnl on fh.product_id = pnl.product_id
where fh.partition_period >= '2024-01'
and pnl.date_reservation_year_month > '2023-01'
and c.reservation_year_month >= date'2023-01-01'
and pnl.line_of_business = 'B2B'
and fh.gestion_date >= date '2024-01-01'
and fh.site = 'Mexico'
and fh.parent_channel = 'API'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
order by mes_reserva
limit 100