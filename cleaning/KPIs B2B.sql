
-------------------------------------------------------------------------------------------------------------------------------------------------------
--- TRÁFICO ---
-------------------------------------------------------------------------------------------------------------------------------------------------------

---  TRÁFICO HTML ----------------------------------

SELECT "source"."date" AS "date", "source"."week" AS "week", "source"."month" AS "month", "source"."country" AS "country", "source"."product_sale" AS "product_sale", "source"."channel" AS "channel", "source"."partner_id" AS "partner_id", "source"."name" AS "name", "source"."routetype" AS "routetype", "source"."destino_city" AS "destino_city", "source"."plataforma" AS "plataforma", "source"."search_searchers" AS "search_searchers", "source"."search_detail" AS "search_detail", "source"."search_pi" AS "search_pi", "source"."search_checkouters" AS "search_checkouters", "source"."bookings_bi" AS "bookings_bi"
FROM (With rank as (
    SELECT
    CAST(bi.date AS DATE) AS date,
    bi.year*100 - 200000 + bi.week as week,
    bi.year*100 - 200000 + bi.month as month,
    bi.country as country,
    bi.producto_fenix as product_sale,
    bi.channel,
    bi.partner_id,
    p.name,
    bi.routetype,
    bi.destino_city,
    bi.plataforma,
    COUNT(DISTINCT IF(bi.flow = 'SEARCH', bi.searchid, NULL)) AS search_searchers,
    COUNT(DISTINCT IF(bi.flow = 'DETAIL', bi.searchid, NULL)) AS search_detail,
    COUNT(DISTINCT IF(bi.flow = 'INTER-XS', bi.searchid, NULL)) AS search_pi,
    COUNT(DISTINCT IF(bi.flow = 'CHECKOUT', bi.searchid, NULL)) AS search_checkouters,
    transaction_code as bookings_bi,
if(bi.transaction_code is not null,ROW_NUMBER() OVER (PARTITION BY bi.transaction_code
ORDER BY bi.date),null) AS row_num
FROM
    data.lake.bi_web_traffic bi
left join lake.ch_bo_partner_partner p
    on bi.partner_id = p.reference_id
WHERE
   cast (bi.date as date) between date_add('day',-32,current_date) and date_add('day',-1,current_date)
   -- cast (bi.date as date) between date '2024-01-01' and date_add('day',-1,current_date)
   AND ispageview = 1
    AND flg_detalle_cp = 0
    AND bi.channel IN ('hoteldo-html-platinum', 'hoteldo-html-gold', 'hoteldo-html-silver', 'hoteldo-html-classic', 'travel-agency-bo', 'travel-agency-whitelabel')
GROUP BY bi.date,2,3,4,5,6,7,8, 9, 10,11, 16
having 
    COUNT(DISTINCT IF(bi.flow = 'SEARCH', bi.searchid, NULL)) > 0
    or COUNT(DISTINCT IF(bi.flow = 'DETAIL', bi.searchid, NULL)) > 0
    or COUNT(DISTINCT IF(bi.flow = 'INTER-XS', bi.searchid, NULL)) > 0
    or COUNT(DISTINCT IF(bi.flow = 'CHECKOUT', bi.searchid, NULL)) > 0
    or transaction_code is not null
)
SELECT
date,
week,
month,
country,
product_sale,
channel,
partner_id,
name,
routetype,
destino_city,
plataforma,
search_searchers,
search_detail,
search_pi,
search_checkouters,
row_num as bookings_bi
FROM rank
WHERE (row_num = 1
    OR row_num IS NULL)
) "source"
limit 100




---  TRÁFICO API ----------------------------------


--- Por Partner ID

select *
from data.analytics.b2b_fact_look_to_book
where hsm_date >= date'2024-10-01'


--- Por Hotel

select * 
from analytics.b2b_fact_hotel_requests
where request_date is not null
limit 100





-------------------------------------------------------------------------------------------------------------------------------------------------------
--- PnL ---
-------------------------------------------------------------------------------------------------------------------------------------------------------

---  PnL API & HTML ----------------------------------

with bt_detail as (
               select
                     fv.transaction_code as tx_code
                    ,fv.product_id
                   	,fv.origin_product_id
                   	,fv.line_of_business_code as lob
                   	,fv.brand                   	
                   	,case fv.site when 'Mexico' 	then '01-Mexico'
			              		  when 'Brasil' 	then '02-Brasil'
			              		  when 'Argentina' 	then '03-Argentina'
			              		  when 'Chile' 		then '04-Chile'
  			              		  when 'Colombia' 	then '05-Colombia'
  			              		  when 'Peru' 		then '06-Peru'
  			              		  else '07-Global'			  
               			end as region
               		,fv.site	
               		,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
                       		country_code,'OT') as country
                   	,fv.parent_channel
                   	,fv.channel
                   	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   	,fv.agency_name
                   	,fv.product_status
                   	,fv.product_is_confirmed_flg as is_confirmed_flg
                   	,fv.trip_type_code as trip_type
                   	,fv.buy_type_code as buy_type
                   	,fv.product
                   --	,detail
                   -- ,hotel_id
                   	--,fv.main_airline_code as iata
                   --	,airline_name
                   --	,tarifa_efectiva
                   	,split_part(fv.destination, ', ', 2) as destination_city
           			,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.gestion_date
           			,fv.booking_date
           			,fv.confirmation_date
           			,fv.checkin_date
           			,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
           			,count(distinct(fv.transaction_code)) as orders
           			,max(fv.confirmation_gradient) as gradient
                    ,sum(fv.gestion_gb) as gb_gestion
                    ,sum(fv.gestion_gb * fv.confirmation_gradient) as gb_gestion_gc 
                    --> Revenue Margin <--
                    ,sum(pnl.fee_net_usd) as fee_neto
 					,sum(pnl.commission_net_usd) as comision_neta
					,-sum(pnl.discounts_net_usd) as descuentos_neto
    				--> Afiliadas <--					
    				,sum(c.agency_fee_total) as fee_agencia
    				,-sum(case when fv.buy_type_code='Carrito' then pnl.affiliates_usd else c.agency_fee_total end) as comision_agencia
					--> *** PROFIT *** <--
    				,sum(pnl.backend_air_usd) as backend_air
    				,sum(pnl.backend_non_air_usd) as backend_nonair
    				,sum(pnl.other_incentives_air_usd) as other_incentives_air
    				,sum(pnl.other_incentives_non_air_usd) as other_incentives_nonair   				
    				,sum(pnl.breakage_revenue_usd) as breakage_revenue
    				,+sum(pnl.media_revenue_usd ) as media_revenue    				
    				,+sum(pnl.discounts_mkt_funds_usd) as mkt_discounts /* alias desc_partner */
    				--> *** LOSS *** <--
					--> COI-CCP <--
    				,-sum(pnl.ccp_usd) as ccp
    				,-sum(pnl.coi_usd) as coi
    				,sum(pnl.coi_interest_usd) as interes_coi
    				--> Cargos Variables <--
					,sum(pnl.customer_service_usd) as customer_service
    				,sum(pnl.errors_usd) as errors 
					,-sum(pnl.affiliates_usd) as afiliadas
					,-sum(pnl.frauds_usd) as frauds				
				    ,-sum(pnl.loyalty_usd) as loyalty
    				,-sum(pnl.ott_usd) as ott
    				,-sum(pnl.revenue_taxes_usd) as revenue_tax
				    ,-sum(pnl.cancellations_usd) as cancelaciones
	    			,-sum(pnl.customer_claims_usd) as customer_claims				
    				,-sum(pnl.revenue_sharing_usd) as revenue_sharing /* comision asociados - b2b2c - islas liverpool */	    			
    				,-sum(pnl.vendor_commission_usd) as vendor_commission /* vendedor - call - islas */
					,-sum(pnl.mkt_cost_net_usd) as mkt_cost
    				--> *** Resultado Financiero *** <--
                    ,sum( case when fv.country_code = 'BR' and fv.product not in ('Vuelos')
                           		then (pnl.net_revenues_usd-pnl.affiliates_usd)
                           else pnl.net_revenues_usd
                       end) as fix_net_revenues
                    ,sum(pnl.net_revenues_usd)as net_revenues
                    ,sum(pnl.npv_net_usd) as npv
                    ,sum(pnl.margin_net_usd) as revenue_margin    --El Revenue Margin (RM) representa el porcentaje de ganancias netas que percibe la compañía de todas sus ventas.
                    ,sum(pnL.margin_usd) as ncrm                  --El Net Comission Revenue Margin (NCRM) representa el porcentaje de ganancias netas que percibe la compañía de todas sus ventas, quitando la comisión que se abona a las agencias afiliadas.
           			,sum(pnL.margin_variable_net_usd) as margen_variable    --El Margen Variable representa la ganancia porcentual después de todos los costos que deban considerarse.
                    ,sum(pnl.variable_charges_without_mkt_usd) as variable_charges_without_mkt
                    ,sum(pr.dif_fx_usd + pr.dif_fx_air_usd) as dif_fx
                    ,sum(pr.currency_hedge_usd + pr.currency_hedge_air_usd) as hedge
           			,sum(pnl.financial_result_usd) as financial_result
                    ,sum(pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd) as npv_calc
             from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month > '2021-01'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month >= date'2021-01-01'
             left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fv.product_id and pr.date_reservation_year_month >= '2021-01'
             where fv.gestion_date between DATE('2023-01-01') and DATE('2024-12-31')
             and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period > '2021-01'
              group by 1,2,3,4,5,6 ,7,8,9,10,11,12,13,14,15,16,17, 18, 19, 20, 21, 22, 23, 24--,18--,19--,20--,21 --,22,23,24
 )
 select * from bt_detail
limit 100




-------------------------------------------------------------------------------------------------------------------------------------------------------
--- PnL 2 - Fecha de actualizacion: 02/ene/2025 ---
-------------------------------------------------------------------------------------------------------------------------------------------------------

---  PnL API & HTML ----------------------------------


--  Mail Diario (Venta PnL Emitida) + adecuaciones a terminología Budget / RR desde RI de cosecha ---> Cosecha Venta Emitida
 
 with bo_tpc as( 
            select
                p.transaction_id as product_id_original
                /*metricas*/
                ,max(p.net_commission_partner * p.conversion_rate) as tpc_usd --third party commission
            from data.lake.channels_bo_product p
            join data.lake.channels_bo_sale s on s.id = p.sale_id
            where cast(s.created as date) >= DATE('2023-01-01') 
            and cast(s.created as date) < CURRENT_DATE
            group by 1
),
bt_detail as (
               select
                     fh.transaction_code as tx_code
                  	,fh.product_id
                   	,fh.origin_product_id
                   	,fh.line_of_business_code as lob
                   	,CASE 
   					 WHEN fh.brand = 'Despegar' THEN 'D!' 
    					WHEN fh.brand = 'Best Day' THEN 'BD!' 
    					ELSE NULL
						END AS Marca                	
                   	/*,case fv.site when 'Mexico' 	then '01-Mexico'
			              		  when 'Brasil' 	then '02-Brasil'
			              		  when 'Argentina' 	then '03-Argentina'
			              		  when 'Chile' 		then '04-Chile'
  			              		  when 'Colombia' 	then '05-Colombia'
  			              		  when 'Peru' 		then '06-Peru'
  			              		  else '07-Global'			  
               			end as region
               		,fv.site	
               		,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
                       		country_code,'OT') as country */
                     ,case 
                        when fh.partner_id in ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') then 'PY'
                        when fh.partner_id in ('P12212', 'AP11666') then 'CR_CTA'
                        when fh.partner_id = 'AP12147' then 'SV_CTA'
                        when fh.partner_id = 'AP12854' then 'SV_CTA'
                        when fh.partner_id in ('AP12509', 'AP11813') then 'GT_CTA'
                        when fh.partner_id = 'AP12158' then 'PA_CTA'
                        when fh.partner_id in ('AP12213', 'AP11843') then 'HN_CTA'
                        when fh.partner_id in ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') then 'DO_CTA'
                        else fh.country_code 
                        end as pais_corregido
                        ,CASE
    WHEN fh.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'Others'
    WHEN fh.partner_id IN ('P12212', 'AP11666') THEN 'Others'
    WHEN fh.partner_id IN ('AP12147', 'AP12854') THEN 'Others'
    WHEN fh.partner_id IN ('AP12509', 'AP11813') THEN 'Others'
    WHEN fh.partner_id = 'AP12158' THEN 'Others'
    WHEN fh.partner_id IN ('AP12213', 'AP11843') THEN 'Others'
    WHEN fh.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'Others'
    WHEN fh.country_code = 'MX' THEN 'Mexico'
    WHEN fh.country_code = 'BR' THEN 'Brasil'
    WHEN fh.country_code = 'CO' THEN 'Colombia'
    WHEN fh.country_code = 'AR' THEN 'Argentina'
    WHEN fh.country_code = 'EC' THEN 'Ecuador'
    WHEN fh.country_code = 'PE' THEN 'Peru'
    WHEN fh.country_code = 'CL' THEN 'Chile'
    WHEN fh.country_code IN ('US', 'PA', 'ES', 'CR') THEN 'USA/ROW'
    WHEN fh.country_code = 'UY' THEN 'Others'
    WHEN fh.country_code = 'BO' THEN 'Others'
    ELSE 'Others'
                     END AS country_metas
                    ,CASE
           				WHEN fh.brand = 'Best Day' AND fh.parent_channel = 'API' THEN 'MAY'
    					WHEN fh.brand = 'Despegar' AND fh.parent_channel = 'API' THEN 'API'
    					WHEN (fh.brand = 'Best Day' OR fh.brand = 'Despegar') AND fh.parent_channel = 'Agencias afiliadas' THEN 'MIN'
    					ELSE NULL -- 
					END AS channel_metas
                   --	,fv.parent_channel
                   --  ,fh.channel
					,case when length(fh.partner_id) > 0 then fh.partner_id else fh.channel end as agency_code
                  	,fh.agency_name
                   	,fh.product_status
                   	,fh.product_is_confirmed_flg as is_confirmed_flg
                   	,CASE
       				 WHEN fh.trip_type_code = 'Nac' THEN 'NAC'
       				 WHEN fh.trip_type_code = 'Int' THEN 'INT'
        		   ELSE NULL 
    				END AS viaje
                   	,fh.buy_type_code as buy_type
                   	,fh.product
                   	-- detail
                   	-- hotel_id
                   	,fh.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fh.recognition_date
           			,fh.booking_date
           			,fh.confirmation_date
           			,fh.checkin_date
           			,fh.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
    					,ROUND(sum(fh.gestion_gb),2) as gb_s_gx,
                        ROUND(sum(fh.gestion_gb * fh.confirmation_gradient),2) as gb_c_gx, 
                        ROUND(sum( case when fh.country_code = 'BR' and fh.product not in ('Vuelos') then (pnl.net_revenues_usd - bo.tpc_usd)
                               when fh.channel = 'expedia' then (pnl.net_revenues_usd - bo.tpc_usd)    
                               else pnl.net_revenues_usd
                            end
                        ),2) as net_revenues_usd,
                       ROUND(sum(pnl.net_revenues_usd),2) as net_revenues_usd_s_ajuste,
                        ROUND(sum(pnl.npv_net_usd),2) as npv_net_usd,
                        ROUND(sum(((pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
        		             + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
        		             + pnl.affiliates_usd)    -- sumamos afiliadas
                           / if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)) ) -- quitar gradiente
                         )
                         - coalesce(bo.tpc_usd,0)     -- quitar tpc (en sustitucion de afiliadas)
                        )
                         * max(if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)))),2) 
                        as fix_npv  
                    from analytics.bi_sales_fact_sales_recognition fh 
                    left join analytics.bi_pnlop_fact_current_model pnl on fh.product_id = pnl.product_id and pnl.date_reservation_year_month > '2021-01'
                    left join analytics.bi_transactional_fact_charges c on fh.product_id = c.product_id and c.reservation_year_month >= date'2021-01-01'
                    left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fh.product_id and pr.date_reservation_year_month >= '2021-01'
                    left join bo_tpc bo on bo.product_id_original = fh.origin_product_id
                    where fh.gestion_date >= DATE('2024-12-01')
        			and fh.gestion_date < CURRENT_DATE -- Excluye el día de hoy
        				and fh.lob_gestion in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
        				and pnl.line_of_business = 'B2B'
        				and fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
        				and partition_period > '2020-01'
          			--	and partner_id IN ('AG00073753', 'AG00037023')
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21)
SELECT *
FROM bt_detail 
where country_metas = 'Others'
order by tx_code
--limit 1000

--Parece que no hay duplicados; agregar tabla pr para hotel name, tarifa efectiva
from data.analytics.bi_transactional_fact_products pr 
join data.analytics.bi_transactional_fact_transactions tx on tx.transaction_code = pr.transaction_code and tx.reservation_year_month >= date('2023-01-01')




