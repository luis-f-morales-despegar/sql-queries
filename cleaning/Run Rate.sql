------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------

 --- Reconocimiento de Ingreso - Run Rate -- Paises original VIC

with bt_detail as (
               select
                   	fv.line_of_business_code as lob
                   	,CASE 
   					 WHEN fv.brand = 'Despegar' THEN 'D!' 
    					WHEN fv.brand = 'Best Day' THEN 'BD!' 
    					ELSE NULL
						END AS brand
              /*     	,case fv.site when 'Mexico' 	then '01-Mexico'
			              		  when 'Brasil' 	then '02-Brasil'
			             		  when 'Argentina' 	then '03-Argentina' 
			             		  when 'Chile' 		then '04-Chile'
  			              		  when 'Colombia' 	then '05-Colombia'
  			             		  when 'Peru' 		then '06-Peru'
  			            		  else '07-Global'			  
             			end as region
               		,fv.site	*/
               		,CASE
   --WHEN fv.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'Others'
    --WHEN fv.partner_id IN ('P12212', 'AP11666') THEN 'Others'
    --WHEN fv.partner_id IN ('AP12147', 'AP12854') THEN 'Others'
   -- WHEN fv.partner_id IN ('AP12509', 'AP11813') THEN 'Others'
   --- WHEN fv.partner_id = 'AP12158' THEN 'Others'
   -- WHEN fv.partner_id IN ('AP12213', 'AP11843') THEN 'Others'
   -- WHEN fv.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'Others'
    WHEN fv.country_code = 'MX' THEN 'Mexico'
    WHEN fv.country_code = 'BR' THEN 'Brasil'
    WHEN fv.country_code = 'CO' THEN 'Colombia'
    WHEN fv.country_code = 'AR' THEN 'Argentina'
    WHEN fv.country_code = 'EC' THEN 'Ecuador'
    WHEN fv.country_code = 'PE' THEN 'Peru'
    WHEN fv.country_code = 'CL' THEN 'Chile'
    WHEN fv.country_code IN ('US', 'PA') THEN 'USA/ROW'
    WHEN fv.country_code = 'UY' THEN 'Others'
    WHEN fv.country_code = 'BO' THEN 'Others'
    ELSE 'Others'
END AS country_metas
            --   		,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
              --       		country_code,'OT') as country
              --     	,fv.parent_channel
                   	,CASE
           				WHEN fv.brand = 'Best Day' AND fv.parent_channel = 'API' THEN 'MAY'
    					WHEN fv.brand = 'Despegar' AND fv.parent_channel = 'API' THEN 'API'
    					WHEN (fv.brand = 'Best Day' OR fv.brand = 'Despegar') AND fv.parent_channel = 'Agencias afiliadas' THEN 'MIN'
    					ELSE NULL -- 
					END AS channel_metas
           --        	,fv.channel
           --        	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
            --       	,fv.product_status
            --       	,fv.product_is_confirmed_flg as is_confirmed_flg
                   ,CASE
       				 WHEN fv.trip_type_code = 'Nac' THEN 'NAC'
       				 WHEN fv.trip_type_code = 'Int' THEN 'INT'
        		   ELSE NULL -- O cualquier valor por defecto que necesites
    				END AS viaje
                   	,fv.buy_type_code as buy_type
            --       	,fv.product
                   	-- detail
                   	-- hotel_id
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			--,fv.recognition_date
           			,fv.recognition_date as recognition_date
         --  			,fv.booking_date
         --  			,fv.confirmation_date
         --  			,fv.checkin_date
         --  			,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
           			,count(distinct(fv.transaction_code)) as orders
                    ,sum(fv.gestion_gb) as gb_RI 
                    ,max(pnl.b2b_gradient_margin) as gradiente_margen
                    ,(sum(pnl.commission_net_usd)/sum(ch.comision_desp)) as gradiente_margen_calc
                    --> Revenue Margin <--
                    ,sum(pnl.fee_net_usd) as fee_neto
 					,sum(pnl.commission_net_usd) as comision_neta
					,-sum(pnl.discounts_net_usd) as descuentos_neto
    				--> Afiliadas <--					
    				,sum(c.agency_fee_total) as fee_agencia
    				,-sum(ch.comision_ch) as comision_agencia_channels
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
					,sum(pnl.frauds_usd) as frauds				
				    ,-sum(pnl.loyalty_usd) as loyalty
    				,-sum(pnl.ott_usd) as ott
    				,-sum(pnl.revenue_taxes_usd) as revenue_tax
				    ,-sum(pnl.cancellations_usd) as cancelaciones
	    			,-sum(pnl.customer_claims_usd) as customer_claims				
    				,-sum(pnl.revenue_sharing_usd) as revenue_sharing /* comision asociados - b2b2c - islas liverpool */	    			
    				,-sum(pnl.vendor_commission_usd) as vendor_commission /* vendedor - call - islas */
					,-sum(pnl.mkt_cost_net_usd) as mkt_cost
					,sum(pnl.agency_backend_usd) as overs_api
    				--> *** Resultado Financiero *** <--
                    ,sum( case when fv.country_code = 'BR' and fv.product not in ('Vuelos')
                           		then (pnl.net_revenues_usd-pnl.affiliates_usd)
                           else pnl.net_revenues_usd
                       end) as fix_net_revenues
                    ,sum(pnl.net_revenues_usd)as net_revenues
                    ,sum(pnl.npv_net_usd) as npv
                    ,sum(pr.dif_fx_usd + pr.dif_fx_air_usd) as dif_fx
                    ,sum(pr.currency_hedge_usd + pr.currency_hedge_air_usd) as hedge
                    ,sum(pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd) as npv_calc
             from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month > '2023-01'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month >= date'2023-01-01'
             left join analytics.bi_transactional_fact_products p on fv.product_id = p.product_id and p.reservation_year_month >= date'2023-01-01'
             left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fv.product_id and pr.date_reservation_year_month >= '2023-01'
             left join 
                (select cast(p.transaction_id as varchar) as product_id, payment_methods, p.status,max(p.penalty), max(conversion_rate*net_commission_partner) as comision_ch,max(conversion_rate*net_commission_despegar) as comision_desp, avg(conversion_rate) as tipo_cambio
                    from data.lake.channels_bo_product p
                    inner join data.lake.channels_bo_sale s on p.sale_id = s.id
                group by 1,2,3) as ch
                on cast(ch.product_id as varchar) = p.reference_id
             where fv.recognition_date between DATE('2024-01-01') and DATE('2024-12-31')
             and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period > '2023-01'
              and fv.parent_channel in ('API', 'Agencias afiliadas')
              --and p.transaction_code = '803280257900'
              group by 1,2,3,4,5,6, 7--, 8, 9--,11--,12,13--,14,15--,16,17,18,19,20,21 --,22,23,24
 )
 select
-- tx_code,
 brand,
 lob,
  buy_type,
 country_metas,
 channel_metas,
viaje,
 --channel,
-- agency_code,
 recognition_date,
-- booking_date,
-- confirmation_date,
-- checkin_date,
-- checkout_date,
 gb_RI,
 --gradiente_margen_calc,
 fix_net_revenues,
 npv,
orders
 from bt_detail
 limit 100
 --where brand = 'BD!' and country_metas = 'USA/ROW' and channel_metas = 'MAY' and viaje = 'INT' and buy_type = 'Hoteles' and recognition_month = DATE('2024-11-01')
--and site = 'Brasil'
--order by recognition_date asc
--limit 1000