with bt_detail as (
               select
                     fv.transaction_code as tx_code
                 	,fv.product_id
                   	,fv.origin_product_id
                   	,fv.line_of_business_code as lob
                   	,fv.brand                   	
                  -- 	,case fv.site when 'Mexico' 	then '01-Mexico'
			       --       		  when 'Brasil' 	then '02-Brasil'
			        --      		  when 'Argentina' 	then '03-Argentina'
			          --    		  when 'Chile' 		then '04-Chile'
  			          --    		  when 'Colombia' 	then '05-Colombia'
  			           --   		  when 'Peru' 		then '06-Peru'
  			            --  		  else '07-Global'			  
               		--	end as region
               		,fv.site	
               	--	,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
                --       		country_code,'OT') as country
                   	,fv.parent_channel
                   	,fv.channel
                   	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
                   	,fv.product_status
                   	,fv.product_is_confirmed_flg as is_confirmed_flg
                   	,fv.trip_type_code as trip_type
                   	,fv.buy_type_code as buy_type
                   	,fv.product
                   	,fpr.hotel_name as Hotel
                   	,fpr.hotel_chain_name as Cadena_Hotel
                   	,fpr.hotel_chain_brand_name as Marca_Hotel
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   	,split_part(fv.destination, ', ', 2) as destination_city
           			,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.gestion_date
           			,fv.booking_date
           			,fv.confirmation_date
           			,fv.checkin_date
           			,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
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
                    ,sum(pr.dif_fx_usd + pr.dif_fx_air_usd) as dif_fx
                    ,sum(pr.currency_hedge_usd + pr.currency_hedge_air_usd) as hedge
                    ,sum(pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd) as npv_calc
             from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month > '2021-01'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month >= date'2021-01-01'
             left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fv.product_id and pr.date_reservation_year_month >= '2021-01'
             LEFT JOIN data.analytics.bi_transactional_fact_products fpr ON CAST(fpr.transaction_code AS bigint) = fv.transaction_code AND fpr.reservation_year_month >= DATE '2021-07-01'
             where fv.gestion_date >= date'2023-01-01'
             and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period > '2021-01'
              group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
 )
 select * from bt_detail
 limit 100