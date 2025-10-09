
--- Mail Diario Excel ---

with bo_tpc as( 
            select
                p.transaction_id as product_id_original
                /*metricas*/
                ,max(p.net_commission_partner * p.conversion_rate) as tpc_usd --third party commission
            from data.lake.channels_bo_product p
            join data.lake.channels_bo_sale s on s.id = p.sale_id
            where cast(s.created as date) >= DATE('2022-01-01') 
            and cast(s.created as date) < CURRENT_DATE
            group by 1
),
bt_detail as (
                    select
                        fh.gestion_date as Fecha, 
                        fh.brand as Marca,
                     -- fh.country_code as pais,
                        case 
                        when fh.partner_id in ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') then 'PY'
                        when fh.partner_id in ('P12212', 'AP11666') then 'CR_CTA'
                        when fh.partner_id = 'AP12147' then 'SV_CTA'
                        when fh.partner_id = 'AP12854' then 'SV_CTA'
                        when fh.partner_id in ('AP12509', 'AP11813') then 'GT_CTA'
                        when fh.partner_id = 'AP12158' then 'PA_CTA'
                        when fh.partner_id in ('AP12213', 'AP11843') then 'HN_CTA'
                        when fh.partner_id in ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') then 'DO_CTA'
                        else fh.country_code 
                        end as pais_corregido,
                       -- if( fh.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
                        --country_code,'OT') as pais,
                        fh.buy_type_code as productooriginal, 
                     --   fh.partner_id,
                    --    fh.agency_name,
   						CASE 
       						 WHEN fh.partner_id IN ('AG00073753', 'AG00037023') THEN 'API'
        					ELSE fh.parent_channel
    					END AS parent_channel_corregido,
    			--		fh.transaction_code as tx_code,
    			--		fh.product_status,
                       -- fh.parent_channel,
                    --    fh.channel,
                     --   fh.trip_type_code as viaje, 
    					ROUND(sum(fh.gestion_gb),2) as gb_s_gradiente,
                        ROUND(sum(fh.gestion_gb * fh.confirmation_gradient),2) as gb, 
                        ROUND(sum(case when fh.country_code = 'BR' and fh.product not in ('Vuelos') then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)) ) ) )
        	                   when fh.channel = 'expedia' then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)) ) ) )	
                               else pnl.net_revenues_usd
                            end
                        )) as fix_net_revenues,
                     ROUND(sum(pnl.net_revenues_usd),2) as net_revenues_usd_s_ajuste,
                        ROUND(sum(pnl.npv_net_usd),2) as npv_net_usd,
                       ROUND(sum(((pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
        		             + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
        		             + pnl.affiliates_usd)    -- sumamos afiliadas
                           / if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)) ) -- quitar gradiente
                         )
                         - coalesce(bo.tpc_usd,0)     -- quitar tpc (en sustitucion de afiliadas)
                        )
                         * max(if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)))) 
                       ) as fix_npv  
                    from analytics.bi_sales_fact_sales_recognition fh 
                    left join analytics.bi_pnlop_fact_current_model pnl on fh.product_id = pnl.product_id and pnl.date_reservation_year_month > '2021-01'
                    left join analytics.bi_transactional_fact_charges c on fh.product_id = c.product_id and c.reservation_year_month >= date'2021-01-01'
                    left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fh.product_id and pr.date_reservation_year_month >= '2021-01'
                    left join bo_tpc bo on bo.product_id_original = fh.origin_product_id
                    where fh.gestion_date >= DATE('2022-01-01')
        			and fh.gestion_date < CURRENT_DATE -- Excluye el día de hoy
        				and fh.lob_gestion in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
        				and pnl.line_of_business = 'B2B'
        				and fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
        				and partition_period > '2020-01'
        				and country_code = 'BR'
        				and parent_channel = 'API'
        				and fh.brand = 'Best Day'
          			--	and partner_id IN ('AG00073753', 'AG00037023')
						group by 1,2,3,4,5)
SELECT * 
FROM bt_detail 
--where product_status = 'Confirmado'
--WHERE Fecha >= DATE('2024-01-01')
--and productooriginal = 'Hoteles'
--and Marca = 'Best Day'
limit 100
--order by 1,2,3,4,5,6
--  select fecha, sum(gb)gb, sum(net_revenues_usd) net_revenues_usd , sum(npv_net_usd)npv_net_usd
--  from bt_detail
--  group by 1
--  order by 1 desc











------------------------
-------------------------



with bt_detail as (
               select
                 --    fv.transaction_code as tx_code
                -- 	,fv.product_id
                 --  	,fv.origin_product_id
                   	fv.line_of_business_code as lob
                   	,fv.brand                   	
                 --  	,case fv.site when 'Mexico' 	then '01-Mexico'
			       ---       		  when 'Brasil' 	then '02-Brasil'
			           --   		  when 'Argentina' 	then '03-Argentina'
			         --     		  when 'Chile' 		then '04-Chile'
  			       --       		  when 'Colombia' 	then '05-Colombia'
  			     --         		  when 'Peru' 		then '06-Peru'
  			   --           		  else '07-Global'			  
             --  			end as region
               		,fv.site	
               	--	,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
                  --     		country_code,'OT') as country
                   	,fv.parent_channel
           --        	,fv.channel
           --        	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
            --       	,fv.product_status
            --       	,fv.product_is_confirmed_flg as is_confirmed_flg
            --       	,fv.trip_type_code as trip_type
                  -- 	,fv.buy_type_code as buy_type
            --       	,fv.product
                   	-- detail
                   	-- hotel_id
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			--,fv.recognition_date
                   ,	YEAR(fv.recognition_date) AS recognition_date
         --  			,fv.booking_date
         --  			,fv.confirmation_date
         --  			,fv.checkin_date
         --  			,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
                    ,ROUND(sum(fv.gestion_gb),2) as gb_RI 
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
    				,ROUND(-sum(pnl.revenue_sharing_usd),2) as revenue_sharing /* comision asociados - b2b2c - islas liverpool */	    	
    				,sum(pnl.margin_net_usd) as revenue_margin 
    				,-sum(pnl.vendor_commission_usd) as vendor_commission /* vendedor - call - islas */
					,-sum(pnl.mkt_cost_net_usd) as mkt_cost
					,sum(pnl.agency_backend_usd) as overs_api
    				--> *** Resultado Financiero *** <--
                    ,ROUND(sum( case when fv.country_code = 'BR' and fv.product not in ('Vuelos')
                           		then (pnl.net_revenues_usd-pnl.affiliates_usd)
                           else pnl.net_revenues_usd
                       end),2) as fix_net_revenues
                    ,sum(pnl.net_revenues_usd)as net_revenues
                    ,ROUND(sum(pnl.npv_net_usd),2) as npv
                    ,sum(pr.dif_fx_usd + pr.dif_fx_air_usd) as dif_fx
                    ,sum(pr.currency_hedge_usd + pr.currency_hedge_air_usd) as hedge
                    ,count(distinct(fv.partner_id)) as agencias
                    ,sum(pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd) as npv_calc
             from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month >= '2018-01'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month >= date'2018-01-01'
             left join analytics.bi_transactional_fact_products p on fv.product_id = p.product_id and p.reservation_year_month >= date'2018-01-01'
             left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fv.product_id and pr.date_reservation_year_month >= '2018-01'
             left join 
                (select cast(p.transaction_id as varchar) as product_id, payment_methods, p.status,max(p.penalty), max(conversion_rate*net_commission_partner) as comision_ch,max(conversion_rate*net_commission_despegar) as comision_desp, avg(conversion_rate) as tipo_cambio
                    from data.lake.channels_bo_product p
                    inner join data.lake.channels_bo_sale s on p.sale_id = s.id
                group by 1,2,3) as ch
                on cast(ch.product_id as varchar) = p.reference_id
             where fv.recognition_date between DATE('2022-01-01') and DATE('2025-12-31')
             and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period >= '2018-01-01'
                 				and country_code = 'BR'
        				and parent_channel = 'API'
        				and fv.brand = 'Best Day'
              --and p.transaction_code = '803280257900'
              group by 1,2,3,4,5-- ,7,8--,9--,10--,11--,12,13--,14,15--,16,17,18,19,20,21 --,22,23,24
 )
 select
-- tx_code,
 brand,
 site,
 parent_channel,
 --channel,
-- agency_code,
 --buy_type,
 recognition_date,
-- booking_date,
-- confirmation_date,
-- checkin_date,
-- checkout_date,
 gb_RI,
 --revenue_margin,
 --gradiente_margen_calc,
 --fix_net_revenues,
 --npv,
 agencias
 from bt_detail
where parent_channel in ('API', 'Agencias afiliadas')
--and site = 'Brasil'
order by recognition_date asc
--limit 1000

 