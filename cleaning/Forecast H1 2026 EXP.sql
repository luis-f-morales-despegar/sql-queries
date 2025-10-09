---------------------------------------------------------------------------
------------------ V2 por RI + variable filtro Planning Financiero (pea.product_state) -> Analisis Expdia as a Client QBR Q1 2025 --------------------------------------

with bt_detail as (
               select
                     --fv.transaction_code as tx_code
                 	--,fv.product_id
                   	--,fv.origin_product_id
                   	fv.line_of_business_code as lob
                   	,fv.brand as brand     
      --             	,pea.product_state as product_state
             --      	,case fv.site when 'Mexico' 	then '01-Mexico'
			   --           		  when 'Brasil' 	then '02-Brasil'
			       --       		  when 'Argentina' 	then '03-Argentina'
			     --         		  when 'Chile' 		then '04-Chile'
  			   --           		  when 'Colombia' 	then '05-Colombia'
  			 --             		  when 'Peru' 		then '06-Peru'
  			--              		  else '07-Global'			  
          --     			end as region
        --      		,fv.site	
           --    		,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
            --           		country_code,'OT') as country
        --           	,fv.parent_channel
                   	,fv.channel as channel
                   	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
      --             	,fv.product_status as product_status
       --            	,fv.product_is_confirmed_flg as is_confirmed_flg
     --              	,fv.trip_type_code as trip_type
  --                 	,fv.buy_type_code as buy_type
       --            	,pr.effective_rate as tarifa
          --         	,fv.product
       /*        	,case when pr.product_type = 'Vuelos' then concat(pr.flight_validatin_carrier,' ',pr.origin_city_code,'>',pr.destination_city_code)	     
                          when pr.product_type IN ('Hoteles','Alquileres') then pr.hotel_name	       
                          when pr.product_type IN ('Excursiones','Traslados','Circuito') then pr.destination_service_service_name 	        
                            else pr.product_type	    
                        end as detail */
         --          	,gateway_code
        --           	,split_part(destination,', ',2) as destination_country	
         --          	, pr.is_latam_destination_flg
           --        	, pr.is_latam_destination     	
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           		--	,fv.gestion_date as gestion_date
           			,date_format(fv.recognition_date, '%Y-%m') as recognition_date
        --   			,fv.confirmation_date
           			,sum(fv.gestion_gb) as gb_RI 
           	--	   ,max(pnl.b2b_gradient_margin) as gradiente_margen
          --          ,(sum(pnl.commission_net_usd)/sum(ch.comision_desp)) as gradiente_margen_calc
          -- 			  ,sum(pnl.fee_net_usd) as fee_neto
 		--			,sum(pnl.commission_net_usd) as comision_neta
		--			,-sum(pnl.discounts_net_usd) as descuentos_neto
		--			,sum(pnl.net_revenues_usd) as net_revenues_s_fix
         --           ,sum(pnl.npv_net_usd) as npv_s_fix
		--			,SUM(
  		--	--		CASE 
    	--			WHEN fv.country_code = 'BR' AND fv.product NOT IN ('Vuelos') THEN (pnl.net_revenues_usd - pnl.affiliates_usd)
   		--			 WHEN fv.channel = 'expedia' THEN (pnl.net_revenues_usd - pnl.affiliates_usd)
   		---			 ELSE pnl.net_revenues_usd
  		--			END
		--				) AS fix_net_revenues
       --    			 ,max(pr.hotel_name) as Hotel
       --    			 ,max(pr.hotel_despegar_id) as hotelid 
       --    			 ,max(pr.hotel_chain_name) as Cadena
           	--		 ,max(a.market) as hotel_market
            --         ,max(a.area) as hotel_area
            --         ,max(a.tipo_de_cuenta) as hotel_category
           			--,fv.booking_date
           			--,fv.checkin_date
           			--,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
      --    			,max(fv.confirmation_gradient) as gradient
       --   				,sum(c.agency_fee_total) as fee_agencia
    --				,-sum(ch.comision_ch) as comision_agencia_channels
    --				,-sum(case when fv.buy_type_code='Carrito' then pnl.affiliates_usd else c.agency_fee_total end) as comision_agencia
					--> *** PROFIT *** <--
    	--			,sum(pnl.backend_air_usd) as backend_air
    	--			,sum(pnl.backend_non_air_usd) as backend_nonair
    	--			,sum(pnl.other_incentives_air_usd) as other_incentives_air
    	--			,sum(pnl.other_incentives_non_air_usd) as other_incentives_nonair   				
    	--			,sum(pnl.breakage_revenue_usd) as breakage_revenue
    	--			,+sum(pnl.media_revenue_usd ) as media_revenue    				
    	--			,+sum(pnl.discounts_mkt_funds_usd) as mkt_discounts /* alias desc_partner */
    				--> *** LOSS *** <--
					--> COI-CCP <--
    	--			,-sum(pnl.ccp_usd) as ccp
    	--			,-sum(pnl.coi_usd) as coi
    	--			,sum(pnl.coi_interest_usd) as interes_coi
    				--> Cargos Variables <--
			--		,sum(pnl.customer_service_usd) as customer_service
    	--			,sum(pnl.errors_usd) as errors 
		--			,-sum(pnl.affiliates_usd) as afiliadas
		--			,sum(pnl.frauds_usd) as frauds				
		--		    ,-sum(pnl.loyalty_usd) as loyalty
    	--			,-sum(pnl.ott_usd) as ott
    --				,-sum(pnl.revenue_taxes_usd) as revenue_tax
	--			    ,-sum(pnl.cancellations_usd) as cancelaciones
	  --  			,-sum(pnl.customer_claims_usd) as customer_claims				
   -- 				,-sum(pnl.revenue_sharing_usd) as revenue_sharing /* comision asociados - b2b2c - islas liverpool */	    			
    --				,-sum(pnl.vendor_commission_usd) as vendor_commission /* vendedor - call - islas */
	--				,-sum(pnl.mkt_cost_net_usd) as mkt_cost
	--				,sum(pnl.agency_backend_usd) as overs_api
        ---            ,sum(fv.gestion_gb) as gb_gestion
     --               ,sum(fv.gestion_gb * fv.confirmation_gradient) as gb_gestion_gc 
                    ,count(distinct fv.transaction_code) as bookings
            from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month > '2021-01'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month >= date'2021-01-01'
             left join analytics.bi_transactional_fact_products p on fv.product_id = p.product_id and p.reservation_year_month >= date'2021-01-01'
             left join analytics.bi_transactional_fact_products_current_state pea on p.product_id = pea.product_id 
        --     left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fv.product_id and pr.date_reservation_year_month >= '2023-01'
             left join analytics.bi_transactional_fact_products pr on pr.product_id = fv.product_id and pr.reservation_year_month >= date'2021-01-01'
             left join 
                (select cast(p.transaction_id as varchar) as product_id, payment_methods, p.status,max(p.penalty), max(conversion_rate*net_commission_partner) as comision_ch,max(conversion_rate*net_commission_despegar) as comision_desp, avg(conversion_rate) as tipo_cambio
                    from data.lake.channels_bo_product p
                    inner join data.lake.channels_bo_sale s on p.sale_id = s.id
                group by 1,2,3) as ch
                on cast(ch.product_id as varchar) = p.reference_id
           WHERE fv.recognition_date >= DATE '2021-01-01'
           AND fv.recognition_date <= DATE '2025-12-31'
           --  and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period > '2021-01'
            AND (pea.product_state IS NULL OR pea.product_state IS NOT NULL)
            and UPPER(fv.channel) like 'EXP%' 
           --   and fv.gestion_date < CURRENT_DATE	
              group by 1,2,3,4,5
 )
 select 
 --gestion_date as Fecha_venta,
 recognition_date as Fecha_reconocimiento,
-- detail as detail,
 agency_code as agency_code,
channel as Canal,
 sum(bookings) as bookings,  
 ROUND(sum(gb_RI ),2) as gb_RI
 from bt_detail 
 --where UPPER(channel) = 'EXPEDIA'
 --and  gestion_date >= date('2025-04-01')
-- and gradient = 1
-- and brand = 'Best Day'
 --and buy_type = 'Hoteles'
-- Where gateway_code in ('HBG', 'EXP')
 --and country = 'USA'
 group by 1,2,3
 order by 1 ASC
 --limit 1000