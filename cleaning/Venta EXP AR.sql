with bt_detail as (
               select
                     --fv.transaction_code as tx_code
                 	--,fv.product_id
                   	--,fv.origin_product_id
                   	fv.line_of_business_code as lob
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
                   	--,fv.channel
                   	--,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
                   	--,fv.product_status
                   	--,fv.product_is_confirmed_flg as is_confirmed_flg
                   	,fv.trip_type_code as trip_type
                   	,fv.buy_type_code as buy_type
                   	,fv.product
                   	,case when pr.product_type = 'Vuelos' then concat(pr.flight_validatin_carrier,' ',pr.origin_city_code,'>',pr.destination_city_code)	     
                          when pr.product_type IN ('Hoteles','Alquileres') then pr.hotel_name	       
                          when pr.product_type IN ('Excursiones','Traslados','Circuito') then pr.destination_service_service_name 	        
                            else pr.product_type	    
                        end as detail
                   	,gateway_code
                   	,split_part(destination,', ',2) as destination_country	
                   	, pr.is_latam_destination_flg
                   	, pr.is_latam_destination
                   	---hotel_id                  	
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.gestion_date
           			 ,max(pr.hotel_name) as Hotel
           			--,fv.booking_date
           			--,fv.confirmation_date
           			--,fv.checkin_date
           			--,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
           			,max(fv.confirmation_gradient) as gradient
                    ,sum(fv.gestion_gb) as gb_gestion
                    ,sum(fv.gestion_gb * fv.confirmation_gradient) as gb_gestion_gc 
                    ,count(distinct fv.transaction_code) as bookings
             from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month >= '2024-01'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month >= date'2024-01-01'
          --   left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fv.product_id and pr.date_reservation_year_month >= '2021-01'
             left join analytics.bi_transactional_fact_products pr on pr.product_id = fv.product_id and pr.reservation_year_month >= date'2024-01-01'
             where fv.gestion_date >= date '2024-06-01' 
             --and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
             and pnl.line_of_business in ('B2B')
              and fv.partition_period > '2021-01'
              group by 1,2,3,4,5,6 ,7,8,9,10,11,12,13,14,15--,15,16,17,18,19--,20,21 --,22,23,24
 )
 select 
 gestion_date as Fecha_venta,
 gateway_code as Proveedor,
 parent_channel as parent_channel,
 trip_type as viaje,
 country as country,
 buy_type as buy_type,
 is_latam_destination as LATAM,
 hotel as Hotel,
 sum(gb_gestion_gc) as GB
 from bt_detail 
 Where gateway_code in ('EXP')
 and country = 'AR'
 group by 1,2,3, 4, 5, 6, 7, 8
 order by 1
 limit 100