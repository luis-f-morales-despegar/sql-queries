
---------------------------------------------------------------------------
------------------ V2 ---- 2025-06-17 se añaden '1' a columnas innecesarias | 2025-05-29 añadimos nr y fvm, se filtra producto hoteles (producto_original hoteles y carrito)

with bo_tpc as( 
            select
                p.transaction_id as product_id_original
                /*metricas*/
                ,max(p.net_commission_partner * p.conversion_rate) as tpc_usd --third party commission
                ,MAX(CAST(p.cancelled AS DATE)) AS cancelled
                ,MAX(
                   CASE 
                   WHEN COALESCE(p.status, '') = '' THEN 'ACTIVE'
                   ELSE p.status
                   END
                   ) AS bo_status
            from data.lake.channels_bo_product p
            join data.lake.channels_bo_sale s on s.id = p.sale_id
            where cast(s.created as date) >= DATE('2023-01-01') 
            and cast(s.created as date) < CURRENT_DATE
            group by 1
),
bt_detail as (
               select
                   bo.cancelled
                   ,bo.bo_status
                     --fv.transaction_code as tx_code
                 	--,fv.product_id
                   	--,fv.origin_product_id
                   	,fv.line_of_business_code as lob
                   	,fv.brand                   	
             --      	,case fv.site when 'Mexico' 	then '01-Mexico'
			   --           		  when 'Brasil' 	then '02-Brasil'
			       --       		  when 'Argentina' 	then '03-Argentina'
			     --         		  when 'Chile' 		then '04-Chile'
  			   --           		  when 'Colombia' 	then '05-Colombia'
  			 --             		  when 'Peru' 		then '06-Peru'
  			--              		  else '07-Global'			  
          --     			end as region
              		,fv.site	
           --    		,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
            --           		country_code,'OT') as country
                   	,fv.parent_channel
                   	--,fv.channel
                   	--,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
                   	--,fv.product_status
                   	--,fv.product_is_confirmed_flg as is_confirmed_flg
       --     ,cast(1 as varchar) as trip_type   --,fv.trip_type_code as trip_type
                   	,fv.buy_type_code as buy_type
                   	,fv.product
                   	--,case when pr.product_type = 'Vuelos' then concat(pr.flight_validatin_carrier,' ',pr.origin_city_code,'>',pr.destination_city_code)	     
                      --    when pr.product_type IN ('Hoteles','Alquileres') then pr.hotel_name	       
                        --  when pr.product_type IN ('Excursiones','Traslados','Circuito') then pr.destination_service_service_name 	        
                          --  else pr.product_type	    
                      --  end as detail
                   	,gateway_code
            --       ,cast(1 as varchar) as destination_country	--,split_part(destination,', ',2) as destination_country	
                   	, pr.is_latam_destination_flg
                   	, pr.is_latam_destination     	
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.gestion_date
           --			,cast(1 as varchar) as Hotel --,max(pr.hotel_name) as Hotel
           	--		 ,cast(1 as varchar) as hotelid --,max(pr.hotel_despegar_id) as hotelid 
           		--	 ,cast(1 as varchar) as Cadena --,max(pr.hotel_chain_name) as Cadena
           			--,cast(1 as varchar) as currency_code--,max(fv.currency_code) as currency_code
           	--		 ,max(a.market) as hotel_market
            --         ,max(a.area) as hotel_area
            --         ,max(a.tipo_de_cuenta) as hotel_category
           			--,fv.booking_date
           			--,fv.confirmation_date
           			--,fv.checkin_date
           			--,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
           			,max(fv.confirmation_gradient) as gradient
                    ,sum(fv.gestion_gb) as gb_gestion
                    ,sum(fv.gb_in_local_currency) as gb_in_local_currency
    --             ,cast(1 as varchar) as exchange_rate_in_local_currency   --,sum(fv.exchange_rate_in_local_currency) as exchange_rate_in_local_currency
                    ,sum(fv.gestion_gb * fv.confirmation_gradient) as gb_gestion_gc 
                    ,sum(case when fv.country_code = 'BR' and fv.product not in ('Vuelos') then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) ) )
        	                   when fv.channel = 'expedia' then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) ) )	
                               else pnl.net_revenues_usd
                            end
                        ) as fix_net_revenues
                     ,ROUND(sum(pnl.net_revenues_usd),2) as net_revenues_usd_s_ajuste
                     ,ROUND(sum(pnl.npv_net_usd),2) as fvm_net_usd
                     ,sum(((pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
        		             + prm.dif_fx_usd + prm.dif_fx_air_usd + prm.currency_hedge_usd + prm.currency_hedge_air_usd
        		             + pnl.affiliates_usd)    -- sumamos afiliadas
                           / if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) -- quitar gradiente
                         )
                         - coalesce(bo.tpc_usd,0)     -- quitar tpc (en sustitucion de afiliadas)
                        )
                         * max(if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)))) as fix_fvm 
                ,count(distinct fv.transaction_code) as bookings
              --  ,count(distinct pr.hotel_despegar_id) as hoteles
             from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month > '2023-12'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month > date'2023-12-31'
             left join analytics.bi_transactional_fact_products pr on pr.product_id = fv.product_id and pr.reservation_year_month > date'2023-12-31'
              left join analytics.bi_pnlop_fact_pricing_model prm on prm.product_id = fv.product_id and prm.date_reservation_year_month >= '2021-01'
           --  left join data.lake.bi_sourcing_cartera_alojamiento a on a.hotel_id = pr.hotel_despegar_id and a.anio_semana >= '2024-01'
             left join bo_tpc bo on bo.product_id_original = fv.origin_product_id
             where fv.gestion_date > date '2023-12-31' 
             --and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
             and pnl.line_of_business in ('B2B')
              and fv.partition_period > '2023-12'
              and fv.gestion_date < CURRENT_DATE	
              and fv.product = 'Hoteles'
              group by 1,2,3,4,5,6 ,7,8,9,10,11,12--,11,12,13--,15,16,17,18,19--,20,21 --,22,23,24
 )
 select 
 cancelled,
 bo_status,
 gestion_date as Fecha_venta,
 gateway_code as Proveedor,
 parent_channel as parent_channel,
-- trip_type as viaje,
-- destination_country AS pais_destino,
 site as site,
 buy_type as buy_type,
 product as product,
 is_latam_destination as LATAM,
-- hotel as Hotel,
-- Cadena,
--hotel_market,
--hotel_area,
--hotel_category,
-- hotelid as hotelid,
 brand as Marca,
-- currency_code as currency_code,
 max(gradient) as gradiente,
 ROUND(sum(gb_gestion),2) as gb_bruto,
-- ROUND(sum(gb_in_local_currency),2) as gb_in_local_currency,
 ROUND(sum(gb_gestion_gc),2) as GB,
 ROUND(sum(fix_net_revenues),2) as fix_net_revenues,
 ROUND(sum(net_revenues_usd_s_ajuste),2) as net_revenues_usd_s_ajuste,
 ROUND(sum(fvm_net_usd),2) as fvm_net_usd,
 ROUND(sum(fix_fvm),2) as fix_fvm,
 sum(bookings) as bookings
 --sum(hoteles) as hoteles
 from bt_detail 
 where  1=1
 and gestion_date > date('2023-12-31')
-- and gradient = 1
-- and brand = 'Best Day'
 --and buy_type = 'Hoteles'
 --and product = 'Hoteles'
-- Where gateway_code in ('HBG', 'EXP')
 --and site = 'Mexico'
group by 1,2,3, 4, 5, 6, 7, 8,9,10--, 9,10, 11, 12,13,14--,14,15,16
-- order by 1,6,3,2 ASC
 limit 1000
 
 
-----------------------------------------------------------------------------------------------------------------------------------------------



---------------------------------------------------------------------------
------------------ V2 ---- 2025-06-17 Version resumida --- se añade bo_statuts

with bo_tpc as( 
            select
                p.transaction_id as product_id_original
                /*metricas*/
                ,max(p.net_commission_partner * p.conversion_rate) as tpc_usd --third party commission
                ,MAX(CAST(p.cancelled AS DATE)) AS cancelled
                ,MAX(
                   CASE 
                   WHEN COALESCE(p.status, '') = '' THEN 'ACTIVE'
                   ELSE p.status
                   END
                   ) AS bo_status
            from data.lake.channels_bo_product p
            join data.lake.channels_bo_sale s on s.id = p.sale_id
            where cast(s.created as date) >= DATE('2023-01-01') 
            and cast(s.created as date) < CURRENT_DATE
            group by 1
),
bt_detail as (
               select
                   bo.cancelled
                   ,bo.bo_status
                     --fv.transaction_code as tx_code
                 	--,fv.product_id
                   	--,fv.origin_product_id
                   	,fv.line_of_business_code as lob
                   	,fv.brand                   	
             --      	,case fv.site when 'Mexico' 	then '01-Mexico'
			   --           		  when 'Brasil' 	then '02-Brasil'
			       --       		  when 'Argentina' 	then '03-Argentina'
			     --         		  when 'Chile' 		then '04-Chile'
  			   --           		  when 'Colombia' 	then '05-Colombia'
  			 --             		  when 'Peru' 		then '06-Peru'
  			--              		  else '07-Global'			  
          --     			end as region
              		,fv.site	
           --    		,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
            --           		country_code,'OT') as country
                   	,fv.parent_channel
                   	--,fv.channel
                   	--,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
                   	,fv.product_status
                   	,fv.product_is_confirmed_flg
       --     ,cast(1 as varchar) as trip_type   --,fv.trip_type_code as trip_type
                   	,fv.buy_type_code as buy_type
                   	,fv.product
                   	--,case when pr.product_type = 'Vuelos' then concat(pr.flight_validatin_carrier,' ',pr.origin_city_code,'>',pr.destination_city_code)	     
                      --    when pr.product_type IN ('Hoteles','Alquileres') then pr.hotel_name	       
                        --  when pr.product_type IN ('Excursiones','Traslados','Circuito') then pr.destination_service_service_name 	        
                          --  else pr.product_type	    
                      --  end as detail
                   	,gateway_code
            --       ,cast(1 as varchar) as destination_country	--,split_part(destination,', ',2) as destination_country	
                   	, pr.is_latam_destination_flg
                   	, pr.is_latam_destination     	
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.gestion_date
           --			,cast(1 as varchar) as Hotel --,max(pr.hotel_name) as Hotel
           	--		 ,cast(1 as varchar) as hotelid --,max(pr.hotel_despegar_id) as hotelid 
           		--	 ,cast(1 as varchar) as Cadena --,max(pr.hotel_chain_name) as Cadena
           			--,cast(1 as varchar) as currency_code--,max(fv.currency_code) as currency_code
           	--		 ,max(a.market) as hotel_market
            --         ,max(a.area) as hotel_area
            --         ,max(a.tipo_de_cuenta) as hotel_category
           			--,fv.booking_date
           			--,fv.confirmation_date
           			--,fv.checkin_date
           			--,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
           			,max(fv.confirmation_gradient) as gradient
                    ,sum(fv.gestion_gb) as gb_gestion
                    ,sum(fv.gb_in_local_currency) as gb_in_local_currency
    --             ,cast(1 as varchar) as exchange_rate_in_local_currency   --,sum(fv.exchange_rate_in_local_currency) as exchange_rate_in_local_currency
                    ,sum(fv.gestion_gb * fv.confirmation_gradient) as gb_gestion_gc 
                    ,sum(case when fv.country_code = 'BR' and fv.product not in ('Vuelos') then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) ) )
        	                   when fv.channel = 'expedia' then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) ) )	
                               else pnl.net_revenues_usd
                            end
                        ) as fix_net_revenues
                     ,ROUND(sum(pnl.net_revenues_usd),2) as net_revenues_usd_s_ajuste
                     ,ROUND(sum(pnl.npv_net_usd),2) as fvm_net_usd
                     ,sum(((pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
        		             + prm.dif_fx_usd + prm.dif_fx_air_usd + prm.currency_hedge_usd + prm.currency_hedge_air_usd
        		             + pnl.affiliates_usd)    -- sumamos afiliadas
                           / if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) -- quitar gradiente
                         )
                         - coalesce(bo.tpc_usd,0)     -- quitar tpc (en sustitucion de afiliadas)
                        )
                         * max(if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)))) as fix_fvm 
                   ,cast(1 as varchar) as bookings --,count(distinct fv.transaction_code) as bookings
             from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month > '2022-12'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month > date'2022-12-31'
             left join analytics.bi_transactional_fact_products pr on pr.product_id = fv.product_id and pr.reservation_year_month > date'2022-12-31'
              left join analytics.bi_pnlop_fact_pricing_model prm on prm.product_id = fv.product_id and prm.date_reservation_year_month >= '2021-01'
           --  left join data.lake.bi_sourcing_cartera_alojamiento a on a.hotel_id = pr.hotel_despegar_id and a.anio_semana >= '2024-01'
             left join bo_tpc bo on bo.product_id_original = fv.origin_product_id
             where 1=1
             and fv.gestion_date > date '2022-12-31' 
             --and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
             and pnl.line_of_business in ('B2B')
              and fv.partition_period > '2022-12'
              and fv.gestion_date < CURRENT_DATE	
         --     and fv.product = 'Hoteles'
              group by 1,2,3,4,5,6 ,7,8,9,10,11,12,13,14
 )
 select 
 gestion_date as Fecha_venta,
 gateway_code as Proveedor,
 parent_channel as parent_channel,
-- trip_type as viaje,
-- destination_country AS pais_destino,
 site as site,
 buy_type as buy_type,
 product as product,
 is_latam_destination as LATAM,
product_status,
product_is_confirmed_flg as is_confirmed_flg,
cancelled,
bo_status,
-- hotel as Hotel,	
-- Cadena,
--hotel_market,
--hotel_area,
--hotel_category,
-- hotelid as hotelid,
 brand as Marca,
-- currency_code as currency_code,
 max(gradient) as gradiente,
 ROUND(sum(gb_gestion),2) as gb_bruto,
-- ROUND(sum(gb_in_local_currency),2) as gb_in_local_currency,
 ROUND(sum(gb_gestion_gc),2) as GB,
 ROUND(sum(fix_net_revenues),2) as fix_net_revenues,
 ROUND(sum(net_revenues_usd_s_ajuste),2) as net_revenues_usd_s_ajuste,
 ROUND(sum(fvm_net_usd),2) as fvm_net_usd,
 ROUND(sum(fix_fvm),2) as fix_fvm
 from bt_detail 
 where  1=1
 and gestion_date > date('2022-12-31')
 and site in ('Usa', 'Panama', 'International', 'España')
-- and gradient = 1
-- and brand = 'Best Day'
 --and buy_type = 'Hoteles'
 --and product = 'Hoteles'
-- Where gateway_code in ('HBG', 'EXP')
 --and site = 'Mexico'
 group by 1,2,3, 4, 5, 6, 7, 8, 9, 10,11,12
 --order by 1,6,3,2 ASC
 limit 1000
 
 
-----------------------------------------------------------------------------------------------------------------------------------------------

  ---- Cartera Agencias Guille con tipo_de_cuenta
 
 select distinct *
        from data.lake.bi_sourcing_cartera_alojamiento a
        left join analytics.bi_transactional_fact_products p on a.hotel_id = cast(p.hotel_despegar_id as varchar)
        where anio_semana is not null 
        --and a.hotel_id in ('1631470','313486','312469','1631477','316485','1871123','214139','1631555','316774','222993','214577','342274','4832448','345181','347875','2169138','278533','1871464','1487777','4962455')
        and reservation_year_month is not null
        and (hotel_id, fecha_actualizacion) in (select hotel_id, max(fecha_actualizacion) as fecha_actualizacion 
        from data.lake.bi_sourcing_cartera_alojamiento 
        where anio_semana is not null 
        group by 1)
limit 0
 
 
 
 
 ---
 
 ---- Cartera Agencias Guille con tipo_de_cuenta
 
 select distinct
             hotel_id
            ,a.nombre_hotel as hotel_name
            ,if(a.cadena is null, p.hotel_chain_name, a.cadena)    as hotel_chain
            ,pais as country
            ,destino as destination
            ,market as market
            ,area as area
            ,tipo_de_cuenta as account_type
           -- ,anio_semana
            --,a.destino
        from data.lake.bi_sourcing_cartera_alojamiento a
        left join analytics.bi_transactional_fact_products p on a.hotel_id = cast(p.hotel_despegar_id as varchar)
        where anio_semana is not null 
        --and a.hotel_id in ('1631470','313486','312469','1631477','316485','1871123','214139','1631555','316774','222993','214577','342274','4832448','345181','347875','2169138','278533','1871464','1487777','4962455')
        and reservation_year_month is not null
            and (hotel_id, fecha_actualizacion) in (select hotel_id, max(fecha_actualizacion) as fecha_actualizacion 
                                                     from data.lake.bi_sourcing_cartera_alojamiento 
                                                     where anio_semana is not null 
                                                     group by 1)

 
 -----------------------------------------------------------------------------------------------------------------------------------------------
---V1 ---------------

with bt_detail as (
               select
                     --fv.transaction_code as tx_code
                 	--,fv.product_id
                   	--,fv.origin_product_id
                   	fv.line_of_business_code as lob
                   	,fv.brand                   	
             --      	,case fv.site when 'Mexico' 	then '01-Mexico'
			   --           		  when 'Brasil' 	then '02-Brasil'
			       --       		  when 'Argentina' 	then '03-Argentina'
			     --         		  when 'Chile' 		then '04-Chile'
  			   --           		  when 'Colombia' 	then '05-Colombia'
  			 --             		  when 'Peru' 		then '06-Peru'
  			--              		  else '07-Global'			  
          --     			end as region
              		,fv.site	
           --    		,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
            --           		country_code,'OT') as country
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
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month >= date'2024-11-01'
          --   left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fv.product_id and pr.date_reservation_year_month >= '2021-01'
             left join analytics.bi_transactional_fact_products pr on pr.product_id = fv.product_id and pr.reservation_year_month >= date'2024-11-01'
             where fv.gestion_date >= date '2024-11-01' 
             --and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
             and pnl.line_of_business in ('B2B')
              and fv.partition_period > '2024-01'
              group by 1,2,3,4,5,6 ,7,8,9,10,11,12,13--,15,16,17,18,19--,20,21 --,22,23,24
 )
 select 
 gestion_date as Fecha_venta,
 gateway_code as Proveedor,
 parent_channel as parent_channel,
 trip_type as viaje,
 site as site,
 buy_type as buy_type,
 is_latam_destination as LATAM,
 hotel as Hotel,
 sum(gb_gestion_gc) as GB
 from bt_detail 
-- Where gateway_code in ('HBG', 'EXP')
 --and country = 'USA'
 group by 1,2,3, 4, 5, 6, 7, 8
 order by 1
 limit 1000
 
 
 
 
 select *
 from analytics.bi_sales_fact_sales_recognition fv 
 where 1=1
 and partition_period > '2023-12-31'
 limit 100
 
 
 gb_in_local_currency
 exchange_rate_in_local_currency
 
 
 -----------------------------------------------------------------------------------------------------------------------------
 ----------- Union version "resumida" con Lead / Director ----------------------------------------------------------------------
 
 WITH 
-- 1) Cálculo de bo_tpc (igual que antes)
bo_tpc AS ( 
    SELECT
        p.transaction_id AS product_id_original,
        MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd,
        MAX(CAST(p.cancelled AS DATE)) AS cancelled,
        MAX(
           CASE 
             WHEN COALESCE(p.status, '') = '' THEN 'ACTIVE'
             ELSE p.status
           END
        ) AS bo_status
    FROM data.lake.channels_bo_product p
    JOIN data.lake.channels_bo_sale s 
      ON s.id = p.sale_id
    WHERE CAST(s.created AS DATE) >= DATE('2023-01-01') 
      AND CAST(s.created AS DATE) < CURRENT_DATE
    GROUP BY 1
),
-- 2) Detalle de hechos, ahora con agency_code
bt_detail AS (
    SELECT
        fv.gestion_date as Fecha_venta
        ,fv.gateway_code as Proveedor
        ,fv.parent_channel
        ,fv.site
        ,fv.buy_type_code AS buy_type
        ,fv.product
        ,pr.is_latam_destination_flg    AS is_latam_destination
        ,fv.product_status
        ,fv.product_is_confirmed_flg     AS is_confirmed_flg
        ,bo.cancelled
        ,bo.bo_status
        ,fv.brand as Marca
        ,MAX(fv.confirmation_gradient)   AS gradient
        ,SUM(fv.gestion_gb)              AS gb_bruto
        ,SUM(fv.gestion_gb * fv.confirmation_gradient) AS GB
   --     max(fv.confirmation_gradient) as gradient
   --     ,sum(fv.gestion_gb) as gb_gestion
        ,sum(fv.gb_in_local_currency) as gb_in_local_currency
    --  ,cast(1 as varchar) as exchange_rate_in_local_currency   --,sum(fv.exchange_rate_in_local_currency) as exchange_rate_in_local_currency
    --    ,sum(fv.gestion_gb * fv.confirmation_gradient) as gb_gestion_gc 
        ,sum(case when fv.country_code = 'BR' and fv.product not in ('Vuelos') then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) ) )
                    when fv.channel = 'expedia' then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) ) )	
                    else pnl.net_revenues_usd
                    end
                   ) as fix_net_revenues
        ,ROUND(sum(pnl.net_revenues_usd),2) as net_revenues_usd_s_ajuste
        ,ROUND(sum(pnl.npv_net_usd),2) as fvm_net_usd
        ,sum(((pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
                 + prm.dif_fx_usd + prm.dif_fx_air_usd + prm.currency_hedge_usd + prm.currency_hedge_air_usd
                 + pnl.affiliates_usd)    -- sumamos afiliadas
                 / if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) -- quitar gradiente
                   )
                 - coalesce(bo.tpc_usd,0)     -- quitar tpc (en sustitucion de afiliadas)
                   )
                  * max(if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)))) as fix_fvm 
        ,cast(1 as varchar) as bookings --,count(distinct fv.transaction_code) as bookings
        -- Aquí agregamos agency_code (anteriormente comentado)
        ,CASE 
          WHEN LENGTH(fv.partner_id) > 0 THEN fv.partner_id 
          ELSE fv.channel 
        END AS agency_code
    FROM analytics.bi_sales_fact_sales_recognition fv 
    LEFT JOIN analytics.bi_pnlop_fact_current_model pnl 
      ON fv.product_id = pnl.product_id 
      AND pnl.date_reservation_year_month > '2022-12'
    LEFT JOIN analytics.bi_transactional_fact_charges c 
      ON fv.product_id = c.product_id 
      AND c.reservation_year_month > DATE '2022-12-31'
    LEFT JOIN analytics.bi_transactional_fact_products pr 
      ON pr.product_id = fv.product_id 
      AND pr.reservation_year_month > DATE '2022-12-31'
    LEFT JOIN analytics.bi_pnlop_fact_pricing_model prm 
      ON prm.product_id = fv.product_id 
      AND prm.date_reservation_year_month >= '2021-01'
    LEFT JOIN bo_tpc bo 
      ON bo.product_id_original = fv.origin_product_id
    WHERE fv.gestion_date > DATE '2022-12-31'
      AND fv.gestion_date < CURRENT_DATE
      AND pnl.line_of_business = 'B2B'
      AND fv.partition_period > '2022-12'
    GROUP BY 
      fv.gestion_date, fv.gateway_code, fv.parent_channel, fv.site,
      fv.buy_type_code, fv.product, pr.is_latam_destination_flg,
      fv.product_status, fv.product_is_confirmed_flg,
      bo.cancelled, bo.bo_status, fv.brand,
      CASE WHEN LENGTH(fv.partner_id) > 0 THEN fv.partner_id ELSE fv.channel END
),
-- 3) Definición de la dimensión de agencias
agencias AS (
    SELECT 
        CASE 
          WHEN channel.channel_name = 'expedia' THEN 'Expedia' 
          ELSE p.partner_code 
        END AS agency_code,
        MAX(CASE 
              WHEN channel.channel_name = 'expedia' THEN 'Expedia' 
              ELSE p.name 
            END) AS fantasy_name,
        MAX(channel.channel_name) AS channel,
        MAX(p.country)          AS market,
        MAX(p.type)             AS type,
        MAX(p.segment)          AS segment,
        MAX(p.status)           AS status,
        MAX(cl.lob)             AS lob,
        MAX(
          CASE 
            WHEN p.country = 'BR' THEN 'Marcio Nogueira'
            WHEN p.country IN ('AR','CO','CL','MX','PE','DO') THEN 'Gaston Carne'
            WHEN p.country IN ('PA','US','UY','CR','EC') THEN 'Veronica Odetti'
            ELSE 'NA'
          END
        ) AS lead
    FROM data.lake.ch_bo_partner_partner p
    INNER JOIN data.lake.ch_bo_partner_channel channel 
      ON p.id = channel.id_partner 
    INNER JOIN raw.b2b_dim_channel_by_lob cl 
      ON cl.channel = channel.channel_name 
      AND cl.lob = 'B2B'
    GROUP BY 1
),
seed_kams AS (
    SELECT 
       agency_code AS ag_code, 
       MAX(director) AS director, 
       MAX(COALESCE(manager,'SC')) AS manager, 
       MAX(kam) AS kam
    FROM raw.seed_b2b_kams 
    GROUP BY 1
),
agencies_dim AS (
    SELECT  
        ag.agency_code,
        MAX(ag.fantasy_name)      AS agency_name,
        MAX(ag.market)            AS market,
        COALESCE(MAX(k.director), MAX(ag.lead)) AS lead,
        MAX(
          CASE 
            WHEN k.manager <> 'SC' THEN k.manager
            WHEN k.director = 'Marcio Nogueira' AND ag.type = 'API' THEN 'Aline Sobreira'
            WHEN k.director = 'Veronica Odetti' THEN k.kam
            WHEN k.director NOT IN ('Marcio Nogueira','Gaston Carne') THEN k.director
            ELSE 'SC'
          END
        ) AS manager,
        MAX(k.kam)                AS kam,
        MAX(ag.segment)           AS segment,
        MAX(ag.status)            AS ag_status,
        MAX(ag.type)              AS type,
        MAX(coalesce(ac.agency_group_code, ag.agency_code)) AS agency_group_code,
        MAX(coalesce(ac.agency_group_name, ag.fantasy_name)) AS agency_group_name
    FROM agencias ag
    LEFT JOIN seed_kams k 
      ON k.ag_code = ag.agency_code
    LEFT JOIN raw.b2b_dim_cluster_agencies ac 
      ON ag.agency_code = ac.agency_code
    GROUP BY ag.agency_code
)
-- 4) Unión final de hechos + dimensión
SELECT
    f.*,
    d.agency_name,
    d.market,
    d.lead        AS agency_lead,
    d.manager     AS agency_manager,
    d.kam         AS agency_kam,
    d.segment     AS agency_segment,
    d.ag_status   AS agency_status,
    d.type        AS agency_type,
    d.agency_group_code,
    d.agency_group_name
FROM bt_detail f
LEFT JOIN agencies_dim d
  ON f.agency_code = d.agency_code

 
 