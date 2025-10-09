---------------------------------------------------------------------------
------------------ V2 por RI + variable filtro Planning Financiero (pea.product_state) -> Analisis Expdia as a Client QBR Q1 2025 --------------------------------------

with bt_detail as (
               select
                     --fv.transaction_code as tx_code
                 	--,fv.product_id
                   	--,fv.origin_product_id
                   	fv.line_of_business_code as lob
                   	,fv.brand as brand     
                   	,pea.product_state as product_state
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
                   	,fv.channel as channel
                   	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
                   	,fv.product_status as product_status
                   	,fv.product_is_confirmed_flg as is_confirmed_flg
                   	,fv.trip_type_code as trip_type
                   	,fv.buy_type_code as buy_type
                   	,pr.effective_rate as tarifa
                   	,fv.product
       /*        	,case when pr.product_type = 'Vuelos' then concat(pr.flight_validatin_carrier,' ',pr.origin_city_code,'>',pr.destination_city_code)	     
                          when pr.product_type IN ('Hoteles','Alquileres') then pr.hotel_name	       
                          when pr.product_type IN ('Excursiones','Traslados','Circuito') then pr.destination_service_service_name 	        
                            else pr.product_type	    
                        end as detail */
                   	,gateway_code
                   	,split_part(destination,', ',2) as destination_country	
                   	, pr.is_latam_destination_flg
                   	, pr.is_latam_destination     	
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.gestion_date as gestion_date
           			,fv.recognition_date as recognition_date
           			,fv.confirmation_date
           			,sum(fv.gestion_gb) as gb_RI 
           		   ,max(pnl.b2b_gradient_margin) as gradiente_margen
                    ,(sum(pnl.commission_net_usd)/sum(ch.comision_desp)) as gradiente_margen_calc
           			  ,sum(pnl.fee_net_usd) as fee_neto
 					,sum(pnl.commission_net_usd) as comision_neta
					,-sum(pnl.discounts_net_usd) as descuentos_neto
					,sum(pnl.net_revenues_usd) as net_revenues_s_fix
                    ,sum(pnl.npv_net_usd) as npv_s_fix
					,SUM(
  					CASE 
    				WHEN fv.country_code = 'BR' AND fv.product NOT IN ('Vuelos') THEN (pnl.net_revenues_usd - pnl.affiliates_usd)
   					 WHEN fv.channel = 'expedia' THEN (pnl.net_revenues_usd - pnl.affiliates_usd)
   					 ELSE pnl.net_revenues_usd
  					END
						) AS fix_net_revenues
           			 ,max(pr.hotel_name) as Hotel
           			 ,max(pr.hotel_despegar_id) as hotelid 
           			 ,max(pr.hotel_chain_name) as Cadena
           	--		 ,max(a.market) as hotel_market
            --         ,max(a.area) as hotel_area
            --         ,max(a.tipo_de_cuenta) as hotel_category
           			--,fv.booking_date
           			--,fv.checkin_date
           			--,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
          			,max(fv.confirmation_gradient) as gradient
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
           WHERE fv.recognition_date >= DATE '2024-01-01'
           AND fv.recognition_date <= DATE '2025-12-31'
           --  and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period > '2021-01'
            AND (pea.product_state IS NULL OR pea.product_state IS NOT NULL)
           --   and fv.gestion_date < CURRENT_DATE	
              group by 1,2,3,4,5,6 ,7,8,9,10,11,12,13,14,15,16,17,18,19,20--,18,19,20,21 --,22,23,24
 )
 select 
 gestion_date as Fecha_venta,
 recognition_date as Fecha_reconocimiento,
 confirmation_date AS confirmation_date,
 product_state as product_state,
 gateway_code as Proveedor,
 parent_channel as parent_channel,
 trip_type as viaje,
 tarifa as tarifa,
 destination_country AS pais_destino,
 site as site,
 buy_type as buy_type,
 product as product,
 is_latam_destination as LATAM,
 hotel as Hotel,
 Cadena,
-- detail as detail,
 agency_code as agency_code,
 product_status as product_satus,
 is_confirmed_flg as is_confirmed_flg,
--hotel_market,
--hotel_area,
--hotel_category,
 hotelid as hotelid,
 brand as Marca,
 max(gradient) as gradiente,
 sum(bookings) as bookings,  
 ROUND(sum(gb_RI ),2) as gb_RI,
    ROUND(SUM(fee_neto), 2) AS fee_net_usd,
    ROUND(SUM(comision_neta), 2) AS commission_net_usd,
    ROUND(SUM(descuentos_neto), 2) AS discounts_net_usd,
    ROUND(SUM(net_revenues_s_fix), 2) AS net_revenues_s_fix,
    ROUND(SUM(npv_s_fix), 2) AS npv_s_fix,
    ROUND(cast(fix_net_revenues as DECIMAL(18,4)) / cast(gradiente_margen as DECIMAL(18,4)),2) as fix_net_revenues,
    ROUND(cast(npv_s_fix as DECIMAL(18,4)) / cast(gradiente_margen as DECIMAL(18,4)),2) as fix_fvm,
SUM(fee_agencia) AS fee_agencia,
SUM(comision_agencia_channels) AS comision_agencia_channels,
SUM(comision_agencia) AS comision_agencia,
SUM(backend_air) AS backend_air,
SUM(backend_nonair) AS backend_nonair,
SUM(other_incentives_air) AS other_incentives_air,
SUM(other_incentives_nonair) AS other_incentives_nonair,
SUM(breakage_revenue) AS breakage_revenue,
SUM(media_revenue) AS media_revenue,
SUM(mkt_discounts) AS mkt_discounts,
SUM(ccp) AS ccp,
SUM(coi) AS coi,
SUM(interes_coi) AS interes_coi,
SUM(customer_service) AS customer_service,
SUM(errors) AS errors,
SUM(afiliadas) AS afiliadas,
SUM(frauds) AS frauds,
SUM(loyalty) AS loyalty,
SUM(ott) AS ott,
SUM(revenue_tax) AS revenue_tax,
SUM(cancelaciones) AS cancelaciones,
SUM(customer_claims) AS customer_claims,
SUM(revenue_sharing) AS revenue_sharing,
SUM(vendor_commission) AS vendor_commission,
SUM(mkt_cost) AS mkt_cost,
SUM(overs_api) AS overs_api
 from bt_detail 
 --where UPPER(channel) = 'EXPEDIA'
 --and  gestion_date >= date('2025-04-01')
-- and gradient = 1
-- and brand = 'Best Day'
 --and buy_type = 'Hoteles'
-- Where gateway_code in ('HBG', 'EXP')
 --and country = 'USA'
 group by 1,2,3, 4, 5, 6, 7, 8, 9,10, 11, 12,13,14,15,16,17,18,19,20, 29,30--,14,15,16
 order by 2 ASC
 --limit 1000
 
-----------------------------------------------------------------------------------------------------------------------------------------------

 ---- Cartera de Hoteles Guille con tipo_de_cuenta
 
 select distinct
            hotel_id,
            a.nombre_hotel,
            if(a.cadena is null, p.hotel_chain_name, a.cadena)    as cadena,
            market,
            area,
            tipo_de_cuenta,
            anio_semana
            --a.destino
        from data.lake.bi_sourcing_cartera_alojamiento a
        left join analytics.bi_transactional_fact_products p on a.hotel_id = cast(p.hotel_despegar_id as varchar)
        where anio_semana is not null 
        --and a.hotel_id in ('1631470','313486','312469','1631477','316485','1871123','214139','1631555','316774','222993','214577','342274','4832448','345181','347875','2169138','278533','1871464','1487777','4962455')
        and reservation_year_month is not null
            and (hotel_id, fecha_actualizacion) in (select hotel_id, max(fecha_actualizacion) as fecha_actualizacion 
                                                     from data.lake.bi_sourcing_cartera_alojamiento 
                                                     where anio_semana is not null 
                                                     group by 1)
limit 100
 
 -----------------------------------------------------------------------------------------------------------------------------------------------

---- Cartera de Agencias
--- Query del Sales Tracking (Dash) de Vic

with agencias as (    
     select 
        case when channel.channel_name = 'expedia' then 'Expedia' else p.partner_code end as agency_code    
       ,max(p.partner_code) as partner_code
       ,max(case when channel.channel_name = 'expedia' then 'Expedia' else p.name end) as fantasy_name      
       ,max(channel.channel_name) as channel 
       ,max(p.country) as market
       ,max(p.type) as type
       ,max(p.segment) as segment
       ,max(p.status) as status
       ,max(cl.lob) as lob
    from data.lake.ch_bo_partner_partner p
    inner join data.lake.ch_bo_partner_channel channel on p.id = channel.id_partner 
    inner join raw.b2b_dim_channel_by_lob cl on cl.channel = channel.channel_name and cl.lob = 'B2B'
    group by 1
),
seed_kams as (
    select 
        seed.agency_code as ag_code,
        max(seed.director) as director,
        max(coalesce(seed.manager, 'SC')) as manager,
        max(seed.kam) as kam
   --     max(coalesce(seed.mail_kam, 'NA')) as mail_kam
    from raw.seed_b2b_kams seed
    group by agency_code
)
select  
     ag.agency_code
    ,max(ag.fantasy_name) as agency_name
    ,max(ag.market) as market
    ,max(case when ag.market in ('BR') then 'Marcio Nogueira'
          when ag.market in ('AR','CO','CL','MX','PE','DO') then 'Gaston Carne'
          when ag.market in ('PA','US','UY','CR','EC') then 'Veronica Odetti'
          else 'NA'
     end) as lead      
    ,max(case when k.manager <> 'SC' then k.manager
          when k.director = 'Marcio Nogueira' and ag.type = 'API' then 'Aline Sobreira'
          when k.director = 'Veronica Odetti' then k.kam          
          when k.director not in ('Marcio Nogueira','Gaston Carne') then k.director
          else 'SC'    
       end) as manager
    ,max(case when k.kam like '%Casimi%' then 'Aline Sobreira' else k.kam end) as kam
    --,max(k.mail_kam) as mail_kam
    ,max(ag.segment) as segment
        ,max(ag.status) as ag_status
    ,max(ag.type) as type
    ,max(coalesce(ac.agency_group_code, ag.agency_code)) as agency_group_code
    ,max(coalesce(ac.agency_group_name, ag.fantasy_name)) as agency_group_name     
from agencias ag
left join seed_kams k on k.ag_code = ag.agency_code
left join raw.b2b_dim_cluster_agencies ac on ag.agency_code = ac.agency_code
--where coalesce(ac.agency_group_name, ag.fantasy_name) like '%Azabache%'
--and ag.fantasy_name like '%Azabache%'
--where coalesce(ac.agency_group_code, ag.agency_code) = 'AG00015637'
group by 1
--limit 100


------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------

---Query Financial Planning:

select
p.is_latam_destination_flg,
p.reservation_date as fecha_creacion,
p.product_confirmation_date as fecha_confirmacion,
p.checkin_date as fecha_checkin,
p.destination_country_code as "Destino.CodigoPais",
p.destination_city_code as "Destino.Codigo",
p.trip_type as Tipo_Viaje,
-- información para cruzar con contabilidad -- 
p.transaction_code as codigo_transaccion,
p.pnr,
p.reference_id as transaction_code_producto,
p.product_type as producto,
t.purchase_type as Prod_Original,
p.payment_type as TipoDePago,
t.site as Site,
p.status as Estado,
p.gateway as Gateway,
t.channel as Channel,
p.provider_code as Id_Proveedor,
p.provider_description as Nombre_Proveedor,
--pea.fechafinalizacion as fecha_Cancel,
--Calulo de las variables de Analisis de ventas:
sum(c.gross_booking) as "GB (USD)",
sum(c.flight_fare) as "Tarifa (USD)",
sum(c.commission) as "Comision Neta",
sum(c.cost) as "Costo (USD)",
sum(c.discount) as "Descuentos (USD)",
sum(c.fee) as "Fee + Impuestos (USD)",
sum((c.fee + c.commission) * 0.252) as comisionEXPCliente,
count (distinct p.transaction_code) as Booking,
sum (sum(c.commission)) over (ORDER BY p.pnr ASC) as Comision_Acumulada,
sum (sum((c.fee + c.commission) * 0.252)) over (ORDER BY p.pnr ASC) as comisionEXPCliente_Acumulada,
ROW_NUMBER() OVER(ORDER BY p.pnr ASC) AS Row
from analytics.bi_transactional_fact_products p
left join analytics.bi_transactional_fact_transactions t on p.transaction_code = t.transaction_code
inner join analytics.bi_transactional_fact_charges         c on p.product_id = CAST(c.product_id as varchar)
left join analytics.bi_transactional_fact_products_current_state pea on p.product_id = pea.product_id
where 
--p.gateway in ('EXP')
p.status in ('Confirmado','Activo')
and p.is_confirmed_flg = 1
AND pea.product_state is null
and p.product_confirmation_date >= cast ('2025-01-01' as date) --Filtro fecha por Confirmacion
and p.product_confirmation_date <= cast ('2025-03-31' as date) --Filtro fecha por Confirmacion
and p.reservation_year_month is not null
and t.reservation_year_month is not null
and c.reservation_year_month is not null
and t.channel LIKE '%exp%'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
order by row desc

--- Resumen por medida

SELECT
    SUM(c.gross_booking) AS "GB (USD)",
    SUM(c.flight_fare) AS "Tarifa (USD)",
    SUM(c.commission) AS "Comision Neta",
    SUM(c.cost) AS "Costo (USD)",
    SUM(c.discount) AS "Descuentos (USD)",
    SUM(c.fee) AS "Fee + Impuestos (USD)",
    SUM((c.fee + c.commission) * 0.252) AS comisionEXPCliente,
    COUNT(DISTINCT p.transaction_code) AS Booking
FROM analytics.bi_transactional_fact_products p
LEFT JOIN analytics.bi_transactional_fact_transactions t 
  ON p.transaction_code = t.transaction_code
INNER JOIN analytics.bi_transactional_fact_charges c 
  ON p.product_id = CAST(c.product_id AS VARCHAR)
LEFT JOIN analytics.bi_transactional_fact_products_current_state pea 
  ON p.product_id = pea.product_id
WHERE 
    p.status IN ('Confirmado', 'Activo')
   AND p.is_confirmed_flg = 1
   and pea.product_state is null ----------------------------------------- *Filtro que genera diferencias con nostros
   and p.product_confirmation_date >= CAST('2024-10-01' AS DATE)
    AND p.product_confirmation_date <= CAST('2024-12-31' AS DATE)
    AND p.reservation_year_month IS NOT NULL
    AND t.reservation_year_month IS NOT NULL
    AND c.reservation_year_month IS NOT null
 --   and t.brand = 'Despegar'
  --  and t.line_of_business = 'B2B'
   AND t.channel LIKE '%exp%';


   
   --
   
   select * 
   from analytics.bi_transactional_fact_products_current_state pea 
   limit 1000
   
   select distinct pea.product_state
    from analytics.bi_transactional_fact_products_current_state pea 
    
     select * 
     from analytics.bi_transactional_fact_transactions t
     where t.reservation_year_month is not NULL
     and t.brand = 'Despegar'
     and t.line_of_business = 'B2B'
     limit 1000
   
  --------- Resumen query nuestra
with bt_detail as (
               select
                     --fv.transaction_code as tx_code
                 	--,fv.product_id
                   	--,fv.origin_product_id
                   	fv.line_of_business_code as lob
                   	,fv.brand as marca                	
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
                   	,fv.channel as channel
                   	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
                   	,fv.product_status as product_status
                   	,fv.product_is_confirmed_flg as is_confirmed_flg
                   	,fv.trip_type_code as trip_type
                   	,fv.buy_type_code as buy_type
                   	,fv.product
       /*        	,case when pr.product_type = 'Vuelos' then concat(pr.flight_validatin_carrier,' ',pr.origin_city_code,'>',pr.destination_city_code)	     
                          when pr.product_type IN ('Hoteles','Alquileres') then pr.hotel_name	       
                          when pr.product_type IN ('Excursiones','Traslados','Circuito') then pr.destination_service_service_name 	        
                            else pr.product_type	    
                        end as detail */
                   	,gateway_code
                   	,split_part(destination,', ',2) as destination_country	
                   	, pr.is_latam_destination_flg
                   	, pr.is_latam_destination     	
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.gestion_date as gestion_date
           			,fv.recognition_date as recognition_date
           			,fv.confirmation_date as confirmation_date
           			,sum(fv.gestion_gb) as gb_RI 
           		   ,max(pnl.b2b_gradient_margin) as gradiente_margen
                    ,(sum(pnl.commission_net_usd)/sum(ch.comision_desp)) as gradiente_margen_calc
           			  ,sum(pnl.fee_net_usd) as fee_neto
 					,sum(pnl.commission_net_usd) as comision_neta
					,-sum(pnl.discounts_net_usd) as descuentos_neto
					,sum(pnl.net_revenues_usd) as net_revenues_s_fix
                    ,sum(pnl.npv_net_usd) as npv_s_fix
                    ,sum( case when fv.country_code = 'BR' and fv.product not in ('Vuelos')
                           		then (pnl.net_revenues_usd-pnl.affiliates_usd)
                           else pnl.net_revenues_usd
                       end) as fix_net_revenues
           			 ,max(pr.hotel_name) as Hotel
           			 ,max(pr.hotel_despegar_id) as hotelid 
           			 ,max(pr.hotel_chain_name) as Cadena
           	--		 ,max(a.market) as hotel_market
            --         ,max(a.area) as hotel_area
            --         ,max(a.tipo_de_cuenta) as hotel_category
           			--,fv.booking_date
           			--,fv.checkin_date
           			--,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
          -- 			,max(fv.confirmation_gradient) as gradient
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
           WHERE fv.recognition_date >= DATE '2024-01-01'
           AND fv.recognition_date <= DATE '2025-12-31'
           --  and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period > '2021-01'
              and fv.product_is_confirmed_flg = 1
              AND (pea.product_state IS NULL OR pea.product_state IS NOT NULL)
           --   and fv.gestion_date < CURRENT_DATE	
              group by 1,2,3,4,5,6 ,7,8,9,10,11,12,13,14,15,16,17,18--,18,19,20,21 --,22,23,24
 )
 select 
 /*gestion_date as Fecha_venta,
 recognition_date as Fecha_reconocimiento,
 confirmation_date AS confirmation_date,
 gateway_code as Proveedor,
 parent_channel as parent_channel,
 trip_type as viaje,
 destination_country AS pais_destino,
 site as site,
 buy_type as buy_type,
 product as product,
 is_latam_destination as LATAM,
 hotel as Hotel,
 Cadena,
-- detail as detail,
 agency_code as agency_code,
 product_status as product_satus,
 is_confirmed_flg as is_confirmed_flg,
--hotel_market,
--hotel_area,
--hotel_category,
 hotelid as hotelid,
 brand as Marca,*/
 --max(gradient) as gradiente,
 sum(bookings) as bookings,  
 ROUND(sum(gb_RI ),2) as gb_RI,
    ROUND(SUM(fee_neto), 2) AS fee_net_usd,
    ROUND(SUM(comision_neta), 2) AS commission_net_usd,
    ROUND(SUM(descuentos_neto), 2) AS discounts_net_usd,
    ROUND(SUM(net_revenues_s_fix), 2) AS net_revenues_s_fix,
    ROUND(SUM(npv_s_fix), 2) AS npv_s_fix
   -- ROUND(cast(fix_net_revenues as DECIMAL(18,4)) / cast(gradiente_margen as DECIMAL(18,4)),2) as fix_net_revenues,
   -- ROUND(cast(npv_s_fix as DECIMAL(18,4)) / cast(gradiente_margen as DECIMAL(18,4)),2) as fix_fvm
 from bt_detail 
 where UPPER(channel) = 'EXPEDIA'
 and  recognition_date >= date('2024-10-01')
  and  recognition_date <= date('2024-12-31')
 -- and marca = 'Despegar'
 -- and lob = 'B2B'
  and product_status IN ('Confirmado', 'Activo')
 --group by 8,9--,14,15,16
 --order by 2 DESC

