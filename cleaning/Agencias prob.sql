with bo_status as (
	select s.transaction_id as tx_code
	       ,p.transaction_id as product_id_original
	       ,cast(p.cancelled as date) as cancelled
	       ,p.status as bo_status
	from data.lake.channels_bo_product p
	join data.lake.channels_bo_sale s on s.id = p.sale_id
	where s.created >= date('2023-01-01') 
	and s.channel in (select cl.channel from raw.b2b_dim_channel_by_lob cl where cl.lob = 'B2B')
	--and s.transaction_id = 373427279000
),
estado_producto as (
    select s.product_id
    	  ,s.origin_product_id
          ,cast(s.transaction_code as varchar) as tx_code
          ,s.pnr_code
          ,s.gestion_date
          ,bo.cancelled
          ,s.main_airline_code
          ,s.route_code
          ,s.gateway_code
          ,s.supplier_code
          ,s.payment_type
          ,s.product_status
          ,bo.bo_status
          ,s.gb
          ,s.cost
          ,s.confirmation_gradient
          ,s.exchange_rate_in_local_currency as xr
    from analytics.bi_sales_fact_sales_recognition s
    left join bo_status bo on bo.product_id_original = s.origin_product_id
    where s.partition_period > '2024-01'
    and s.line_of_business_code = 'B2B'
    and s.booking_date >= date'2024-01-01'
),
agencias as (
	select 
		case when channel.channel_name = 'expedia' then 'Expedia' else p.partner_code end as agency_code 
		,max(p.name) as fantasy_name
		--l.legal_name,
		--channel.channel_name, 
		,max(p.country) as agency_country
		,max(p.type) as agency_type
		,max(p.segment) as agency_segment
	    --p.status
	from data.lake.ch_bo_partner_partner p
	inner join data.lake.ch_bo_partner_channel channel on p.id = channel.id_partner 
	left join data.lake.ch_bo_partner_legal_info l on l.partner_id = p.id
	where channel.channel_name  in (select cl.channel from raw.b2b_dim_channel_by_lob cl where cl.lob in ('B2B'))
	group by 1
)
SELECT 
		/*keys*/
 		 tx.transaction_code as tx_code
        ,pr.product_id as product_id
        /*dimensiones*/
        ,case when tx.line_of_business = 'unknown' then cl.lob else tx.line_of_business end as lob
        ,cl.brand
        ,tx.site as site
        ,tx.country_code
        ,case tx.site when 'Mexico' 	then 'MX' --'01-Mexico'
		              when 'Brasil' 	then 'BR' --'02-Brasil'
		              when 'Argentina' 	then 'AR' --'03-Argentina'
		              when 'Chile' 		then 'CL' --''04-Chile'
  		              when 'Colombia' 	then 'CO' --'05-Colombia'
  		              when 'Peru' 		then 'PE' --'06-Peru'
  		              else 'Global'			  
             end as region
        ,coalesce(tx.parent_channel, cl.parent_channel) as parent_channel
        ,tx.channel as channel
        ,cl.canal_venta as sale_channel
        /*Agencias*/         
        ,case when tx.channel = 'expedia' then 'Expedia' else tx.partner_data_id end as agency_code
        ,a.fantasy_name as agency_name
        ,a.agency_segment
        /*status*/
        ,ea.product_status as current_product_status
		,ea.bo_status
        /*fechas*/
    	,tx.reservation_date
		,ea.cancelled as cancel_date
		,pr.checkin_date
    	,pr.checkout_date
         /*producto*/
		,pr.trip_type
		,tx.purchase_type as original_product
		,pr.product_type as product	
	    ,case when pr.product_type = 'Vuelos' then concat(pr.flight_validatin_carrier,' ',pr.origin_city_code,'>',pr.destination_city_code)	     
                 when pr.product_type IN ('Hoteles','Alquileres') then pr.hotel_name	       
                 when pr.product_type IN ('Excursiones','Traslados','Circuito') then pr.destination_service_service_name 	        
                 else pr.product_type	    
             end as detail
		,pr.hotel_despegar_id as hotel_id
		,pr.hotel_chain_brand_name as hotel_chain
		,pr.hotel_contract_type
		,pr.flight_validatin_carrier as iata
		,al.airline_name
		,ea.supplier_code
		,ea.gateway_code as gateway
		,ea.payment_type
		,pr.is_refundable_flg
		,UPPER(CASE when COALESCE(pr.effective_rate,pr.flight_fare_type) like '%advanced_purchase%' then 'CUPOS'
							when position(',' in pr.effective_rate) > 0 THEN 'COMBINED' 
							else replace(COALESCE(pr.effective_rate,pr.flight_fare_type),'advanced_purchase','CUPOS') 
						END) as effective_rate
		/*geografia*/
		,split_part(pr.destination_city,',',1) as destination_city
		,split_part(pr.destination_city,', ',2) as destination_country		
		/*Metricas*/
    	,sum(pr.total_passengers_quantity) as pax
    	,sum(c.total) as gb_usd
    	,sum(case when year(tx.reservation_date)=2024 and tx.brand='Best Day' and tx.purchase_type='Hoteles' then c.total * (1-ngc.gradient)
                  when year(tx.reservation_date)=2023 and tx.brand='Best Day' and tx.purchase_type='Hoteles' then c.total * (1-gc.gradient)
                  when tx.brand='Despegar' and tx.site='Argentina' and ea.product_status='Confirmado' then (c.gross_booking-c.perceptions-c.tax_pais) --tx.gb_sin_fisco_usd
                  when tx.brand='Despegar' and tx.purchase_type='Hoteles' and ea.product_status='Confirmado' then c.total
                  when tx.purchase_type<>'Hoteles' and ea.product_status='Confirmado' then c.total
                 else 0
        	   end) as gb_gc
       	,max(case when year(tx.reservation_date)=2024 and tx.brand = 'Best Day' and tx.purchase_type = 'Hoteles' then (1-ngc.gradient) 
       	          when year(tx.reservation_date)=2023 and tx.brand = 'Best Day' and tx.purchase_type = 'Hoteles' then (1-gc.gradient)      
       	          else 1 
       	     end ) as gradient	   
       	,max(case when tx.brand = 'Best Day' and tx.purchase_type = 'Hoteles' then big.confirmado else '1' end) as bi_gradient     
    
from analytics.bi_transactional_fact_products pr
left join analytics.bi_transactional_fact_transactions tx on tx.transaction_code = pr.transaction_code and  tx.reservation_year_month >= date'2024-01-01'
inner join raw.b2b_dim_channel_by_lob cl on cl.channel = tx.channel and cl.lob = 'B2B'
left join analytics.bi_transactional_fact_charges c on pr.product_id = c.product_id and c.reservation_year_month >= date'2024-01-01'
left join estado_producto ea on ea.product_id = pr.product_id
left join raw.seed_b2b__airlines al on al.airline_iata = pr.flight_validatin_carrier
left join data.lake.b2b_gb_gradient_2023 gc on year(tx.reservation_date) = 2023 and gc.canal = cl.canal_venta and gc.site = tx.site 
left join data.lake.b2b_gb_gradient_2024 ngc on ngc.canal = cl.canal_venta and year(tx.reservation_date) = ngc.anio and month(tx.reservation_date) = ngc.mes
			and ngc.site = case when tx.site in ('Mexico','Brasil','Colombia','Usa','Panama','Ecuador','Argentina','Peru','Chile','Uruguay') then tx.site else 'Otros' end
left join raw.bi_input_pnl_b2b_gradient_gb big on cl.canal_venta = case when big.channel = 'hoteldo-api' then 'API' else 'Afiliadas' end 
        and year(tx.reservation_date) = cast(big.anio as int) and month(tx.reservation_date) = cast(big.mes as int) 
        and case when tx.country_code in ('AR','BR','CL','CO','EC','MX','PA','PE','UY','US') then tx.country_code else 'OT' end = big.pais_codigo 

left join agencias a on a.agency_code = case when tx.channel = 'expedia' then 'expedia' else tx.partner_data_id end 			
where pr.reservation_year_month >= date'2024-01-01'
and tx.reservation_date between {{fecha_ini}} and {{fecha_fin}}
and country_code = 'MX'
---and sale_channel = "Afiliadas"
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35
limit 100