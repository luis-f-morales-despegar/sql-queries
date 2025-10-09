---------------------------------------------------------------------------------
----------- 2025-08-11 Fact actuales detalle agencia  --------------------------------------------------

WITH bo_tpc AS (
  SELECT 
  p.transaction_id AS product_id_original,
  /* Métricas */
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd -- Third Party Commission
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE('2024-01-01') 
  AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY 1
)
  SELECT 
  gestion_date,
 -- fh.partner_id, 
  IF(tx.channel = 'expedia', 'Expedia', tx.partner_data_id) AS partner_id,     ---- se añade Expedia para que matche partner.id = reference.id de cartera Agencias_MB
  kam.partner_code as cartera_partner_code,
 -- kam.kam_name,
  --
case when fh.parent_channel = 'API' then pnl.brand else 'Best Day' end as brand,
  case when pnl.line_of_business = 'B2B2C' then 'B2B2C'
 	else fh.parent_channel
  end as parent_channel,
CASE
  WHEN fh.partner_id IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  WHEN fh.partner_id IN (
    'AP12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  WHEN fh.country_code IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN fh.country_code
  WHEN fh.country_code IN ('US','PA','ES','CR') THEN 'GL'
  WHEN fh.country_code IN ('UY','BO') THEN 'UY'
  ELSE 'GL'
END
AS country_code,
case when fh.buy_type_code in ('Hoteles', 'Vuelos', 'Carrito', 'Traslados' ) then  fh.buy_type_code  else 'DS' end AS productooriginal,
--  case when fh.buy_type_code in ('Vuelos') then  'Vuelos'  else 'Other' end AS productooriginal,
 ---------- METRICAS ----------------------------------
  ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb 
--   ,count(distinct bo.product_id_original) as bookings
  ,ROUND(
    SUM(
      CASE 
      WHEN fh.country_code = 'BR' AND fh.product NOT IN ('Vuelos') 
      THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                                      IF(pnl.b2b_gradient_margin = '1', 1, 
                                         CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))))
      WHEN fh.channel = 'expedia' 
      THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                                      IF(pnl.b2b_gradient_margin = '1', 1, 
                                         CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))))
      ELSE pnl.net_revenues_usd
      END
    )
    ,2) AS nr,
  --ROUND(SUM(pnl.npv_net_usd), 2) AS npv_net_usd,
  ROUND(
    SUM(
      (
        (pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
         + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
         + pnl.affiliates_usd) -- Sumamos afiliadas
        / IF(pnl.b2b_gradient_margin = '1', 1, 
             CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5))) -- Quitar gradiente
      ) 
      - COALESCE(bo.tpc_usd,0) -- Quitar TPC (en sustitución de afiliadas)
    ) 
    * MAX(IF(pnl.b2b_gradient_margin = '1', 1, 
             CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))) 
  ) AS fvm  
  ----- FUENTE Y LEFT JOINS ------
  FROM analytics.bi_sales_fact_sales_recognition fh 
  LEFT JOIN analytics.bi_pnlop_fact_current_model pnl 
  ON fh.product_id = pnl.product_id 
  AND pnl.date_reservation_year_month > '2023-12'
  LEFT JOIN analytics.bi_transactional_fact_charges c 
  ON fh.product_id = c.product_id 
  AND c.reservation_year_month >= DATE '2024-01-01'
  LEFT JOIN data.analytics.bi_transactional_fact_products prod 
  ON fh.product_id = prod.product_id 
  AND prod.reservation_year_month >= DATE '2024-01-01'
  left join data.analytics.bi_transactional_fact_transactions tx ON cast(fh.transaction_code as VARCHAR) = tx.transaction_code   -------
  and tx.reservation_year_month >= DATE '2024-01-01'
  LEFT JOIN analytics.bi_pnlop_fact_pricing_model pr 
  ON pr.product_id = fh.product_id 
  AND pr.date_reservation_year_month >= '2024-01'
  LEFT JOIN bo_tpc bo 
  ON bo.product_id_original = fh.origin_product_id
LEFT JOIN raw.cartera_b2b_b2b2c_test kam
  ON kam.partner_code = CASE 
                           WHEN fh.channel = 'expedia' THEN 'AG72472'
                           ELSE fh.partner_id
                         END
 AND kam.partition IS NOT null
 --- FILTROS GENERALES ----
  WHERE 1=1
  and fh.partition_period > '2023-12'
       and fh.gestion_date >= DATE('2024-01-01')
       AND month(fh.gestion_date) <= month(current_date)
        and fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
        AND pnl.line_of_business = 'B2B'
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
 --     and fh.channel = 'expedia' 
     -- and kam.partner_code is null
  GROUP BY 1,2,3,4,5,6,7
  limit 100
  




------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------

with sales_bo as (
	select s.transaction_id as tx_code
	       ,p.transaction_id as product_id_original
	       ,max(cast(p.cancelled as date)) as cancelled
	       ,max(if(p.status='','ACTIVE',p.status)) as bo_status
	       ,max(p.net_commission_partner * p.conversion_rate) as tpc_usd --third party commission
	from data.lake.channels_bo_product p
	join data.lake.channels_bo_sale s on s.id = p.sale_id
	where s.created >= date('2025-01-01') 
	and s.channel in (select cl.channel from raw.b2b_dim_channel_by_lob cl where cl.lob = 'B2B')
        group by 1,2
),
hoteles as (
	select pr.hotel_despegar_id as hotel_id
	    ,max(pr.hotel_name) as hotel_name
		,max(pr.hotel_chain_brand_name) as hotel_chain
	from analytics.bi_transactional_fact_products pr 
	where pr.reservation_year_month >= date'2025-01-01'
	group by 1
),
fact_cancel as (
    select
    --       s.product
          cl.canal_venta as channel
          ,s.site as site      
          ,s.booking_date
          --,s.product_status              
          ,bo.bo_status             
          ,round(sum(s.gb),2) as gb
    from analytics.bi_sales_fact_sales_recognition s
    join raw.b2b_dim_channel_by_lob cl on cl.channel = s.channel and cl.lob = 'B2B' and cl.brand = 'BestDay'
    left join sales_bo bo on bo.product_id_original = s.origin_product_id
    where s.partition_period >= '2025-01'
    and s.gestion_date >= date_add('month', -3, current_date) 
 --   and s.product = 'Hoteles'
    and s.line_of_business_code = 'B2B'
    group by 1,2,3,4
)
,cancel_summary as (
		select fv.channel, fv.site--, fv.product
			--,week(fv.booking_date) as semana
			,month(fv.booking_date) as mes
	  		,sum(fv.gb) as total_gb
	  		,sum(case when fv.bo_status = 'CANCELED' then gb else 0 end) as gb_x
  		from fact_cancel as fv
		group by 1,2,3
)
,cancel_rate_lm as(
		select channel, site--, product --, mes --, semana
			,round(avg(gb_x / total_gb), 4) as cancel_rate_gb
		from cancel_summary
		group by 1,2
)
,fact_sales as (
    select s.product_id
    	  ,s.origin_product_id
          ,cast(s.transaction_code as varchar) as tx_code
          ,'B2B' as lob
          ,if(cl.brand='BestDay','HotelDo',cl.brand) as brand
          ,s.site as site
          ,s.country_code
          ,case s.site when 'Mexico' 	then 'MX' --'01-Mexico'
		               when 'Brasil' 	then 'BR' --'02-Brasil'
		               when 'Argentina' then 'AR' --'03-Argentina'
		               when 'Chile' 	then 'CL' --''04-Chile'
  		               when 'Colombia' 	then 'CO' --'05-Colombia'
  		               when 'Peru' 		then 'PE' --'06-Peru'
  		               else 'Global'			  
             end as region
          ,coalesce(s.parent_channel, cl.parent_channel) as parent_channel
          ,s.channel
          ,cl.canal_venta as sale_channel
          ,tx.platform
          ,case when s.channel = 'expedia' then 'Expedia' else s.partner_id end as agency_code
          ,s.product_status
          ,s.gestion_date
          ,s.booking_date as reservation_date
      --    ,s.checkin_date
      --    ,s.checkout_date
      --    ,s.trip_type_code as trip_type
          ,s.buy_type_code as original_product
      --    ,s.product
     --     ,s.main_airline_code
    --      ,s.route_code
          ,s.gateway_code
          ,s.supplier_code
    --      ,s.payment_type
   --       ,split_part(s.destination,',',1) as destination_city
--	      ,split_part(s.destination,', ',2) as destination_country	
	--      ,s.currency_code
          /*Metricas*/    	      
          ,s.gestion_gb as gb_usd
          ,s.gb_in_local_currency as gb_ml
          ,s.gestion_gb * s.confirmation_gradient as gestion_gb_usd
          ,s.gb_in_local_currency * s.confirmation_gradient as gestion_gb_ml
          ,s.confirmation_gradient as bi_gradient
          ,s.cost
          ,s.exchange_rate_in_local_currency as xr
          --> Revenue Margin <--
          ,pnl.fee_net_usd as fee_neto
	 	  ,pnl.commission_net_usd as comision_neta
		  ,pnl.discounts_net_usd as descuentos_neto
		  ,pnl.net_revenues_usd
    	  --> Afiliadas <--				
    	  ,pnl.affiliates_usd as afiliadas
    	  ,pnl.b2b_gradient_margin
    	  ,(((pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
        		 + pri.dif_fx_usd + pri.dif_fx_air_usd + pri.currency_hedge_usd + pri.currency_hedge_air_usd
        		 + ( case when s.parent_channel = 'Agencias afiliadas' and s.buy_type_code in ('Hoteles','Alquileres') and s.product_is_confirmed_flg = 0    -- Fix COI CCP - Promesas de Pago
        		                        then (pnl.coi_usd + pnl.ccp_usd - pnl.financial_result_usd + coalesce(fpp.coi_fix_con_gradiente,-pnl.coi_usd+pnl.financial_result_usd) + coalesce(fpp.ccp_fix_con_gradiente,-pnl.ccp_usd)) 
        		                        else 0 end ) 
        		 + pnl.affiliates_usd)    -- sumamos afiliadas
                       / if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) -- quitar gradiente
                )
               - coalesce(s.tpc_fix_iva,0)     -- quitar tpc (en sustitucion de afiliadas)
             )
              * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3))) 
              as fix_npv 
    from ( select *
			   ,case when fv_prev.parent_channel = 'Agencias afiliadas' 
			   			then tpc_usd/(1+coalesce(cast(fix_iva.iva as decimal(5,5)),0)) 
			   			else tpc_usd 
			   	end as tpc_fix_iva 
		from analytics.bi_sales_fact_sales_recognition fv_prev
		left join sales_bo bo on bo.product_id_original = fv_prev.origin_product_id
		left join raw.b2b_dim_html_iva_fix fix_iva on fix_iva.country = fv_prev.country_code
		where fv_prev.partition_period >= '2025-01'
		) s    
    join analytics.bi_pnlop_fact_current_model pnl on s.product_id = pnl.product_id and pnl.date_reservation_year_month >= '2023-01'
    join analytics.bi_transactional_fact_transactions tx on tx.transaction_code = cast(s.transaction_code as varchar) and tx.reservation_year_month_period >= '2023-01'
    join raw.b2b_dim_channel_by_lob cl on cl.channel = s.channel and cl.lob = 'B2B'
    left join analytics.bi_pnlop_fact_pricing_model pri on pri.product_id = s.product_id and pri.date_reservation_year_month >= '2021-01'
    left join data.lake.b2b_fix_coi_ccp fpp on fpp.transaction_code = cast(s.transaction_code as varchar) -- Fix Promesas Pago
    where s.partition_period >= '2025-01'
    and s.gestion_date >= date'2025-01-01'
    and tx.reservation_year_month >= date'2025-01-01'
    and s.line_of_business_code = 'B2B'
  /*  and pnl.line_of_business = 'B2B'*/
),
ultimate_facts as (
  select 
         fv.product_id
        ,fv.tx_code
        ,max(fv.lob) as lob
        ,max(fv.brand) as brand
        ,max(fv.site) as site
        ,max(case when fv.country_code not in ('MX','BR','CO','AR','CL','PE') then 'Global' else fv.country_code end) as country_code
        ,max(fv.region) as region
        ,max(fv.parent_channel) as parent_channel
  --      ,max(fv.channel) as channel
        ,max(fv.sale_channel) as sale_channel
        ,max(fv.platform) as platform
--        ,max(fv.agency_code) as agency_code
--        ,max(fv.product_status) as current_product_status
        ,max(bo.bo_status) as bo_status
        ,max(fv.reservation_date) as reservation_date  
        ,max(fv.gestion_date) as gestion_date
--        ,max(bo.cancelled) as cancel_date
--        ,max(fv.checkin_date) as checkin_date
--        ,max(fv.checkout_date) as checkout_date
--        ,max(fv.trip_type) as trip_type
        ,max(fv.original_product) as original_product
 --       ,max(fv.product) as product
 --       ,max(case when pr.product_type = 'Vuelos' then concat(pr.flight_validatin_carrier,' ',pr.origin_city_code,'>',pr.destination_city_code)	     
 --             when pr.product_type IN ('Hoteles','Alquileres') then pr.hotel_name	       
 --             when pr.product_type IN ('Excursiones','Traslados','Circuito') then pr.destination_service_service_name 	        
 --             else pr.product_type	    
 --           end) as detail
 --       ,max(pr.hotel_despegar_id) as hotel_id
  	   -- ,max(h.hotel_chain) as hotel_chain
--	    ,max(pr.hotel_contract_type) as hotel_contract_type
--	    ,max(pr.duration) as duration
 --       ,max(pr.flight_validatin_carrier) as iata    
 --       ,max(al.airline_name) as airline_name
  --      ,max(fv.route_code) as route_code
 --       ,max(fv.supplier_code) as supplier_code
 --       ,max(fv.gateway_code) as gateway
 --       ,max(fv.payment_type) as payment_type
 --       ,max(if(pr.is_refundable_flg=1,'Yes','No')) as is_refundable_flg
 --       ,max(UPPER(CASE when COALESCE(pr.effective_rate,pr.flight_fare_type) like '%advanced_purchase%' then 'CUPOS'
--	  	    when position(',' in pr.effective_rate) > 0 THEN 'COMBINED' 
--		    else replace(COALESCE(pr.effective_rate,pr.flight_fare_type),'advanced_purchase','CUPOS') 
--	 	END)) as effective_rate
--	    ,max(fv.destination_city) as destination_city
--	    ,max(fv.destination_country) as destination_country
--	    ,max(fv.currency_code) as currency_code 
        /*Metricas*/    
--	    ,max(case when fv.reservation_date <= date_add('day',2,date_add('year',-1,current_date)) then 1 else 0 end) as sleep
  --      ,sum(pr.total_passengers_quantity) as pax
        ,sum(fv.gb_usd) as gb_usd
        ,sum(fv.gb_ml) as gb_ml
        ,sum(fv.gestion_gb_usd) as gestion_gb
        ,sum(fv.gestion_gb_ml) as gestion_gb_ml
        ,max(fv.bi_gradient) as bi_gradient
        ,sum(fv.cost) as cost
        ,avg(fv.xr) as xr
      	--> Revenue Margin <--
        ,sum(fv.fee_neto) as fee_neto        
    	,sum(fv.comision_neta) as comision_neta
	    ,sum(fv.descuentos_neto) as descuentos_neto
--	    ,sum( case when fv.country_code = 'BR' and fv.product not in ('Vuelos') then (fv.net_revenues_usd - (bo.tpc_usd * if(fv.b2b_gradient_margin = '1', 1, cast(fv.b2b_gradient_margin as decimal(4,3)) ) ) )
  --      	                   when fv.channel = 'expedia' then (fv.net_revenues_usd - (bo.tpc_usd * if(fv.b2b_gradient_margin = '1', 1, cast(fv.b2b_gradient_margin as decimal(4,3)) ) ) )	
    --                           else fv.net_revenues_usd
      --                      end
        --                ) as fix_net_revenues
        --> Afiliadas <--				
        ,sum(fv.afiliadas) as afiliadas
        ,sum(fv.fix_npv) as fix_npv
    --    ,avg(case when fv.checkin_date > current_date then coalesce(cx.cancel_rate_gb,0) else 0 end) as cancel_rate_fc     
        ,sum(c.fee) as c_fee
        ,sum(c.cost) as c_cost
    from fact_sales as fv
    join analytics.bi_transactional_fact_products pr on pr.product_id = fv.product_id and pr.reservation_year_month >= date'2025-01-01'
    left join analytics.bi_transactional_fact_charges c on pr.product_id = c.product_id and c.reservation_year_month >= date'2025-01-01'
    left join analytics.b2b_dim_airlines al on al.airline_iata = pr.flight_validatin_carrier
   -- left join hoteles h on h.hotel_id = pr.hotel_despegar_id
    left join sales_bo bo on bo.product_id_original = fv.origin_product_id
    left join cancel_rate_lm cx on cx.channel = fv.sale_channel and cx.site = fv.site --and cx.product = fv.product
--    where pr.hotel_despegar_id = '2867398'
    where 1=1
    --fv.checkin_date >= date'2024-01-01'
    group by 1,2 --,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
)
select *
--		,case when checkin_date > current_date 
	--			then gb_usd * cancel_rate_fc 
		--		else if(bo_status='CANCELED', gb_usd, 0)
--		end as gb_cx_fc
	-- 	,case when product = 'Vuelos' 
	-- 			then case when (case when c_cost > 0 then c_fee/c_cost else 0 end) > 0 --Markup > 0 --> Netas Negociadas
	-- 						then 'Netas Negociadas'
	-- 						else 'No Negociadas'
	-- 				 end		
	-- 			else 'No Aplica'
--	 	 end as flight_type_rate			 			
from ultimate_facts