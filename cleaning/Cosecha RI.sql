
 --------------------------------------------------------------------------------------------------------------------------------------------
 -------------------------------------------------------------------------------------------------------------------------------------------
 
 ----
 
 --Original RI + adecuaciones a terminología Budget / RR ---> Cosecha RI (*Paises original Vic) (actual 2025-04-28; sacamos gradiente de NR y FVM) -- Añadimos channel y sacamos varios dates
 ---Agregamos gateway 2025-05-27
 
 
 with bt_detail as (
               select
          --           fv.transaction_code as tx_code
           --       	,fv.product_id
           --        	,fv.origin_product_id
                   fv.line_of_business_code as lob
                   	,CASE 
   					 WHEN fv.brand = 'Despegar' THEN 'D!' 
    					WHEN fv.brand = 'Best Day' THEN 'BD!' 
    					ELSE NULL
						END AS Marca
			    ,CASE 
                      WHEN p.provider_code IN ('DESP', 'PAM') THEN 'PAM'
                      WHEN p.provider_code = 'EXP' THEN 'EXP'
                      WHEN p.provider_code = 'HBG' THEN 'HBG'
                    ELSE 'Other'
                     END AS gateway
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
                    ,CASE
  --  WHEN fv.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'Others'
  --  WHEN fv.partner_id IN ('P12212', 'AP11666') THEN 'Others'
  --  WHEN fv.partner_id IN ('AP12147', 'AP12854') THEN 'Others'
  --  WHEN fv.partner_id IN ('AP12509', 'AP11813') THEN 'Others'
  --  WHEN fv.partner_id = 'AP12158' THEN 'Others'
  --  WHEN fv.partner_id IN ('AP12213', 'AP11843') THEN 'Others'
  --  WHEN fv.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'Others'
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
                    ,CASE
           				WHEN fv.brand = 'Best Day' AND fv.parent_channel = 'API' THEN 'MAY'
    					WHEN fv.brand = 'Despegar' AND fv.parent_channel = 'API' THEN 'API'
    					WHEN (fv.brand = 'Best Day' OR fv.brand = 'Despegar') AND fv.parent_channel = 'Agencias afiliadas' THEN 'MIN'
    					ELSE NULL -- 
					END AS channel_metas
                  -- 	,fv.parent_channel
                  	,CASE 
                       WHEN	UPPER(fv.channel) = 'EXPEDIA' then 'expedia'
                       else 'Other'
                       end as channel_exp
                  	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
                   	,fv.product_status
                   	,fv.product_is_confirmed_flg as is_confirmed_flg
                   	,CASE
       				 WHEN fv.trip_type_code = 'Nac' THEN 'NAC'
       				 WHEN fv.trip_type_code = 'Int' THEN 'INT'
        		   ELSE NULL 
    				END AS viaje
                   	,fv.buy_type_code as buy_type
                   	,fv.product
                   	-- detail
                   	-- hotel_id
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.recognition_date
           			,fv.booking_date
        --   			,fv.confirmation_date
      --     			,fv.checkin_date
       --    			,fv.checkout_date
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
    				,-sum(pnl.ccp_usd) as CCP
    				,-sum(pnl.coi_usd) as COI
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
                       end) as "fix_net_revenues"
                    ,sum(pnl.net_revenues_usd) as "Net Revenues"
                    ,sum(pnl.npv_net_usd) as NPV
                    ,sum(pr.dif_fx_usd + pr.dif_fx_air_usd) as "DIF FX"
                    ,sum(pr.currency_hedge_usd + pr.currency_hedge_air_usd) as hedge
                    ,sum(pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd) as NPV_calc
             from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month >= '2021-01'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month >= date'2021-01-01'
             left join analytics.bi_transactional_fact_products p on fv.product_id = p.product_id and p.reservation_year_month >= date'2021-01-01'
             left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fv.product_id and pr.date_reservation_year_month >= '2021-01'
             left join 
                (select cast(p.transaction_id as varchar) as product_id, payment_methods, p.status,max(p.penalty), max(conversion_rate*net_commission_partner) as comision_ch,max(conversion_rate*net_commission_despegar) as comision_desp, avg(conversion_rate) as tipo_cambio
                    from data.lake.channels_bo_product p
                    inner join data.lake.channels_bo_sale s on p.sale_id = s.id
                group by 1,2,3) as ch
                on cast(ch.product_id as varchar) = p.reference_id
             where fv.recognition_date > date('2022-12-31')  --- between date('2023-01-01') and date('2024-12-31')
             and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period >= '2021-01'
              --and p.transaction_code = '803280257900'
              group by 1,2,3,4,5,6 ,7,8,9,10,11,12,13,14--,14--,15--,16--,17, 18,19--,19,20,21 --,22,23,24
 )
 select 
 --tx_code,
--product_id,
--origin_product_id,
lob,
Marca,
country_metas,
channel_metas,
channel_exp,
gateway,
agency_code,
product_status,
is_confirmed_flg,
viaje,
buy_type,
product,
recognition_date,
booking_date,
--confirmation_date,
--checkin_date,
--checkout_date,
orders,
gb_RI,
gradiente_margen,
gradiente_margen_calc,
fee_neto,
comision_neta,
descuentos_neto,
fee_agencia,
comision_agencia_channels,
comision_agencia,
backend_air,
backend_nonair,
other_incentives_air,
other_incentives_nonair,
breakage_revenue,
media_revenue,
mkt_discounts,
CCP,
COI,
interes_coi,
customer_service,
errors,
afiliadas,
frauds,
loyalty,
ott,
revenue_tax,
cancelaciones,
customer_claims,
revenue_sharing,
vendor_commission,
mkt_cost,
overs_api,
--"Fix Net Revenues" as "anterior_fix_nr",
"Net Revenues",
NPV,
"DIF FX",
hedge,
NPV_calc,
 cast(fix_net_revenues as DECIMAL(18,4)) / cast(gradiente_margen as DECIMAL(18,4)) as fix_net_revenues,
cast(npv as DECIMAL(18,4)) / cast(gradiente_margen as DECIMAL(18,4))  as fix_fvm
 from bt_detail
 where recognition_date >= booking_date
 and booking_date < CURRENT_DATE
 --and channel_exp <> 'Other'
 limit 100

 

 --------------------------------------------------------------------------------------------------------------------------------------------
 -------------------------------------------------------------------------------------------------------------------------------------------
 
 ----
 
 --Original RI + adecuaciones a terminología Budget / RR ---> Cosecha RI (*Paises original Vic) (actual 2025-04-28; sacamos gradiente de NR y FVM) -- Añadimos channel y sacamos varios dates
 ---Agregamos gateway 2025-05-27
 
 ----- RESUMIDO
 
 
 with bt_detail as (
               select
          --           fv.transaction_code as tx_code
           --       	,fv.product_id
           --        	,fv.origin_product_id
                   fv.line_of_business_code as lob
                   	,CASE 
   					 WHEN fv.brand = 'Despegar' THEN 'D!' 
    					WHEN fv.brand = 'Best Day' THEN 'BD!' 
    					ELSE NULL
						END AS Marca
			    ,CASE 
                      WHEN p.provider_code IN ('DESP', 'PAM') THEN 'PAM'
                      WHEN p.provider_code = 'EXP' THEN 'EXP'
                      WHEN p.provider_code = 'HBG' THEN 'HBG'
                    ELSE 'Other'
                     END AS gateway
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
                    ,CASE
  --  WHEN fv.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'Others'
  --  WHEN fv.partner_id IN ('P12212', 'AP11666') THEN 'Others'
  --  WHEN fv.partner_id IN ('AP12147', 'AP12854') THEN 'Others'
  --  WHEN fv.partner_id IN ('AP12509', 'AP11813') THEN 'Others'
  --  WHEN fv.partner_id = 'AP12158' THEN 'Others'
  --  WHEN fv.partner_id IN ('AP12213', 'AP11843') THEN 'Others'
  --  WHEN fv.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'Others'
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
                    ,CASE
           				WHEN fv.brand = 'Best Day' AND fv.parent_channel = 'API' THEN 'MAY'
    					WHEN fv.brand = 'Despegar' AND fv.parent_channel = 'API' THEN 'API'
    					WHEN (fv.brand = 'Best Day' OR fv.brand = 'Despegar') AND fv.parent_channel = 'Agencias afiliadas' THEN 'MIN'
    					ELSE NULL -- 
					END AS channel_metas
                  -- 	,fv.parent_channel
                  	,CASE 
                       WHEN	UPPER(fv.channel) = 'EXPEDIA' then 'expedia'
                       else 'Other'
                       end as channel_exp
                  	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
                   	,fv.product_status
                   	,fv.product_is_confirmed_flg as is_confirmed_flg
                   	,CASE
       				 WHEN fv.trip_type_code = 'Nac' THEN 'NAC'
       				 WHEN fv.trip_type_code = 'Int' THEN 'INT'
        		   ELSE NULL 
    				END AS viaje
                   	,fv.buy_type_code as buy_type
                   	,fv.product
                   	-- detail
                   	-- hotel_id
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.recognition_date
           			,fv.booking_date
        --   			,fv.confirmation_date
      --     			,fv.checkin_date
       --    			,fv.checkout_date
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
    				,-sum(pnl.ccp_usd) as CCP
    				,-sum(pnl.coi_usd) as COI
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
                       end) as "fix_net_revenues"
                    ,sum(pnl.net_revenues_usd) as "Net Revenues"
                    ,sum(pnl.npv_net_usd) as NPV
                    ,sum(pr.dif_fx_usd + pr.dif_fx_air_usd) as "DIF FX"
                    ,sum(pr.currency_hedge_usd + pr.currency_hedge_air_usd) as hedge
                    ,sum(pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd) as NPV_calc
             from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month >= '2021-01'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month >= date'2021-01-01'
             left join analytics.bi_transactional_fact_products p on fv.product_id = p.product_id and p.reservation_year_month >= date'2021-01-01'
             left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fv.product_id and pr.date_reservation_year_month >= '2021-01'
             left join 
                (select cast(p.transaction_id as varchar) as product_id, payment_methods, p.status,max(p.penalty), max(conversion_rate*net_commission_partner) as comision_ch,max(conversion_rate*net_commission_despegar) as comision_desp, avg(conversion_rate) as tipo_cambio
                    from data.lake.channels_bo_product p
                    inner join data.lake.channels_bo_sale s on p.sale_id = s.id
                group by 1,2,3) as ch
                on cast(ch.product_id as varchar) = p.reference_id
             where fv.recognition_date > date('2022-12-31')  --- between date('2023-01-01') and date('2024-12-31')
             and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period >= '2021-01'
              --and p.transaction_code = '803280257900'
              group by 1,2,3,4,5,6 ,7,8,9,10,11,12,13,14--,14--,15--,16--,17, 18,19--,19,20,21 --,22,23,24
 )
 select 
 --tx_code,
--product_id,
--origin_product_id,
lob,
Marca,
country_metas,
channel_metas,
channel_exp,
gateway,
agency_code,
product_status,
is_confirmed_flg,
viaje,
buy_type,
product,
recognition_date,
booking_date,
--confirmation_date,
--checkin_date,
--checkout_date,
orders,
gb_RI,
gradiente_margen,
gradiente_margen_calc,
fee_neto,
comision_neta,
descuentos_neto,
fee_agencia,
comision_agencia_channels,
comision_agencia,
backend_air,
backend_nonair,
other_incentives_air,
other_incentives_nonair,
breakage_revenue,
media_revenue,
mkt_discounts,
CCP,
COI,
interes_coi,
customer_service,
errors,
afiliadas,
frauds,
loyalty,
ott,
revenue_tax,
cancelaciones,
customer_claims,
revenue_sharing,
vendor_commission,
mkt_cost,
overs_api,
--"Fix Net Revenues" as "anterior_fix_nr",
"Net Revenues",
NPV,
"DIF FX",
hedge,
NPV_calc,
 cast(fix_net_revenues as DECIMAL(18,4)) / cast(gradiente_margen as DECIMAL(18,4)) as fix_net_revenues,
cast(npv as DECIMAL(18,4)) / cast(gradiente_margen as DECIMAL(18,4))  as fix_fvm
 from bt_detail
 where recognition_date >= booking_date
 and booking_date < CURRENT_DATE
 --and channel_exp <> 'Other'
 limit 100
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 --------------------------------------------------------------------------------------------------------------------------------------------
 -------------------------------------------------------------------------------------------------------------------------------------------
 
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
                   --	,fv.channel
                  	,case when length(fh.partner_id) > 0 then fh.partner_id else fh.channel end as agency_code
                   --	,fv.agency_name
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
                   	--,fv.main_airline_code as iata
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
                    where fh.gestion_date >= DATE('2024-01-01')
        			and fh.gestion_date < CURRENT_DATE -- Excluye el día de hoy
        				and fh.lob_gestion in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
        				and pnl.line_of_business = 'B2B'
        				and fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
        				and partition_period > '2023-01'
          			--	and partner_id IN ('AG00073753', 'AG00037023')
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
SELECT *
FROM bt_detail 
limit 1000
 
 
 
 
 