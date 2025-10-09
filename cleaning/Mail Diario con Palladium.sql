

--Query ajuste semana Palladium NR por Ivan 20250314 -- Ajuste paises PY 2025-04-17
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
),
bt_detail AS (
    SELECT 
        fh.gestion_date AS Fecha, 
        fh.brand AS Marca,
        /* Corrección de país */
        CASE 
            when fh.partner_id in ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148', 'AG00015606', 'AP13029', 'AP13030') then 'PY'
            WHEN fh.partner_id IN ('AG00017056', 'AP13049', 'AG00017054', 'AP13050') THEN 'UY'
            WHEN fh.partner_id IN ('AP12212', 'AP11666') THEN 'CR_CTA'
            WHEN fh.partner_id = 'AP12147' THEN 'SV_CTA'
            WHEN fh.partner_id = 'AP12854' THEN 'SV_CTA'
            WHEN fh.partner_id IN ('AP12509', 'AP11813') THEN 'GT_CTA'
            WHEN fh.partner_id = 'AP12158' THEN 'PA_CTA'
            WHEN fh.partner_id IN ('AP12213', 'AP11843') THEN 'HN_CTA'
            WHEN fh.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'DO_CTA'
            ELSE fh.country_code 
        END AS pais_corregido,
        fh.buy_type_code AS productooriginal, 
        fh.product,
        /* Corrección del canal */
        CASE 
            WHEN fh.partner_id IN ('AG00073753', 'AG00037023') THEN 'API'
            ELSE fh.parent_channel
        END AS parent_channel_corregido,
        /* Cálculos */
        ROUND(SUM(fh.gestion_gb), 2) AS gb_s_gradiente,
        ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb, 
        ROUND(
            SUM(
                CASE 
                    WHEN fh.country_code = 'BR' AND fh.product NOT IN ('Vuelos') 
                        THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                        IF(pnl.b2b_gradient_margin = '1', 1, 
                        CAST(pnl.b2b_gradient_margin AS DECIMAL(2,2)))))
                    WHEN fh.channel = 'expedia' 
                        THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                        IF(pnl.b2b_gradient_margin = '1', 1, 
                        CAST(pnl.b2b_gradient_margin AS DECIMAL(2,2)))))
                    ELSE pnl.net_revenues_usd
                END
            )
        ,2) AS fix_net_revenues,
        ROUND(
        SUM(
            CASE 
                WHEN prod.hotel_despegar_id IN ('2169138','316774','1631470','1631555','214577','312469','316485') AND fh.gestion_date between date'2025-03-10' and date'2025-03-16'
                    THEN 
                        (CASE 
                            WHEN fh.country_code = 'BR' AND fh.product NOT IN ('Vuelos') 
                                THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                                IF(pnl.b2b_gradient_margin = '1', 1, 
                                CAST(pnl.b2b_gradient_margin AS DECIMAL(2,2)))))
                            WHEN fh.channel = 'expedia' 
                                THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                                IF(pnl.b2b_gradient_margin = '1', 1, 
                                CAST(pnl.b2b_gradient_margin AS DECIMAL(2,2)))))
                            ELSE pnl.net_revenues_usd
                        END) + (fh.gestion_gb * fh.confirmation_gradient * 0.05)
                ELSE 
                    (CASE 
                        WHEN fh.country_code = 'BR' AND fh.product NOT IN ('Vuelos') 
                            THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                            IF(pnl.b2b_gradient_margin = '1', 1, 
                            CAST(pnl.b2b_gradient_margin AS DECIMAL(2,2)))))
                        WHEN fh.channel = 'expedia' 
                            THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                            IF(pnl.b2b_gradient_margin = '1', 1, 
                            CAST(pnl.b2b_gradient_margin AS DECIMAL(2,2)))))
                        ELSE pnl.net_revenues_usd
                    END)
            END
        ), 2
        ) AS fix_net_revenues_usd_ajuste_palladium,
        ROUND(SUM(pnl.net_revenues_usd), 2) AS net_revenues_usd_s_ajuste,
        ROUND(SUM(pnl.npv_net_usd), 2) AS npv_net_usd,
        ROUND(
            SUM(
                (
                    (pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
                    + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
                    + pnl.affiliates_usd) -- Sumamos afiliadas
                    / IF(pnl.b2b_gradient_margin = '1', 1, 
                        CAST(pnl.b2b_gradient_margin AS DECIMAL(2,2))) -- Quitar gradiente
                ) 
                - COALESCE(bo.tpc_usd,0) -- Quitar TPC (en sustitución de afiliadas)
            ) 
            * MAX(IF(pnl.b2b_gradient_margin = '1', 1, 
                CAST(pnl.b2b_gradient_margin AS DECIMAL(2,2)))) 
        ) AS fix_npv  
    FROM analytics.bi_sales_fact_sales_recognition fh 
    LEFT JOIN analytics.bi_pnlop_fact_current_model pnl 
        ON fh.product_id = pnl.product_id 
        AND pnl.date_reservation_year_month >= '2024-01'
    LEFT JOIN analytics.bi_transactional_fact_charges c 
        ON fh.product_id = c.product_id 
        AND c.reservation_year_month >= DATE '2024-01-01'
    LEFT JOIN data.analytics.bi_transactional_fact_products prod 
        ON fh.product_id = prod.product_id 
        AND prod.reservation_year_month >= DATE '2024-01-01'
    LEFT JOIN analytics.bi_pnlop_fact_pricing_model pr 
        ON pr.product_id = fh.product_id 
        AND pr.date_reservation_year_month >= '2024-01'
    LEFT JOIN bo_tpc bo 
        ON bo.product_id_original = fh.origin_product_id
    WHERE fh.gestion_date >= DATE('2024-01-01')
        AND fh.gestion_date < CURRENT_DATE -- Excluye el día de hoy
        AND fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
        AND pnl.line_of_business = 'B2B'
        AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
        AND partition_period >= '2024-01'
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT * 
FROM bt_detail
limit 100







------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------


 --- Reconocimiento de Ingreso - Mail Diario Excel -- NR y FVM ajustados por gradiente 20250304 -- Ajuste paises PY -- Ajuste country igual que en Venta 20250410
---- Fixes 2025-05-09 : NRs y Revenue Margin - Se quitan TPC, Afilliates caso Exp, y coin_interest caso BR

with bo_tpc as( 
            select
                p.transaction_id as product_id_original
                ,max(p.net_commission_partner * p.conversion_rate) as tpc_usd 
              --  ,max(payment_methods) as payment_methods
              --  ,max(credit_card_payment_type) as credit_card_payment_type
              --  ,max(installments) as installments
              --  ,max(card_type) as card_type
            from data.lake.channels_bo_product p
            join data.lake.channels_bo_sale s on s.id = p.sale_id
            where cast(s.created as date) between date_add('month',-1,date('2024-01-01')) and date_add('month',1,CURRENT_DATE) 
            group by 1
    ),
bt_detail as (
               select
                 --    fv.transaction_code as tx_code
                -- 	,fv.product_id
                 --  	,fv.origin_product_id
                   	fv.line_of_business_code as lob
                   	,fv.brand as brand                 	
                 --  	,case fv.site when 'Mexico' 	then '01-Mexico'
			       ---       		  when 'Brasil' 	then '02-Brasil'
			           --   		  when 'Argentina' 	then '03-Argentina'
			         --     		  when 'Chile' 		then '04-Chile'
  			       --       		  when 'Colombia' 	then '05-Colombia'
  			     --         		  when 'Peru' 		then '06-Peru'
  			   --           		  else '07-Global'			  
             --  			end as region
          --     		,fv.site	
               	--	,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
                  --     		country_code,'OT') as country
          --         	,fv.parent_channel
           --        	,fv.channel   	
                   	    ,CASE 
            when fv.partner_id in ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148', 'AG00015606', 'AP13029', 'AP13030') then 'PY'
            WHEN fv.partner_id IN ('AG00017056', 'AP13049', 'AG00017054', 'AP13050') THEN 'UY'
            WHEN fv.partner_id IN ('AP12212', 'AP11666') THEN 'CR_CTA'
            WHEN fv.partner_id = 'AP12147' THEN 'SV_CTA'
            WHEN fv.partner_id = 'AP12854' THEN 'SV_CTA'
            WHEN fv.partner_id IN ('AP12509', 'AP11813') THEN 'GT_CTA'
            WHEN fv.partner_id = 'AP12158' THEN 'PA_CTA'
            WHEN fv.partner_id IN ('AP12213', 'AP11843') THEN 'HN_CTA'
            WHEN fv.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'DO_CTA'
            ELSE fv.country_code 
        END AS pais_corregido
        /* Corrección del canal */
        ,CASE 
            WHEN fv.partner_id IN ('AG00073753', 'AG00037023', 'AG00017056', 'AP13049', 'AG00017054', 'AP13050') THEN 'API'
            ELSE fv.parent_channel
        END AS parent_channel_corregido
           --        	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
            --       	,fv.product_status
            --       	,fv.product_is_confirmed_flg as is_confirmed_flg
            --       	,fv.trip_type_code as trip_type
                   	,fv.buy_type_code as buy_type
            --       	,fv.product
                   	-- detail
                   	-- hotel_id
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.recognition_date
         --  			,fv.booking_date
         --  			,fv.confirmation_date
         --  			,fv.checkin_date
         --  			,fv.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
                    ,sum(fv.gestion_gb) as gb_RI 
                    ,max(pnl.b2b_gradient_margin) as gradiente_margen
                    ,(sum(pnl.commission_net_usd)/sum(ch.comision_desp)) as gradiente_margen_calc
                    --> Revenue Margin <--
                    ,sum(pnl.fee_net_usd) as fee_neto
 					,sum(pnl.commission_net_usd) as comision_neta   -- aka upfront
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
    				,sum(pnl.margin_net_usd) as revenue_margin
                    ,SUM(
  CASE
    -- Brasil y producto ≠ Vuelos: margen ajustado menos IVA + coi_interest
    WHEN fv.country_code = 'BR'
      AND fv.product NOT IN ('Vuelos')
    THEN
      (
        pnl.margin_net_usd
        / IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(5,5)))
      )
      - fv.tpc_fix_iva
      + pnl.coi_interest_usd
    -- Cualquier BR (incluye Vuelos): margen ajustado + coi_interest
    WHEN fv.country_code = 'BR'
    THEN
      (
        pnl.margin_net_usd
        / IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(5,5)))
      )
      + pnl.coi_interest_usd
    -- Canal Expedia (resta IVA, sin coi_interest)
    WHEN fv.channel = 'expedia'
    THEN
      (
        pnl.margin_net_usd
        / IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(5,5)))
      )
      - fv.tpc_fix_iva
    -- Resto de casos: solo margen ajustado
    ELSE
      pnl.margin_net_usd
      / IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(5,5)))
  END
) AS fix_revenue_margin
    				,-sum(pnl.vendor_commission_usd) as vendor_commission /* vendedor - call - islas */
					,-sum(pnl.mkt_cost_net_usd) as mkt_cost
					,sum(pnl.agency_backend_usd) as overs_api
    				--> *** Resultado Financiero *** <--                       
                    ,sum( case when fv.country_code = 'BR' and fv.product not in ('Vuelos') then ((pnl.net_revenues_usd/if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(5,5)))) - (fv.tpc_fix_iva))
                          when fv.channel = 'expedia' then ((pnl.net_revenues_usd/if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(5,5)))) - (fv.tpc_fix_iva))    
                          else pnl.net_revenues_usd/if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(5,5)))
                          end
                        ) as fix_net_revenues
                    ,sum(pnl.net_revenues_usd)as net_revenues
                    ,sum(pnl.npv_net_usd) as fvm
                    ,sum(pr.dif_fx_usd + pr.dif_fx_air_usd) as dif_fx
                    ,sum(pr.currency_hedge_usd + pr.currency_hedge_air_usd) as hedge
                    ,sum(pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd) as fvm_calc
                 from (select *
            , case when fv_prev.parent_channel = 'Agencias afiliadas' 
                then tpc_usd/(1+coalesce(cast(fix_iva.iva as decimal(5,5)),0)) 
                else tpc_usd end as tpc_fix_iva 
                from analytics.bi_sales_fact_sales_recognition fv_prev               
            left join bo_tpc bo on bo.product_id_original = fv_prev.origin_product_id
            left join raw.b2b_dim_html_iva_fix fix_iva on fix_iva.country = fv_prev.country_code) fv
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
             LEFT JOIN bo_tpc bo ON bo.product_id_original = fv.origin_product_id
            -- left join raw.bi_input_pnl_b2b_gradient_gb gc 
            --     on cast(gc.anio as int) = year(fv.recognition_date) 
            --     and cast(gc.mes as int) = month(fv.recognition_date) 
            --     and gc.tipodecompra = fv.original_product
            --     and  split_part(gc.channel,'-',2) = split_part(fv.channel,'-',2)
            --     and gc.pais_codigo = fv.country_code
            where fv.recognition_date between DATE('2024-01-01') and DATE('2025-12-31')
                and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
                and pnl.line_of_business = 'B2B'
                and fv.partition_period >= '2020-12-31'
                --and p.transaction_code = '803280257900'
            group by 1,2,3,4,5,6-- ,7,8--,9--,10--,11--,12,13--,14,15--,16,17,18,19,20,21 --,22,23,24
                )
 select
-- tx_code,
 lob,
 brand,
pais_corregido,
parent_channel_corregido,
 --channel,
-- agency_code,
 buy_type,
 recognition_date,
-- booking_date,
-- confirmation_date,
-- checkin_date,
-- checkout_date,
 gb_RI,
-- RM ---
 fee_neto,
ROUND(comision_neta,2) as comision_neta,  -- aka upfront
ROUND(descuentos_neto,2) as descuentos_neto,
 ROUND(revenue_margin,2) as revenue_margin,
 ROUND(fix_revenue_margin,2) as fix_revenue_margin,
 --gradiente_margen_calc,
cast(fix_net_revenues as DECIMAL(18,4)) / cast(gradiente_margen as DECIMAL(18,4)) as fix_net_revenues,
cast(fvm as DECIMAL(18,4)) / cast(gradiente_margen as DECIMAL(18,4)) as fix_fvm,
ROUND(SUM(fee_neto + comision_neta + descuentos_neto),2) as rm_calc 
from bt_detail
where lob = 'B2B'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
limit 100



