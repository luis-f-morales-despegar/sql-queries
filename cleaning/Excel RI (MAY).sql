--Original RI + adecuaciones a terminología Budget / RR ---> Cosecha RI (*Paises original Vic) (actual 2025-03-13; sacamos gradiente de NR y FVM)
 
 
 
 with bt_detail as (
               select
                     fv.transaction_code as tx_code
                  	,fv.product_id
                   	,fv.origin_product_id
                   	,fv.line_of_business_code as lob
                   	,CASE 
   					 WHEN fv.brand = 'Despegar' THEN 'D!' 
    					WHEN fv.brand = 'Best Day' THEN 'BD!' 
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
                   --	,fv.parent_channel
                   --	,fv.channel
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
           			,fv.confirmation_date
           			,fv.checkin_date
           			,fv.checkout_date
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
             where fv.recognition_date >= date('2024-01-01')  --- between date('2023-01-01') and date('2024-12-31')
             and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period >= '2021-01'
              --and p.transaction_code = '803280257900'
              group by 1,2,3,4,5,6 ,7,8,9,10,11,12,13,14,15,16,17, 18--,19,20,21 --,22,23,24
 )
 select 
 tx_code,
product_id,
origin_product_id,
lob,
Marca,
country_metas,
channel_metas,
agency_code,
product_status,
is_confirmed_flg,
viaje,
buy_type,
product,
recognition_date,
booking_date,
confirmation_date,
checkin_date,
checkout_date,
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
 and channel_metas = 'MAY'
 limit 100
 
 
 -------------------------------------
 -------------------------------------------
 
 
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



-------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------

-------------- Calendario PowerQuery Advanced Editor

let
    // Definir el rango de fechas
    StartDate = #date(2024, 1, 1),
    EndDate = #date(2027, 12, 31),
    DaysList = List.Dates(StartDate, Number.From(EndDate - StartDate) + 1, #duration(1,0,0,0)),
    CalendarTable = Table.FromList(DaysList, Splitter.SplitByNothing(), {"Date"}),
    ChangedType = Table.TransformColumnTypes(CalendarTable, {{"Date", type date}}),

    // Agregar columna de día de la semana donde lunes = 1
    WeekDayColumn = Table.AddColumn(ChangedType, "Week Day", each Date.DayOfWeek([Date], Day.Monday) + 1, type number),

    // Extraer Año
    YearColumn = Table.AddColumn(WeekDayColumn, "Year", each Date.Year([Date]), type number),

    // Formato Año-Mes YYYY-MM
    YearMonthColumn = Table.AddColumn(YearColumn, "Year-Month", each 
        Text.PadStart(Text.From(Date.Year([Date])), 4, "0") & "-" & 
        Text.PadStart(Text.From(Date.Month([Date])), 2, "0"), type text),

    // Formato Trimestre YYYY-QX
    QuarterColumn = Table.AddColumn(YearMonthColumn, "Quarter", each 
        Text.PadStart(Text.From(Date.Year([Date])), 4, "0") & "-Q" & 
        Text.From(Date.QuarterOfYear([Date])), type text),

    // Calcular Año ISO correctamente
    ISOYearColumn = Table.AddColumn(QuarterColumn, "ISO Year", each 
        let
            CurrentYear = Date.Year([Date]),
            FirstThursday = Date.AddDays(#date(CurrentYear, 1, 4), -Date.DayOfWeek(#date(CurrentYear, 1, 4), Day.Monday)), // Encuentra el primer jueves del año ISO
            WeekNumber = Date.WeekOfYear([Date], 2), // Semana ISO con lunes como inicio
            ISOYear = if WeekNumber = 1 and Date.Month([Date]) = 12 then CurrentYear + 1
                      else if WeekNumber >= 52 and Date.Month([Date]) = 1 then CurrentYear - 1 
                      else CurrentYear
        in
            ISOYear, type number),

    // Calcular ISO Week correctamente (y eliminar la semana 53 si no existe)
    ISOWeekColumn = Table.AddColumn(ISOYearColumn, "ISO Week", each 
        let
            FirstThursday = Date.AddDays(#date([ISO Year], 1, 4), -Date.DayOfWeek(#date([ISO Year], 1, 4), Day.Monday)), // Encuentra el primer jueves del año ISO
            ISOWeek = Number.RoundDown((Number.From([Date] - FirstThursday) / 7) + 1, 0)
        in
            if ISOWeek > 52 then Text.PadStart(Text.From([ISO Year] + 1), 4, "0") & "-01" // Corregir si hay una semana 53 inexistente
            else Text.PadStart(Text.From([ISO Year]), 4, "0") & "-" & Text.PadStart(Text.From(ISOWeek), 2, "0"), type text),

    #"Changed Type" = Table.TransformColumnTypes(ISOWeekColumn, {{"ISO Year", type text}}),
    #"Removed Columns" = Table.RemoveColumns(#"Changed Type", {"ISO Year"}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Removed Columns", {{"Year", type text}})
in
    #"Changed Type1"