--- Proporcionada por Andrés Wajnsztok

select 
querydate,
eventdate,
case when channel = 'expedia' then 'expedia' else partnerid end as partnerid,
--ca.ag_name as ag_name,
--ca.director as director,
--ca.group_code as group_code,
--ca.group_name as group_name,
--ca.director as director,
--channel,
guestnationality,
sum(requeststohsm) as requests_to_hsm,
sum(hotelamountrequestedtohsm) as hotel_amount_requested_to_hsm,
sum(hotelamountrequestedtosource) as hotel_amount_requested_to_source,
sum(hotelamountwithavailability) as hotel_amount_with_availability,
sum(hotelamountrequestedtosource) - sum(hotelamountwithavailability) as hotel_amount_without_availability
from data.lake.api_b2b_hsm_events_summarized_by_day tr
left join raw.cartera_b2b_v1 ca
  on ca.agency_code = tr.partnerid
where 1=1
and eventdate is not null
AND eventdate > CURRENT_DATE - INTERVAL '31' DAY
and eventdate < CURRENT_DATE
--and (channel = 'expedia' or partnerid in ('AP12576', 'AP12577'))
--and channel = 'expedia'
--and partnerid in ('AP12174')
--and guestnationality in ('DO')
--and channel <> 'hoteldo-api-g2-block'
--and ttl <> 0
 --------> filtro Globales
AND (
        ca.director = 'veronica.odetti@hoteldo.com'
     OR tr.channel = 'expedia'
      or (
          TR.partnerid IN (
        'AP11809',
        'AP12822',
        'AP12226',
        'AP11683',
        'AP13048',
        'AP12956',
        'AP12955',
        'AP12877',
        'AP12876',
        'AP12875',
        'AP12874',
        'AP11761',
        'AP11759',
        'AP12622',
        'AP11760',
        'AP11676',
        'AP11674',
        'AP12908',
        'AP12907',
        'AP12910',
        'AP13154',
        'AP11697',
        'AP11699',
        'AP11698',
        'AP12678',
        'AP13002',
        'AP11651',
        'AP12679',
        'AP12408',
        'AP11686',
        'AP12769',
        'AP12770',
        'AP12771',
        'AP12772',
        'AP12691',
        'AP12692',
        'AP11744',
        'AP12380',
        'AP12557',
        'AP11810',
        'AP12718',
        'AP12236',
        'AP12549',
        'AG72472')
      )
  )
  group by 1,2,3,4
  ----------------


  
  
  --- Proporcionada por Andrés Wajnsztok

select 
querydate,
eventdate,
case when channel = 'expedia' then 'expedia' else partnerid end as partnerid,
--ca.ag_name as ag_name,
--ca.director as director,
--ca.group_code as group_code,
--ca.group_name as group_name,
--ca.director as director,
--channel,
guestnationality,
sum(requeststohsm) as requests_to_hsm,
sum(hotelamountrequestedtohsm) as hotel_amount_requested_to_hsm,
sum(hotelamountrequestedtosource) as hotel_amount_requested_to_source,
sum(hotelamountwithavailability) as hotel_amount_with_availability,
sum(hotelamountrequestedtosource) - sum(hotelamountwithavailability) as hotel_amount_without_availability
from data.lake.api_b2b_hsm_events_summarized_by_day tr
where 1=1
and eventdate is not null
AND eventdate > CURRENT_DATE - INTERVAL '31' DAY
and eventdate < CURRENT_DATE
--and (channel = 'expedia' or partnerid in ('AP12576', 'AP12577'))
--and channel = 'expedia'
--and partnerid in ('AP12174')
--and guestnationality in ('DO')
--and channel <> 'hoteldo-api-g2-block'
--and ttl <> 0
 --------> filtro Globales
AND (
      tr.channel = 'expedia'
         OR TR.partnerid IN (
        'AP11809',
        'AP12822',
        'AP12226',
        'AP11683',
        'AP13048',
        'AP12956',
        'AP12955',
        'AP12877',
        'AP12876',
        'AP12875',
        'AP12874',
        'AP11761',
        'AP11759',
        'AP12622',
        'AP11760',
        'AP11676',
        'AP11674',
        'AP12908',
        'AP12907',
        'AP12910',
        'AP13154',
        'AP11697',
        'AP11699',
        'AP11698',
        'AP12678',
        'AP13002',
        'AP11651',
        'AP12679',
        'AP12408',
        'AP11686',
        'AP12769',
        'AP12770',
        'AP12771',
        'AP12772',
        'AP12691',
        'AP12692',
        'AP11744',
        'AP12380',
        'AP12557',
        'AP11810',
        'AP12718',
        'AP12236',
        'AP12549',
        'AG72472')
      )
  group by 1,2,3,4
  
  
  


select *
from data.lake.api_b2b_hsm_events_summarized_by_day
where 1=1
and eventdate is not null
AND eventdate > CURRENT_DATE - INTERVAL '31' DAY
and eventdate < CURRENT_DATE

-- Conteo de filas de la query resumida

WITH base_rows AS (
  SELECT 
      querydate,
      eventdate,
    --  channel,
      partnerid,
      guestnationality,
      SUM(requeststohsm) AS requests_to_hsm,
      SUM(hotelamountrequestedtohsm) AS hotel_amount_requested_to_hsm,
      SUM(hotelamountrequestedtosource) AS hotel_amount_requested_to_source,
      SUM(hotelamountwithavailability) AS hotel_amount_with_availability,
      SUM(hotelamountrequestedtosource) - SUM(hotelamountwithavailability) AS hotel_amount_without_availability
  FROM data.lake.api_b2b_hsm_events_summarized_by_day
  WHERE eventdate IS NOT NULL
    AND eventdate > CURRENT_DATE - INTERVAL '31' DAY
    AND eventdate < CURRENT_DATE
 -- AND (channel = 'expedia' OR partnerid IN ('AP12576', 'AP12577'))
  GROUP BY 1,2,3,4
)
SELECT COUNT(*) AS total_filas
FROM base_rows;

select *
from data.sot.hsm_search_by_ids_data
where datetime is not null
limit 100

-- Conteo de filas en api_b2b_hsm_events_summarized_by_day

SELECT COUNT(*) AS total_filas
from data.lake.api_b2b_hsm_events_summarized_by_day
where 1=1
and eventdate is not null
and eventdate > date'2025-06-30'

----
select * 
from raw.trk_pageview_conversion
where event_date is not null
--and partner_id in ('AP12908', 'AP12909', 'AP12576')
limit 100

parsed_events pe
 
unique_transactions

hourly_counts hc

selected_dates sd

average_past_4_weeks ap4w

select *
from data.analytics.mkt_users_dim_users
where creation_date is not null
limit 100


select * 
from data.lake.chewie_traveler
limit 100

select *
from lake.comdev_tendencias_partners_mensual