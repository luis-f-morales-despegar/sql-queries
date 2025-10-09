WITH rp AS (
 SELECT
 partner_id,
 count(distinct transaction_code) as Bookings,
 ROW_NUMBER() OVER (ORDER BY count(distinct transaction_code) DESC) AS ranking
 FROM lake.bi_web_traffic bi
 where 1=1
 and bi.date > '2025-08-01'
  and bi.date <= '2025-08-03'
 and partner_id IS NOT NULL
 and parent_channel = 'White Label'
-- and year  between {{anio_desde}} and {{anio_hasta}}  -- año a analizar
-- and week  between {{semana_desde}} and {{semana_hasta}}
 group by 1
-- order by 3 DESC
)
SELECT
--ranking,
    case when bwt.country in ('BR','MX','AR','CO','CL','PE') then bwt.country
         else ('Otros') end as site_agrupado,
    parent_channel,
    plataforma,
    case when bwt.producto_fenix in ('Hoteles', 'Vuelos') then bwt.producto_fenix
         when bwt.producto_fenix in ('Actividades','Buses','Autos','Cruceros','Asistencia al viajero','Traslados') then 'ONA'
         when bwt.producto_fenix in ('Carrito', 'Bundles', 'Gateways', 'Escapadas') then 'COMBINED_PRODUCT'
         else bwt.producto_fenix end as producto,
    IF (ranking <= 21 OR rp.partner_id in ('latam-pe','bbva-pe','bbva-co','bbva-pe','bbva-pe-loyalty','bbva-uy','latam-ar','latam-br-canje','latam-cl-canje','latam-pe','COPPEL','suburbia','ElektraV','livelo-bb','scotiabankcl','RiuMX','EMP','LiveloPontos','latam-pe-canje','Sicredi','Didi'), bwt.channel, 'Others') 
    channel,
    IF (ranking <= 21 OR rp.partner_id in ('latam-pe','bbva-pe','bbva-co','bbva-pe','bbva-pe-loyalty','bbva-uy','latam-ar','latam-br-canje','latam-cl-canje','latam-pe','COPPEL','suburbia','ElektraV','livelo-bb','scotiabankcl','RiuMX','EMP','LiveloPontos','latam-pe-canje','Sicredi','Didi'), rp.partner_id, 'Others') 
    partner,
    routetype,
    gs.tipo_de_white_label,
    gs.tipo_de_partner,
    cast(bwt.date as date) as date,
  --year(cast(date as date)) as year,
  --week(cast(date as date)) as semana,
    count(distinct bwt.userid) as usuarios,
    count(distinct if(bwt.flow = 'HOME', bwt.userid, NULL)) as usuarios_home,
    count(distinct if(bwt.flow = 'LANDING', bwt.userid, NULL)) as usuarios_landing,
    count(distinct if(bwt.flow = 'SEARCH', bwt.userid, NULL)) as usuarios_searchers,
    count(distinct if(bwt.flow = 'DETAIL', bwt.userid, NULL)) as usuarios_detail,
    count(distinct if(bwt.flow = 'INTER-XS', bwt.userid, NULL)) as usuarios_PI,
    count(distinct if(bwt.flow = 'CHECKOUT', bwt.userid, NULL)) as usuarios_checkouters,
    count(distinct if(bwt.flow = 'THANKS', bwt.userid, NULL)) as usuarios_thankers,
    count(distinct bwt.transaction_code) as Bookings
FROM lake.bi_web_traffic bwt left join rp on bwt.partner_id = rp.partner_id
join tmp.glosarios_partners_V9 gs on gs.partner_data_id = bwt.partner_id
where 1=1 
and date > '2025-08-01'
  and date <= '2025-08-03'
--and year  between {{anio_desde}} and {{anio_hasta}}  -- año a analizar
--and week  between {{semana_desde}} and {{semana_hasta}}
and bwt.partner_id IS NOT NULL
and bwt.parent_channel = 'White Label'
and is_bot = 0
and ispageview = 1
and flg_detalle_cp = 0
and bwt.flow not in ('AS-HOME')
and bwt.plataforma <> 'App'
and channel <> 'puntosbonus-pe'
GROUP BY 1,2,3,4,5,6,7,8,9,10
limit 100



select *
from lake.bi_web_traffic bwt
where 1=1
and date is not null
limit 100


----
---

WITH rp AS (
  SELECT
    partner_id,
    COUNT(DISTINCT transaction_code) AS bookings,
    ROW_NUMBER() OVER (
      ORDER BY COUNT(DISTINCT transaction_code) DESC
    ) AS ranking
  FROM lake.bi_web_traffic
  WHERE date >= DATE '2024-01-01'
    AND date <= DATE '2024-01-05'
    AND partner_id IS NOT NULL
    AND parent_channel = 'White Label'
  GROUP BY partner_id
)
SELECT
  CASE
    WHEN bwt.country IN ('BR','MX','AR','CO','CL','PE') THEN bwt.country
    ELSE 'Otros'
  END AS site_agrupado,
  bwt.parent_channel,
  bwt.plataforma,
  CASE
    WHEN bwt.producto_fenix IN ('Hoteles','Vuelos') THEN bwt.producto_fenix
    WHEN bwt.producto_fenix IN (
      'Actividades','Buses','Autos','Cruceros',
      'Asistencia al viajero','Traslados'
    ) THEN 'ONA'
    WHEN bwt.producto_fenix IN (
      'Carrito','Bundles','Gateways','Escapadas'
    ) THEN 'COMBINED_PRODUCT'
    ELSE bwt.producto_fenix
  END AS producto,
  CASE
    WHEN rp.ranking <= 21
      OR rp.partner_id IN (
        'latam-pe','bbva-pe','bbva-co','bbva-pe','bbva-pe-loyalty',
        'bbva-uy','latam-ar','latam-br-canje','latam-cl-canje',
        'latam-pe','COPPEL','suburbia','ElektraV','livelo-bb',
        'scotiabankcl','RiuMX','EMP','LiveloPontos','latam-pe-canje',
        'Sicredi','Didi'
      )
    THEN bwt.channel
    ELSE 'Others'
  END AS channel,
  CASE
    WHEN rp.ranking <= 21
      OR rp.partner_id IN (
        'latam-pe','bbva-pe','bbva-co','bbva-pe','bbva-pe-loyalty',
        'bbva-uy','latam-ar','latam-br-canje','latam-cl-canje',
        'latam-pe','COPPEL','suburbia','ElektraV','livelo-bb',
        'scotiabankcl','RiuMX','EMP','LiveloPontos','latam-pe-canje',
        'Sicredi','Didi'
      )
    THEN rp.partner_id
    ELSE 'Others'
  END AS partner,
  bwt.routetype,
  gs.tipo_de_white_label,
  gs.tipo_de_partner,
  COUNT(DISTINCT bwt.userid)                                                             AS usuarios,
  CAST(bwt.date AS date)                                                               AS date_day,
  COUNT(DISTINCT bwt.userid) FILTER (WHERE bwt.flow = 'HOME')                          AS usuarios_home,
  COUNT(DISTINCT bwt.userid) FILTER (WHERE bwt.flow = 'LANDING')                       AS usuarios_landing,
  COUNT(DISTINCT bwt.userid) FILTER (WHERE bwt.flow = 'SEARCH')                        AS usuarios_searchers,
  COUNT(DISTINCT bwt.userid) FILTER (WHERE bwt.flow = 'DETAIL')                        AS usuarios_detail,
  COUNT(DISTINCT bwt.userid) FILTER (WHERE bwt.flow = 'INTER-XS')                      AS usuarios_PI,
  COUNT(DISTINCT bwt.userid) FILTER (WHERE bwt.flow = 'CHECKOUT')                      AS usuarios_checkouters,
  COUNT(DISTINCT bwt.userid) FILTER (WHERE bwt.flow = 'THANKS')                        AS usuarios_thankers,
  COUNT(DISTINCT bwt.transaction_code)                                                 AS bookings
FROM data.lake.bi_web_traffic bwt
LEFT JOIN rp
  ON bwt.partner_id = rp.partner_id
JOIN tmp.glosarios_partners_V9 gs
  ON gs.partner_data_id = bwt.partner_id
WHERE bwt.date >= DATE '2024-01-01'
  AND bwt.date <= DATE '2024-01-05'
  AND bwt.partner_id IS NOT NULL
  AND bwt.parent_channel = 'White Label'
  AND bwt.is_bot = 0
  AND bwt.ispageview = 1
  AND bwt.flg_detalle_cp = 0
  AND bwt.flow NOT IN ('AS-HOME')
  AND bwt.plataforma <> 'App'
  AND bwt.channel <> 'puntosbonus-pe'
GROUP BY 1,2,3,4,5,6,7,8,9,10
LIMIT 100;



--------------------------------------------------------------------------------
----------------------------------------------------------------------------

----query Emilio  fix Others


With trafico as (
    SELECT
         CAST(bwt.date AS DATE) AS fecha
        ,case   when bwt.country IN ('BR','AR','CO','CL','MX','PE') then bwt.country
                else 'Global'
            end as region
        ,bwt.channel
        ,bwt.partner_id
        ,bwt.parent_channel
        ,bwt.plataforma
        ,bwt.routetype
        ,gs.tipo_de_white_label
        ,gs.tipo_de_partner
        ,case when bwt.producto_fenix in ('Carrito','Bundles','Escapadas')    then 'Carrito'            
            else bwt.producto_fenix
         end as original_product
        ,count(distinct transaction_code) as bookings
        ,count(distinct bwt.userid) as usuarios
        ,count(distinct if(bwt.flow = 'HOME', bwt.userid, NULL)) as usuarios_home
        ,count(distinct if(bwt.flow = 'LANDING', bwt.userid, NULL)) as usuarios_landing
        ,count(distinct if(bwt.flow = 'SEARCH', bwt.userid, NULL)) as usuarios_searchers
        ,count(distinct if(bwt.flow = 'DETAIL', bwt.userid, NULL)) as usuarios_detail
        ,count(distinct if(bwt.flow = 'INTER-XS', bwt.userid, NULL)) as usuarios_PI
        ,count(distinct if(bwt.flow = 'CHECKOUT', bwt.userid, NULL)) as usuarios_checkouters
        ,count(distinct if(bwt.flow = 'THANKS', bwt.userid, NULL)) as usuarios_thankers
        ,COUNT(DISTINCT IF(bwt.flow = 'SEARCH', bwt.searchid, NULL)) AS search_searchers
        ,COUNT(DISTINCT IF(bwt.flow = 'DETAIL', bwt.searchid, NULL)) AS search_detail
        ,COUNT(DISTINCT IF(bwt.flow = 'CHECKOUT', bwt.searchid, NULL)) AS search_checkouters
from data.lake.bi_web_traffic bwt
left join lake.ch_bo_partner_partner p on cast(p.partner_code as varchar) = bwt.partner_id
join tmp.glosarios_partners_V9 gs on gs.partner_data_id = bwt.partner_id
where bwt.date >= '2025-01-01'
    and bwt.date <= '2025-01-03'
    and ispageview = 1
    and is_bot = 0
    and flg_detalle_cp = 0
    and routetype <> ''
    and bwt.parent_channel in ('White Label')
    and bwt.partner_id IS NOT NULL
  --  and bwt.year  between {{anio_desde}} and {{anio_hasta}}  -- año a analizar
  --  and bwt.week  between {{semana_desde}} and {{semana_hasta}}  -- semanas a analizar
GROUP BY  1,2,3,4,5,6,7,8,9,10
)
select fecha
        ,region
        ,channel
        ,parent_channel
        ,partner_id
        ,plataforma
        ,routetype
        ,tipo_de_white_label
        ,tipo_de_partner
        ,original_product
        ,sum(bookings) as bookings
        ,sum(usuarios) as visitantes
        ,sum(usuarios_home) as usuarios_home
        ,sum(usuarios_landing) as usuarios_landing
        ,sum(usuarios_searchers) as usuarios_searchers
        ,sum(usuarios_detail) as usuarios_detail
        ,sum(usuarios_PI) as usuarios_PI
        ,sum(usuarios_checkouters) as usuarios_checkouters
        ,sum(usuarios_thankers) as usuarios_thankers
        ,sum(search_searchers) as searchers
        ,case when sum(cast(search_searchers as decimal(10,3))) = 0 then 0 else sum(cast(bookings as decimal (10,3))) / sum(cast(search_searchers as decimal(10,3))) end as CVR
from trafico
where search_searchers > 0
and usuarios_home > 0
group by 1,2,3,4,5,6,7,8,9,10
limit 100
--order by usuarios_home desc
 






With trafico as (
    SELECT
         CAST(bwt.date AS DATE) AS fecha
        ,case   when bwt.country IN ('BR','AR','CO','CL','MX','PE') then bwt.country
                else 'Global'
            end as region
        ,bwt.channel
        ,bwt.partner_id
        ,bwt.parent_channel
        ,bwt.plataforma
        ,bwt.routetype
        ,gs.tipo_de_white_label
        ,gs.tipo_de_partner
        ,case when bwt.producto_fenix in ('Carrito','Bundles','Escapadas')    then 'Carrito'
            else bwt.producto_fenix
         end as original_product
        ,count(distinct transaction_code) as bookings
        ,count(distinct bwt.userid) as usuarios
        ,count(distinct if(bwt.flow = 'HOME', bwt.userid, NULL)) as usuarios_home
        ,count(distinct if(bwt.flow = 'LANDING', bwt.userid, NULL)) as usuarios_landing
        ,count(distinct if(bwt.flow = 'SEARCH', bwt.userid, NULL)) as usuarios_searchers
        ,count(distinct if(bwt.flow = 'DETAIL', bwt.userid, NULL)) as usuarios_detail
        ,count(distinct if(bwt.flow = 'INTER-XS', bwt.userid, NULL)) as usuarios_PI
        ,count(distinct if(bwt.flow = 'CHECKOUT', bwt.userid, NULL)) as usuarios_checkouters
        ,count(distinct if(bwt.flow = 'THANKS', bwt.userid, NULL)) as usuarios_thankers
        ,COUNT(DISTINCT IF(bwt.flow = 'SEARCH', bwt.searchid, NULL)) AS search_searchers
        ,COUNT(DISTINCT IF(bwt.flow = 'DETAIL', bwt.searchid, NULL)) AS search_detail
        ,COUNT(DISTINCT IF(bwt.flow = 'CHECKOUT', bwt.searchid, NULL)) AS search_checkouters
from data.lake.bi_web_traffic bwt
left join lake.ch_bo_partner_partner p on cast(p.partner_code as varchar) = bwt.partner_id
join tmp.glosarios_partners_V9 gs on gs.partner_data_id = bwt.partner_id
where bwt.date >= '2025-01-01'
    and bwt.date <= '2025-01-03'
    and ispageview = 1
    and is_bot = 0
    and flg_detalle_cp = 0
    and routetype <> ''
    and bwt.parent_channel in ('White Label')
    and bwt.partner_id IS NOT NULL
 --   and bwt.year between {{anio_desde}} and {{anio_hasta}}  -- año a analizar
   -- and bwt.week  between {{semana_desde}} and {{semana_hasta}}  -- semanas a analizar
GROUP BY  1,2,3,4,5,6,7,8,9,10
)
select fecha
        ,region
        ,channel
        ,parent_channel
        ,partner_id
        ,plataforma
        ,routetype
        ,tipo_de_white_label
        ,tipo_de_partner
        ,original_product
        ,count(DISTINCT bookings) as bookings
        ,sum(usuarios) as visitantes
        ,sum(usuarios_home) as usuarios_home
        ,sum(usuarios_landing) as usuarios_landing
        ,sum(usuarios_searchers) as usuarios_searchers
        ,sum(usuarios_detail) as usuarios_detail
        ,sum(usuarios_PI) as usuarios_PI
        ,sum(usuarios_checkouters) as usuarios_checkouters
        ,sum(usuarios_thankers) as usuarios_thankers
        ,sum(search_searchers) as searchers
        ,case when sum(cast(search_searchers as decimal(10,3))) = 0 then 0 else sum(cast(bookings as decimal (10,3))) / sum(cast(search_searchers as decimal(10,3))) end as CVR
from trafico
where search_searchers > 0
group by 1,2,3,4,5,6,7,8,9,10
