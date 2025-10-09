Select *
from raw.b2b_comdev_metas_comerciales_stg comdev
where 1=1
--and region in ('OT', 'Global')
and parent_channel = 'API'
and anio_mes = '2025-07'
--limit 100

Select *
from lake.b2b_comdev_metas_comerciales comdev
where 1=1
--and region in ('OT', 'Global')
and parent_channel = 'API'
and anio_mes = '2025-07'
--limit 100

Select 
lob
, anio_mes
, ROUND(SUM(CAST(meta_comercial AS DOUBLE)),2) AS meta_comercial
from raw.b2b_comdev_metas_comerciales comdev
where 1=1
group by 1,2
--limit 100





WITH bo_tpc AS (
  SELECT 
    p.transaction_id AS product_id_original,
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE('2024-01-01') 
    AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY 1
)
SELECT 
  gestion_date,
  CASE WHEN fh.parent_channel = 'API' THEN pnl.brand ELSE 'Best Day' END AS brand,
  CASE WHEN pnl.line_of_business = 'B2B2C' THEN 'B2B2C' ELSE fh.parent_channel END AS parent_channel,
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
  WHEN fh.country_code IN ('MX','BR','CO','AR','EC','PE','CL','PY') THEN fh.country_code
  WHEN fh.country_code IN ('US','PA','ES','CR') THEN 'GL'
  WHEN fh.country_code IN ('UY','BO') THEN 'UY'
  ELSE 'GL'
END AS country_code,
  CASE WHEN fh.buy_type_code IN ('Hoteles','Vuelos','Carrito','Traslados') THEN fh.buy_type_code ELSE 'DS' END AS productooriginal,
  ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb,
  ROUND(
    SUM(
      CASE 
        WHEN fh.country_code = 'BR' AND fh.product NOT IN ('Vuelos') THEN
          (pnl.net_revenues_usd - (bo.tpc_usd * IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))))
        WHEN fh.channel = 'expedia' THEN
          (pnl.net_revenues_usd - (bo.tpc_usd * IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))))
        ELSE pnl.net_revenues_usd
      END
    ), 2
  ) AS nr,
  ROUND(
    SUM(
      (
        (pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
         + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
         + pnl.affiliates_usd)
        / IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))
      ) 
      - COALESCE(bo.tpc_usd,0)
    ) 
    * MAX(IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5))))
  ) AS fvm
FROM analytics.bi_sales_fact_sales_recognition fh 
LEFT JOIN analytics.bi_pnlop_fact_current_model pnl 
  ON fh.product_id = pnl.product_id 
  AND pnl.date_reservation_year_month > '2023-01'
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
WHERE 
  fh.gestion_date >= DATE('2024-01-01')
  AND fh.gestion_date < CURRENT_DATE
  AND partition_period > '2023-01'
  AND date_trunc('month', fh.gestion_date) <= date_trunc('month', date_add('month', 1, current_date))
  AND fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
  AND pnl.line_of_business = 'B2B'
  AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
GROUP BY 1,2,3,4,5


------------



select *
from raw.b2b_planning_budget_reconocimiento_stg

select *
from raw.b2b_planning_budget_venta_stg


--------------------------------------------------

---Metas comerciales B2B
Select *
from lake.b2b_comdev_metas_comerciales dev

---Budget de reconocimiento diarizado B2B
Select *
from lake.b2b_planning_budget_reconocimiento pl
order by fecha asc

---Budget de venta diarizado B2B
Select *
from lake.b2b_planning_budget_venta pl

--- Trafico WLs
Select *
from lake.comdev_web_traffic_wls
where 1=1
and date is not null
order by date desc


---- metas por KAM
select *
FROM lake.comdev_metas_b2b_kam

select 
director,
sum(meta_gb) as meta_g
FROM tmp.metas_b2b_kam
where 1=1
group by 1

select *
FROM raw.comdev_metas_b2b_kam

-------
SELECT 
  parent_channel,
  SUM(CAST(GB AS DOUBLE)) AS gb
FROM raw.b2b_planning_budget_reconocimiento
WHERE 1=1
GROUP BY parent_channel
LIMIT 100



-------

-- Budget WLs

SELECT *
FROM raw.wls_planning_budget_reconocimiento_stg wls

SELECT *
FROM lake.wls_planning_budget_reconocimiento wls

SELECT lob,
sum(gb) as gb, 
sum(net_revenues) as nr, 
sum(fvm) as fvm
FROM lake.wls_planning_budget_reconocimiento wls
group by 1


---Metas comerciales B2B
SELECT 
  site,
  region,
  CASE 
    WHEN site = 'Globales' THEN 'GL'
    WHEN site = 'Paraguay' THEN 'PY'
    WHEN site = 'Uruguay' THEN 'UY'
    WHEN site = 'Centroamerica' THEN 'CTA'
    ELSE region
  END AS country_code,
  brand,
  parent_channel,
  buy_type,
  lob,
  anio,
  anio_mes,
  SUM(meta_comercial) AS meta_comercial
FROM lake.b2b_comdev_metas_comerciales dev
WHERE anio_mes = '2025-08'
GROUP BY 1,2,3,4,5,6,7,8,9


-----

---Cartera KAMs IVA:

select 
agency_code as agency_code,
ag_name as ag_name,
group_code as group_code,
group_name as group_name,
country as market,
CASE
  WHEN agency_code IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  WHEN agency_code IN (
    'AP12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  WHEN country IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN country
  WHEN country IN ('US','PA','ES','CR') THEN 'GL'
  WHEN country IN ('UY','BO') THEN 'UY'
  ELSE 'GL'
END
AS country_code_corregido,
parent_channel as parent_channel,
region as region_html,
kam as kam,
director as lead,
manager as manager,
partition_date as partition_date
from  lake.cartera_kam_ag 
where partition_date = date'2025-08-20'

WHERE partition_date = (
    SELECT MAX(partition_date) 

--date_add('day',-1,CURRENT_DATE) 


------

---- Segmentación agencias Iva

select * from 
lake.segmentacion_ag_b2b
where partition_date = date_add('day',-1,CURRENT_DATE) 


---
---Cartera KAMs IVA:

select 
agency_code as agency_code,
ag_name as ag_name,
group_code as group_code,
group_name as group_name,
country as market,
CASE
  WHEN agency_code IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  WHEN agency_code IN (
    'AP12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  WHEN country IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN country
  WHEN country IN ('US','PA','ES','CR') THEN 'GL'
  WHEN country IN ('UY','BO') THEN 'UY'
  ELSE 'GL'
END
AS country_code_corregido,
parent_channel as parent_channel,
region as region_html,
kam as kam,
director as lead,
manager as manager,
partition_date as partition_date
from  lake.cartera_kam_ag 
where partition_date = date'2025-08-20'


---Cartera actualizada + segmentacion

SELECT 
    * 
FROM 
    lake.segment_ag_group a
LEFT JOIN (
    SELECT 
        * 
    FROM 
        raw.cartera_b2b_v1 b
    where partition_date = (select max(partition_date) from raw.cartera_b2b_v1) 
    ) b on a.group_code = coalesce(b.group_code, b.agency_code)
where 1=1
and a.partition_date = date_add('day',-1,CURRENT_DATE) 
and a.group_code = 'AAG00002711'



---
--Cartera Agencias B2B version 2025-09-04
select *
from raw.cartera_b2b_v1
where 1=1
--and agency_code = 'AP12147'





--Cartera Agencias adaptado a PBI de KAMS alcance metas
select 
agency_code,
ag_name,
group_code,
group_name,
country as market,
CASE
  WHEN agency_code IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  WHEN agency_code IN (
    'AP12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  WHEN country IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN country
  WHEN country IN ('US','PA','ES','CR') THEN 'GL'
  WHEN country IN ('UY','BO') THEN 'UY'
  ELSE 'GL'
END
AS country_code_corregido,
parent_channel,
region as region_html,
kam,
director as lead,
manager,
partition_date
from raw.cartera_b2b_v1
where 1=1
--and agency_code = 'AP12147'

--columnas:
agency_code,
ag_name,
group_code,
group_name,
country,
parent_channel,
region,
kam,
director,
manager,
partition_date


AAG00002711
AAG00002708
AAG00002717

----
--Segmentacion Agencias B2B version 2025-10-03
SELECT * 
FROM 
    lake.segment_ag_group a
    where 1=1
  --  and partition_date is not null
    and partition_date = date_add('day',-1,CURRENT_DATE) 
    
--columnas:
    partition_date
    group_code,
    active_segment,
    first_sale,
    last_sale,
    month_since_f_sale,
    new_agency_flg,
    
    share_vuelos,
    share_actividades,
    share_hoteles,
    share_paquetes,
    share_otros,
    product_segment,

    asp,
    segmento_asp,
    segmento_meses_aniguedad,
    intermitencia_promedio_dias,
    segmento_intermitencia,
    
    gb_mtd,
    gb_mtd_ly,
    gb_ytd,
    gb_ytd_ly,

 
    -- Dimeniones -- 
    -- Medidas --
    
    
    -----
    
    --- Forecast 2 ed venta por dia - Planning:
    
    SELECT
    TRY_CAST(date_parse(trim(fecha), '%d/%m/%Y') AS DATE) AS fecha,
    --mes_proyectado,
    escenario,
    CASE
        WHEN lob_canal = 'B2B-MAY' THEN 'API'
        WHEN lob_canal = 'B2B-MIN' THEN 'HTML'
    END AS parent_channel,
    pais AS site,
   -- CASE
   --     WHEN producto = 'Hotels'            THEN 'Hoteles'
   --     WHEN producto = 'Flights'           THEN 'Vuelos'
   --     WHEN producto = 'Packages General'  THEN 'Carrito'
   --     WHEN producto = 'Packages Others'   THEN 'Carrito'
   --     ELSE 'ONA'
  --  END AS original_product,
   -- CASE
   --     WHEN viaje = 'International' THEN 'Int'
   --     WHEN viaje = 'Domestic'      THEN 'Nac'
   --     ELSE viaje
   -- END AS trip_type,
    ROUND(SUM(TRY_CAST(gross_bookings AS DOUBLE)),0)  AS gross_bookings,
    ROUND(SUM(TRY_CAST(net_revenue AS DOUBLE)),0)     AS net_revenue,
    ROUND(SUM(TRY_CAST(fvm AS DOUBLE)),0)             AS fvm
FROM raw.b2b_metas_planning
WHERE lob_canal IN ('B2B-MAY','B2B-MIN')
--and pais = 'Other Countries'
--and mes_proyectado = 'Septiembre'
group by 1,2,3,4



    SELECT
    TRY_CAST(date_parse(trim(fecha), '%d/%m/%Y') AS DATE) AS fecha,
    escenario,
    CASE
        WHEN lob_canal = 'B2B-MAY' THEN 'API'
        WHEN lob_canal = 'B2B-MIN' THEN 'HTML'
    END AS parent_channel,
    pais AS site,
    CASE
        WHEN producto = 'Hotels'            THEN 'Hoteles'
        WHEN producto = 'Flights'           THEN 'Vuelos'
        WHEN producto = 'Packages General'  THEN 'Carrito'
        WHEN producto = 'Packages Others'   THEN 'Carrito'
        ELSE 'ONA'
    END AS original_product,
    CASE
        WHEN viaje = 'International' THEN 'Int'
        WHEN viaje = 'Domestic'      THEN 'Nac'
        ELSE viaje
    END AS trip_type,
    TRY_CAST(gross_bookings AS DOUBLE)  AS gross_bookings,
    TRY_CAST(net_revenue AS DOUBLE)     AS net_revenue,
    TRY_CAST(fvm AS DOUBLE)             AS fvm
FROM raw.b2b_metas_planning
WHERE lob_canal IN ('B2B-MAY','B2B-MIN')
and pais = 'Other Countries'
--group by 1,2,3,4,5,6


select *
FROM raw.b2b_metas_planning

select distinct
escenario as escenario,
pais AS site
FROM raw.b2b_metas_planning
WHERE lob_canal IN ('B2B-MAY','B2B-MIN')
    

---

Select *
from data.lake.cartera_b2b_v1
where partition_date is not null
order by partition_date desc

Select *
from raw.cartera_b2b_v1
where 1=1
and ag_name not like '%Test%'
order by director desc
 

---------------------------------


--Cartera Agencias adaptado a PBI de KAMS alcance metas
select 
agency_code,
ag_name,
group_code,
group_name,
country as market,
CASE
  WHEN agency_code IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  WHEN agency_code IN (
    'AP12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  WHEN country IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN country
  WHEN country IN ('US','PA','ES','CR') THEN 'GL'
  WHEN country IN ('UY','BO') THEN 'UY'
  ELSE 'GL'
END
AS country_code_corregido,
parent_channel,
region as region_html,
kam,
director as lead,
manager,
partition_date
from raw.cartera_b2b_v1
where 1=1

select *
from raw.cartera_b2b_v1
where 1=1
and (agency_code in 
('AG00005243',
'logi',
'AG00004547',
'AG00005659',
'AG00005077',
'AG00003421',
'hbeds',
'AG00005240',
'AG00004548') 
or group_code in 
('AG00005243',
'logi',
'AG00004547',
'AG00005659',
'AG00005077',
'AG00003421',
'hbeds',
'AG00005240',
'AG00004548') 
)

