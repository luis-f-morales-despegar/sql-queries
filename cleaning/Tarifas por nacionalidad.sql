

select * 
from data.lake.bookedia_rate_plan
where allowed_passenger_nationalities is not null
and allowed_passenger_nationalities <> ''
and hotel_id = 266101
and type ='ADVANCED_PURCHASE'
and default_cancel_policy = 'Refundable - 48 Hrs'
and id = 1181117
limit 1000



SELECT
    t.hotel_id,
    pr.hotel_name,
    t.id,
    t.default_cancel_policy,
    t.type,
    u.allowed_passenger_nationalities
FROM data.lake.bookedia_rate_plan t
LEFT JOIN data.analytics.bi_transactional_fact_products pr
    ON pr.hotel_despegar_id = CAST(t.hotel_id AS VARCHAR)
   AND pr.reservation_year_month IS NOT NULL      
CROSS JOIN UNNEST(
    CASE
        WHEN t.allowed_passenger_nationalities IS NULL
            THEN ARRAY[CAST(NULL AS VARCHAR)]
        WHEN trim(t.allowed_passenger_nationalities) = ''
            THEN ARRAY['']
        ELSE split(t.allowed_passenger_nationalities, '|')
    END
) AS u(allowed_passenger_nationalities)
where t.allowed_passenger_nationalities is not null and t.allowed_passenger_nationalities <> ''



    ---- cartera de alojamiento
   
    
    WITH max_week AS (
  SELECT anio_semana
  FROM data.lake."bi_sourcing_cartera_alojamiento"
  where anio_semana is not null
  ORDER BY anio_semana DESC
  LIMIT 1
)
SELECT 
distinct(t.cadena), 
max(t.anio_semana) as partition_anio_semana
FROM data.lake.bi_sourcing_cartera_alojamiento t
JOIN max_week u ON t.anio_semana = u.anio_semana
where cadena is not null
and cadena <> 'HTL hoteles'
and cadena <> 'Casa hotéis'
and cadena <> 'Sirenis Hotels & resorts'
and cadena <> ''
and cadena <> 'Voa'
group by 1
order by cadena





 MAX(pr.hotel_name)            AS Hotel,
 
 select 
 hotel_despegar_id,
 hotel_name
	    FROM data.analytics.bi_transactional_fact_products pr
	    where reservation_year_month is not null
	    and hotel_name is not null

left join pr.hotel_despegar_id on t.hotel_id






------------------

--- Andres W

select --*
distinct hotel_id
--id,
--countries,
,allowed_passenger_nationalities
--channel, count(*)
from data.lake.bookedia_rate_plan rp
--group by 1 order by 2 desc
where 1=1
	--[Aptos API]
	and ( channel = 'default' or channel like '%api%' )
	and payment_method = 'PREPAID'
	--[Activos]
	and current_date between start_date and end_date
	--[Tarifas por Nat Tag]
	and allowed_passenger_nationalities is not null
	and allowed_passenger_nationalities != ''
	
	
	-----
	
	---- Nacionaldiad por transacción realizada ->  Juan Fra
	
	SELECT DISTINCT 
	t.transaction_code,
	brr.client_nationality AS nattag
from data.analytics.bi_transactional_fact_products p
	left join data.analytics.bi_transactional_fact_transactions t on p.transaction_code = t.transaction_code
	left join data.lake.bookedia_rsv_reservation brr              on p.product_id = concat('20', brr.transaction_id)
where 1=1
	and t.reservation_year_month >= cast('2025-01-01' as date)
	and p.reservation_year_month >= cast('2025-01-01' as date)
	AND t.parent_channel = 'API'
	and t.reservation_date >= date '2025-06-27'
