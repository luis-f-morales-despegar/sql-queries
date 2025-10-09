
-- Query de Marian + Market, Lead, Manager y Kam desde Query de Vic -- se añade info Expedia y de HBX Vero, y ag Ruben 2025-06-26

WITH agencias AS (    
    SELECT 
        CASE WHEN channel.channel_name = 'expedia' THEN 'Expedia' ELSE p.partner_code END AS agency_code,    
        MAX(p.partner_code) AS partner_code,
        MAX(CASE WHEN channel.channel_name = 'expedia' THEN 'Expedia' ELSE p.name END) AS fantasy_name,
        MAX(channel.channel_name) AS channel,
        MAX(p.country) AS market,
        MAX(p.type) AS type,
        MAX(p.segment) AS segment,
        MAX(p.status) AS status,
        MAX(cl.lob) AS lob
    FROM data.lake.ch_bo_partner_partner p
    INNER JOIN data.lake.ch_bo_partner_channel channel ON p.id = channel.id_partner 
    INNER JOIN raw.b2b_dim_channel_by_lob cl ON cl.channel = channel.channel_name AND cl.lob = 'B2B'
    GROUP BY 1
),
seed_kams AS (
    SELECT 
        seed.agency_code AS ag_code,
        MAX(seed.director) AS director,
        MAX(COALESCE(seed.manager, 'SC')) AS manager,
        MAX(seed.kam) AS kam
    FROM raw.seed_b2b_kams seed
    GROUP BY seed.agency_code
),
vic AS (
    SELECT  
        ag.agency_code,
        MAX(ag.market) AS market,
        MAX(CASE 
            WHEN ag.market = 'BR' THEN 'Marcio Nogueira'
            WHEN ag.market IN ('AR','CO','CL','MX','PE','DO') THEN 'Gaston Carne'
            WHEN ag.market IN ('PA','US','UY','CR','EC') THEN 'Veronica Odetti'
            ELSE 'NA'
        END) AS lead,
        MAX(CASE 
            WHEN k.manager <> 'SC' THEN k.manager
            WHEN k.director = 'Marcio Nogueira' AND ag.type = 'API' THEN 'Aline Sobreira'
            WHEN k.director = 'Veronica Odetti' THEN k.kam
            WHEN k.director NOT IN ('Marcio Nogueira','Gaston Carne') THEN k.director
            ELSE 'SC'
        END) AS manager,
        MAX(CASE 
            WHEN k.kam LIKE '%Casimi%' THEN 'Aline Sobreira'
            ELSE k.kam
        END) AS kam
    FROM agencias ag
    LEFT JOIN seed_kams k ON k.ag_code = ag.agency_code
    LEFT JOIN raw.b2b_dim_cluster_agencies ac ON ag.agency_code = ac.agency_code
    GROUP BY ag.agency_code
)
SELECT
    pp.name,
    pp.type,
    pp.reference_id,
    pp.status,
    IF(pp.country IN ('AR','CO','EC','PE','BR','CL','MX'), pp.country, 'OT') AS pais,
    pp.partner_code,
    DATE_FORMAT(TRY_CAST(pp.created AS TIMESTAMP), '%M %d, %Y') AS created,
    DATE_FORMAT(TRY_CAST(pp.last_modification_date AS TIMESTAMP), '%M %d, %Y') AS last_modification_date,
    pp.current_account_status,
    pp.business,
CASE
    WHEN ch.channel_name = 'expedia' THEN 'Expedia'
    ELSE COALESCE(ag.name, pp.name)
END AS nombre_final,
    ch.channel_name,
    ctr.conector,
    vic.market,
CASE
    WHEN ch.channel_name = 'expedia' THEN 'Veronica Odetti'
    WHEN ag.name LIKE '%Hotelbeds%' OR pp.name LIKE '%Hotelbeds%' THEN 'Veronica Odetti'
    WHEN ag.name LIKE '%Toctoc Viajes%' OR pp.name LIKE '%Toctoc Viajes%' THEN 'Gaston Carne'
    WHEN ag.name LIKE '%Hiperviajes%' OR pp.name LIKE '%Hiperviajes%' THEN 'Gaston Carne'
    WHEN ag.name LIKE '%JETMAR VIAJES%' OR pp.name LIKE '%JETMAR VIAJES%' THEN 'Gaston Carne'
    WHEN ag.name LIKE '%Turisport Centro%' OR pp.name LIKE '%Turisport Centro%' THEN 'Gaston Carne'
    WHEN ag.name LIKE '%BN Tours - 60929%' OR pp.name LIKE '%BN Tours - 60929%' THEN 'Gaston Carne'
    WHEN ag.name LIKE '%Maral Turismo%' OR pp.name LIKE '%Maral Turismo%' THEN 'Gaston Carne'
    ELSE vic.lead
END AS lead,
    vic.manager,
   IF(ch.channel_name = 'expedia', 'Arina Garutti', vic.kam) AS kam
FROM data.lake.ch_bo_partner_partner pp
LEFT JOIN data.lake.ch_bo_partner_partner_intermediary pi ON pp.partner_intermediary_id = pp.id
LEFT JOIN data.lake.ch_bo_partner_partner_joiner pj ON pj.attached = pp.id
LEFT JOIN data.lake.ch_bo_partner_partner ag ON pj.joiner = ag.id
LEFT JOIN data.lake.ch_bo_partner_channel ch ON pp.id = ch.id_partner
LEFT JOIN data.raw.dim_table_hoteldo_api_partner_wrapper_conector ctr ON ctr.partner_code = pp.partner_code
LEFT JOIN vic ON vic.agency_code = pp.partner_code
where
    1=1
   and (pp.business IN ('hoteldo','hoteldo_affiliated') 
     OR (pp.business = 'despegar' AND channel_name IN ('expedia','agency-pam-pp-ctrip')))
--     and ch.channel_name = 'expedia'
--     and ag.name like '%Toctoc Viajes%'
 --   AND conector LIKE '%TRAVELGATE%'
   -- and lead = 'Veronica Odetti'
 --   and pp.reference_id in('AP12904') 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;



-------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------

--- Query de Marian

Select
    pp.name,
    pp.type,
    pp.reference_id,
    pp.status,
    if( pp.country in ('AR','CO','EC','PE','BR','CL','MX'),
    pp.country,'OT') as pais,
    pp.partner_code,
    DATE_FORMAT(TRY_CAST(pp.created AS TIMESTAMP), '%M %d, %Y') AS created,
    DATE_FORMAT(TRY_CAST(pp.last_modification_date AS TIMESTAMP), '%M %d, %Y') AS last_modification_date,
    pp.current_account_status,
    pp.business,
    case when ag.name is null then pp.name else ag.name end as nombre_final,
    ch.channel_name,
    ctr.conector
From data.lake.ch_bo_partner_partner pp
left join data.lake.ch_bo_partner_partner_intermediary pi on pp.partner_intermediary_id = pp.id
left join data.lake.ch_bo_partner_partner_joiner pj on pj.attached = pp.id
left join data.lake.ch_bo_partner_partner ag on pj.joiner = ag.id
left join data.lake.ch_bo_partner_channel ch on pp.id = ch.id_partner
left join data.raw.dim_table_hoteldo_api_partner_wrapper_conector ctr on ctr.partner_code = pp.partner_code
Where
(pp.business in ('hoteldo','hoteldo_affiliated') or (pp.business='despegar' and channel_name in ('expedia','agency-pam-pp-ctrip')))
and conector like '%TRAVELGATE%'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13

------

select *
From data.lake.ch_bo_partner_partner pp
left join data.lake.ch_bo_partner_partner_intermediary pi on pp.partner_intermediary_id = pp.id
left join data.lake.ch_bo_partner_partner_joiner pj on pj.attached = pp.id
left join data.lake.ch_bo_partner_partner ag on pj.joiner = ag.id
left join data.lake.ch_bo_partner_channel ch on pp.id = ch.id_partner
left join data.raw.dim_table_hoteldo_api_partner_wrapper_conector ctr on ctr.partner_code = pp.partner_code
Where
(pp.business in ('hoteldo','hoteldo_affiliated') or (pp.business='despegar' and channel_name in ('expedia','agency-pam-pp-ctrip')))
limit 100

--- Conectores: Compartida por Andres Wajn$ztok

select *
from data.raw.dim_table_hoteldo_api_partner_wrapper_conector
where partner_code in ('AP12910', 'AP12907', 'AP12908', 'AP12230', 'AP11775')


--------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

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




---
select *
from raw.seed_b2b_kams seed
limit 100


--- Tabla en desuso - Tablero Arge

SELECT * 
FROM raw.b2b_dim_agencies_by_kam
WHERE type = 'API'
AND agency_code IN ('AP11944','AP12913')
LIMIT 100;

--AND agency_code LIKE '%AP11944%'


--------------------------------
--Alta agencias


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
       ,max( case when p.country in ('BR') then 'Marcio Nogueira'
                    when p.country in ('AR','CO','CL','MX','PE','DO') then 'Gaston Carne'
                    when p.country in ('PA','US','UY','CR','EC') then 'Veronica Odetti'
                    else 'NA'
                 end) as lead    
    from data.lake.ch_bo_partner_partner p
    inner join data.lake.ch_bo_partner_channel channel on p.id = channel.id_partner 
    inner join raw.b2b_dim_channel_by_lob cl on cl.channel = channel.channel_name and cl.lob = 'B2B'
    where 1=1
   -- and p.partner_code in ('AP12903','AP12904') 
    group by 1

    
    select *
    from data.lake.ch_bo_partner_partner p
    where 1=1
  --  and p.partner_code in ('AP12903','AP12904') 
   -- and p.partner_code in ('AG72472')
    limit 100
    
    
    
    ------
    
   -- Query "Agencias" Fran Sales Tracking:
    
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
	   ,max( case when p.country in ('BR') then 'Marcio Nogueira'
		  		  when p.country in ('AR','CO','CL','MX','PE','DO') then 'Gaston Carne'
		  		  when p.country in ('PA','US','UY','CR','EC') then 'Veronica Odetti'
		  		  else 'NA'
		 		end) as lead	
	from data.lake.ch_bo_partner_partner p
	inner join data.lake.ch_bo_partner_channel channel on p.id = channel.id_partner 
	inner join raw.b2b_dim_channel_by_lob cl on cl.channel = channel.channel_name and cl.lob = 'B2B'
	group by 1
),
seed_kams as (
	select agency_code as ag_code, max(director) as director, max(coalesce(manager,'SC')) as manager, max(kam) as kam /*, max(coalesce(mail_kam,'NA')) as mail_kam*/
	from raw.seed_b2b_kams 
	group by 1
)
select  
	 ag.agency_code
	,max(ag.fantasy_name) as agency_name
	,max(ag.market) as market
	,max(coalesce(k.director, ag.lead)) as lead	  
	,max(case when k.manager <> 'SC' then k.manager
	      when k.director = 'Marcio Nogueira' and ag.type = 'API' then 'Aline Sobreira'
	      when k.director = 'Veronica Odetti' then k.kam	      
		  when k.director not in ('Marcio Nogueira','Gaston Carne') then k.director
	      else 'SC'	
	   end) as manager
	,max(k.kam) as kam
	--,max(k.mail_kam) as mail_kam
	,max(ag.segment) as segment
        ,max(ag.status) as ag_status
	,max(ag.type) as type
	,max(coalesce(ac.agency_group_code, ag.agency_code)) as agency_group_code
	,max(coalesce(ac.agency_group_name, ag.fantasy_name)) as agency_group_name 	
from agencias ag
left join seed_kams k on k.ag_code = ag.agency_code
left join raw.b2b_dim_cluster_agencies ac on ag.agency_code = ac.agency_code
group by 1