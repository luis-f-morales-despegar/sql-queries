--- Consulta en Python:

with agencias as (
    select
        case when channel.channel_name = 'expedia' then 'expedia' else p.partner_code end as agency_code
        ,MAX(gr.agency_group_code) as group_code
 ,MAX(gr.agency_group_name) as group_name
        ,max(CASE
            WHEN ag.name IS NULL THEN p.name
            ELSE ag.name
            END) as ag_name
        ,max(p.country) as agency_country
        ,max(p.type) as agency_type
        ,max(p.segment) as agency_segment
        ,max(p.status) as status
        ,max(kk.kam) as kam
        ,max(kk.manager) as manager
        ,max(kk.director) as director
        ,max(kk.region) as region
    from data.lake.ch_bo_partner_partner p
    inner join data.lake.ch_bo_partner_channel channel on p.id = channel.id_partner
    left join data.lake.ch_bo_partner_legal_info l on l.partner_id = p.id
    left join raw.seed_b2b_kams k on k.agency_code = case when channel.channel_name = 'expedia' then 'Expedia' else p.partner_code end
    left join (
        SELECT
            *
        FROM
            raw.cartera_b2b_v1 a
            where
                partition_date = (select max(partition_date) from raw.cartera_b2b_v1)
    )  kk on kk.agency_code = p.partner_code
    LEFT JOIN data.lake.ch_bo_partner_partner_joiner pj ON pj.attached = p.id
    LEFT JOIN data.lake.ch_bo_partner_partner ag ON pj.joiner = ag.id
    left join raw.b2b_dim_cluster_agencies gr on gr.agency_code = p.partner_code
    where channel.channel_name  in (select cl.channel from raw.b2b_dim_channel_by_lob cl where cl.lob in ('B2B'))
    group by 1
),
fact_sales as (
    select
          case when s.channel = 'expedia' then 'Expedia' else s.partner_id end as agency_code
          ,s.country_code
          ,s.gestion_date
          ,case
            when coalesce(s.parent_channel, cl.parent_channel) = 'Agencias afiliadas' then 'AGENCY'
            ELSE coalesce(s.parent_channel, cl.parent_channel)
           END as parent_channel
          ,s.buy_type_code as original_product
          ,s.gestion_gb * s.confirmation_gradient as gb
    from analytics.bi_sales_fact_sales_recognition s
    join analytics.bi_pnlop_fact_current_model pnl on s.product_id = pnl.product_id and pnl.date_reservation_year_month >= '2024-01'
    join analytics.bi_transactional_fact_transactions tx on tx.transaction_code = cast(s.transaction_code as varchar) and tx.reservation_year_month_period >= '2024-01'
    join raw.b2b_dim_channel_by_lob cl on cl.channel = s.channel and cl.lob = 'B2B'
    where s.partition_period >= '2020-01'
    and s.gestion_date >= date'2024-01-01'
    and tx.reservation_year_month >= date'2023-01-01'
    and s.line_of_business_code = 'B2B'
    and pnl.line_of_business = 'B2B'
)
SELECT
    s.agency_code,
    a.ag_name,
    group_code,
    group_name,
    s.parent_channel,
    coalesce(a.agency_country, s.country_code ) as country,
    coalesce(a.kam, 'TBD') as kam_name,
    coalesce(a.manager, 'TBD') as kam_manager,
    coalesce(a.director, 'TBD') as director,
    coalesce(a.region, 'TBD') as region,
    sum(case when gestion_date between date_add('day',-31,CURRENT_DATE) and date_add('day',-1,CURRENT_DATE) then gb else 0 end) as gb_l30d,
    SUM(CASE WHEN s.gestion_date >= DATE_TRUNC('year', CURRENT_DATE) THEN s.gb ELSE 0 END) as gb_ytd,
    SUM(CASE WHEN s.gestion_date >= DATE_TRUNC('year', date_add('year',-1,CURRENT_DATE)) AND s.gestion_date < DATE_TRUNC('year', CURRENT_DATE) THEN s.gb ELSE 0 END) AS gb_ytd_ly,
    max(gestion_date) as last_sale
FROM fact_sales s
LEFT JOIN agencias a on a.agency_code = s.agency_code and a.agency_type = s.parent_channel
where a.status = 'ACTIVE' or a.status is null
--and  s.ag_name is null
--and s.agency_code in ('AG00010440','AG00008858', 'AG00008874')
group by 1,2,3,4,5,6,7,8,9,10



    from data.lake.ch_bo_partner_partner p
    inner join data.lake.ch_bo_partner_channel channel on p.id = channel.id_partner
    left join data.lake.ch_bo_partner_legal_info l on l.partner_id = p.id
    left join raw.seed_b2b_kams k on k.agency_code = case when channel.channel_name = 'expedia' then 'Expedia' else p.partner_code end
    
    select *
    from data.lake.ch_bo_partner_legal_info
    where legal_name in 
    
    
    ('AG00010440','AG00008858', 'AG00008874')
    
    select *
    from data.lake.ch_bo_partner_partner p
    
    
    select 
    p.partner_code,
    p.name,
    l.legal_name
        from data.lake.ch_bo_partner_partner p
    inner join data.lake.ch_bo_partner_channel channel on p.id = channel.id_partner
    left join data.lake.ch_bo_partner_legal_info l on l.partner_id = p.id
    where p.partner_code in ('AG87696','AG36461', 'AG00039316')
    












    
    
    
    --- Version agrupada
    
    WITH agencias AS (
    SELECT
        CASE WHEN channel.channel_name = 'expedia' THEN 'Expedia' ELSE p.partner_code END AS agency_code,
        MAX(gr.agency_group_code) AS group_code,
        MAX(gr.agency_group_name) AS group_name,
        MAX(CASE WHEN ag.name IS NULL THEN p.name ELSE ag.name END) AS ag_name,
        MAX(p.country) AS agency_country,
        MAX(p.type) AS agency_type,
        MAX(p.segment) AS agency_segment,
        MAX(p.status) AS status,
        MAX(kk.kam) AS kam,
        MAX(kk.manager) AS manager,
        MAX(kk.director) AS director,
        MAX(kk.lead) as lead,
        MAX(kk.region) AS region
    FROM data.lake.ch_bo_partner_partner p
    INNER JOIN data.lake.ch_bo_partner_channel channel ON p.id = channel.id_partner
    LEFT JOIN data.lake.ch_bo_partner_legal_info l ON l.partner_id = p.id
    LEFT JOIN raw.seed_b2b_kams k
        ON k.agency_code = CASE WHEN channel.channel_name = 'expedia' THEN 'Expedia' ELSE p.partner_code END
    LEFT JOIN (
        SELECT *
        FROM raw.cartera_b2b_v1
        WHERE partition_date = (SELECT MAX(partition_date) FROM raw.cartera_b2b_v1)
    ) kk ON kk.agency_code = p.partner_code
    LEFT JOIN data.lake.ch_bo_partner_partner_joiner pj ON pj.attached = p.id
    LEFT JOIN data.lake.ch_bo_partner_partner ag ON pj.joiner = ag.id
    LEFT JOIN raw.b2b_dim_cluster_agencies gr ON gr.agency_code = p.partner_code
    WHERE channel.channel_name IN (
        SELECT cl.channel FROM raw.b2b_dim_channel_by_lob cl WHERE cl.lob IN ('B2B')
    )
    GROUP BY 1
),
fact_sales AS (
    SELECT
        CASE WHEN s.channel = 'expedia' THEN 'Expedia' ELSE s.partner_id END AS agency_code,
        s.country_code,
        s.gestion_date,
        CASE
            WHEN COALESCE(s.parent_channel, cl.parent_channel) = 'Agencias afiliadas' THEN 'AGENCY'
            ELSE COALESCE(s.parent_channel, cl.parent_channel)
        END AS parent_channel,
        s.buy_type_code AS original_product,
        s.gestion_gb * s.confirmation_gradient AS gb
    FROM analytics.bi_sales_fact_sales_recognition s
    JOIN analytics.bi_pnlop_fact_current_model pnl
        ON s.product_id = pnl.product_id AND pnl.date_reservation_year_month >= '2024-01'
    JOIN analytics.bi_transactional_fact_transactions tx
        ON tx.transaction_code = CAST(s.transaction_code AS VARCHAR)
       AND tx.reservation_year_month_period >= '2024-01'
    JOIN raw.b2b_dim_channel_by_lob cl
        ON cl.channel = s.channel AND cl.lob = 'B2B'
    WHERE s.partition_period >= '2020-01'  
      AND s.gestion_date >= DATE '2024-01-01'
      AND tx.reservation_year_month >= DATE '2023-01-01'
      AND s.line_of_business_code = 'B2B'
      AND pnl.line_of_business = 'B2B'
),
-- 🔒 Encapsulamos el resultado “base”
base AS (
    SELECT
        s.agency_code,
        a.ag_name,
        group_code,
        group_name,
        s.parent_channel,
        COALESCE(a.agency_country, s.country_code) AS country,
        COALESCE(a.kam, 'TBD')      AS kam_name,
        COALESCE(a.manager, 'TBD')  AS kam_manager,
        COALESCE(a.lead, 'TBD')  AS kam_lead,  
        COALESCE(a.director, 'TBD') AS director,
        COALESCE(a.region, 'TBD')   AS region,
        SUM(CASE
              WHEN gestion_date BETWEEN DATE_ADD('day', -31, CURRENT_DATE)
                                   AND DATE_ADD('day',  -1, CURRENT_DATE)
              THEN gb ELSE 0 END) AS gb_l30d,
        SUM(CASE WHEN s.gestion_date >= DATE_TRUNC('year', CURRENT_DATE)
                 THEN s.gb ELSE 0 END) AS gb_ytd,
        SUM(CASE
              WHEN s.gestion_date >= DATE_TRUNC('year', DATE_ADD('year', -1, CURRENT_DATE))
               AND s.gestion_date <  DATE_TRUNC('year', CURRENT_DATE)
              THEN s.gb ELSE 0 END) AS gb_ytd_ly,
        MAX(gestion_date) AS last_sale
    FROM fact_sales s
    LEFT JOIN agencias a
      ON a.agency_code = s.agency_code
     AND a.agency_type = s.parent_channel
    WHERE 1=1
   -- and a.status = 'ACTIVE' OR a.status IS NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
-- 🧾 Aquí eliges las columnas específicas desde la subconsulta/CTE “base”
SELECT
    agency_code,
    ag_name,
    group_code,
    group_name,
    parent_channel,
    country,
    region,
    director,
    kam_manager,
    kam_lead,
    kam_name,
    gb_l30d,
    gb_ytd,
    gb_ytd_ly,
    last_sale
FROM base
where 1=1
and ag_name IS NULL

--and kam_lead <> 'TBD'


AND (
      ag_name IS NULL
   OR ag_name NOT LIKE '%Test%'
)

select *
from data.lake.ch_bo_partner_legal_info l
where 1=1


select *
from data.lake.ch_bo_partner_partner p

created


------------------------------------------------
-----------------------------------------

WITH agencias AS (
    SELECT
        CASE
            WHEN channel.channel_name = 'expedia' THEN 'expedia'
            ELSE p.partner_code
        END AS agency_code,
        MAX(p.created)  AS created,
        MAX(gr.agency_group_code)  AS group_code,
        MAX(gr.agency_group_name)  AS group_name,
        MAX(
          COALESCE(
            NULLIF(
              TRIM( CASE WHEN p.name IS NULL THEN ag.name ELSE p.name END ),
              ''
            ),
            NULLIF(TRIM(l.legal_name), ''),
            p.name
          )
        ) AS ag_name,
        MAX(p.country)  AS agency_country,
        MAX(p.type)     AS agency_type,
        MAX(p.segment)  AS agency_segment,
        MAX(p.status)   AS status,
        MAX(kk.kam)     AS kam,
        MAX(kk.manager) AS manager,
        MAX(kk.director)AS director,
        MAX(kk.lead)    AS lead,
        MAX(kk.region)  AS region
    FROM data.lake.ch_bo_partner_partner p
    INNER JOIN data.lake.ch_bo_partner_channel channel ON p.id = channel.id_partner
    LEFT JOIN data.lake.ch_bo_partner_legal_info l ON l.partner_id = p.id
    LEFT JOIN raw.seed_b2b_kams k ON k.agency_code = CASE WHEN channel.channel_name = 'expedia' THEN 'expedia' ELSE p.partner_code END
    LEFT JOIN (SELECT * FROM raw.cartera_b2b_v1 WHERE partition_date = (SELECT MAX(partition_date) FROM raw.cartera_b2b_v1)) kk ON kk.agency_code = p.partner_code
    LEFT JOIN data.lake.ch_bo_partner_partner_joiner pj ON pj.attached = p.id
    LEFT JOIN data.lake.ch_bo_partner_partner ag ON pj.joiner = ag.id
    LEFT JOIN raw.b2b_dim_cluster_agencies gr ON gr.agency_code = p.partner_code
    WHERE channel.channel_name IN (SELECT cl.channel FROM raw.b2b_dim_channel_by_lob cl WHERE cl.lob IN ('B2B'))
    GROUP BY 1
),
fact_sales AS (
    SELECT
        CASE WHEN s.channel = 'expedia' THEN 'expedia' ELSE s.partner_id END AS agency_code,
        s.country_code,
        s.gestion_date,
        CASE WHEN COALESCE(s.parent_channel, cl.parent_channel) = 'Agencias afiliadas' THEN 'AGENCY' ELSE COALESCE(s.parent_channel, cl.parent_channel) END AS parent_channel,
        s.buy_type_code AS original_product,
        s.gestion_gb * s.confirmation_gradient AS gb
    FROM analytics.bi_sales_fact_sales_recognition s
    JOIN analytics.bi_pnlop_fact_current_model pnl ON s.product_id = pnl.product_id AND pnl.date_reservation_year_month >= '2024-01'
    JOIN analytics.bi_transactional_fact_transactions tx ON tx.transaction_code = CAST(s.transaction_code AS VARCHAR) AND tx.reservation_year_month_period >= '2024-01'
    JOIN raw.b2b_dim_channel_by_lob cl ON cl.channel = s.channel AND cl.lob = 'B2B'
    WHERE s.partition_period >= '2024-01'
      AND s.gestion_date >= DATE '2024-01-01'
      AND tx.reservation_year_month >= DATE '2023-01-01'
      AND s.line_of_business_code = 'B2B'
      AND pnl.line_of_business = 'B2B'
),
base AS (
    SELECT
        s.agency_code,
        a.ag_name,
        a.status,
        cast(a.created as date) as created,
        a.group_code,
        a.group_name,
        s.parent_channel,
        COALESCE(a.agency_country, s.country_code) AS country,
        COALESCE(a.kam,      'TBD') AS kam_name,
        COALESCE(a.manager,  'TBD') AS kam_manager,
        COALESCE(a.lead,     'TBD') AS kam_lead,
        COALESCE(a.director, 'TBD') AS director,
        COALESCE(a.region,   'TBD') AS region,
        ROUND(SUM(CASE WHEN s.gestion_date BETWEEN date_add('day', -31, current_date) AND date_add('day', -1, current_date) THEN s.gb ELSE 0 END),2) AS gb_l30d,
        ROUND(SUM(CASE WHEN s.gestion_date >= date_trunc('year', current_date) THEN s.gb ELSE 0 END),2) AS gb_ytd,
        ROUND(SUM(CASE WHEN s.gestion_date >= date_trunc('year', date_add('year', -1, current_date)) AND s.gestion_date < date_trunc('year', current_date) THEN s.gb ELSE 0 END),2) AS gb_ytd_ly,
        MAX(s.gestion_date) AS last_sale
    FROM fact_sales s
    LEFT JOIN agencias a ON a.agency_code = s.agency_code   --AND a.agency_type = s.parent_channel
    WHERE 1=1
 --   and a.status = 'ACTIVE' OR a.status IS NULL
    GROUP BY s.agency_code,a.ag_name,a.status,a.created, a.group_code,a.group_name,s.parent_channel,COALESCE(a.agency_country, s.country_code),COALESCE(a.kam, 'TBD'),COALESCE(a.manager, 'TBD'),COALESCE(a.lead, 'TBD'),COALESCE(a.director, 'TBD'),COALESCE(a.region, 'TBD')
),
base_ranked AS (
    SELECT b.*,ROW_NUMBER() OVER (PARTITION BY b.agency_code ORDER BY b.last_sale DESC NULLS LAST) AS rn
    FROM base b
)
SELECT
    agency_code,
    ag_name,
    group_code,
    group_name,
    parent_channel,
    country,
    region,
    director,
    kam_manager,
    kam_lead,
    kam_name,
    gb_l30d,
    gb_ytd,
    gb_ytd_ly,
    last_sale,
    status,
    created
FROM base_ranked
WHERE 1=1
and rn = 1  --- esto hace que se traiga el agency_code por parent channel con la venta más reciente
and agency_code is not null
and agency_code <> ''
and agency_code <> 'agency_code'
and ag_name like '%Dida%'
--AND ag_name IS null
--and agency_code in ('WLAG00070653','WLAG00066510', 'WLAG00057339', 'AG00085174','AG00077446', 'AG00063520')
--order by agency_code desc
--and agency_code = 'AG00008165'


AG87696
AG36461
AG00039316



WITH agencias AS (


    SELECT
        CASE
            WHEN channel.channel_name = 'expedia' THEN 'expedia'
            ELSE p.partner_code
        END AS agency_code,
        MAX(gr.agency_group_code)  AS group_code,
        MAX(gr.agency_group_name)  AS group_name,
        MAX(
          COALESCE(
            NULLIF(
              TRIM( CASE WHEN p.name IS NULL THEN ag.name ELSE p.name END ),
              ''
            ),
            NULLIF(TRIM(l.legal_name), ''),
            p.name
          )
        ) AS ag_name,
        MAX(p.country)  AS agency_country,
        MAX(p.type)     AS agency_type,
        MAX(p.segment)  AS agency_segment,
        MAX(p.status)   AS status,
        MAX(kk.kam)     AS kam,
        MAX(kk.manager) AS manager,
        MAX(kk.director)AS director,
        MAX(kk.lead)    AS lead,
        MAX(kk.region)  AS region
    FROM data.lake.ch_bo_partner_partner p
    INNER JOIN data.lake.ch_bo_partner_channel channel ON p.id = channel.id_partner
    LEFT JOIN data.lake.ch_bo_partner_legal_info l ON l.partner_id = p.id
    LEFT JOIN raw.seed_b2b_kams k ON k.agency_code = CASE WHEN channel.channel_name = 'expedia' THEN 'expedia' ELSE p.partner_code END
    LEFT JOIN (SELECT * FROM raw.cartera_b2b_v1 WHERE partition_date = (SELECT MAX(partition_date) FROM raw.cartera_b2b_v1)) kk ON kk.agency_code = p.partner_code
    LEFT JOIN data.lake.ch_bo_partner_partner_joiner pj ON pj.attached = p.id
    LEFT JOIN data.lake.ch_bo_partner_partner ag ON pj.joiner = ag.id
    LEFT JOIN raw.b2b_dim_cluster_agencies gr ON gr.agency_code = p.partner_code
    WHERE channel.channel_name IN (SELECT cl.channel FROM raw.b2b_dim_channel_by_lob cl WHERE cl.lob IN ('B2B'))
    and p.partner_code in ('WLAG00070653','WLAG00066510', 'WLAG00057339', 'AG00085174','AG00077446', 'AG00063520')
    GROUP BY 1
    
    
    
    fact_sales AS (
    
    
    SELECT
        CASE WHEN s.channel = 'expedia' THEN 'expedia' ELSE s.partner_id END AS agency_code,
        s.country_code,
        s.gestion_date,
        CASE WHEN COALESCE(s.parent_channel, cl.parent_channel) = 'Agencias afiliadas' THEN 'AGENCY' ELSE COALESCE(s.parent_channel, cl.parent_channel) END AS parent_channel,
        s.buy_type_code AS original_product,
        s.gestion_gb * s.confirmation_gradient AS gb
    FROM analytics.bi_sales_fact_sales_recognition s
    JOIN analytics.bi_pnlop_fact_current_model pnl ON s.product_id = pnl.product_id AND pnl.date_reservation_year_month >= '2024-01'
    JOIN analytics.bi_transactional_fact_transactions tx ON tx.transaction_code = CAST(s.transaction_code AS VARCHAR) AND tx.reservation_year_month_period >= '2024-01'
    JOIN raw.b2b_dim_channel_by_lob cl ON cl.channel = s.channel AND cl.lob = 'B2B'
    WHERE s.partition_period >= '2024-01'
      AND s.gestion_date >= DATE '2024-01-01'
      AND tx.reservation_year_month >= DATE '2023-01-01'
      AND s.line_of_business_code = 'B2B'
      AND pnl.line_of_business = 'B2B'
    