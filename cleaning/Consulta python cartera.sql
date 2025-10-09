-- consulta python

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
    where s.partition_period >= '202-01'
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
group by 1,2,3,4,5,6,7,8,9,10