select distinct
            hotel_id,
            a.nombre_hotel,
            if(a.cadena is null, p.hotel_chain_name, a.cadena)    as cadena,
            market,
            area,
            tipo_de_cuenta
            --a.destino
        from data.lake.bi_sourcing_cartera_alojamiento a
        left join analytics.bi_transactional_fact_products p on a.hotel_id = cast(p.hotel_despegar_id as varchar)
        where anio_semana is not null 
        --and a.hotel_id in ('1631470','313486','312469','1631477','316485','1871123','214139','1631555','316774','222993','214577','342274','4832448','345181','347875','2169138','278533','1871464','1487777','4962455')
        and reservation_year_month is not null
            and (hotel_id, fecha_actualizacion) in (select hotel_id, max(fecha_actualizacion) as fecha_actualizacion 
                                                     from data.lake.bi_sourcing_cartera_alojamiento 
                                                     where anio_semana is not null 
                                                     group by 1)
limit 100