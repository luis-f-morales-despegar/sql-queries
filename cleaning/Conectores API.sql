--- Compartida por Andres Wajn$ztok

select *
from data.raw.dim_table_hoteldo_api_partner_wrapper_conector
where partner_code in ('AP12907', 'AP12908', 'AP12910')
--where partner_code in ('AP12910', 'AP12907', 'AP12908', 'AP12230', 'AP11775')

