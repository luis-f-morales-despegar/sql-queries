------- Conteo de Agencias Nuevas HTML México -----

---------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

--- MODIFICADA: Query para agencias MIN HDO -- Viajes Bumeran ----------

SELECT
 --   "lake"."channels_bo_sale"."id" AS "id",
   -- "lake"."channels_bo_sale"."transaction_id" AS "transaction_id",
  --  "lake"."channels_bo_sale"."created" AS "created",
    date_format("lake"."channels_bo_sale"."created", '%Y') AS "year_created",
    date_format("lake"."channels_bo_sale"."created", '%m') AS "month_created",
 --   "lake"."channels_bo_sale"."status" AS "status",
  --  "lake"."channels_bo_sale"."type" AS "type",
 --   "lake"."channels_bo_sale"."agent_code" AS "agent_code",
  --  "lake"."channels_bo_sale"."partner_code" AS "partner_code", 
    CASE 
        WHEN lake.channels_bo_sale.partner_code = 'AG00008349' 
        THEN COALESCE(lake.channels_bo_sale.agent_code, 'agent_code_nulo')
        ELSE lake.channels_bo_sale.partner_code
    END AS partner_code_bumeran,
    "lake"."channels_bo_sale"."channel" AS "channel",
  --  "lake"."channels_bo_sale"."payment_status" AS "payment_status",
 --   "channels_bo_product"."sale_id" AS "channels_bo_product__sale_id",
 --   "channels_bo_product"."transaction_id" AS "channels_bo_product__transaction_id",
 --   "channels_bo_product"."checkin" AS "channels_bo_product__checkin",
 --   "channels_bo_product"."checkout" AS "channels_bo_product__checkout",
--    "channels_bo_product"."destination_country" AS "channels_bo_product__destination_country",
 --   "channels_bo_product"."destination_city" AS "channels_bo_product__destination_city",
 --   "channels_bo_product"."description" AS "channels_bo_product__description",
 --   "channels_bo_product"."emitted" AS "channels_bo_product__emitted",
--    "channels_bo_product"."cancelled" AS "channels_bo_product__cancelled",
--    "channels_bo_product"."payment_methods" AS "channels_bo_product__payment_methods",
 --   "channels_bo_product"."credit_card_payment_type" AS "channels_bo_product__credit_card_payment_type",
 --   "channels_bo_product"."coupon_type" AS "channels_bo_product__coupon_type",
 --   "channels_bo_product"."installments" AS "channels_bo_product__installments",
  --  "channels_bo_product"."card_type" AS "channels_bo_product__card_type",
--    "channels_bo_product"."currency" AS "channels_bo_product__currency",
 --   "channels_bo_product"."net_commission_partner" AS "channels_bo_product__net_commission_partner",
 --   "channels_bo_product"."gross_commission_partner" AS "channels_bo_product__gross_commission_partner",
 --   "channels_bo_product"."commission_admin_charges_partner" AS "channels_bo_product__commission_admin_charges_partner",
    "ch_bo_partner_partner"."name" AS "ch_bo_partner_partner__name",
    "ch_bo_partner_partner"."type" AS "ch_bo_partner_partner__type",
    "ch_bo_partner_partner"."reference_id" AS "ch_bo_partner_partner__reference_id",
    "ch_bo_partner_partner"."country" AS "ch_bo_partner_partner__country",
     SUM("channels_bo_product"."total") AS "channels_bo_product__total",
 --   "channels_bo_product"."net_cost" AS "channels_bo_product__net_cost",
 --   "channels_bo_product"."cost_tax" AS "channels_bo_product__cost_tax",
    ROUND(AVG("channels_bo_product"."conversion_rate"),4) AS "channels_bo_product__conversion_rate",
    ROUND(SUM("channels_bo_product"."total" * "channels_bo_product"."conversion_rate"),2) AS "gb_usd"
FROM "lake"."channels_bo_sale"
LEFT JOIN "lake"."channels_bo_product" "channels_bo_product"
       ON "lake"."channels_bo_sale"."id" = "channels_bo_product"."sale_id"
LEFT JOIN "lake"."ch_bo_partner_partner" "ch_bo_partner_partner"
       ON "lake"."channels_bo_sale"."partner_code" = "ch_bo_partner_partner"."reference_id"
WHERE
    (
        (LOWER("lake"."channels_bo_sale"."channel") LIKE '%hoteldo%')
        AND "ch_bo_partner_partner"."country" = 'MX'
        AND (
            "lake"."channels_bo_sale"."status" = 'ACTIVE'
            OR "lake"."channels_bo_sale"."status" = 'EMITTED'
        )
    )
and  date_format("lake"."channels_bo_sale"."created", '%Y') >= '2023'
--and     date_format("lake"."channels_bo_sale"."created", '%Y') ='2025'
-- and   date_format("lake"."channels_bo_sale"."created", '%m') = '01'
--and "ch_bo_partner_partner"."name" like '%Viajes Bumeran%'
group by 1,2,3,4,5,6,7,8

---------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

--- MODIFICIADA: Query para agencias Afiliadas D! --- Viajes Bumeran ----------

SELECT
 --   "lake"."channels_bo_sale"."id" AS "id",
 --   "lake"."channels_bo_sale"."transaction_id" AS "transaction_id",
 --  "lake"."channels_bo_sale"."created" AS "created",
    date_format("lake"."channels_bo_sale"."created", '%Y') AS "year_created",
    date_format("lake"."channels_bo_sale"."created", '%m') AS "month_created",
--    "lake"."channels_bo_sale"."status" AS "status",
--    "lake"."channels_bo_sale"."type" AS "type",
--    "lake"."channels_bo_sale"."agent_code" AS "agent_code",
--    "lake"."channels_bo_sale"."partner_code" AS "partner_code",
    COALESCE(
        CASE 
            WHEN lake.channels_bo_sale.partner_code IN ('AG00027437', 'AG21656') 
            THEN lake.channels_bo_sale.agent_code
            ELSE lake.channels_bo_sale.partner_code
        end, 'agent_code_nulo') as "partner_code_bumeran",
    "lake"."channels_bo_sale"."channel" AS "channel",
 --   "lake"."channels_bo_sale"."payment_status" AS "payment_status",
--    "channels_bo_product"."sale_id" AS "channels_bo_product__sale_id",
  --  "channels_bo_product"."transaction_id" AS "channels_bo_product__transaction_id",
 --   "channels_bo_product"."checkin" AS "channels_bo_product__checkin",
 --   "channels_bo_product"."checkout" AS "channels_bo_product__checkout",
 --   "channels_bo_product"."destination_country" AS "channels_bo_product__destination_country",
 --   "channels_bo_product"."destination_city" AS "channels_bo_product__destination_city",
 --   "channels_bo_product"."description" AS "channels_bo_product__description",
 --   "channels_bo_product"."emitted" AS "channels_bo_product__emitted",
--    "channels_bo_product"."cancelled" AS "channels_bo_product__cancelled",
--    "channels_bo_product"."payment_methods" AS "channels_bo_product__payment_methods",
 --   "channels_bo_product"."credit_card_payment_type" AS "channels_bo_product__credit_card_payment_type",
--    "channels_bo_product"."coupon_type" AS "channels_bo_product__coupon_type",
 --   "channels_bo_product"."installments" AS "channels_bo_product__installments",
 --   "channels_bo_product"."card_type" AS "channels_bo_product__card_type",
--    "channels_bo_product"."currency" AS "channels_bo_product__currency",
 --   "channels_bo_product"."net_commission_partner" AS "channels_bo_product__net_commission_partner",
 --   "channels_bo_product"."gross_commission_partner" AS "channels_bo_product__gross_commission_partner",
 --   "channels_bo_product"."commission_admin_charges_partner" AS "channels_bo_product__commission_admin_charges_partner",
    "ch_bo_partner_partner"."name" AS "ch_bo_partner_partner__name",
    "ch_bo_partner_partner"."type" AS "ch_bo_partner_partner__type",
    "ch_bo_partner_partner"."reference_id" AS "ch_bo_partner_partner__reference_id",
    "ch_bo_partner_partner"."country" AS "ch_bo_partner_partner__country",
    SUM("channels_bo_product"."total") AS "channels_bo_product__total",
 --   "channels_bo_product"."net_cost" AS "channels_bo_product__net_cost",
 --   "channels_bo_product"."cost_tax" AS "channels_bo_product__cost_tax",
    ROUND(AVG("channels_bo_product"."conversion_rate"),4) AS "channels_bo_product__conversion_rate",
    ROUND(SUM("channels_bo_product"."total" * "channels_bo_product"."conversion_rate"),2) AS "gb_usd"
FROM "lake"."channels_bo_sale"
LEFT JOIN "lake"."channels_bo_product" "channels_bo_product"
       ON "lake"."channels_bo_sale"."id" = "channels_bo_product"."sale_id"
LEFT JOIN "lake"."ch_bo_partner_partner" "ch_bo_partner_partner"
       ON "lake"."channels_bo_sale"."partner_code" = "ch_bo_partner_partner"."reference_id"
WHERE
    "ch_bo_partner_partner"."country" = 'MX'
    AND (
        "lake"."channels_bo_sale"."channel" = 'travel-agency-bo'
        OR "lake"."channels_bo_sale"."channel" = 'travel-agency-whitelabel'
    )
    and ("lake"."channels_bo_sale"."status" = 'EMITTED' or "lake"."channels_bo_sale"."status" = 'ACTIVE')
    and date_format("lake"."channels_bo_sale"."created", '%Y') >= '2023'
 --   and ch_bo_partner_partner.name like '%Viajes Bumeran%'
group by 1,2,3,4,5,6,7,8