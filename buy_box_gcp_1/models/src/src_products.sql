WITH source_data AS (

    SELECT 

        PARSE_JSON(data.message_body) AS raw_data,

    FROM 

        {{source("dbt-437212","BB")}} as data

),

flatten_data AS (

    SELECT

        JSON_VALUE(raw_data.Payload.AnyOfferChangedNotification.OfferChangeTrigger.ASIN)AS ASIN,

        JSON_VALUE(raw_data.Payload.AnyOfferChangedNotification.OfferChangeTrigger.MarketplaceId)AS MarketplaceId

    FROM source_data

)

SELECT * FROM flatten_data