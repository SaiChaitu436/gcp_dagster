
  
    

    create or replace table `dbt-437212`.`wellbefore`.`src_products`
      
    
    

    OPTIONS()
    as (
      WITH source_data AS (

    SELECT 

        PARSE_JSON(data.message_body) AS raw_data,

    FROM 

        `dbt-437212`.`wellbefore`.`BuyBox_raw_ds_first20_entries` as data

),

flatten_data AS (

    SELECT

        JSON_VALUE(raw_data.Payload.AnyOfferChangedNotification.OfferChangeTrigger.ASIN)AS ASIN,

        JSON_VALUE(raw_data.Payload.AnyOfferChangedNotification.OfferChangeTrigger.MarketplaceId)AS MarketplaceId

    FROM source_data

)

SELECT * FROM flatten_data
    );
  