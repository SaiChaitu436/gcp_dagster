WITH source_data AS (
    SELECT
        JSON_EXTRACT(MESSAGE_BODY, "$") AS raw_data
    FROM
        dbt-437212.wellbefore.BuyBox_raw_ds_first20_entries
),

flatten_payload AS (
    SELECT
        JSON_EXTRACT_SCALAR(raw_data, "$.EventTime") AS EventTime,
        JSON_EXTRACT_SCALAR(raw_data, "$.NotificationMetadata.ApplicationId") AS ApplicationId,
        JSON_EXTRACT_SCALAR(raw_data, "$.NotificationMetadata.NotificationId") AS NotificationId,
        TIMESTAMP(JSON_EXTRACT_SCALAR(raw_data, "$.NotificationMetadata.PublishTime")) AS PublishTime,
        JSON_EXTRACT_SCALAR(raw_data, "$.NotificationMetadata.SubscriptionId") AS SubscriptionId,
        JSON_EXTRACT_SCALAR(raw_data, "$.NotificationType") AS NotificationType,
        JSON_EXTRACT_SCALAR(raw_data, "$.NotificationVersion") AS NotificationVersion,
        JSON_EXTRACT_SCALAR(raw_data, "$.Payload.AnyOfferChangedNotification.OfferChangeTrigger.ASIN") AS ASIN,
        JSON_EXTRACT_SCALAR(raw_data, "$.Payload.AnyOfferChangedNotification.OfferChangeTrigger.ItemCondition") AS ItemCondition,
        JSON_EXTRACT_SCALAR(raw_data, "$.Payload.AnyOfferChangedNotification.OfferChangeTrigger.MarketplaceId") AS MarketplaceId,
        JSON_EXTRACT_SCALAR(raw_data, "$.Payload.AnyOfferChangedNotification.OfferChangeTrigger.OfferChangeType") AS OfferChangeType,
        JSON_EXTRACT_SCALAR(raw_data, "$.Payload.AnyOfferChangedNotification.OfferChangeTrigger.TimeOfOfferChange") AS TimeOfOfferChange,
        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Offers") AS offers,
        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.BuyBoxPrices") AS bbp,
        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.LowestPrices") AS lop,
        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.NumberOfBuyBoxEligibleOffers") AS neo,
        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.NumberOfOffers") AS noo,
        JSON_EXTRACT(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.ListPrice") AS lp,  -- Access the entire ListPrice object
        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.SalesRankings") AS sr
    FROM
        source_data
),

flattened_payload AS (
    SELECT
        EventTime,
        ApplicationId,
        NotificationId,
        PublishTime,
        SubscriptionId,
        NotificationType,
        NotificationVersion,
        ASIN,
        ItemCondition,
        MarketplaceId,
        OfferChangeType,
        TimeOfOfferChange,

        CAST(JSON_EXTRACT_SCALAR(offer, "$.IsBuyBoxWinner") AS BOOLEAN) AS IsBuyBoxWinner,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.IsFeaturedMerchant") AS BOOLEAN) AS IsFeaturedMerchant,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.IsFulfilledByAmazon") AS BOOLEAN) AS IsFulfilledByAmazon,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.ListingPrice.Amount") AS FLOAT64) AS ListingPriceAmount,
        JSON_EXTRACT_SCALAR(offer, "$.ListingPrice.CurrencyCode") AS ListingPriceCurrencyCode,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.PrimeInformation.IsOfferNationalPrime") AS BOOLEAN) AS IsOfferNationalPrime,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.PrimeInformation.IsOfferPrime") AS BOOLEAN) AS IsOfferPrime,
        JSON_EXTRACT_SCALAR(offer, "$.SellerId") AS SellerId,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.SellerFeedbackRating.FeedbackCount") AS INT64) AS SellerFeedbackCount,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.SellerFeedbackRating.SellerPositiveFeedbackRating") AS INT64) AS SellerPositiveFeedbackRating,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.Shipping.Amount") AS FLOAT64) AS ShippingAmount,
        JSON_EXTRACT_SCALAR(offer, "$.Shipping.CurrencyCode") AS ShippingCurrencyCode,
        JSON_EXTRACT_SCALAR(offer, "$.ShippingTime.AvailabilityType") AS ShippingAvailabilityType,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.ShippingTime.MaximumHours") AS INT64) AS ShippingMaxHours,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.ShippingTime.MinimumHours") AS INT64) AS ShippingMinHours,
        CAST(JSON_EXTRACT_SCALAR(offer, "$.ShipsDomestically") AS BOOLEAN) AS ShipsDomestically,
        JSON_EXTRACT_SCALAR(offer, "$.ShipsFrom.Country") AS ShipsFromCountry,
        JSON_EXTRACT_SCALAR(offer, "$.ShipsFrom.State") AS ShipsFromState,
        JSON_EXTRACT_SCALAR(offer, "$.SubCondition") AS SubCondition,

        JSON_EXTRACT_SCALAR(bbp, "$.Condition") AS BuyBoxPricesCondition,
        CAST(JSON_EXTRACT_SCALAR(bbp, "$.LandedPrice.Amount") AS FLOAT64) AS BuyBoxLandedPriceAmount,
        JSON_EXTRACT_SCALAR(bbp, "$.LandedPrice.CurrencyCode") AS BuyBoxLandedPriceCurrencyCode,
        CAST(JSON_EXTRACT_SCALAR(bbp, "$.ListingPrice.Amount") AS FLOAT64) AS BuyBoxListingPriceAmount,
        JSON_EXTRACT_SCALAR(bbp, "$.ListingPrice.CurrencyCode") AS BuyBoxListingPriceCurrencyCode,
        CAST(JSON_EXTRACT_SCALAR(bbp, "$.Shipping.Amount") AS FLOAT64) AS BuyBoxShippingAmount,
        JSON_EXTRACT_SCALAR(bbp, "$.Shipping.CurrencyCode") AS BuyBoxShippingCurrencyCode,

        JSON_EXTRACT_SCALAR(lop, "$.Condition") AS LowestPricesCondition,
        JSON_EXTRACT_SCALAR(lop, "$.FulfillmentChannel") AS LowestPricesFulfillmentChannel,
        CAST(JSON_EXTRACT_SCALAR(lop, "$.LandedPrice.Amount") AS FLOAT64) AS LandedPriceAmount,
        JSON_EXTRACT_SCALAR(lop, "$.LandedPrice.CurrencyCode") AS LandedPriceCurrencyCode,
        CAST(JSON_EXTRACT_SCALAR(lop, "$.ListingPrice.Amount") AS FLOAT64) AS LowestPricesListingPriceAmount,
        JSON_EXTRACT_SCALAR(lop, "$.ListingPrice.CurrencyCode") AS LowestPricesListingCurrencyCode,
        CAST(JSON_EXTRACT_SCALAR(lop, "$.Shipping.Amount") AS FLOAT64) AS LowestPricesShippingPriceAmount,
        JSON_EXTRACT_SCALAR(lop, "$.Shipping.CurrencyCode") AS LowestPricesShippingCurrencyCode,

        JSON_EXTRACT_SCALAR(neo, "$.Condition") AS BuyBoxEligibleOffersCondition,
        JSON_EXTRACT_SCALAR(neo, "$.FulfillmentChannel") AS BuyBoxEligibleOffersFulfillmentChannel,
        JSON_EXTRACT_SCALAR(neo, "$.OfferCount") AS BuyBoxEligibleOffersCount,

        JSON_EXTRACT_SCALAR(noo, "$.Condition") AS NumberOfOffersCondition,
        JSON_EXTRACT_SCALAR(noo, "$.FulfillmentChannel") AS NumberOfOffersFulfillmentChannel,
        JSON_EXTRACT_SCALAR(noo, "$.OfferCount") AS NumberOfOffersCount,

        JSON_EXTRACT_SCALAR(sr, "$.ProductCategoryId") AS SalesRankingsProductCategoryId,
        CAST(JSON_EXTRACT_SCALAR(sr, "$.Rank") AS INT64) AS SalesRankingsRank,

        CAST(JSON_EXTRACT_SCALAR(lp, "$.Amount") AS FLOAT64) AS ListPriceAmount,
        JSON_EXTRACT_SCALAR(lp, "$.CurrencyCode") AS ListPriceCurrencyCode

    FROM
        flatten_payload,
        UNNEST(offers) AS offer,
        UNNEST(bbp) AS bbp,
        UNNEST(lop) AS lop,
        UNNEST(neo) AS neo,
        UNNEST(noo) AS noo,
        UNNEST(sr) AS sr
)

SELECT *
FROM flattened_payload