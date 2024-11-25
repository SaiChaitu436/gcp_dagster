SELECT *
FROM dbt-437212.wellbefore.dim_offers_values AS offers
JOIN dbt-437212.wellbefore.dim_summary_values AS summary
ON 
    offers.ASIN = summary.ASIN 
    AND offers.EventTime = summary.EventTime