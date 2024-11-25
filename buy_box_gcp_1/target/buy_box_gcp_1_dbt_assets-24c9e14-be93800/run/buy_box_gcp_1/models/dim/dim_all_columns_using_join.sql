

  create or replace view `dbt-437212`.`wellbefore`.`dim_all_columns_using_join`
  OPTIONS()
  as SELECT *
FROM dbt-437212.wellbefore.dim_offers_values AS offers
JOIN dbt-437212.wellbefore.dim_summary_values AS summary
ON 
    offers.ASIN = summary.ASIN 
    AND offers.EventTime = summary.EventTime;

