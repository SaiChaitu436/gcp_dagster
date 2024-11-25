from dagster import resource, op, job, get_dagster_logger, In, Out, ConfigurableResource
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq
import numpy as np


@resource
def bigquery_client_resource(context):
    credentials_path = "C:/Users/SAILS-DM204/OneDrive - Sails Software Solutions Pvt Ltd/Desktop/Junk Folder/DBT/dbt-437212-4031e98ab84c.json"
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

# Op to provide the seeding file name
@op(out={"seeding_file_name": Out(str)})
def provide_seeding_file_name():
    return "BuyBox_raw_ds_first20_entries"

# Fetch and transform data
@op(required_resource_keys={"bigquery_client"}, ins={"seeding_file_name": In(str)}, out={"aggregated_df": Out(pd.DataFrame)})
def fetch_and_transform_data(context, seeding_file_name: str):
    logger = get_dagster_logger()
    client = context.resources.bigquery_client

    # Query to fetch data
    logger.info("Fetching data from BigQuery...")
    query = """
        SELECT *
        FROM `dbt-437212.wellbefore.dim_all_columns`
    """
    df = client.query(query).to_dataframe()

    # Data transformation
    logger.info("Transforming data...")
    df['EventTime'] = pd.to_datetime(df['EventTime'], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce')
    df['prev_price'] = df.groupby(['SellerId', 'ASIN'])['ListingPriceAmount'].shift(1)
    df['price_ratio'] = df['ListingPriceAmount'] / df['prev_price']
    df['price_ratio'] = df['price_ratio'].fillna(1)
    df['shipping_range'] = df['ShippingMaxHours'] - df['ShippingMinHours']
    df['ISBUYBOXWINNER_win_count'] = df.groupby(['SellerId', 'ASIN'])['IsBuyBoxWinner'].cumsum()
    df['price_std_dev'] = df.groupby(['SellerId', 'ASIN'])['ListingPriceAmount'].transform('std')
    df['customer_experience'] = (
        0.5 * (df['SellerPositiveFeedbackRating'] / 100) + 0.5 * df['IsBuyBoxWinner']
    )

    # Aggregation
    logger.info("Aggregating data...")
    aggregation_dict = {
        'IsBuyBoxWinner': 'max',
        'IsFulfilledByAmazon': 'first',
        'IsOfferPrime': 'first',
        'ListingPriceAmount': 'mean',
        'SellerFeedbackCount': 'sum',
        'SellerPositiveFeedbackRating': 'mean',
        'ShippingAmount': 'mean',
        'LandedPriceAmount': 'mean',
        'price_ratio': 'mean',
        'shipping_range': 'mean',
        'ISBUYBOXWINNER_win_count': 'sum',
        'price_std_dev': 'mean',
        'customer_experience': 'mean'
    }
    aggregated_df = df.groupby(['ASIN', 'SellerId']).agg(aggregation_dict).reset_index() #Removed SellerName in the groupby clause, add it later

    # Normalize and calculate indices
    logger.info("Calculating indices...")
    def normalize_series(series):
        min_val, max_val = series.min(), series.max()
        return np.zeros(len(series)) if min_val == max_val else (series - min_val) / (max_val - min_val)

    aggregated_df['norm_price_ratio'] = normalize_series(aggregated_df['price_ratio'])
    aggregated_df['norm_feedback'] = normalize_series(aggregated_df['SellerPositiveFeedbackRating'])
    aggregated_df['norm_fulfillment'] = aggregated_df['IsFulfilledByAmazon'].astype(float)
    aggregated_df['norm_customer_experience'] = normalize_series(aggregated_df['customer_experience'])

    # Seller Success Index
    weights = {"price_ratio": 0.25, "feedback": 0.15, "fulfillment": 0.25, "customer_experience": 0.20}
    aggregated_df['seller_success_index'] = (
        weights['price_ratio'] * aggregated_df['norm_price_ratio'] +
        weights['feedback'] * aggregated_df['norm_feedback'] +
        weights['fulfillment'] * aggregated_df['norm_fulfillment'] +
        weights['customer_experience'] * aggregated_df['norm_customer_experience']
    )

    return aggregated_df

# Upload transformed data
@op(required_resource_keys={"bigquery_client"}, ins={"aggregated_df": In(pd.DataFrame), "seeding_file_name": In(str)})
def upload_to_bigquery(context, aggregated_df, seeding_file_name: str):
    logger = get_dagster_logger()
    client = context.resources.bigquery_client

    # Upload to BigQuery
    destination_table = f"dbt-437212.wellbefore.aggregated_table_{seeding_file_name}"
    logger.info(f"Uploading data to {destination_table}...")
    pandas_gbq.to_gbq(
        aggregated_df,
        destination_table=destination_table,
        project_id=client.project,
        if_exists='replace',
        credentials=client._credentials,
    )
    logger.info("Data successfully uploaded.")

# Define the job
@job(resource_defs={"bigquery_client": bigquery_client_resource})
def aggregation_job():
    seeding_file_name = provide_seeding_file_name()
    aggregated_df = fetch_and_transform_data(seeding_file_name)
    upload_to_bigquery(aggregated_df, seeding_file_name)


