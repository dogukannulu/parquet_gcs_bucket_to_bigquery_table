"""
This script gets the currency rates based on USD from currency API, 
creates a pandas dataframe and writes it to BigQuery table
"""
import os
import base64
import requests
import pandas as pd
from google.cloud import bigquery

# Obtain the environmental variable defined in Cloud function
currency_api_token = os.getenv('currency_api_token')
project_id = os.getenv('project_id')
bucket_name = os.getenv('bucket_name')
dataset_name = os.getenv('dataset_name')

# Global variables
client = bigquery.Client()

table_name = 'currency_rate'
table_id = f"{project_id}.{dataset_name}.{table_name}"


def create_currency_rate_table():
    """
    Creates the currency_rate table if not exists
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_name}.{table_name}` (
        currency_code STRING,
        date DATETIME,
        rates FLOAT)"""

    job_config = bigquery.QueryJobConfig()

    table_ref = client.dataset(f"{dataset_name}").table(f"{table_name}")
    job_config.destination = table_ref

    query_job = client.query(sql, job_config=job_config)

    query_job.result()


def create_dict(response) -> dict:
    """
    Obtains the response from currency API and creates the dictionary accordingly
    """
    response_dict = eval((response.text).replace('true', 'True', 1)) # Compatibility with Python
    del response_dict['valid'], response_dict['base'] # Unnecessary fields

    return response_dict


def create_dataframe_from_dict(response_dict:dict):
    """
    Creates a pandas dataframe from the response dictionary
    """
    df = pd.DataFrame.from_dict(response_dict)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "currency_code", "updated": "date"}, inplace=True)
    df['date'] = df['date'].astype('datetime64[s]') # Necessary for compatibility with GCP

    return df


def load_dataframe_to_table(df, client, table_id):
    """
    Defines a job configuration and loads the pandas dataframe to BigQuery table currency_rates
    """
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField(f"{df.columns[0]}", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(f"{df.columns[1]}", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField(f"{df.columns[2]}", bigquery.enums.SqlTypeNames.FLOAT),
    ],
    )

    try:
        job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config)
        job.result()

        print("Dataframe was loaded successfully")
    except Exception as e:
        print(Exception, e)


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(f"Target of the function: {pubsub_message}")
    
    try:
        url = f'https://currencyapi.net/api/v1/rates?key={currency_api_token}'
        response = requests.request("GET", url)
    except Exception as e:
        print(Exception, e)

    response_dict = create_dict(response)
    df = create_dataframe_from_dict(response_dict)

    load_dataframe_to_table(df, client, table_id)

    try:
        table = client.get_table(table_id)
    except Exception as e:
        print(Exception, e)
    
    print("{} rows inserted into {} \n There are {} rows and {} columns in {}"
    .format(len(df), table_id, table.num_rows, len(table.schema), table_id))
