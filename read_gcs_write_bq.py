"""
Gets parquet files from the public GCS bucket https://storage.googleapis.com/bucket_name
Applies the requested changes. Obtains a final dataframe and inserts it into BigQuery table.
"""
import os
import gcsfs
import base64
import pandas as pd
from pyarrow import parquet as pq
from google.cloud import storage, bigquery
from concurrent.futures import ThreadPoolExecutor

# Obtain the environmental variables defined in Cloud function
project_id = os.getenv('project_id')
bucket_name = os.getenv('bucket_name')
dataset_name = os.getenv('dataset_name')

# Initialize the Google Cloud Storage and BigQuery clients
storage_client = storage.Client(project=project_id)
bq_client = bigquery.Client(project=project_id)

# Define bucket variable
bucket = storage_client.bucket(bucket_name)

# Define BigQuery table names
ad_network_table = "ad_network"
currency_rate_table = "currency_rate"

ad_network_table_id = f"{project_id}.{dataset_name}.{ad_network_table}"

def create_ad_network_table():
    """
    Creates the ad_network table if not exists
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_name}.{ad_network_table}` (
        dt DATETIME,
        network STRING,
        currency STRING,
        platform STRING,
        cost FLOAT,
        cost_usd FLOAT,
        dt STRING,
        file_name_with_date STRING)"""

    job_config = bigquery.QueryJobConfig()

    table_ref = bq_client.dataset(f"{dataset_name}").table(f"{ad_network_table}")
    job_config.destination = table_ref

    query_job = bq_client.query(sql, job_config=job_config)

    query_job.result()


def get_parquet_files_from_gcs():
    """ 
    Creates a list of parquet files' paths such as path_to_file/date/my_file.parquet
    """
    blobs = bucket.list_blobs()
    parquet_files = [str(blob.name) for blob in blobs if blob.name.endswith('.parquet')]
    return parquet_files


def get_processed_file_list():
    """
    ad_network table's file_name_with_date column includes the processed files' paths. 
    Returns a list which stores the unique file paths
    """
    query = f"SELECT DISTINCT(file_name_with_date) FROM `{project_id}.{dataset_name}.{ad_network_table}`"
    df = bq_client.query(query).to_dataframe()
    processed_file_list = df['file_name_with_date'].tolist()
    return processed_file_list


def delete_records_from_non_existing_files(must_delete_file_names):
    """
    Checks if there are any files that were processed before but doesn't exist in the GCS bucket anymore.
    If so, deletes the records belonging to that file.
    """
    queries = []
    for file_name_with_date in must_delete_file_names:
        queries.append(f"DELETE FROM `{project_id}.{dataset_name}.{ad_network_table}` WHERE file_name_with_date = '{file_name_with_date}'")
    
    run_multiple_queries(queries)
    if len(queries) != 0:
        print(f"Deleted {len(queries)} records from processed and non defunct files")
    else:
        print(f"{len(queries)} rows deleted from {ad_network_table_id}")


def run_multiple_queries(queries):
    """
    Runs multiple queries asynchronously.
    """
    # This is a good practice for multiple queries 
    # since it decreases the runtime and increases the efficiency
    with ThreadPoolExecutor() as executor:
        executor.map(run_single_query, queries)


def run_single_query(query):
    """
    Executes a query which triggers the BigQuery table.
    """
    bq_client.query(query).result()


def read_parquet_from_gcs(bucket_name, file_name, project_id):
    """
    Reads the parquet file from the GCS bucket and creates a pandas dataframe
    """
    gcs_uri = f"gs://{bucket_name}/{file_name}"
    fs = gcsfs.GCSFileSystem(project=project_id)
    dataset = pq.ParquetDataset(gcs_uri, filesystem=fs)
    table = dataset.read()
    df = table.to_pandas()
    return df


def fetch_currency_rates():
    """
    Reads the currency_rate table in BigQuery and returns a pandas dataframe out of it.
    """
    query = f"""
    SELECT currency_code, rates
    FROM `{project_id}.{dataset_name}.{currency_rate_table}`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY date DESC) = 1
    """
    currency_rate_df = bq_client.query(query).to_dataframe()
   
    return currency_rate_df


def create_main_df(file_name_with_date):
    """
    Creates the main dataframe from parquet files, adds cost_usd and file_name_with_date columns.
    """
    df = read_parquet_from_gcs(bucket_name, file_name_with_date, project_id)
    
    # Brings only the max cost row when there is a dt, network, currency, platform breakdown
    df = df.groupby(["dt", "network", "currency", "platform"]).agg({'cost': 'max'}).reset_index()

    currency_rate_df = fetch_currency_rates()
    df = pd.merge(df, currency_rate_df, left_on='currency', right_on='currency_code', how='left')
    
    # cost_usd and file_name_with_date is added to be compatible with BQ table
    df['cost_usd'] = df['rates'] * df['cost']
    df.drop(['currency_code', 'rates'], axis=1, inplace=True)

    df["file_name_with_date"] = file_name_with_date
    main_df = df.round({'cost': 2, 'cost_usd': 2})

    return main_df


def create_df_from_newly_added_files(newly_added_files):
    """
    Checks the newly added files list. Returns a single dataframe obtained from the list 
    """
    if len(newly_added_files) == 0:
        df = pd.DataFrame()
        return df
    elif len(newly_added_files) == 1:
        df = create_main_df(newly_added_files[0])
        return df
    else:
        # If there are multiple files in the list, it will be better to create a single dataframe
        # for performance issues. Taking the maximum cost if there is a dt, network, currency, platform
        # breakdown.
        dfs = []
        for file_name in newly_added_files:
            df = create_main_df(file_name)
            dfs.append(df)

        df = pd.concat(dfs, ignore_index=True)
        df['row_number'] = df.sort_values('cost', ascending=False).groupby(['dt','network','currency','platform']).cumcount() + 1

        df = df[df['row_number'] == 1]
        df.drop('row_number', axis=1, inplace=True)
        df = df.reset_index(drop=True)

        return df


def process_file(newly_added_files):
    """
    Inserts or updates the dataframe's rows to the BigQuery table depending on the conditions.
    """
    df = create_df_from_newly_added_files(newly_added_files)
    # values to be inserted, update_queries to be updated    
    values = []
    update_queries = []

    for _, row in df.iterrows():
        # Checks the breakdown of dt, network, currency, platform
        select_query = f"""SELECT cost, cost_usd, file_name_with_date FROM `{project_id}.{dataset_name}.{ad_network_table}` 
        WHERE dt = '{row['dt']}' AND network = '{row['network']}' AND currency = '{row['currency']}' 
        AND platform = '{row['platform']}'"""
        
        query_job = bq_client.query(select_query)
        bq_row = query_job.to_dataframe()
        
        # If there is not the same breakdown, row is inserted to the table
        if len(bq_row) == 0:
            values.append(f"('{row['dt']}', '{row['network']}', '{row['currency']}', '{row['platform']}', {row['cost']}, {row['cost_usd']}, '{row['file_name_with_date']}')")
        else:
            # Checks if the cost values are equal. If so, it means that
            # this file's content is equal to a previously processed file.
            # Updates the row values with the new file's values
            if row['cost'] == bq_row['cost']:
                update_queries.append(f"""
                UPDATE `{project_id}.{dataset_name}.{ad_network_table}`
                SET file_name_with_date = '{row['file_name_with_date']}'
                WHERE cost = {row['cost']}
                """)
            # There is the same breakdown. Checks the cost,
            # updates if the new cost is higher    
            elif row['cost'] > bq_row['cost']:
                update_queries.append(f"""
                UPDATE `{project_id}.{dataset_name}.{ad_network_table}`
                SET cost = {row['cost']}, cost_usd = {row['cost_usd']}, file_name_with_date = '{row['file_name_with_date']}'
                WHERE dt = '{row['dt']}' AND network = '{row['network']}' AND currency = '{row['currency']}' AND platform = '{row['platform']}'
                """)
           
    if len(values) != 0:
        # Insert will be procesed as bulk. This will increase performance due to BigQuery's best practices.
        insert_query = f"""
        INSERT INTO `{project_id}.{dataset_name}.{ad_network_table}` (dt, network, currency, platform, cost, cost_usd, file_name_with_date)
        VALUES {','.join(values)}
        """
        run_single_query(insert_query)

    if len(update_queries) != 0:
        run_multiple_queries(update_queries)
    
    print(f"""{len(values)} rows inserted into {ad_network_table_id} \n
          {len(update_queries)} rows updated in {ad_network_table_id}""")


def hello_pubsub(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(f"Target of the function: {pubsub_message}")

    parquet_files = get_parquet_files_from_gcs()
    processed_file_list = get_processed_file_list()
    
    # Only newly added files will be processed to reduce runtime
    newly_added_files = list(set(parquet_files) - set(processed_file_list))
    must_delete_files = list(set(processed_file_list) - set(parquet_files))

    delete_records_from_non_existing_files(must_delete_files)
    process_file(newly_added_files)

    table = bq_client.get_table(ad_network_table_id)

    print("Process completed successfully. There are {} rows and {} columns in {}".format(
    table.num_rows, len(table.schema), ad_network_table_id))