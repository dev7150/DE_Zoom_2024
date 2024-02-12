import io
import pandas as pd
import requests
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    table_id = 'dtc-de-410504.week_4.green_cab_data'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='append',  # Specify resolution policy if table name already exists
    )

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    taxi_dtypes = {
        'VendorID': 'Int64',
        'store_and_fwd_flag': 'str',
        'RatecodeID': 'Int64',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'ehail_fee': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'payment_type': 'float64',
        'trip_type': 'float64',
        'congestion_surcharge': 'float64'
    }

    parse_dates_yellow_taxi = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    var = kwargs['url']
    final_data = pd.DataFrame()
    url = 1
    while url < 13:
        url_green_taxi_2022 = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-{url:02d}.csv.gz'
        print(url_green_taxi_2022)
        url += 1
        df = pd.read_csv(url_green_taxi_2022, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates_yellow_taxi)
        export_data_to_big_query(df)
            # final_data = pd.concat([final_data, df], ignore_index=True)
    return df    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
