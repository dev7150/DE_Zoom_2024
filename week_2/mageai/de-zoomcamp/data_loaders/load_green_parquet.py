import io
import pandas as pd
import requests
import dlt

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    
    url = 1
    final_data = pd.DataFrame()
    while url < 13:
        url_green_taxi_2022 = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{url:02d}.parquet'
        url += 1
        df = pd.read_parquet(url_green_taxi_2022)
        final_data = pd.concat([final_data, df], ignore_index=True)
        # print(url_green_taxi_2022)
    return final_data
    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
