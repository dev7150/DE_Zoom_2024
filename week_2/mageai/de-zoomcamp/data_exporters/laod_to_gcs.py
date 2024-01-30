import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/dtc-de-410504-635094b69091.json"

bucket_name = 'terraform-dtc-de-410504-terra-bucket'
project_id = 'dtc-de-410504'

table_name="nyc_green_taxi_data"

root_path=f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(data,*args,**kwargs):
   
    
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )