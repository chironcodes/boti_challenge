# import functions_framework
# from dotenv import load_dotenv
# load_dotenv()

import pandas as pd
from google.cloud import bigquery


import logging
from google.cloud.exceptions import NotFound
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

def get_logger(LOG_LEVEL="INFO"):
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client, name="vendas-landing-to-raw")
    cloud_logger = logging.getLogger("vendas-landing-to-raw")
    cloud_logger.setLevel(LOG_LEVEL)
    cloud_logger.addHandler(handler)
    return cloud_logger


class BigQueryError(Exception):
    """Exception raised whenever a BigQuery error happened"""

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error["errors"])
        return json.dumps(err)


class BQApiClient:
    """BQ Client to process bq requests"""
    def __init__(self, location="southamerica-east1"):
        self._client = bigquery.Client()
        self._location = location

    def get_client(self):
        return self._client





def hello_gcs():
# def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    _LOG = get_logger()

    file = event
    file_name = file['name']

    BQ = BQApiClient()
    client = BQ.get_client()

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('ID_MARCA', "INTEGER"),
            bigquery.SchemaField('MARCA', "STRING"),
            bigquery.SchemaField('ID_LINHA', "INTEGER"),
            bigquery.SchemaField('LINHA', "STRING"),
            bigquery.SchemaField('DATA_VENDA', "DATETIME"),
            bigquery.SchemaField('QTD_VENDA', "INTEGER")
        ],
        write_disposition="WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="DATA_VENDA"
        ),
    )

    df = pd.read_excel(f'gs://raw_venda/{file_name}')

    table_ref = "polybox-394517.raw.venda"


    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    job.result()


    tabela = client.get_table(table_ref)
    
    print("Existem agora {} linhas na tabela {}" \
        .format(tabela.num_rows, table_ref))

    # client.query("CALL dataset.create_stagging()").result()
