import json
import os
import logging
from google.cloud import bigquery, storage
from google.cloud.bigquery import TimePartitioning

def load_to_bq(task, filter_='1261*'):
    client = bigquery.Client('footprint-etl-internal')
    job_config = bigquery.LoadJobConfig()
    schema_path = os.path.join('/Users/pen/cryptoProject/solana-etl/src/resource/schema/{task}.json'.format(task=task))
    schema = read_bigquery_schema_from_file(schema_path)
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.ignore_unknown_values = True
    job_config.time_partitioning = TimePartitioning(field='time' if task == 'transactions' else 'timestamp')
    export_location_uri = f'gs://crypto_etl/solana_export/{task}/{filter_}'
    table_ref = client.dataset('crypto_solana_temp', project='footprint-blockchain-etl').table(task + '_' + filter_.replace('*', '_star').replace('/', '_').replace('.', '_point'))
    load_job = client.load_table_from_uri(export_location_uri, table_ref, job_config=job_config)
    submit_bigquery_job(load_job, job_config)

def read_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content

def submit_bigquery_job(job, configuration):
    try:
        logging.info('Creating a job: ' + json.dumps(configuration.to_api_repr()))
        result = job.result()
        logging.info(result)
        assert job.errors is None or len(job.errors) == 0
        return result
    except Exception:
        logging.info(job.errors)
        raise

def read_bigquery_schema_from_file(filepath):
    file_content = read_file(filepath)
    json_content = json.loads(file_content)
    return read_bigquery_schema_from_json_recursive(json_content)


def read_bigquery_schema_from_json_recursive(json_schema):
    """
    CAUTION: Recursive function
    This method can generate BQ schemas for nested records
    """
    result = []
    for field in json_schema:
        if field.get('type').lower() == 'record' and field.get('fields'):
            schema = bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description'),
                fields=read_bigquery_schema_from_json_recursive(field.get('fields'))
            )
        else:
            schema = bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description')
            )
        result.append(schema)
    return result


if __name__ == '__main__':
    for task in [
        'blocks',
        'transactions',
        'token_transfers'
    ]:
        try:
            load_to_bq(task, filter_=f'129990000*')
        except Exception as e:
            print(e)  # 126, 127 有问题