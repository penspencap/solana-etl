import json
import os
import logging
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning

# k = []
# for _block in [*range(12973, 12999)]:
#     block = _block * 10000
#     k.append(
#         f"""export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/load/FileOutput.py --tasks all --temp_dir /solana_data/temp --blocks_dir /solana_data/data/{block}/ --destination_dir /solana_data/bq_data/{block} --destination_format jsonl;/opt/service/python3.8.10/bin/python3 src/load/Upload.py {block} 10;""")
# print(''.join(k))
# k = []
# for block in [
# 127860000, 127920000, 127980000, 128040000, 128100000, 128160000, 128220000, 128280000, 128340000, 129230000, 129290000,
# 127870000, 127930000, 127990000, 128050000, 128110000, 128170000, 128230000, 128290000, 128350000, 129240000,
# 127880000, 127940000, 128000000, 128060000, 128120000, 128180000, 128240000, 128300000, 129070000, 129250000,
# 127890000, 127950000, 128010000, 128070000, 128130000, 128190000, 128250000, 128310000, 129200000, 129260000,
# 127900000, 127960000, 128020000, 128080000, 128140000, 128200000, 128260000, 128320000, 129210000, 129270000,
# 127910000, 127970000, 128030000, 128090000, 128150000, 128210000, 128270000, 128330000, 129220000, 129280000,
# ]:
#     k.append(f'/opt/service/python3.8.10/bin/python3 src/load/Upload.py {block} 10;')
# print(''.join(k))
# block = 129070000
# print(f"""export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/load/FileOutput.py --tasks all --temp_dir /solana_data/temp --blocks_dir /solana_data/data/{block}/ --destination_dir /solana_data/bq_data/{block} --destination_format jsonl;/opt/service/python3.8.10/bin/python3 src/load/Upload.py {block} 10;""")
# # start = 12886
# #
# # query = f'scp -r -P 2208 root@10.202.0.35:/solana_data/data/{start}0000/ /soalana_data/data/'
# # Kalengo2go
# scp -r -P 2208 root@10.202.0.35:/solana_data/data/1292*/ /solana_data/data/





def load_to_bq(task):
    client = bigquery.Client('footprint-etl-internal')
    job_config = bigquery.LoadJobConfig()
    schema_path = os.path.join('/Users/pen/cryptoProject/solana-etl/src/resource/schema/{task}.json'.format(task=task))
    schema = read_bigquery_schema_from_file(schema_path)
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.ignore_unknown_values = True
    job_config.time_partitioning = TimePartitioning(field='time' if task == 'transactions' else 'timestamp')
    export_location_uri = 'gs://crypto_etl/solana_export/{task}/1286*'.format(task=task)
    table_ref = client.dataset('crypto_solana_raw', project='footprint-blockchain-etl').table(task)
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


# if __name__ == '__main__':
#     load_to_bq('blocks')
