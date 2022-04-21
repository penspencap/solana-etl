import json
import os
import logging
from google.cloud import bigquery, storage
from google.cloud.bigquery import TimePartitioning

# k = []
# for _block in [
#     12904,
#     ]:
#     block = _block * 10000
#     k.append(
#         f"""export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/load/FileOutput.py --tasks all --temp_dir /solana_data/temp --blocks_dir /solana_data/data/{block}/ --destination_dir /solana_data/bq_data/{block} --destination_format jsonl;/opt/service/python3.8.10/bin/python3 src/load/Upload.py {block} 10;""")
# print(''.join(k))
# k = []
# d = list({
#     12988,
#     12904,
#     12683,
#     12741,
#     12778,
#     12725,
#     12732,
#     12981,
#     12675,
#     12924,
#     12927,
#     12888,
#     12827,
#     12759,
#     12692,
#     12657,
#     12862,
#     12662,
#     12746,
#     12787,
#     12923,
#     12742,
#     12855,
#     12944,
#     12956,
#     12663,
#     12750,
#     12821,
#     12650,
#     12921,
#     12894,
#     12831,
#     12718,
#     12959,
#     12646,
#     12729,
#     12823,
#     12709,
#     12723,
#     12768,
#     12820,
#     12753,
#     12785,
#     12838,
#     12737,
#     12887,
#     12733,
#     12680,
#     12865,
#     12955,
#     12864,
#     12969,
#     12707,
#     12934,
#     12687,
#     12895,
#     12820,
#     12749,
#     12826,
#     12751,
#     12985,
#     12848,
#     12760,
#     12780,
#     12768,
#     12786,
#     12769,
#     12976,
#     12977,
#     12979,
#     12980,
#     12734,
#     12777,
#     12946,
#     12844,
#     12984,
#     12990,
#     12966,
# })
# d.sort(reverse=True)
# print(len(d))
# for block in d[38:]:
#     block = block*10000
#     k.append(f'export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start {block} --end {block+9999} --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 16 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl;')
# print(''.join(k))
# block = 129070000
# print(f"""export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/load/FileOutput.py --tasks all --temp_dir /solana_data/temp --blocks_dir /solana_data/data/{block}/ --destination_dir /solana_data/bq_data/{block} --destination_format jsonl;/opt/service/python3.8.10/bin/python3 src/load/Upload.py {block} 10;""")
# # start = 12886
# #
# # query = f'scp -r -P 2208 root@10.202.0.35:/solana_data/data/{start}0000/ /soalana_data/data/'
# # Kalengo2go
# scp -r -P 2208 root@10.202.0.35:/solana_data/data/1292*/ /solana_data/data/
#
# k = []
# for block in [
#     125360000,
#     125370000,
#     126860000,
#     126870000,
#     126880000,
#     126890000,
#     126900000,
#     126910000,
#     126920000,
#     126930000,
#     126940000,
#     126950000,
#     126960000,
#     126970000,
#     126980000,
#     127000000,
#     127150000,
#     127220000,
#     123880000,
# ]:
#     k.append(f'/opt/service/python3.8.10/bin/python3 src/load/Upload.py {block} 10;')
# print(''.join(k))


# export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 130000000 --end 130249999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl

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


# if __name__ == '__main__':
#     # for i in range(10):
#         try:
#             load_to_bq('transactions', filter_=f'12*')
#         except Exception as e:
#             print(e)  # 126, 127 有问题
        # else:
        #     print(i, 'success')

# export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 125360000 --end 125369999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 125370000 --end 125379999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126860000 --end 126869999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126870000 --end 126879999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126880000 --end 126889999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126890000 --end 126899999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126900000 --end 126909999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126910000 --end 126919999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126920000 --end 126929999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126930000 --end 126939999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126940000 --end 126949999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126950000 --end 126959999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126960000 --end 126969999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126970000 --end 126979999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126980000 --end 126989999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 127000000 --end 127009999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 127150000 --end 127159999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 127220000 --end 127229999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 123880000 --end 124359999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl
# import os
# print(os.path.getsize('/Users/pen/cryptoProject/solana-etl/bq_data/errors/125910000/0.part ) == 1)
def remove_empty_blob(task, blocks, bucket='crypto_etl', n_jobs=4):
    mapper = {
        'blocks': 'blocks',
        'transactions': 'transactions',
        'transfers': 'token_transfers',
    }
    print('blocks==', blocks)
    # _objects = f'solana_export/{mapper[task]}/{blocks}/'
    _objects = f'solana_export/{mapper[task]}/{blocks}/'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    blobs = bucket.list_blobs(prefix=_objects)
    for blob in blobs:
        if blob.size == 1:
            print('delete blob', blob.name)
            blob.delete()

# for k in [116430000,
#     ]:
#         remove_empty_blob('transfers', str(k))
