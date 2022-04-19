import json
import os
import logging
from google.cloud import bigquery, storage
from google.cloud.bigquery import TimePartitioning

# k = []
# for _block in [*range(12973, 12999)]:
#     block = _block * 10000
#     k.append(
#         f"""export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/load/FileOutput.py --tasks all --temp_dir /solana_data/temp --blocks_dir /solana_data/data/{block}/ --destination_dir /solana_data/bq_data/{block} --destination_format jsonl;/opt/service/python3.8.10/bin/python3 src/load/Upload.py {block} 10;""")
# print(''.join(k))
# k = []
# for block in [
# 125360000,
# 125370000,
# 126860000,
# 126870000,
# 126880000,
# 126890000,
# 126900000,
# 126910000,
# 126920000,
# 126930000,
# 126940000,
# 126950000,
# 126960000,
# 126970000,
# 126980000,
# 127000000,
# 127150000,
# 127220000,
# ]:
#     k.append(f'export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start {block} --end {block+9999} --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;')
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
# 125370000,
# 126890000,
# 126910000,
# 126930000,
# 127000000,
# 127150000,
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


if __name__ == '__main__':
    for i in range(10):
        try:
            load_to_bq('token_transfers', filter_=f'126{i}*')
        except Exception as e:
            print(e) #126, 127 有问题
        else:
            print(i, 'success')

# export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 125360000 --end 125369999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 125370000 --end 125379999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126860000 --end 126869999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126870000 --end 126879999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126880000 --end 126889999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126890000 --end 126899999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126900000 --end 126909999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126910000 --end 126919999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126920000 --end 126929999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126930000 --end 126939999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126940000 --end 126949999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126950000 --end 126959999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126960000 --end 126969999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126970000 --end 126979999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 126980000 --end 126989999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 127000000 --end 127009999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 127150000 --end 127159999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 127220000 --end 127229999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl --skip_download True;export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 123880000 --end 124359999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl
# import os
# print(os.path.getsize('/Users/pen/cryptoProject/solana-etl/bq_data/errors/125910000/0.part') == 1)
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
    # 116440000,
    # 123860000,
    # 123870000,
    # 123880000,
    # 125880000,
    # 125890000,
    # 125900000,
    # 125910000,
    # 125920000,
    # 125930000,
    # 125940000,
    # 125950000,
    # 125960000,
    # 125970000,
    # 125980000,
    # 125990000,
    # 126000000,
    # 126010000,
    # 126020000,
    # 126030000,
    # 126040000,
    # 126050000,
    # 126060000,
    # 126070000,
    # 126080000,
    # 126090000,
    # 126360000,
    # 126370000,
    # 126380000,
    # 126390000,
    # 126400000,
    # 126410000,
    # 126420000,
    # 126430000,
    # 126440000,
    # 126450000,
    # 126460000,
    # 126470000,
    # 126480000,
    # 126490000,
    # 126500000,
    # 126510000,
    # 126520000,
    # 126530000,
    # 126540000,
    # 126550000,
    # 126560000,
    # 126570000,
    # 126580000,
    # 126590000,
    # 126600000,
    # 126610000,
    # 126620000,
    # 126630000,
    # 126640000,
    # 126650000,
    # 126660000,
    # 126670000,
    # 126680000,
    # 126690000,
    # 126700000,
    # 126710000,
    # 126720000,
    # 126730000,
    # 126740000,
    # 126750000,
    # 126760000,
    # 126770000,
    # 126780000,
    # 126790000,
    # 126800000,
    # 126810000,
    # 126820000,
    # 126830000,
    # 126840000,
    # 126850000,
    # 126860000,
    # 126870000,
    # 126880000,
    # 126900000,
    # 126920000,
    # 126940000,
    # 126950000,
    # 126960000,
    # 126970000,
    # 126980000,
    # 126990000,
    # 127010000,
    # 127020000,
    # 127030000,
    # 127040000,
    # 127050000,
    # 127060000,
    # 127070000,
    # 127080000,
    # 127090000,
    # 127100000,
    # 127110000,
    # 127120000,
    # 127130000,
    # 127140000,
    # 127160000,
    # 127170000,
    # 127180000,
    # 127190000,
    # 127200000,
    # 127210000,
    # 127220000,
    # 127230000,
    # 127240000,
    # 127250000,
    # 127260000,
    # 127270000,
    # 127280000,
    # 127290000,
    # 127300000,
    # 127310000,
    # 127320000,
    # 127330000,
    # 127340000,
    # 127350000,
    # 127360000,
    # 127370000,
    # 127380000,
    # 127390000,
    # 127400000,
    # 127410000,
    # 127420000,
    # 127430000,
    # 127440000,
    # 127450000,
    # 127460000,
    # 127470000,
    # 127480000,
    # 127490000,
    # 127500000,
    # 127510000,
    # 127520000,
    # 127530000,
    # 127540000,
    # 127550000,
    # 127560000,
    # 127570000,
    # 127580000,
    # 127590000,
    # 127600000,
    # 127610000,
    # 127620000,
    # 127630000,
    # 127640000,
    # 127650000,
    # 127660000,
    # 127670000,
    # 127680000,
    # 127690000,
    # 127700000,
    # 127710000,
    # 127720000,
    # 127730000,
    # 127740000,
    # 127750000,
    # 127760000,
    # 127770000,
    # 127780000,
    # 127790000,
    # 127800000,
    # 127810000,
    # 127820000,
    # 127830000,
    # 127840000,
    # 127850000,
    # 127860000,
    # 127870000,
    # 127880000,
    # 127890000,
    # 127900000,
    # 127910000,
    # 127920000,
    # 127930000,
    # 127940000,
    # 127950000,
    # 127960000,
    # 127970000,
    # 127980000,
    # 127990000,
    # 128000000,
    # 128010000,
    # 128020000,
    # 128030000,
    # 128040000,
    # 128050000,
    # 128060000,
    # 128070000,
    # 128080000,
    # 128090000,
    # 128100000,
    # 128110000,
    # 128120000,
    # 128130000,
    # 128140000,
    # 128150000,
    # 128160000,
    # 128170000,
    # 128180000,
    # 128190000,
    # 128200000,
    # 128210000,
    # 128220000,
    # 128230000,
    # 128240000,
    # 128250000,
    # 128260000,
    # 128270000,
    # 128280000,
    # 128290000,
    # 128300000,
    # 128310000,
    # 128320000,
    # 128330000,
    # 128340000,
    # 128350000,
    # 128360000,
    # 128370000,
    # 128380000,
    # 128390000,
    # 128400000,
    # 128410000,
    # 128420000,
    # 128430000,
    # 128440000,
    # 128450000,
    # 128460000,
    # 128470000,
    # 128480000,
    # 128490000,
    # 128500000,
    # 128510000,
    # 128520000,
    # 128530000,
    # 128540000,
    # 128550000,
    # 128560000,
    # 128570000,
    # 128580000,
    # 128590000,
    # 128600000,
    # 128610000,
    # 128620000,
    # 128630000,
    # 128640000,
    # 128650000,
    # 128660000,
    # 128670000,
    # 128680000,
    # 128690000,
    # 128700000,
    # 128860000,
    # 128870000,
    # 128880000,
    # 128890000,
    # 128900000,
    # 128910000,
    # 128920000,
    # 128930000,
    # 128940000,
    # 128950000,
    # 128960000,
    # 128970000,
    # 128980000,
    # 128990000,
    # 129000000,
    # 129010000,
    # 129020000,
    # 129030000,
    # 129040000,
    # 129050000,
    # 129060000,
    # 129070000,
    # 129080000,
    # 129090000,
    # 129200000,
    # 129210000,
    # 129220000,
    # 129230000,
    # 129240000,
    # 129250000,
    # 129260000,
    # 129270000,
    # 129280000,
    # 129290000,
    # 129300000,
    # 129310000,
    # 129320000,
    # 129330000,
    # 129340000,
    # 129350000,
    # 129360000,
    # 129370000,
    # 129380000,
    # 129390000,
    # 129400000,
    # 129410000,
    # 129420000,
    # 129430000,
    # 129440000,
    # 129450000,
    # 129460000,
    # 129470000,
    # 129480000,
    # 129490000,
    # 129500000,
    # 129510000,
    # 129520000,
    # 129530000,
    # 129540000,
    # 129550000,
    # 129560000,
    # 129570000,
    # 129580000,
    # 129590000,
    # 129600000,
    # 129610000,
    # 129620000,
    # 129630000,
    # 129640000,
    # 129650000,
    # 129660000,
    # 129680000,
    # 129690000,
    # 129700000,
    # 129710000,
    # 129720000,
    # 129730000,
    # 129740000,
    # 129750000,
    # 129770000,
    # 129780000,
    # 129790000,
    # 129800000,
    # 129810000,
    # 129820000,
    # 129830000,
    # 129840000,
    # 129850000,
    # 129860000,
    # 129870000,
    # 129880000,
    # 129890000,
    # 129900000,
    # 129910000,
    # 129920000,
    # 129930000,
    # 129940000,
    # 129950000,
    # 129960000,
    # 129970000,
    # 129980000,
    # 130000000,
    # 130010000,
    # 130020000,
    # 130030000,
    # ]:
    #     remove_empty_blob('transfers', str(k))