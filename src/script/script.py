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
#     12969,
#     12920,
#     12718,
#     12848,
#     12732,
#     12707,
#     12823,
#     12692,
# })
# d.sort(reverse=True)
# print(len(d))
# for block in d[2::3]:
#     block = block*10000
#     k.append(f'export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start {block} --end {block+9999} --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl;')
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
# """
# export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start 121860000 --end 122859999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl
# """
# k = []
# for i, j in [
# ["127319991", "127320000"],
# ["129195335", "129195344"],
# ["126921282", "126921286"],
# ["128476709", "128476712"],
# ["129687368", "129687371"],
# ["127071164", "127071167"],
# ["128232620", "128232623"],
# ["127179079", "127179082"],
# ]:
#
#     k.append(f""" python3 src/ExportLoadPeriod.py /Users/pen/cryptoProject/solana-etl/data --start {i} --end {j} --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 2 --tasks all --temp_dir /Users/pen/cryptoProject/solana-etl/temp --destination_dir /Users/pen/cryptoProject/solana-etl/bq_data/ --destination_format jsonl""")
# print(';'.join(k))








