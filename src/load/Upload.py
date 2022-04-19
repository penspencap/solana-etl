import os
import time
from google.cloud import storage
from joblib import Parallel, delayed

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/solana_data/solana-etl/blockchain-data-process@footprint-blockchain-etl.iam.gserviceaccount.com.json'
from argparse import ArgumentParser
import os, tarfile


def run_upload(data, retry = 0):
    try:
        bucket, src, target = data
        empty_file = os.path.getsize(src) == 1
        if empty_file:
            return
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket)
        blob = bucket.blob(target)
        blob.upload_from_filename(src)
        print(f'{src} success upload to target {target}')
    except:
        time.sleep(retry*2)
        if retry < 3:
            run_upload(data, retry+1)


def upload_data_to_gcs(task, blocks, bucket='crypto_etl', n_jobs=4):
    mapper = {
        'blocks': 'blocks',
        'transactions': 'transactions',
        'transfers': 'token_transfers',
    }
    _objects = f'solana_export/{mapper[task]}/{blocks}/'
    filename = f'/solana_data/bq_data/{task}/{blocks}/'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    blobs = bucket.list_blobs(prefix=_objects)
    for blob in blobs:
        blob.delete()
        print(_objects, 'was delete')

    Parallel(n_jobs=n_jobs, backend='multiprocessing')(
        delayed(run_upload)((bucket, filename + _filename, _objects + _filename,)) for _filename in
        os.listdir(filename))


def upload_block_raw_to_gcs(blocks, bucket='crypto_etl'):
    _objects = f'solana_export/blocks_raw/'
    _filename = f'{blocks}.gz'
    filename = f'/solana_data/data/{blocks}/'
    raw_block_dir = f'/solana_data/blocks_raw/'
    _make_targz_one_by_one(raw_block_dir + _filename, filename)
    run_upload((bucket, raw_block_dir + _filename, _objects + _filename))


def _make_targz_one_by_one(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        for root, _dir, files in os.walk(source_dir):
            for file in files:
                path_file = os.path.join(root, file)
                tar.add(path_file)


def main():
    parser = ArgumentParser(description='Extract solana blocks from rpc.')

    parser.add_argument(
        'dir_blocks', type=int, help='Directory to dump block responses.'
    )
    parser.add_argument(
        'n_jobs', type=int, help='nodes to update data.'
    )
    args = parser.parse_args()
    for _data_type in ['blocks', 'transfers', 'transactions']:
        upload_data_to_gcs(_data_type, args.dir_blocks)

    upload_block_raw_to_gcs(args.dir_blocks)


if __name__ == '__main__':
    main()

