import os
from google.cloud import storage
from google.cloud.storage import Blob
from joblib import Parallel, delayed
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/solana_data/solana-etl/blockchain-data-process@footprint-blockchain-etl.iam.gserviceaccount.com.json'
from argparse import ArgumentParser


def run_upload(data):
    bucket, src, target = data
    blob = bucket.blob(target)
    blob.upload_from_filename(src)


def upload_data_to_gcs(task, blocks, bucket='crypto_etl', n_jobs=4):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    mapper = {
        'blocks': 'blocks',
        'transactions': 'transactions',
        'transfers': 'token_transfers',
    }
    _objects = f'solana_export/{task}/{blocks}/'
    filename = f'/solana_data/bq_data/{mapper[task]}/{blocks}/'

    Parallel(n_jobs=n_jobs)(delayed(run_upload)((bucket, filename+_filename, _objects+_filename, )) for _filename in os.listdir(filename))


def upload_block_raw_to_gcs(blocks, bucket='crypto_etl', n_jobs=4):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    _objects = f'solana_export/blocks_raw/{blocks}/'
    filename = f'/solana_data/data/{blocks}/'
    Parallel(n_jobs=n_jobs)(
        delayed(run_upload)((bucket, filename + _filename, _objects + _filename,)) for _filename in os.listdir(filename)
    )


def main():
    parser = ArgumentParser(description='Extract solana blocks from rpc.')

    parser.add_argument(
        'dir_blocks', type=int, help='Directory to dump block responses.'
    )
    args = parser.parse_args()
    for _data_type in ['blocks', 'transfer']:
        upload_data_to_gcs(_data_type, args.dir_blocks)

    upload_block_raw_to_gcs(args.dir_blocks, n_jobs=args.n_jobs)


if __name__ == '__main__':
    main()

