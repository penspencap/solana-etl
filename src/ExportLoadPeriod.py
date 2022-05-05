from argparse import ArgumentParser

from src.extract.ExtractBatch import ExtractBatch
from src.load.FileOutput import FileOutput, FileOutputFormat
from src.load.TransformTask import TransformTask
from src.load.Upload import upload_data_to_gcs, upload_block_raw_to_gcs
import time


def split(start: int, end: int, inter: int):
    return [(max(start, (start+i*inter)//inter*inter), min(end, (start+(i+1)*inter)//inter*inter-1), (start+i*inter)//inter*inter) for i in range(end//inter-start//inter+1)]

def confirm_blocks(startBlock: int, endBlock: int, solana_client, count=0):
    try:
        return solana_client.get_blocks(startBlock, endBlock)['result']
    except Exception as e:
        if count <= 2:
            return confirm_blocks(startBlock, endBlock, solana_client, count + 1)
        raise Exception(f"{startBlock}-{endBlock} error")

def main():
    parser = ArgumentParser(description='Extract solana blocks from rpc.')

    parser.add_argument(
        'output_loc', type=str, help='Directory to dump block responses.'
    )
    parser.add_argument(
        '--endpoint', type=str, help='Which network to use.', default='https://api.mainnet-beta.solana.com'
    )
    parser.add_argument(
        '--start', type=int, help='Slot to start extract.'
    )
    parser.add_argument(
        '--end',
        type=int,
        help='Slot to end extract, if less than start count down from start, if None keep counting up with backoff.',
        default=None
    )
    parser.add_argument(
        '--slots_per_dir',  type=int, help='Number of slots to stream to the same file.', default=10_000
    )
    parser.add_argument(
        '--n_jobs',  type=int, help='Number of process.', default=1
    )
    parser.add_argument('--tasks', nargs='+', help='List of tasks to execute or all.', required=True)

    parser.add_argument('--temp_dir', type=str, help='Temp directory for dask when spilling to disk.', required=True)
    parser.add_argument('--destination_dir', type=str, help='Where to write the results.', required=True)
    parser.add_argument('--destination_format', type=str, help='File format of results.', required=True)
    parser.add_argument('--skip_download', type=bool, default=False, help='download blocks or not.', required=False)

    parser.add_argument('--keep_subdirs', help='Produce results for each subdir of source.', action='store_true')

    args = parser.parse_args()

    extract = ExtractBatch(args.endpoint, args.output_loc, args.slots_per_dir)
    from solana.rpc.api import Client
    solana_client = Client(
        args.endpoint,
        timeout=600
    )

    for start, end, dir_path_block in split(args.start, args.end, args.slots_per_dir):
        st = time.time()
        print('Running data===', start, end)
        if not args.skip_download:
            def get_range(start, end, count=0):
                try:
                    slots = []
                    for startBlock, endBlock, dir in split(start, end, 1000):
                        try:
                            slots.extend(confirm_blocks(startBlock, endBlock, solana_client))
                        except Exception as e:
                            raise e
                    return slots
                except Exception as e:
                    if count <= 2:
                        return get_range(start, end, count+1)
                    raise Exception('call error!!')
            extract.start_multi(start, end, args.n_jobs, _range=get_range(start, end))
        with FileOutput.with_local_cluster(temp_dir=args.temp_dir, blocks_dir=args.output_loc + f'/{dir_path_block}', n_workers=args.n_jobs) as output:
            output.write(
                TransformTask.from_names(args.tasks),
                args.destination_dir + f'/{dir_path_block}',
                FileOutputFormat[args.destination_format.upper()],
                args.keep_subdirs
            )

        for _data_type in ['blocks', 'transfers', 'transactions']:
            upload_data_to_gcs(_data_type, dir_path_block, n_jobs=args.n_jobs)
        if not args.skip_download:
            upload_block_raw_to_gcs(dir_path_block)
        print('Finish data===', start, end, ' Total using time: ', time.time()-st)


if __name__ == '__main__':
    main()
