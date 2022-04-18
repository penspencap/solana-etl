from argparse import ArgumentParser

from src.extract.ExtractBatch import ExtractBatch
from src.load.FileOutput import FileOutput, FileOutputFormat
from src.load.TransformTask import TransformTask
from src.load.Upload import upload_data_to_gcs, upload_block_raw_to_gcs


def split(start: int, end: int):
    return [(max(start, (start+i*10000)//10000*10000), min(end, (start+(i+1)*10000)//10000*10000-1), (start+i*10000)//10000*10000) for i in range(end//10000-start//10000+1)]


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

    parser.add_argument('--keep_subdirs', help='Produce results for each subdir of source.', action='store_true')

    args = parser.parse_args()

    extract = ExtractBatch(args.endpoint, args.output_loc, args.slots_per_dir)
    from solana.rpc.api import Client
    solana_client = Client(
        args.endpoint,
        timeout=600
    )

    for start, end, dir_path_block in split(args.start, args.end):
        print('running data===', start, end)
        extract.start_multi(start, end, args.n_jobs, _range=solana_client.get_confirmed_blocks(start, end)['result'])
        with FileOutput.with_local_cluster(temp_dir=args.temp_dir, blocks_dir=args.output_loc + f'/{dir_path_block}') as output:
            output.write(
                TransformTask.from_names(args.tasks),
                args.destination_dir + f'/{dir_path_block}',
                FileOutputFormat[args.destination_format.upper()],
                args.keep_subdirs
            )

        for _data_type in ['blocks', 'transfers', 'transactions']:
            upload_data_to_gcs(_data_type, dir_path_block)
        upload_block_raw_to_gcs(dir_path_block)


if __name__ == '__main__':
    main()
