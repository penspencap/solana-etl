import json
from argparse import ArgumentParser
from typing import Set, Dict

from pandas import DataFrame

from src.extract.Extract import Extract
from src.load.TransformTask import TransformTask
from src.transform.Block import Block


class ExtractStreaming(Extract):
    """
    Extract blocks and stream them directly through transforms and to file.

    @author zuyezheng
    """

    tasks: Set[TransformTask]

    def __init__(self, endpoint: str, output_loc: str, slots_per_dir: int, tasks: Set[TransformTask]):
        super().__init__(endpoint, output_loc, slots_per_dir)

        self.tasks = tasks

    def process_block(self, slot: int, block_json: Dict):
        path_base = self.output_path.joinpath(str(slot // self.slots_per_dir * self.slots_per_dir))

        def write_rows(name: str, df: DataFrame):
            file_path = path_base.with_name(f'{path_base.name}_{name.lower()}.json')
            if file_path.exists():
                with open(file_path, 'a+') as f:
                    df.to_json(f, orient='records', lines=True)
            else:
                df.to_json(str(file_path), orient='records', lines=True)

        try:
            block = Block(block_json, str(slot))

            # aggregate results and errors for each task
            for task in self.tasks:
                results_and_errors = task.transform(block)

                write_rows(task.name, task.to_df(results_and_errors[0]))
                write_rows('errors', TransformTask.errors_to_df(results_and_errors[1]))
            raw_block_path = path_base.with_name(f'{path_base.name}_blocks_raw.txt')
            if raw_block_path.exists():
                with open(raw_block_path, 'a+') as f:
                    f.write(json.dumps(block_json) + '\n')
            else:
                with open(raw_block_path, 'w') as f:
                    f.write(json.dumps(block_json) + '\n')

        except Exception as e:
            write_rows('errors', TransformTask.errors_to_df([['process_block', slot, str(e)]]))


def main():
    parser = ArgumentParser(description='Extract, transform and load solana blocks from rpc to file.')

    parser.add_argument(
        'output_loc', type=str, help='Directory to stream transformed rows.'
    )
    parser.add_argument('--tasks', nargs='+', help='List of tasks to execute or all.', required=True)
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
        '--slots_per_file',  type=int, help='Number of slots to stream to the same file.', default=10_000
    )

    args = parser.parse_args()

    extract = ExtractStreaming(
        args.endpoint,
        args.output_loc,
        args.slots_per_file,
        TransformTask.from_names(args.tasks)
    )
    extract.start(args.start, args.end)


if __name__ == '__main__':
    main()
