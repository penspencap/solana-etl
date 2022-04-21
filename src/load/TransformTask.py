from __future__ import annotations

import json
from enum import Enum
from typing import Iterable, Set, List, Tuple, Callable

from pandas import DataFrame
import traceback

from src.transform.Block import Block
from src.transform.Interactions import Interactions
from src.transform.Transfer import Transfer

ResultsAndErrors = Tuple[List[List[any]], List[List[any]]]
Transform = Callable[[Block], ResultsAndErrors]


def block_to_transactions(block: Block) -> ResultsAndErrors:
    rows = []
    errors = []

    for transaction in block.transactions:
        try:
            instructions = transaction.instructions
            programs = list(map(lambda a: a.key, instructions.programs))
            mints = list(transaction.mints)
            accounts = list(transaction.accounts._accounts_by_key.keys())
            signers = transaction.signers
            rows.append([
                block.epoch,
                block.block_slot,
                block.block_height,
                transaction.signature,
                transaction.fee,
                transaction.is_successful,
                len(instructions),
                [{
                    "program": instruction.program.key,
                    "gen_id": instruction.gen_id,
                    "info_values": json.dumps(getattr(instruction, 'info_values', '')),
                    "data": getattr(instruction, 'data', None),
                    "instruction_type": getattr(instruction, 'instruction_type', None),
                    "accounts": [account.key for account in instruction.accounts],
                } for instruction in instructions],
                signers,
                signers[0],
                programs,
                accounts,
                len(accounts),
                len(mints),
                mints,  # 留意, 非原来顺序
                block.hash,
                list(transaction.log_messages())
            ])
        except Exception as e:
            errors.append(['blocks_to_transactions', str(block.source), str(str(traceback.format_exc()))])

    return rows, errors


def block_to_transfers(block: Block) -> ResultsAndErrors:
    """
    For each block, return a tuple of rows of parsed transfers and rows of errors.
    """
    rows = []
    errors = []

    interactions = Interactions([block])
    for interaction in interactions:
        try:
            if isinstance(interaction, Transfer):
                rows.append([
                    block.epoch,
                    block.block_slot,
                    block.block_height,
                    interaction.source,
                    interaction.destination,
                    interaction.mint,
                    interaction.value.v,
                    interaction.value.scale,
                    interaction.transaction_signature,
                    block.hash
                ])
        except Exception as e:
            errors.append(['blocks_to_transfers', str(block.source), str(str(traceback.format_exc()))])

    return rows, errors


def block_info(block: Block) -> ResultsAndErrors:
    row = [
        block.epoch,
        block.block_slot,
        block.block_height,
        block.hash,
        block.previous_hash,
        block.previous_number,
        len(block.transactions)
    ]

    return [row], []


# def block_instruction(block: Block) -> ResultsAndErrors:
#     rows = []
#     errors = []
#
#     for transaction in block.transactions:
#         for instruction in transaction.instructions:
#             try:
#                 rows.append([
#                     block.epoch,
#                     block.block_slot,
#                     block.block_height,
#                     instruction.program,
#                     instruction.program_id,
#                     instruction.type,
#                     instruction.info,
#                     interaction.value.scale,
#                     transaction.signers,
#                     transaction.signature,
#                     block.hash
#                 ])
#             except Exception as e:
#                 errors.append(['block_instruction', str(block.source), str(e)])
#     return rows, errors

class TransformTask(Enum):
    """
    Tasks that perform a set of transformations and returns a set of loadable results and metadata.

    @author zuyezheng
    """

    TRANSACTIONS = (
        block_to_transactions,
        [
            ('time', 'int64'),
            ('block_number', 'int64'),
            ('block_height', 'int64'),
            ('signature', 'string'),
            ('fee', 'int64'),
            ('is_successful', 'bool'),
            ('num_instructions', 'int8'),
            ('instructions', 'object'),
            ('signers', 'object'),
            ('main_signer', 'string'),
            ('programs', 'object'),
            ('accounts', 'object'),
            ('num_accounts', 'int8'),
            ('num_mints', 'int8'),
            ('mints', 'object'),
            ('block_hash', 'string'),
            ('logMessages', 'object'),
        ]
    )
    TRANSFERS = (
        block_to_transfers,
        [
            ('timestamp', 'int64'),
            ('block_number', 'int64'),
            ('block_height', 'int64'),
            ('source', 'string'),
            ('destination', 'string'),
            ('mint', 'string'),
            ('value', 'int64'),
            ('scale', 'int8'),
            ('signature', 'string'),
            ('block_hash', 'string')
        ]
    )
    BLOCKS = (
        block_info,
        [
            ('timestamp', 'int64'),
            ('number', 'int64'),
            ('block_height', 'int64'),
            ('hash', 'string'),
            ('previous_hash', 'string'),
            ('previous_number', 'int64'),
            ('num_transactions', 'int64'),
        ]
    )
    # INSTRUCTIONS = (
    #     block_instruction,
    #     [
    #         ('timestamp', 'int64'),
    #         ('block_number', 'int64'),
    #         ('block_height', 'int64'),
    #         ('signature', 'string'),
    #         ('block_hash', 'string'),
    #         ('program_id', 'string'),
    #         ('program_name', 'string'),
    #         ('accounts', 'string'),
    #         ('type', 'string'),
    #         ('info', 'object'),
    #     ]
    # )

    @staticmethod
    def all() -> Set[TransformTask]:
        return set([task for task in TransformTask])

    @staticmethod
    def from_names(names: Iterable[str]) -> Set[TransformTask]:
        tasks = set()
        for name in names:
            normalized_name = name.upper()
            if normalized_name == 'ALL':
                return TransformTask.all()
            else:
                tasks.add(TransformTask[normalized_name])

        return tasks

    @staticmethod
    def errors_to_df(errors: List[List[any]]) -> DataFrame:
        return DataFrame(errors, columns=['name', 'block', 'message'])

    transform: Transform
    meta: List[(str, str)]

    def __init__(self, transform: Transform, meta: List[(str, str)]):
        self.transform = transform
        self.meta = meta

    def to_df(self, rows: List[List[any]]) -> DataFrame:
        return DataFrame(rows, columns=list(map(lambda c: c[0], self.meta)))
