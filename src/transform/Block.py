from __future__ import annotations

import gzip
import json
import time
from functools import cached_property
from pathlib import Path
from typing import Dict

from src.transform.Transaction import Transaction
from src.transform.Transactions import Transactions


class Block:
    """
    Parse JSON metadata for a block.

    @author zuyezheng
    """

    result: Dict[str, any] | None
    source: str
    missing: bool

    @staticmethod
    def open(path: Path):
        def _open():
            if path.suffix == '.gz':
                return gzip.open(path)
            else:
                return open(path)

        with _open() as f:
            return Block(json.load(f), path)

    def __init__(self, block_meta: Dict, source: str):
        self.source = source

        if 'result' in block_meta:
            self.result = block_meta['result']
            self.missing = False
        else:
            self.result = None
            self.missing = True

    @property
    def hash(self) -> str:
        return self.result['blockhash']

    @property
    def previous_hash(self) -> str:
        return self.result['previousBlockhash']

    @property
    def previous_number(self) -> str:
        return self.result['parentSlot']

    def has_transactions(self) -> bool:
        return not self.missing and len(self.result['transactions']) > 0

    @property
    def epoch(self) -> int:
        return self.result['blockTime']

    def time(self) -> time:
        return time.gmtime(self.result['blockTime'])

    @property
    def block_slot(self):
        return self.result['blockSlot']

    @property
    def block_height(self):
        return self.result['blockHeight']

    @cached_property
    def transactions(self) -> Transactions:
        """ Parse and return all transactions in the block. """
        if self.has_transactions():
            return Transactions(list(map(
                lambda t: Transaction(t),
                self.result['transactions']
            )))
        else:
            return Transactions([])

    def find_transaction(self, signature: str) -> Transaction | None:
        """ Linear search for an instruction with the given signature. """
        for transaction in self.transactions:
            if signature in transaction.signatures():
                return transaction

        return None
