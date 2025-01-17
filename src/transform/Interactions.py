from collections import defaultdict
from functools import partial
from typing import List, Type, TypeVar, Dict

from src.transform.Block import Block
from src.transform.ProgramInstruction import ProgramInstruction
from src.transform.Interaction import Interaction
from src.transform.Transfer import CoinTransfer, TokenTransfer


T = TypeVar('T', bound=Interaction)


class Interactions:
    """
    Form interactions from a list of blocks.

    @author zuye.zheng
    """

    interactions: List[Interaction]

    def __init__(self, blocks: List[Block]):
        self.interactions = []
        for block in blocks:
            for transaction in block.transactions.successful:
                # add coin transfers
                self.interactions.extend(map(
                    partial(CoinTransfer.from_instruction, transaction),
                    ProgramInstruction.SYSTEM_TRANSFER.filter(transaction.instructions, True)
                ))

                # add token transfers
                # self.interactions.extend(map(
                #     partial(TokenTransfer.from_instruction, transaction),
                #     ProgramInstruction.SPL_TRANSFER.filter(transaction.instructions, True)
                # ))
                # fix token_transfers source = destination
                balance_changes = transaction.token_balance_changes
                self.interactions.extend(
                    [partial(TokenTransfer.from_instruction, transaction)(i) for i in ProgramInstruction.SPL_TRANSFER.filter(transaction.instructions, True) if i.info_accounts['source'] in balance_changes or i.info_accounts['destination'] in balance_changes]
                )

    def __iter__(self):
        return self.interactions.__iter__()

    def __len__(self):
        return len(self.interactions)

    def by_type(self) -> Dict[Type[T], List[Interaction]]:
        by_type = defaultdict(lambda: [])

        for interaction in self.interactions:
            by_type[type(interaction)].append(interaction)

        return by_type
