from typing import TypeVar, Union, Sequence, List, Tuple
from numbers import Number


T = TypeVar('T', bound=Number)
MySQLResults = Union[
    str,
    int,
    Sequence[str],
    Sequence[T],
    List[Tuple[Union[str, T], ...]]
]

__all__ = ['MySQLResults']
