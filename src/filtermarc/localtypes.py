"""Contains custom vars, protocols, etc. used for type hinting."""
from collections.abc import Mapping, Sequence
import os
import sys
from typing import Callable, Protocol, Union

from pymarc import Field

if sys.version_info >= (3, 11):
    from typing import Self  # noqa:F401
else:
    from typing_extensions import Self  # noqa:F401


# Misc type vars defined here.

FieldSpec = Union[str, Sequence[str]]
PathLike = Union[str, os.PathLike]
RecordFilter = Callable[['RecordCacheLike'], bool]
SubfieldSpec = Union[str, Sequence[str]]


# Protocols defined here.

class RecordCacheLike(Protocol):
    """Protocol for types that are RecordCache-like."""
    cache: Mapping[str, Sequence[Field]]
    all_fields: Sequence[Field]
