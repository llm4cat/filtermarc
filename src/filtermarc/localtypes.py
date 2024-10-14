"""Contains custom vars, protocols, etc. used for type hinting."""
from collections.abc import Sequence
import os
import sys
from typing import Any, Callable, Protocol, Union

from pymarc import Field

if sys.version_info >= (3, 11):
    from typing import Self as Self  # noqa:F401
else:
    from typing_extensions import Self as Self  # noqa:F401


# Misc type vars defined here.

ComparisonOp = Callable[[Any, Any], bool]
FieldSpec = Union[str, Sequence[str]]
PathLike = Union[str, os.PathLike[str]]
RecordFilter = Callable[['RecordCacheLike'], bool]
SubfieldSpec = Union[str, Sequence[str]]


# Protocols defined here.

class RecordCacheLike(Protocol):
    """Protocol for types that are RecordCache-like."""
    cache: dict[str, list[Field]]
    all_fields: list[Field]
