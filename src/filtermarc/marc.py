"""Contains utilities for massaging MARC data."""
from collections.abc import Iterable
from typing import Optional

from filtermarc.localtypes import FieldSpec
from pymarc import Field


class RecordCache:
    """Class for auto-caching of fields on pymarc Record objects.

    Attributes:
        cache: A dict mapping MARC tags to lists of pymarc Fields.
        all_fields: A list of all fields in this cache.
    """

    def __init__(self, fields: Optional[Iterable[Field]] = None) -> None:
        """Inits a new RecordCache instance.

        Args:
            fields: Optional. Pass in the sequence of pymarc Fields you
                want to cache.
        """
        self.reset()
        if fields:
            self.add_fields(fields)

    def reset(self) -> None:
        """Resets this RecordCache instance."""
        self.cache: dict[str, list[Field]] = {}
        self.all_fields: list[Field] = []

    def add_fields(self, fields: Iterable[Field]) -> None:
        """Adds pymarc Field objects to this RecordCache instance.

        Args:
            fields: A sequence of pymarc Fields to cache.
        """
        cache = self.cache
        for field in fields:
            marc_tag = field.tag
            tag_fields = list(cache.get(marc_tag, []))
            tag_fields.append(field)
            cache[marc_tag] = tag_fields
        self.cache = cache
        self.all_fields.extend(fields)


def parse_fieldspec(field_spec: FieldSpec) -> set[str]:
    """Utility function to parse a user-entered field spec.

    Returns a tuple containing tags as strings.

    Args:
        field_spec: A string or sequence specifying one or more MARC
            tags. The string may be one MARC tag or a comma-separated
            list.
    """
    if hasattr(field_spec, 'split'):
        return set([tag.strip() for tag in field_spec.split(',')])
    return set(field_spec)
