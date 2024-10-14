"""Contains classes and tools for filtering MARC records into sets."""
from collections.abc import Generator, Iterable, Sequence
import operator
from typing import Optional, Union

from filtermarc.localtypes import (
    ComparisonOp, FieldSpec, RecordCacheLike, RecordFilter, Self, SubfieldSpec
)
from filtermarc.marc import RecordCache, parse_fieldspec
from pymarc import Record


class RecordFilterPipeline:
    """Class for filtering MARC records into sets.

    Attributes:
        filters: One or more filter functions used in this pipeline to
            filter records. A filter function should take a
            RecordCacheLike object (i.e., a dict mapping MARC tags to
            pymarc Fields) and return a boolean indicating whether a
            given record matches that filter or not.
    """

    def __init__(self, *filters: RecordFilter) -> None:
        """Inits a RecordFilterPipeline instance.

        Args:
            filters: One or more filter args. See 'filters' attribute.
        """
        self.filters = list(filters)

    def _clone(self) -> Self:
        cloned = type(self)()
        cloned.filters = list(self.filters)
        return cloned

    def _add_filters(self, *filters: RecordFilter) -> None:
        self.filters.extend([f for f in filters if f not in self.filters])

    def add(self, *filters: RecordFilter) -> Self:
        """Creates a new pipeline with filters added.

        Returns a copy of this pipeline instance with the new filters
        applied.

        Args:
            filters: One or more arguments, each of which is a filter
                function to use in this pipeline to filter records.
        """
        cloned = self._clone()
        cloned._add_filters(*filters)
        return cloned

    def intersect(self, other: Self) -> Self:
        """Combines two pipelines, requiring both to pass.

        Returns a new RecordFilterPipeline with both sets of filters.

        Args:
            other: Another RecordFilterPipeline that you wish to
                combine with this one.
        """
        return self.add(*other.filters)

    def union(self, other: Self) -> Self:
        """Combines two pipelines, requiring one of them to pass.

        Returns a new RecordFilterPipeline with filters configured
        appropriately.

        Args:
            other: Another RecordFilterPipeline that you wish to
                combine with this one.
        """
        if self.filters == other.filters:
            return self._clone()

        def _union_filter(fields: RecordCacheLike) -> bool:
            return self.check_record(fields) or other.check_record(fields)

        return type(self)(_union_filter)

    def check_record(self, record: Union[Record, RecordCacheLike]) -> bool:
        """Returns True if a record matches this filter pipeline.

        Args:
            record: The record to attempt to match against. This may be
            a pymarc Record object OR a RecordCache.
        """
        if isinstance(record, Record):
            record = RecordCache(record)
        for filtr in self.filters:
            if not filtr(record):
                return False
        return True

    def run(self, records: Iterable[Record]) -> list[Record]:
        """Returns a list of records matching filter criteria.

        Args:
            records: An iterable of pymarc Record objects to filter.
        """
        return [record for record in records if self.check_record(record)]

    def run_generator(self, records: Iterable[Record]) -> Generator[Record]:
        """Gets a generator yielding recs matching filter criteria.

        Args:
            records: An iterable of pymarc Record objects to filter.
        """
        for record in records:
            if self.check_record(record):
                yield record


# Below are filter factory functions. They create functions conforming
# to the RecordFilter typevar, which can be used in a
# RecordFilterPipeline for filtering records.

def by_character_position(
    marc_tags: FieldSpec,
    cp_range: Sequence[int],
    match_val: Union[str, int],
    compare: ComparisonOp = operator.eq,
    subfields: Optional[SubfieldSpec] = None
) -> RecordFilter:
    """Creates a record filter for matching by character position.

    The return value is a record filter function that you can use in
    the RecordFilterPipeline. It can match by character position
    against fixed field data (001 to 009) OR one or more subfields.
    The first match found returns True.

    Args:
        marc_tags: The MARC tag or tags you want to check. May be a
            str -- one tag or a comma-separated list -- or a sequence.
        cp_range: The character positions to match from the field data.
            Must be a sequence of two integers representing the start
            and end character position to match on. This is inclusive
            and zero-indexed. E.g.: (0, 3) matches against character
            positions 0, 1, 2, and 3.
        match_val: The value you want to match against. Provide a str
            or an int, depending on your comparison operator.
        op: The operator to use when matching. This should be the
            appropriate function from the 'operator' module; default is
            operator.eq (equal).
        subfields: Optional. If a field being tested is a control field
            (001 to 009), this is ignored. Otherwise, it specifies the
            subfields to check. Each subfield instance is checked
            independently against the match value. If None, all are
            checked. You can provide a string or a sequence of strings.
            'abcd' and ['a', 'b', 'c', 'd'] are equivalent.
    """
    subfields = list(subfields) if subfields else []
    tags = parse_fieldspec(marc_tags)

    def _filter(fields: RecordCacheLike) -> bool:
        for tag in tags:
            for field in fields.cache.get(tag, []):
                if field.data:
                    vals = [field.data]
                elif subfields:
                    vals = field.get_subfields(*subfields)
                else:
                    vals = [subf.value for subf in field.subfields]
                for val in vals:
                    cmp_val = type(match_val)(val[cp_range[0]:cp_range[1] + 1])
                    if compare == operator.contains:
                        args = (cmp_val, match_val)
                    else:
                        args = (match_val, cmp_val)
                    if compare(*args):
                        return True
        return False
    return _filter


def by_field_exists(marc_tags: FieldSpec) -> RecordFilter:
    """Creates a record filter, True if a field exists and has data.

    The return value is a filter function you can use in a
    RecordFilterPipeline.

    Args:
        marc_tags: The MARC tag or tags you want to check. May be a
            str -- one tag or a comma-separated list -- or a sequence.
    """
    tags = parse_fieldspec(marc_tags)

    def _filter(fields: RecordCacheLike) -> bool:
        for tag in tags:
            for field in fields.cache.get(tag, []):
                if field.value():
                    return True
        return False
    return _filter
