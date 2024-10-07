"""Contains tests for the 'marc' module."""
from unittest.mock import patch

from pymarc import Record
import pytest

from filtermarc.marc import RecordCache, parse_fieldspec


def test_recordcache_addfields(make_marc_records):
    record = make_marc_records(1)[0]
    rcache = RecordCache()
    assert not rcache.all_fields
    assert not rcache.cache

    rcache.add_fields(record)
    for field in record:
        assert field in rcache.all_fields
        assert field in rcache.cache[field.tag]


def test_recordcache_init_calls_addfields(make_marc_records):
    record = make_marc_records(1)[0]
    with patch.object(RecordCache, 'add_fields') as mock_method:
        rcache = RecordCache(record)
    mock_method.assert_called_with(record)


@pytest.mark.parametrize('fieldspec, expected', [
    ('500', {'500'}),
    ('100,120', {'100', '120'}),
    ('100, 120', {'100', '120'}),
    ('100,120,100', {'100', '120'}),
    (['500'], {'500'}),
    (['100', '120'], {'100', '120'}),
    (['100', '120', '100'], {'100', '120'})
])
def test_parsefieldspec(fieldspec, expected):
    assert parse_fieldspec(fieldspec) == expected
