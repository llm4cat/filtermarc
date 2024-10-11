"""Contains tests for the 'formats' module."""
from typing import Sequence

import orjson
from pymarc import Record
import pytest

from filtermarc.formats import RecordFormat, Marc, MarcInJson


# Fixtures and test data used only in this file

FileInfo = tuple[str, str, bytes]


@pytest.fixture
def simulate_one_rec():
    """Fixture: Returns a func that simulates 1-record output."""
    def _sim(fmt: RecordFormat, record: Record) -> FileInfo:
        filename = f'test{fmt.file_extension}'
        output = [fmt.header, fmt(record), fmt.footer]
        return filename, fmt.mode, b''.join(output)

    return _sim


@pytest.fixture
def simulate_multi_rec():
    """Fixture: Returns a func that simulates multi-record output."""
    def _sim(fmt: RecordFormat, records: Sequence[Record]) -> FileInfo:
        filename = f'test{fmt.file_extension}'
        rec_output = fmt.multi_separator.join([fmt(rec) for rec in records])
        output = [
            fmt.header, fmt.multi_prefix, rec_output, fmt.multi_suffix,
            fmt.footer
        ]
        return filename, fmt.mode, b''.join(output)

    return _sim


# Tests

def test_marc_single(make_marc_records, simulate_one_rec):
    record = make_marc_records(1)[0]
    fmt = Marc()
    filename, mode, output = simulate_one_rec(fmt, record)
    assert filename == 'test.mrc'
    assert mode == 'wb'
    assert output == record.as_marc()


def test_marc_multi(make_marc_records, simulate_multi_rec):
    records = make_marc_records(2)
    fmt = Marc()
    filename, mode, output = simulate_multi_rec(fmt, records)
    assert filename == 'test.mrc'
    assert mode == 'wb'
    assert output == b''.join([rec.as_marc() for rec in records])


def test_marcinjson_single_nopretty(make_marc_records, simulate_one_rec):
    record = make_marc_records(1)[0]
    fmt = MarcInJson(pretty_print=False)
    filename, mode, output = simulate_one_rec(fmt, record)
    assert filename == 'test.json'
    assert mode == 'wb'
    assert output == orjson.dumps(record.as_dict())


def test_marcinjson_single_withpretty(make_marc_records, simulate_one_rec):
    record = make_marc_records(1)[0]
    fmt = MarcInJson(pretty_print=True)
    filename, mode, output = simulate_one_rec(fmt, record)
    assert filename == 'test.json'
    assert mode == 'wb'
    assert output == orjson.dumps(record.as_dict(), option=orjson.OPT_INDENT_2)


def test_marcinjson_multi_nopretty(make_marc_records, simulate_multi_rec):
    records = make_marc_records(2)
    fmt = MarcInJson(pretty_print=False)
    filename, mode, output = simulate_multi_rec(fmt, records)
    assert filename == 'test.json'
    assert mode == 'wb'
    assert output == b''.join([
        b'[',
        orjson.dumps(records[0].as_dict()),
        b',',
        orjson.dumps(records[1].as_dict()),
        b']'
    ])


def test_marcinjson_multi_withpretty(make_marc_records, simulate_multi_rec):
    records = make_marc_records(2)
    fmt = MarcInJson(pretty_print=True)
    filename, mode, output = simulate_multi_rec(fmt, records)
    assert filename == 'test.json'
    assert mode == 'wb'
    assert output == b''.join([
        b'[\n',
        orjson.dumps(records[0].as_dict(), option=orjson.OPT_INDENT_2),
        b',\n',
        orjson.dumps(records[1].as_dict(), option=orjson.OPT_INDENT_2),
        b'\n]'
    ])
