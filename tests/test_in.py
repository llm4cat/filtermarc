"""Contains tests for 'in_' module."""
import pytest

from filtermarc.in_ import stream_records_from_files


@pytest.mark.parametrize('paths', [
    ('in/test1.mrc',),
    ('in/test1.mrc', 'in/test2.mrc'),
    ('in/test1.json',),
    ('in/test1.json', 'in/test2.json'),
    ('in/test1.json', 'in/test2.mrc', 'in/test3.json')
])
def test_stream_records_from_files(paths, make_marc_records, marc_file):
    files = [marc_file(
        make_marc_records(10), path, path.endswith('.json')
    ) for path in paths]
    records = list(stream_records_from_files(*files))
    assert len(records) == len(paths) * 10
    assert records[0]['245'].subfields[0].value == 'Test Title 0'
