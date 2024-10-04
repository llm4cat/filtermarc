"""Configuration and shared fixtures etc. for tests."""
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Optional, Tuple, Union

import orjson
import pytest

from filtermarc.localtypes import PathLike
from pymarc import Field, JSONWriter, MARCWriter, Record, Subfield


# Here are Type vars used in the fixtures, below.

FixedFieldPos = Tuple[int, str]
FixedFieldSpec = Sequence[FixedFieldPos]
FieldData = Mapping[str, Union[Sequence[FixedFieldSpec], Sequence[str]]]


@pytest.fixture
def make_marc_records():
    """Fixture: returns a function for making dummy MARC records."""
    def _make_fixedfield(tag: str, raw: FixedFieldSpec) -> Field:
        fdata = []
        last_pos = 0
        for start, pos_data in sorted(raw):
            fdata.extend(['#' for i in range(last_pos, start)])
            fdata.append(pos_data)
            last_pos = start
        return Field(tag, data=''.join(fdata).replace('#', ' '))

    def _make_varfield(tag: str, raw: str) -> Field:
        fdata = []
        ind, sfdata = list(raw[0:2].replace('#', ' ')), raw[2:].split('$')[1:]
        return Field(tag, indicators=ind, subfields=[
            Subfield(code=sf[0], value=sf[1:]) for sf in sfdata
        ])

    def _make_marc_records(num: int, fdata: FieldData = {}) -> list[Record]:
        records = []
        req_tags = {'001', '008', '100', '245'}
        tags = sorted(list(req_tags | set(fdata.keys())))
        for i in range(num):
            record_data = {
                '001': [str(i)],
                '008': [[(0, '01012000s2020####xx############001#0#eng##')]],
                '100': [f'## $aTest Author {i}'],
                '245': [f'## $aTest Title {i}']
            }
            record_data.update(fdata)
            record = Record()
            for tag, fieldvals in record_data.items():
                tagval = int(tag)
                for fieldval in fieldvals:
                    if tagval < 6:
                        field = Field(tag, data=fieldval)
                    elif tagval < 10:
                        field = _make_fixedfield(tag, fieldval)
                    else:
                        field = _make_varfield(tag, fieldval)
                    record.add_field(field)
            records.append(record)
        return records

    return _make_marc_records


@pytest.fixture
def marc_file(tmp_path):
    """Fixture: creates a temporary MARC file for testing."""
    def _marc_file(
        records: Sequence[Record],
        relpath: PathLike,
        is_json: bool = False
    ) -> Path:
        filepath = tmp_path / Path(relpath)
        if is_json:
            mode = 'wt'
            writer_cls = JSONWriter
        else:
            mode = 'wb'
            writer_cls = MARCWriter
        print(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, mode) as fh:
            writer = writer_cls(fh)
            for record in records:
                writer.write(record)
            writer.close()
        return filepath
    return _marc_file
