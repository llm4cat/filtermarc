"""Contains tools for reading MARC data from disk."""
from collections.abc import Generator
from pathlib import Path
from typing import Any

from filtermarc.localtypes import PathLike
from pymarc import JSONReader, MARCReader, Record


def stream_records_from_files(
    *filepaths: PathLike,
    **kwargs: Any
) -> Generator[Record]:
    """Creates a generator yielding pymarc.Records from MARC files.

    MARC files may be in binary MARC format or the MARC-in-JSON format
    you get from pymarc.Record.as_dict().

    Args:
        filepaths: One or more paths to files containing MARC records
            you want to extract.
        kwargs: Any kwargs to pass to the pymarc.MarcReader object that
            will read the files.
    """
    for path in filepaths:
        with Path(path).open('rb') as fh:
            binary_marc = next(MARCReader(fh, **kwargs), None)
            reader_cls = JSONReader if binary_marc is None else MARCReader
            fh.seek(0)
            for record in reader_cls(fh, **kwargs):  # type: ignore
                yield record
