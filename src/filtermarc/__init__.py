"""Tools for filtering MARC record data sets."""
from importlib import metadata

from . import localtypes, out, filters, formats, marc
from .in_ import stream_records_from_files
from .filters import RecordFilterPipeline
from .out import Output, Job


__version__ = metadata.version('filtermarc')
__all__ = [
    'localtypes', 'marc', 'filters', 'formats', 'out',
    'stream_records_from_files', 'RecordFilterPipeline', 'filters', 'Output',
    'Job'
]
