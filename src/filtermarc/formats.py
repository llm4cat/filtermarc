"""Contains classes for formatting output records."""
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

import orjson
from pymarc import Record


class RecordFormat(ABC):
    """Abstract base class for representing record output formats.

    To implement: create a subclass, assign a file extension attribute,
    specify other attributes or override properties as needed, and
    override the __call__ method to define how to convert a pymarc
    Record object to the appropriate bytes data to write to a
    file of this type.

    Attributes:
        file_extension: The file extension to use for files of this
            type (string). Should include the preceding dot.
        mode: The file mode for writing to a new file of this type.
        header: Data to output before outputting any records.
        footer: Data to output after outputting records.
        multi_prefix: The prefix to output for multi-record files.
        multi_suffix: The suffix to output for multi-record files.
        multi_separator: The separator between multiple records.
    """
    file_extension = ''
    mode = 'wb'
    header = b''
    footer = b''
    multi_prefix = b''
    multi_suffix = b''
    multi_separator = b''

    @abstractmethod
    def __call__(self, record: Record) -> bytes:
        """Converts one pymarc Record to bytes output data.

        Subclasses must implement this method.

        Args:
            record: The pymarc Record object to convert.
        """
        return None


class Marc(RecordFormat):
    """Class representing the raw MARC binary format."""
    file_extension = '.mrc'

    def __call__(self, record: Record) -> bytes:
        """Converts a pymarc Record to a raw MARC binary record.

        Args:
            record: The pymarc Record object to convert.
        """
        return record.as_marc()


class Json(RecordFormat):
    """Abstract base class for JSON output files.

    Note that, since we're using orjson to output JSON, this outputs
    UTF-8 bytes.
    """
    file_extension = '.json'

    def __init__(self, pretty_print: bool = False):
        """Inits a MarcInJson RecordFormat instance.
        
        Args:
            pretty_print: If True, output is pretty printed using
                orjson.option.OPT_INDENT_2.
        """
        self.pretty_print = pretty_print
        self.option = orjson.OPT_INDENT_2 if pretty_print else None
    
    @property
    def multi_prefix(self) -> bytes:
        """The prefix to output for multi-record files."""
        if self.pretty_print:
            return b'[\n'
        return b'['

    @property
    def multi_suffix(self) -> bytes:
        """The suffix to output for multi-record files."""
        if self.pretty_print:
            return b'\n]'
        return b']'

    @property
    def multi_separator(self) -> bytes:
        """The separator between multiple records."""
        if self.pretty_print:
            return b',\n'
        return b','

    def as_json(self, record: Mapping[str, Any]) -> bytes:
        """Serialize a dict or other mapping to JSON."""
        return orjson.dumps(record, option=self.option)


class MarcInJson(Json):
    """Class representing the MARC-in-JSON record format."""

    def __call__(self, record: Record) -> bytes:
        """Converts a pymarc Record to a MARC-in-JSON str.

        Args:
            record: The pymarc Record object to convert.
        """
        return self.as_json(record.as_dict())
