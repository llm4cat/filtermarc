"""Contains classes for outputting filtered record sets."""
from collections.abc import Generator, Sequence
from contextlib import contextmanager, ExitStack, nullcontext
from pathlib import Path
import sys
from typing import Any, IO, Iterable, Optional, TextIO, Type
from types import TracebackType

from filtermarc.filters import RecordFilterPipeline
from filtermarc.formats import RecordFormat, Marc
from filtermarc.localtypes import PathLike, Self
from filtermarc.marc import RecordCache
from pymarc import Record


class RecordFileWriter:
    """Class for managing writing a batch of output files.

    A "batch of output files" means one set of files containing records
    matching one output filter.

    This is intended to be used as a context manager.

        with RecordFileWriter(*args) as writer:
            for record in records:
                writer.write(record)

    Opening and closing files are handled as needed without the user
    having to know any of the details.

    This class is not private, but it is not exposed from the top-level
    package because users of the filtermarc package should never need
    to instantiate this -- these are created and used by the Job class.

    Attributes:
        dirpath: A Path for the directory where this batch of output
            files goes.
        basefilename: A string. If writing multiple files, they are
            named {basefilename}-0001 etc., with the file extension
            coming from the record_format.
        record_format: The RecordFormat object that defines how to
            format the file data for this output batch.
        max_per_file: An int giving the maximum number of records to
            write to a single file in this batch. I.e., if a
            max_per_file of 1000 is used for a batch of 10000 records,
            the final output will comprise 10 files. A value <1
            indicates no limit, and all records will go into the same
            file.
        active_file_count: An int tracking the cardinality of the
            currently active file in the batch, so far. This is used
            to generate the filename.
        active_record_count: An int tracking the cardinality of the
            current record within the current file. Resets to 0 when
            starting a new file.
        multi: True if files in this batch contain multiple records.
        active_fh: A file handle, IO object, for the currently active
            file.
    """

    def __init__(
        self,
        dirpath: PathLike,
        basefilename: str,
        record_format: RecordFormat,
        max_per_file: int = 0,
    ) -> None:
        """Inits a RecordFileWriter instance.

        Args:
            dirpath: A str or Path, becomes the 'dirpath' attribute.
            basefilename: See 'basefilename' attribute.
            record_format: See 'record_format' attribute.
            max_per_file: See 'max_per_file' attribute.
        """
        self.dirpath = Path(dirpath)
        self.basefilename = basefilename
        self.record_format = record_format
        self.max_per_file = 0 if max_per_file < 1 else max_per_file
        self.active_file_count = 0
        self.active_record_count = 0
        self.active_fh: Optional[IO[Any]] = None

    def __enter__(self) -> Self:
        self.open_next()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType]
    ) -> None:
        self.close_active()

    @property
    def multi(self) -> bool:
        """True if files in this batch contain multiple records."""
        return self.max_per_file != 1

    def path_to_nth_file(self, nth: int) -> Path:
        """A Path (absolute) to the nth file.

        Args:
            nth: An integer for the number of file you want to create a
                Path for.
        """
        filename = f'{self.basefilename}-{nth:04}'
        filesuffix = self.record_format.file_extension
        return Path(self.dirpath / filename).with_suffix(filesuffix)

    def open_next(self) -> IO[Any]:
        """Opens the next file in the sequence and returns the IO obj.

        If an active file is already open, this closes it an opens the
        next one.
        """
        self.close_active()
        self.active_file_count += 1
        path = self.path_to_nth_file(self.active_file_count)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.active_fh = path.open(self.record_format.mode)
        self.active_fh.write(self.record_format.header)
        if self.multi:
            self.active_fh.write(self.record_format.multi_prefix)
        return self.active_fh

    def close_active(self) -> None:
        """Closes the currently active file."""
        if self.active_fh:
            if self.multi:
                self.active_fh.write(self.record_format.multi_suffix)
            self.active_fh.write(self.record_format.footer)
            self.active_fh.close()
            self.active_fh = None
        self.active_record_count = 0

    def write(self, record: Record) -> None:
        """Writes a record to the currently active output file.

        If the output file is full after the write, it closes it.

        Args:
            record: The pymarc Record to write.
        """
        fh = self.active_fh or self.open_next()
        if self.active_record_count > 0:
            fh.write(self.record_format.multi_separator)
        fh.write(self.record_format(record))
        self.active_record_count += 1
        if self.active_record_count == self.max_per_file:
            self.close_active()


class Output:
    """Class defining parameters for one output data set.

    Attributes:
        name: A string name for this data set, used for naming output
            files.
        pipeline: The RecordFilterPipeline object containing filters
            for this data set.
        record_format: A RecordFormat object that defines the desired
            output format for records in this data set. None allows
            setting this at the Job level.
        limit: An int indicating the max number of records for this
            data set. Use <1 for unlimited. None allows setting this at
            the Job level.
    """

    def __init__(
        self,
        name: str,
        pipeline: RecordFilterPipeline = RecordFilterPipeline(),
        record_format: Optional[RecordFormat] = None,
        limit: Optional[int] = None,
    ) -> None:
        """Inits an Output instance.

        Args:
            name: See 'name' attribute.
            pipeline: Optional. See 'pipeline' attribute. Defaults to a
                filter that lets everything through.
            record_format: Optional. See 'record_format' attribute.
                Default is None.
            limit: Optional. See 'limit' attribute. Default is None.
        """
        self.name = name
        self.pipeline = pipeline
        self.record_format = record_format
        self.limit = limit


class Job:
    """Class for running multi-filter/format output jobs on records.

    The goal is to let you create multiple data sets based on one
    large, canonical set of MARC records without having to loop through
    the large data set more than once. Define Output objects for each
    data set you want made, instantiate your Job object with the needed
    settings, and then pass an iterable containing pymarc Record
    objects to the 'run' method. Using a generator lets you stream
    records from/to disk without memory concerns.

    Attributes:
        rfwriter_cls: The class to use for writing records to output
            files. Should be RecordFileWriter or a subclass.
        outputs: A sequence of Output objects representing the data
            sets you want made. Note that specific 'record_format' and
            'limit' Output attributes override any defaults here.
        base_path: The Path (absolute) where you want all data for this
            job to go. For each Output, it creates a subdirectory
            directly under the base path (using the Output name), and
            under that it creates a series of files contaning records
            that match your desired output pipeline, in the desired
            format.
        log_path: The Path (absolute) for the log file. If None, log
            output goes to stdout.
        log_every: An int for how often to update the log output, in
            numbers of records processed. A value <1 suppresses log
            output.
        log_fh: A TextIO object for interacting with the log file.
        max_per_file: An int for the maximum number of records to write
            to each file for each output batch. A value <1 indicates
            no maximum.
        default_record_format: The RecordFormat object you want to use
            as the default for this job. If an Output object lacks its
            own record_format, this is used.
        default_output_limit: An int for the limit you want to use as
            the default for this job. If an Output object lacks its own
            limit, this is used. A value <1 indicates no limit.
    """
    rfwriter_cls: Type[RecordFileWriter] = RecordFileWriter
    _BatchInfoType = tuple[
        dict[str, int],
        dict[str, int],
        dict[str, Output],
        dict[str, RecordFileWriter]
    ]

    def __init__(
        self,
        outputs: Sequence[Output],
        base_path: PathLike,
        log_path: Optional[PathLike] = None,
        log_every: int = 10000,
        max_per_file: int = 0,
        default_record_format: RecordFormat = Marc(),
        default_output_limit: int = 100000
    ) -> None:
        """Inits a new Job object.

        Args:
            outputs: See the 'outputs' attribute.
            base_path: See the 'base_path' attribute.
            log_path: Optional. See the 'log_path' attribute. Default
                is None, which sends log output to stdout.
            log_every: Optional. See 'log_every' attribute. Default is
                10000.
            max_per_file: Optional. See 'max_per_file' attribute.
                Default is 0, no maximum.
            default_record_format: Optional. See
                'default_record_format' attribute. Default is
                formats.Marc, or raw MARC binary.
        """
        self.outputs = outputs
        self.base_path = Path(base_path)
        self.log_path = log_path
        self.log_every = log_every
        self.log_fh: Optional[TextIO] = None
        self.max_per_file = 0 if max_per_file < 0 else max_per_file
        self.default_record_format = default_record_format
        self.default_output_limit = default_output_limit

    @contextmanager
    def open_log(self) -> Generator[TextIO]:
        """Opens a log file for writing.

        This is a context manager. Use:

            with myjob.open_log() as log_fh:
                ...
        """
        if self.log_path:
            self.log_fh = Path(self.log_path).open('w')
        else:
            self.log_fh = sys.stdout
        yield self.log_fh
        if self.log_fh and self.log_fh != sys.stdout:
            self.log_fh.close()

    def log(self, *msg: Optional[str]) -> None:
        """Prints one or more messages to the current log file.

        Args:
            *msg: One or more string messages you wish to print.
        """
        print(*msg, file=self.log_fh)

    def log_state(
        self,
        total_records: int,
        counts: dict[str, int],
        limits: dict[str, int]
    ) -> None:
        """Prints current job state to the log.

        Args:
            total_records: An int for how many total records have been
                processed so far.
            counts: A dict mapping Output names to counts of records in
                each group found so far.
            limits: A dict mapping Output names to the limit for each
                group.
        """
        self.log(f"=== {total_records} Processed ===")
        for output in self.outputs:
            self.log(''.join([
                f"{counts[output.name]} \"{output.name}\" records found",
                f" (max {limits[output.name]})" if limits[output.name] else ''
            ]))

    def _init_batches(self) -> _BatchInfoType:
        counts = {}
        limits = {}
        active = {}
        batches = {}
        with ExitStack() as stack:
            for output in self.outputs:
                counts[output.name] = 0
                if output.limit is None:
                    limit = self.default_output_limit
                else:
                    limit = output.limit
                limits[output.name] = 0 if limit < 1 else limit
                active[output.name] = output
                batches[output.name] = stack.enter_context(
                    self.rfwriter_cls(
                        self.base_path / output.name,
                        output.name,
                        output.record_format or self.default_record_format,
                        self.max_per_file
                    )
                )
            return (counts, limits, active, batches)

    def run(self, records: Iterable[Record]) -> dict[str, RecordFileWriter]:
        """Runs the currently configured job on a records iterable.

        Args:
            records: An iterable of pymarc Record objects. Use a
                generator if data sets are large enough for memory to
                be a possible problem.

        Returns:
            A dict that maps output names to RecordFileWriter objects
            representing the batches used in this job.
        """
        total_records = 0
        verbose = self.log_every > 0
        counts, limits, active, batches = self._init_batches()
        with self.open_log() if verbose else nullcontext():
            for record in records:
                finished = []
                cached = RecordCache(record)
                for name, output in active.items():
                    if output.pipeline.check_record(cached):
                        batches[name].write(record)
                        counts[name] += 1
                        if limits[name] and counts[name] == limits[name]:
                            finished.append(name)
                total_records += 1

                for name in finished:
                    active.pop(name)
                    batches[name].close_active()
                if not active:
                    if verbose:
                        self.log('Reached limit for all output sets.')
                    break

                if verbose and total_records % self.log_every == 0:
                    self.log_state(total_records, counts.copy(), limits.copy())

            if verbose:
                self.log()
                self.log('*** FINAL SUMMARY ***')
                self.log_state(total_records, counts.copy(), limits.copy())
                self.log('Done.\n')
        return batches
