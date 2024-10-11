"""Contains tests for 'out' module."""
from pathlib import Path
import sys
from unittest.mock import call, DEFAULT, Mock, patch

from pymarc import Record
import pytest

from filtermarc.filters import RecordFilterPipeline
from filtermarc.formats import RecordFormat, Marc
from filtermarc.marc import RecordCache
from filtermarc.out import RecordFileWriter, Output, Job


# Fixtures and test data used only in this file

@pytest.fixture
def dummy_format():
    """Fixture: Returns a dummy RecordFormat instance for testing."""
    class DummyFormat(RecordFormat):
        file_extension = '.dum'
        header = b'<START>\n'
        footer = b'\n<END>'
        multi_prefix = b'<MULTI>\n'
        multi_suffix = b'<END MULTI>'
        multi_separator = b'<SEP>\n'

        def __call__(self, record: Record) -> bytes:
            return b''.join([
                b'Data for record ',
                bytes(record.get_fields('001')[0].value(), encoding='utf8'),
                b'.\n'
            ])

    return DummyFormat()


@pytest.fixture
def dummy_outputs(dummy_format):
    """Fixture: Returns a series of dummy Outputs for testing."""
    def _make_mock_rfp(match_range):
        rfp = RecordFilterPipeline()
        rfp.check_record = Mock(
            side_effect=lambda rc: int(rc.cache['001'][0].data) in match_range
        )
        return rfp

    def _make_outputs():
        return [
            Output('f001_lt5', _make_mock_rfp(range(0, 5)), dummy_format),
            Output('f001_gt10', _make_mock_rfp(range(11, 1000)), dummy_format)
        ]

    return _make_outputs()


@pytest.fixture
def dummy_rfwriter_cls():
    """Fixture: Returns a dummy RecordFileWriter class, for testing."""
    class DummyRecordFileWriter(RecordFileWriter):

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.write_calls = []

        def write(self, record):
            self.write_calls.append(record)
            return super().write(record)

    return DummyRecordFileWriter


# Tests

def test_recordfilewriter_init_default_state(dummy_format, tmp_path):
    rfw = RecordFileWriter(str(tmp_path), 'testfile', dummy_format)
    assert rfw.dirpath == tmp_path
    assert rfw.basefilename == 'testfile'
    assert rfw.record_format == dummy_format
    assert rfw.max_per_file == 0
    assert rfw.active_file_count == 0
    assert rfw.active_record_count == 0
    assert rfw.active_fh is None


@pytest.mark.parametrize('mpf, exp_mpf, exp_multi', [
    (1, 1, False),
    (10, 10, True),
    (0, 0, True),
    (-1, 0, True),
    (-10, 0, True)
])
def test_recordfilewriter_init_multi(mpf, exp_mpf, exp_multi, dummy_format):
    rfw = RecordFileWriter('/home/test', 'testfile', dummy_format, mpf)
    # The 'multi' and 'max_per_file' (mpf) properties are set based on
    # the provided 'max_per_file' value.
    assert rfw.max_per_file == exp_mpf
    assert rfw.multi == exp_multi


@pytest.mark.parametrize('bfname, nth, exp_fname', [
    ('testfile', 0, 'testfile-0000.dum'),
    ('data', 100, 'data-0100.dum')
])
def test_recordfilewriter_pathtonthfile(bfname, nth, exp_fname, dummy_format,
                                        tmp_path):
    rfw = RecordFileWriter(tmp_path, bfname, dummy_format)
    assert rfw.path_to_nth_file(nth) == tmp_path / Path(exp_fname)


@pytest.mark.parametrize('max_per_file', [1, 10])
def test_recordfilewriter_opennext(max_per_file, dummy_format, tmp_path):
    rfw = RecordFileWriter(tmp_path, 'testfile', dummy_format, max_per_file)
    # The 'open_next' method should always:
    # - Close the currently open file (if any).
    # - Increment the active_file_count.
    # - Open a new file, named using the new active_file_count.
    # - Write the record_format.header.
    # - Write the record_format.multi_prefix, for multi-record files.
    start_fh = Mock()
    rfw.active_fh = start_fh
    rfw.active_file_count = 2

    with patch.object(rfw, 'close_active') as mock_close:
        new_fh = rfw.open_next()
        new_fh.write(b'test')
        new_fh.close()

    mock_close.assert_called_once()
    assert start_fh != new_fh
    assert new_fh == rfw.active_fh
    assert not (tmp_path / Path('testfile-0002.dum')).exists()
    assert rfw.active_file_count == 3

    with (tmp_path / Path('testfile-0003.dum')).open('rb') as read_fh:
        file_contents = read_fh.read()

    if rfw.multi:
        assert file_contents == b''.join([
            dummy_format.header, dummy_format.multi_prefix, b'test'
        ])
    else:
        assert file_contents == b''.join([dummy_format.header, b'test'])


@pytest.mark.parametrize('max_per_file', [1, 10])
def test_recordfilewriter_closeactive(max_per_file, dummy_format, tmp_path):
    rfw = RecordFileWriter(tmp_path, 'testfile', dummy_format, max_per_file)
    # The 'close_active' method should always:
    # - Set the active_record_count to 0.
    # - Write the record_format.multi_suffix, for multi-record files.
    # - Write the record_format.footer.
    # - Close the current active_fh.
    # - Clear the current active_fh property.
    rfw.active_record_count = max_per_file
    write_fh = (tmp_path / Path('testfile-0001.dum')).open('wb')
    rfw.active_fh = write_fh
    rfw.active_fh.write(b'test')
    rfw.close_active()

    assert write_fh.closed
    assert rfw.active_fh is None
    assert rfw.active_record_count == 0

    with (tmp_path / Path('testfile-0001.dum')).open('rb') as read_fh:
        file_contents = read_fh.read()

    if rfw.multi:
        assert file_contents == b''.join([
            b'test', dummy_format.multi_suffix, dummy_format.footer
        ])
    else:
        assert file_contents == b''.join([b'test', dummy_format.footer])


def test_recordfilewriter_closeactive_nothing(dummy_format, tmp_path):
    rfw = RecordFileWriter(tmp_path, 'testfile', dummy_format)
    # Calling 'close_active' when no active file is open does nothing
    # and does not raise an error.
    start_state = (rfw.active_record_count, rfw.active_fh)
    rfw.close_active()
    end_state = (rfw.active_record_count, rfw.active_fh)
    assert start_state == end_state


def test_recordfilewriter_write_1st(make_marc_records, dummy_format, tmp_path):
    rfw = RecordFileWriter(tmp_path, 'testfile', dummy_format, 0)
    record = make_marc_records(1)[0]
    # Writing the first record to a file should always:
    # - Open the next file for writing.
    # - Write the record.
    # - Increment the active_record_count.
    # - Leave the file open.
    with patch.object(rfw, 'open_next') as mock_open:
        mock_fh = Mock()
        mock_open.return_value = mock_fh
        with patch.object(rfw, 'close_active') as mock_close:
            rfw.write(record)

    mock_open.assert_called_once()
    mock_close.assert_not_called()
    mock_fh.write.assert_called_with(b'Data for record 0.\n')
    assert rfw.active_record_count == 1


def test_recordfilewriter_write_lst(make_marc_records, dummy_format, tmp_path):
    rfw = RecordFileWriter(tmp_path, 'testfile', dummy_format, 2)
    record = make_marc_records(1)[0]
    # Writing the last record to a file should always:
    # - Write the record.
    # - Increment the active_record_count.
    # - Close the active file.
    rfw.active_record_count = 1
    mock_fh = Mock()
    rfw.active_fh = mock_fh
    with patch.object(rfw, 'open_next') as mock_open:
        with patch.object(rfw, 'close_active') as mock_close:
            rfw.write(record)

    mock_open.assert_not_called()
    mock_close.assert_called_once()
    mock_fh.write.assert_called_with(b'Data for record 0.\n')
    assert rfw.active_record_count == 2


def test_recordfilewriter_write_nth(make_marc_records, dummy_format, tmp_path):
    rfw = RecordFileWriter(tmp_path, 'testfile', dummy_format, 0)
    record = make_marc_records(1)[0]
    # Writing a record that isn't the first or last to a multi-record
    # file should always:
    # - Write the record_format.multi_separator.
    # - Write the record.
    # - Increment the active_record_count.
    rfw.active_record_count = 10
    mock_fh = Mock()
    rfw.active_fh = mock_fh
    with patch.object(rfw, 'open_next') as mock_open:
        with patch.object(rfw, 'close_active') as mock_close:
            rfw.write(record)

    mock_open.assert_not_called()
    mock_close.assert_not_called()
    mock_fh.write.assert_has_calls(
        [call(dummy_format.multi_separator)],
        [call(b'Data for record 0.\n')]
    )
    assert rfw.active_record_count == 11


def test_recordfilewriter_as_context_manager(make_marc_records, dummy_format,
                                             tmp_path):
    # This is more of an integration test -- but, it tests/demonstrates
    # using RecordFileWriter as a context manager, which is how it's
    # intended to be used.
    records = make_marc_records(50)
    with RecordFileWriter(tmp_path, 'testfile', dummy_format, 10) as rfw:
        for record in records:
            rfw.write(record)

    for fn in range(1, 6):
        with (tmp_path / Path(f'testfile-{fn:04}.dum')).open('rb') as read_fh:
            content = read_fh.read()
        assert content == '\n'.join([
            '<START>',
            '<MULTI>',
            '\n<SEP>\n'.join([
                f'Data for record {rn + (fn - 1) * 10}.' for rn in range(10)
            ]),
            '<END MULTI>',
            '<END>'
        ]).encode('utf8')

    assert not (tmp_path / Path('testfile-0006.dum')).exists()
    assert rfw.active_fh is None


def test_output_init_default_state():
    pl = RecordFilterPipeline()
    output = Output('my_record_set', pl)
    assert output.name == 'my_record_set'
    assert output.pipeline == pl
    assert output.record_format is None
    assert output.limit is None


def test_job_init_default_state(dummy_outputs):
    job = Job(dummy_outputs, '/home/test')
    assert job.outputs == dummy_outputs
    assert job.base_path == Path('/home/test')
    assert job.log_every == 10000
    assert job.max_per_file == 0
    assert isinstance(job.default_record_format, Marc)
    assert job.default_output_limit == 100000
    assert job.log_path is None
    assert job.log_every == 10000
    assert job.log_fh is None


def test_job_openlog_stdout():
    # The default job.log_path of None forces the job to log to stdout.
    job = Job([], '')
    with job.open_log() as log_fh:
        assert log_fh == sys.stdout
        assert job.log_fh == log_fh
    assert job.log_fh == sys.stdout
    assert not job.log_fh.closed


def test_job_openlog_file(tmp_path):
    # Setting the job's log_path to a valid filepath logs to that file.
    job = Job([], '', log_path=(tmp_path / 'testlog.txt'))
    with job.open_log() as log_fh:
        log_fh.write('Testing123')
        assert job.log_fh == log_fh
    assert job.log_fh.closed
    with (tmp_path / 'testlog.txt').open('r') as read_fh:
        contents = read_fh.read()
    assert contents == 'Testing123'


def test_job_log():
    # The 'log' Job method prints to the Job's 'log_fh'.
    job = Job([], '')
    job.log_fh = Mock()
    job.log('Testing123')
    job.log_fh.write.assert_has_calls([call('Testing123'), call('\n')])


def test_job_logstate(dummy_outputs):
    # The 'log_state' Job method should log a summary of the current
    # state, using the 'log' method.
    job = Job(dummy_outputs, '')
    with patch.object(job, 'log') as mock_log:
        job.log_state(
            100,
            {'f001_lt5': 10, 'f001_gt10': 23},
            {'f001_lt5': 10, 'f001_gt10': 0}
        )
    mock_log.assert_has_calls([
        call('=== 100 Processed ==='),
        call('10 "f001_lt5" records found (max 10)'),
        call('23 "f001_gt10" records found'),
    ])


@pytest.mark.parametrize('def_limit, limits, exp_limits', [
    # No default / output limit ==> Output limit
    (0, [5, 5], [5, 5]),
    # Default / None output limit ==> Default
    (5, [None, None], [5, 5]),
    # No default / None output limit ==> No limit
    (-1, [None, None], [0, 0]),
    # Default / no output limit ==> No limit
    (5, [0, -1], [0, 0]),
])
def test_job_run_limits(def_limit, limits, exp_limits, make_marc_records,
                        dummy_outputs, dummy_format, dummy_rfwriter_cls):
    records = make_marc_records(20)
    dummy_outputs[0].limit = limits[0]
    dummy_outputs[1].limit = limits[1]
    job = Job(
        dummy_outputs, '/test', log_every=0, default_output_limit=def_limit
    )
    with patch.object(dummy_rfwriter_cls, 'open_next'):
        job_patches = {'log': DEFAULT, 'log_state': DEFAULT}
        with patch.multiple(job, **job_patches):
            job.rfwriter_cls = dummy_rfwriter_cls
            batches = job.run((rec for rec in records))

    # Make sure the expected records were matched and written, for each
    # output -- depending on the expected limit.
    matched_slices = [
        slice(0, (exp_limits[0] if exp_limits[0] < 5 else 5) or 5),
        slice(11, (11 + ((exp_limits[1] if exp_limits[1] < 9 else 9) or 9)))
    ]
    assert batches['f001_lt5'].write_calls == records[matched_slices[0]]
    assert batches['f001_gt10'].write_calls == records[matched_slices[1]]

    # Make sure records up to the expected limit (and only up to the
    # expected limit) were checked. If the limit is greater than the
    # number of matching records in the batch, then all should have
    # been checked.
    checked_slices = [
        slice(0, (exp_limits[0] if exp_limits[0] <= 5 else 20) or 20),
        slice(0, (11 + ((exp_limits[1] if exp_limits[1] <= 11 else 20) or 20)))
    ]
    for slice_, output in zip(checked_slices, job.outputs):
        mcalls = output.pipeline.check_record.mock_calls
        exp_records = records[slice_]
        assert len(mcalls) == len(exp_records)
        assert all([
            mc.args[0].cache == RecordCache(rec).cache for mc, rec in zip(
                mcalls, exp_records
            )
        ])


@pytest.mark.parametrize('use_default_fmt', [True, False])
def test_job_run_batch_params(use_default_fmt, make_marc_records,
                              dummy_outputs, dummy_format, dummy_rfwriter_cls):
    records = make_marc_records(20)
    if use_default_fmt:
        dummy_outputs[0].record_format = None
        dummy_outputs[1].record_format = None
        default_fmt = dummy_format
    else:
        default_fmt = None
    job = Job(
        dummy_outputs, '/test', log_every=0, max_per_file=5,
        default_record_format=default_fmt
    )
    with patch.object(dummy_rfwriter_cls, 'open_next'):
        job_patches = {'log': DEFAULT, 'log_state': DEFAULT}
        with patch.multiple(job, **job_patches):
            job.rfwriter_cls = dummy_rfwriter_cls
            batches = job.run((rec for rec in records))

    for name, batch in batches.items():
        assert batch.dirpath == Path('/test') / Path(name)
        assert batch.basefilename == name
        assert batch.record_format == dummy_format
        assert batch.max_per_file == 5


@pytest.mark.parametrize(
    'limits, log_every, exp_log_limit, exp_log_summary, exp_logst_args', [
        ([0, 0], 0, False, False, []),
        ([0, 0], -1, False, False, []),
        ([0, 0], 5, False, True, [
            (5, (5, 0)),
            (10, (5, 0)),
            (15, (5, 4)),
            (20, (5, 9)),
            (20, (5, 9))
        ]),
        ([0, 0], 6, False, True, [
            (6, (5, 0)),
            (12, (5, 1)),
            (18, (5, 7)),
            (20, (5, 9))
        ]),
        ([3, 3], 3, True, True, [
            (3, (3, 0)),
            (6, (3, 0)),
            (9, (3, 0)),
            (12, (3, 1)),
            (14, (3, 3))
        ]),
        ([3, 3], 25, True, True, [
            (14, (3, 3))
        ])
    ])
def test_job_run_logging(limits, log_every, exp_log_limit, exp_log_summary,
                         exp_logst_args, make_marc_records, dummy_outputs,
                         dummy_format, dummy_rfwriter_cls):
    records = make_marc_records(20)
    dummy_outputs[0].limit = limits[0]
    dummy_outputs[1].limit = limits[1]
    job = Job(dummy_outputs, '/test', log_every=log_every)
    with patch.object(dummy_rfwriter_cls, 'open_next'):
        job_patches = {'log': DEFAULT, 'log_state': DEFAULT}
        with patch.multiple(job, **job_patches) as job_mocks:
            job.rfwriter_cls = dummy_rfwriter_cls
            job.run((rec for rec in records))

    # Log output should reflect the given parameters.
    exp_log_calls = []
    if exp_log_limit:
        exp_log_calls.append(call('Reached limit for all output sets.'))
    if exp_log_summary:
        exp_log_calls.extend([
            call(),
            call('*** FINAL SUMMARY ***'),
            call('Done.\n')
        ])
    job_mocks['log'].assert_has_calls(exp_log_calls)
    job_mocks['log_state'].assert_has_calls([
        call(
            args[0],
            {'f001_lt5': args[1][0], 'f001_gt10': args[1][1]},
            {'f001_lt5': limits[0], 'f001_gt10': limits[1]}
        ) for args in exp_logst_args
    ])
