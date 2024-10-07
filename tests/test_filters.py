"""Contains tests for the 'filters' module."""
from collections.abc import Generator
import operator
from unittest.mock import call, Mock, patch

import pytest

from filtermarc import filters
from filtermarc.filters import RecordFilterPipeline
from filtermarc.localtypes import RecordFilter
from filtermarc.marc import RecordCache


# Fixtures and test data used only in this file

@pytest.fixture
def match_001() -> RecordFilter:
    """Fixture: returns a function for making dummy RecordFilters.
    
    Note that the filters that this makes are also Mock objects so that
    calls made to them etc. can be tracked and tested.
    """
    def _match_001(op, test_value):
        def _filter(fields):
            for field in fields.cache.get('001'):
                if op(int(field.value()), test_value):
                    return True
            return False
        return Mock(side_effect=_filter)
    return _match_001


# Tests

def test_recordfilterpipeline_init(match_001):
    my_filters = [
        match_001(operator.ge, 2),
        match_001(operator.le, 10)
    ]
    pipeline = RecordFilterPipeline(*my_filters)
    assert list(pipeline.filters) == my_filters


@pytest.mark.parametrize('args1, args2', [
    ([], [(operator.ge, 2), (operator.le, 10)]),
    ([(operator.ge, 2)], [(operator.le, 10)]),
    ([(operator.ge, 2), (operator.le, 10)], []),
])
def test_recordfilterpipeline_add(args1, args2, match_001):
    filters1 = [match_001(*args) for args in args1]
    filters2 = [match_001(*args) for args in args2]

    # Adding filters to an existing pipeline should copy the pipeline
    # and add the new filters to the copy, allowing you to create base
    # pipelines and build multiple pipelines from them.
    pipeline1 = RecordFilterPipeline(*filters1)
    pipeline2 = pipeline1.add(*filters2)
    assert list(pipeline1.filters) == filters1
    assert list(pipeline2.filters) == filters1 + filters2


def test_recordfilterpipeline_checkrecord(make_marc_records, match_001):
    records = make_marc_records(50)
    my_filters = [
        match_001(operator.ge, 5),
        match_001(operator.lt, 15)
    ]
    pipeline = RecordFilterPipeline(*my_filters)
    result = [pipeline.check_record(rec) for rec in records]

    # The actual results should match what's expected based on the 001
    # values and the given filter checks.
    assert result == ([False] * 5) + ([True] * 10) + ([False] * 35)

    # Each filter function should have been called with an appropriate
    # RecordCache for each record.
    exp = [RecordCache(rec).cache for rec in records]
    # The first filter should have been called for ALL records.
    assert [c.args[0].cache for c in my_filters[0].mock_calls] == exp

    # The second filter should have been skipped for the first 5
    # records. (The non-match for the first filter should return False
    # without even trying the second filter.)
    assert [c.args[0].cache for c in my_filters[1].mock_calls] == exp[5:]


def test_recordfilterpipeline_run(make_marc_records, match_001):
    records = make_marc_records(10)
    pipeline = RecordFilterPipeline(match_001(operator.ge, 5))
    assert pipeline.run(records) == records[5:]
    with patch.object(pipeline, 'check_record') as mock_check:
        pipeline.run(records)

    # For some reason mock_check ends up with a 'call().__bool()' after
    # each expected call, which we need to filter out.
    calls = [c for c in mock_check.mock_calls if c != call().__bool__()]
    assert calls == [call(rec) for rec in records]


def test_recordfilterpipeline_rungenerator(make_marc_records, match_001):
    records = make_marc_records(10)
    pipeline = RecordFilterPipeline(match_001(operator.ge, 5))
    rgen = pipeline.run_generator(records)
    assert isinstance(rgen, Generator)
    assert list(rgen) == records[5:]
    with patch.object(pipeline, 'check_record') as mock_check:
        list(pipeline.run_generator(records))

    # For some reason mock_check ends up with a 'call().__bool()' after
    # each expected call, which we need to filter out.
    calls = [c for c in mock_check.mock_calls if c != call().__bool__()]
    assert calls == [call(rec) for rec in records]


def test_recordfilterpipeline_intersect(make_marc_records, match_001):
    records = make_marc_records(50)
    ge5 = match_001(operator.ge, 5)
    lt15 = match_001(operator.lt, 15)
    pl_ge5 = RecordFilterPipeline(ge5)
    pl_lt15 = RecordFilterPipeline(lt15)

    # Using the 'intersect' method is the same as passing all filters
    # for both pipelines to a new pipeline. Which pipeline is
    # intersected with which doesn't matter.
    pl_ge5lt15 = pl_ge5.intersect(pl_lt15)
    pl_lt15ge5 = pl_lt15.intersect(pl_ge5)
    assert list(pl_ge5.filters) == [ge5]
    assert list(pl_lt15.filters) == [lt15]
    assert list(pl_ge5lt15.filters) == [ge5, lt15]
    assert list(pl_lt15ge5.filters) == [lt15, ge5]
    assert pl_ge5lt15.run(records) == records[5:15]
    assert pl_lt15ge5.run(records) == records[5:15]


def test_recordfilterpipeline_intersect_same(make_marc_records, match_001):
    records = make_marc_records(50)
    ge5 = match_001(operator.ge, 5)
    pl_ge5 = RecordFilterPipeline(ge5)

    # Using 'intersect' on the same pipeline should essentially just
    # return a copy of that pipeline.
    pl_same = pl_ge5.intersect(pl_ge5)
    assert list(pl_same.filters) == [ge5]
    assert pl_same.run(records) == records[5:]


def test_recordfilterpipeline_union(make_marc_records, match_001):
    records = make_marc_records(50)
    lt5 = match_001(operator.lt, 5)
    gt30 = match_001(operator.gt, 30)
    pl_lt5 = RecordFilterPipeline(lt5)
    pl_gt30 = RecordFilterPipeline(gt30)

    # Using the 'union' method creates a new pipeline that matches
    # either of the component pipelines. The filter the new pipeline
    # uses is a special internal '_union_filter' that calls
    # 'check_record' on each of the component pipelines. Which pipeline
    # is 'union'ed with which doesn't matter.
    pl_lt5_or_gt30 = pl_lt5.union(pl_gt30)
    pl_gt30_or_lt5 = pl_gt30.union(pl_lt5)
    assert pl_lt5_or_gt30.run(records) == records[:5] + records[31:]
    assert pl_gt30_or_lt5.run(records) == records[:5] + records[31:]


def test_recordfilterpipeline_union_same(make_marc_records, match_001):
    records = make_marc_records(50)
    lt5 = match_001(operator.lt, 5)
    pl_lt5 = RecordFilterPipeline(lt5)

    # Using 'union' on the same pipeline should essentially just return
    # a copy of that pipeline.
    pl_same = pl_lt5.union(pl_lt5)
    assert pl_same.filters == pl_lt5.filters
    assert pl_same.run(records) == records[:5]


# Below are tests that test the various filter factory functions in
# the 'filters' module.
# TODO: Add tests to test filter factories more thoroughly.


def test_filters_bycharacterposition(make_marc_records):
    records = make_marc_records(1, {'008': [[(35, 'eng')]]})
    records += make_marc_records(1, {'008': [[(35, 'fre')]]})
    records += make_marc_records(1, {'008': [[(35, 'ger')]]})
    rcaches = [RecordCache(rec) for rec in records]
    f008_fre = filters.by_character_position('008', (35, 37), 'fre')
    assert [f008_fre(rcache) for rcache in rcaches] == [False, True, False]


def test_filters_byfieldexists(make_marc_records):
    records = make_marc_records(1, {'520': ['##$aTest Summary']})
    records += make_marc_records(1, {'520': ['##$aAnother Summary']})
    records += make_marc_records(1, {'520': ['##$a']})
    records += make_marc_records(1)
    records += make_marc_records(1, {'505': ['##$aSome TOC']})
    rcaches = [RecordCache(rec) for rec in records]
    f505_520_exists = filters.by_field_exists(['505', '520'])
    expected = [True, True, False, False, True]
    assert [f505_520_exists(rcache) for rcache in rcaches] == expected
