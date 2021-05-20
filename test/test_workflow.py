import shutil
import os
import time
from datetime import datetime, date, timedelta
from os.path import dirname, join, abspath, isdir, basename, isfile
from unittest import mock

import pytest
import yaml
from click.testing import CliRunner
from numpy.compat import os_PathLike

from cli import ResultDir, process, cli  #, convert, todatetime
from stats import ParabolicScore2Weight, LinearScore2Weight

TESTDATA_DIR = join(dirname(__file__), 'data')
S2SCONFIG_DIR = join(dirname(dirname(__file__)), 's2s_config')
TMP_TEST_DATA_DIRS = [join(TESTDATA_DIR, 'mecomputed-tmp')]
ME_TEST_DATA_ROOT = join(TESTDATA_DIR, 'mecomputed')


# Fixture that cleans up test dir
@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    """Cleanup a testing directory once we are finished."""
    def remove_test_dir():
        for _ in TMP_TEST_DATA_DIRS:
            try:
                shutil.rmtree(_)
            except Exception:
                pass
    request.addfinalizer(remove_test_dir)


def test_result_dir():
    root = TMP_TEST_DATA_DIRS[0]
    # provide two years way in the future so they cannot conflict with potential
    # directories created in other tests:
    r = ResultDir(root, '2219-07-01', '2221-12-31')
    r2 = ResultDir(root, '2219-07-01', '2221-12-31T00:00:00')
    r3 = ResultDir(root, '2219-07-01T00:00:00', '2221-12-31')
    r4 = ResultDir(root, '2219-07-01T00:00:00', '2221-12-31T00:00:00')
    assert r == r2 == r3 == r4

    assert not isdir(r)
    assert not ResultDir.get_resultfile_path(r)  # missing process file thereing

    r = ResultDir(root, '2219-07-01', '2221-12-31', mkdir=True)
    assert isdir(r)
    assert not ResultDir.get_resultfile_path(r)
    with open(join(r, r.filename_prefix + '.hdf'), 'w'):
        pass
    assert ResultDir.get_resultfile_path(r)

    # assert r.start == datetime(year=2019,month=7, day=1)
    # assert r.end == datetime(year=2221, month=12, day=31)

    with pytest.raises(Exception):  # June does not have 31 days:
        r = ResultDir(root, '2019-06-31', '2221-12-31')
    with pytest.raises(Exception):
        r = ResultDir('a', 'mecompute_2019-01-01_2221_07_01')
    with pytest.raises(Exception):
        r = ResultDir(root, '202e0-06-03')
    with pytest.raises(Exception):
        r = ResultDir('2020-06-03')
    # with pytest.raises(Exception):
    #     r = ResultDir('rmecompute_2020-06-03_2020-01-01')
    # with pytest.raises(Exception):
    #     r = ResultDir('rmecompute_2020-06-03_2020-01-01r')


@mock.patch('cli.s2s_process')
def test_run(mock_process):
    # dburl = yaml.safe_load(join(dirname(dirname(__file__))), 's2s_config',
    #                        'download.private.yaml')['dburl']
    rootdir = TMP_TEST_DATA_DIRS[0]
    runner = CliRunner()
    result = runner.invoke(cli, ['process', rootdir])
    assert not result.exception
    # check args:
    kwargs = mock_process.call_args_list[0][1]
    # check dburl is the url in download.private.yaml

    with open(join(S2SCONFIG_DIR, 'download.private.yaml')) as _:
        assert kwargs['dburl'] == yaml.safe_load(_)['dburl']
    # check that python file is the file in our s2s config dir:
    assert abspath(kwargs['pyfile']) == abspath(join(S2SCONFIG_DIR, 'process.py'))

    # check that output files are all written in the same output directory
    assert dirname(kwargs['config']) == dirname(kwargs['logfile']) == dirname(kwargs['outfile'])
    # check that output directory end time is now:
    _, start, end = basename(dirname(kwargs['config'])).split(ResultDir.FILENAME_SEPARATOR)
    start, end = ResultDir.to_datetime(start), ResultDir.to_datetime(end)
    # _, start, end = ResultDir.timebounds(basename(dirname(kwargs['config'])))

    now = datetime.utcnow()
    assert end.year == now.year and end.month == now.month and end.day == now.day
    # check that output directory start time is now - 7 days:
    then = now - timedelta(days=7)
    assert start.year == then.year and start.month == then.month \
           and start.day == then.day


def test_run_real():
    # dburl = yaml.safe_load(join(dirname(dirname(__file__))), 's2s_config',
    #                        'download.private.yaml')['dburl']
    rootdir = TMP_TEST_DATA_DIRS[0]
    runner = CliRunner()

    # take a single event that we inspected by issuing this:
    # select events.time, events.id, count(segments.id)
    # from events join segments on
    # events.id = segments.event_id
    # group by events.id
    # order by events.time desc
    #
    # here a list of events (uncomment to choose your preferred, it should have a minimum
    # of some segments to be tested for the HTML generation):
    # first event: (uncomment ot use it) 331 segments, 2 processed
    start, end = '2021-05-01T07:08:51', '2021-05-01T07:08:52'
    # second event (~=800 events 5 processed, takes 5 minutes?):
    # start, end = '2021-04-05T20:20:00', '2021-04-05T20:40:00'
    # start, end = '2021-04-03 01:10:00', '2021-04-03 01:20:00'
    result = runner.invoke(cli, ['process', rootdir, '-s', start, '-e', end])
    assert not result.exception
    # check that the produced hdf file exists
    # (i.e. the method below doesn't return None):
    assert ResultDir.get_resultfile_path(ResultDir(rootdir, start, end, mkdir=False))


def test_report_fromreportdir():
    rootdir = ME_TEST_DATA_ROOT
    report_dir = 'mecomputed--2021-05-01T07:08:51--2021-05-01T07:08:52'
    report_file = 'process--2021-05-01T07:08:51--2021-05-01T07:08:52'
    report = join(rootdir, report_dir, report_file + '.html')
    try:
        assert not isfile(report)
        runner = CliRunner()
        result = runner.invoke(cli, ['report', rootdir])
        assert not result.exception
        mtime = os.stat(report).st_mtime
        time.sleep(1)
        result = runner.invoke(cli, ['report', rootdir])
        assert not result.exception
        assert os.stat(report).st_mtime == mtime
        result = runner.invoke(cli, ['report', '-f', rootdir])
        assert not result.exception
        assert os.stat(report).st_mtime > mtime
    finally:
        if isfile(report):
            os.remove(report)


def test_report_fromfile():
    rootdir = dirname(TMP_TEST_DATA_DIRS[0])
    for fname in ('process.result.singleevent.hdf', 'process.result.multievent.hdf'):
        input = join(rootdir, fname)
        output = input.replace('.hdf', '.html')

        if isfile(output):
            os.remove(output)
        assert not isfile(output)

        runner = CliRunner()

        result = runner.invoke(cli, ['report', input])
        assert not result.exception
        assert isfile(output)


def test_weighter():
    """Tests scores to weight converters by assuring an array of increasing scores
    is converted in an array of non-increasing weights
    """
    import numpy as np
    scores = np.arange(0.4, 0.9, 0.1)
    for cls in (LinearScore2Weight, ParabolicScore2Weight):
        values = cls.convert(scores)
        assert (np.diff(values) <= 0).all()
