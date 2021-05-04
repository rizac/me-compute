from datetime import datetime, date, timedelta
from os.path import dirname, join, abspath, isdir, basename
from unittest import mock

import pytest
import yaml
from click.testing import CliRunner
from stream2segment.cli import process


# def test_workflow():
#     src = join(dirname(dirname(__file__)), 's2s_config')
#     runner = CliRunner()
#     result = runner.invoke(process(), [
#         '-d', join(src, 'download.yaml').replace(),
#         '-c', join(src, 'process.yaml'),
#         '-p', join(src, 'process.py')
#     ])
from run import ResultDir, process, cli, convert, todatetime


TESTDATA_DIR = join(dirname(__file__), 'data')
S2SCONFIG_DIR = join(dirname(dirname(__file__)), 's2s_config')


def test_todatetime():
    dt = datetime.utcnow().replace(microsecond=345654)  # assure microseconds not 0
    assert todatetime(dt) is dt
    assert todatetime(dt.isoformat()) == dt
    dt = dt.replace(microsecond=0)
    assert todatetime(dt.isoformat()) == dt
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    assert todatetime(dt.isoformat()) == dt


@pytest.mark.parametrize('object', [
    datetime.utcnow(),
    datetime(1511, 3, 3, 5, 6, 4, 45676),
    datetime(2311, 1, 31),
    date.today()
])
def test_convert(object):
    conv = convert(object, to=date if type(object) == datetime else datetime)
    for att in ['year', 'month', 'day']:
        assert getattr(object, att) == getattr(conv, att)
    if type(object) == date:
        for att in ['hour', 'minute', 'second', 'microsecond']:
            assert getattr(conv, att) == 0
        assert conv.isoformat().startswith(object.isoformat())
        assert convert(object, to=date) is object
    else:
        assert object.isoformat().startswith(conv.isoformat())
        assert convert(object, to=datetime) is object


def test_result_dir():
    r = ResultDir(TESTDATA_DIR, 'mecomputed', 'mecomputed_2019-07-01_2221-12-31')
    assert r.start == datetime(year=2019,month=7, day=1)
    assert r.end == datetime(year=2221, month=12, day=31)

    with pytest.raises(Exception):  # June does not have 31 days:
        r = ResultDir(TESTDATA_DIR, 'mecomputed', 'mecomputed_2019-06-31_2221-12-31')
    with pytest.raises(Exception):  # dirname ok, but HDF/CSV file does not exist in dir:
        r = ResultDir('a', 'mecompute_2019-01-01_2221-07-01')
    with pytest.raises(Exception):
        r = ResultDir('a', 'mecompute_2019-01-01_2221_07_01')
    with pytest.raises(Exception):
        r = ResultDir('rmecompute_2020-06-03')
    with pytest.raises(Exception):
        r = ResultDir('rmecompute_202e0-06-03')
    with pytest.raises(Exception):
        r = ResultDir('rmecompute_2020-06-03')
    with pytest.raises(Exception):
        r = ResultDir('rmecompute_2020-06-03_2020-01-01')
    with pytest.raises(Exception):
        r = ResultDir('rmecompute_2020-06-03_2020-01-01r')


@mock.patch('run.s2s_process')
def test_run(mock_process):
    # dburl = yaml.safe_load(join(dirname(dirname(__file__))), 's2s_config',
    #                        'download.private.yaml')['dburl']
    rootdir = join(dirname(__file__), 'data', 'mecomputed', 'test')
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
    # checkthat output directory end time is now:
    start, end = ResultDir.timebounds(basename(dirname(kwargs['config'])))
    now = datetime.utcnow()
    assert end.year == now.year and end.month == now.month and end.day == now.day
    # check that output directory start time is now - 7 days:
    then = now - timedelta(days=7)
    assert start.year == then.year and start.month == then.month \
           and start.day == then.day


def test_run_real():
    # dburl = yaml.safe_load(join(dirname(dirname(__file__))), 's2s_config',
    #                        'download.private.yaml')['dburl']
    rootdir = join(dirname(__file__), 'data', 'mecomputed', 'test')
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
    # start, end = '2021-05-01T07:08:51', '2021-05-01T07:08:52'
    # second event ():
    # start, end = '2021-04-05T20:20:00', '2021-04-05T20:40:00'
    start, end = '2021-04-03 01:10:00', '2021-04-03 01:20:00'
    result = runner.invoke(cli, ['process', rootdir, '-s', start, '-e', end])
    assert not result.exception
    # check args:
    # kwargs = mock_process.call_args_list[0][1]
    # # check dburl is the url in download.private.yaml
    #
    # with open(join(S2SCONFIG_DIR, 'download.private.yaml')) as _:
    #     assert kwargs['dburl'] == yaml.safe_load(_)['dburl']
    # # check that python file is the file in our s2s config dir:
    # assert abspath(kwargs['pyfile']) == abspath(join(S2SCONFIG_DIR, 'process.py'))
    #
    # # check that output files are all written in the same output directory
    # assert dirname(kwargs['config']) == dirname(kwargs['logfile']) == dirname(kwargs['outfile'])
    # # checkthat output directory end time is now:
    # start, end = ResultDir.timebounds(basename(dirname(kwargs['config'])))
    # now = datetime.utcnow()
    # assert end.year == now.year and end.month == now.month and end.day == now.day
    # # check that output directory start time is now - 7 days:
    # then = now - timedelta(days=7)
    # assert start.year == then.year and start.month == then.month \
    #        and start.day == then.day