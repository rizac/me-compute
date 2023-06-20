import shutil
import os
from os.path import dirname, join, abspath, isdir, isfile, splitext, basename

import pandas as pd
import pytest
import yaml
from click.testing import CliRunner

from mecompute.run import cli, DOWNLOAD_CONFIG_PATH, _get_processing_output_dirname
from mecompute.stats import ParabolicScore2Weight, LinearScore2Weight

TEST_DATA_DIR = abspath(join(dirname(__file__), 'data'))
START = '2022-05-20T09:00:00'
END = '2022-05-20T13:00:00'

TEST_TMP_ROOT_DIR = abspath(join(TEST_DATA_DIR, 'tmp'))
TEST_DOWNLOAD_CONFIG_PATH = join(TEST_TMP_ROOT_DIR, 'download.yaml')
TEST_DB_FILE_PATH = join(TEST_TMP_ROOT_DIR, 'db.sqlite')
TEST_PROCESSING_HDF_PATH = join(TEST_TMP_ROOT_DIR,
                                'me-computed_2022-05-20T09:00:00_2022-05-20T13:00:00.hdf')


assert not isdir(TEST_TMP_ROOT_DIR), \
    f"{TEST_TMP_ROOT_DIR} should not exist, check the " \
    f"FileSystem and:\n1. Delete the dir manually, or " \
    f"\n2. Change TEST_TMP_DIR in {__file__}"


# Fixture that cleans up test dir
@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    """Cleanup a testing directory once we are finished."""
    # if isdir(TEST_TMP_DIR):  # remove for safety
    #     shutil.rmtree(TEST_TMP_DIR)
    os.makedirs(TEST_TMP_ROOT_DIR)

    # copy download config into tmp dir just created from the "standard"
    # download config path:
    with open(DOWNLOAD_CONFIG_PATH, 'r') as _:
        ret = yaml.safe_load(_)
    ret['start' if 'start' in ret else 'starttime'] = START
    ret['end' if 'end' in ret else 'endtime'] = END
    ret['dburl'] = 'sqlite:///' + TEST_DB_FILE_PATH
    ret['station'] = "RUS,JEM,PTNY"
    ret['data_url'] = ['iris']
    ret['channel'] = 'BHZ'
    with open(TEST_DOWNLOAD_CONFIG_PATH, 'w') as _:
        yaml.dump(ret, _)
    # copy DB (whose address is written in the download config just created)
    # into tmp dir (this db is necessary for test processing):
    shutil.copy(join(TEST_DATA_DIR, basename(TEST_DB_FILE_PATH)),
                TEST_DB_FILE_PATH)
    # copy the processing HDF (this file is needed to test the report):
    processing_out = join(TEST_DATA_DIR, basename(TEST_PROCESSING_HDF_PATH))
    shutil.copy(processing_out, TEST_PROCESSING_HDF_PATH)

    def remove_test_dir():
        # output_dir = join(TEST_TMP_ROOT_DIR, _get_processing_output_dirname(START, END))
        # for src_file in [
        #     join(output_dir, basename(output_dir) + '.hdf'),
        #     join(output_dir, basename(output_dir) + '.html'),
        #     TEST_DB_FILE_PATH
        # ]:
        #     dest_file = join(TEST_DATA_DIR, basename(src_file))
        #     if not isfile(dest_file):
        #         os.rename(src_file, dest_file)
        try:
            shutil.rmtree(TEST_TMP_ROOT_DIR)
        except Exception:
            pass
    request.addfinalizer(remove_test_dir)


def test_download():
    """test the download routine"""
    runner = CliRunner()
    result = runner.invoke(cli, ['download', '-c', TEST_DOWNLOAD_CONFIG_PATH])
    assert not result.exception
    assert isfile(TEST_DB_FILE_PATH)


@pytest.mark.parametrize('params', [
    ['-s', START, '-e', END],
    ['-d', 100*365]
])
def test_process(params):
    """test the processing routine"""
    runner = CliRunner()
    # We will infer the name of the directory we are about to create by checking which
    # new directory is created after the process finishes. To do so, we need to store
    # the content of OUTPUT_DIR beforehand:
    existing = set(os.listdir(TEST_TMP_ROOT_DIR))
    # test from today back 100 years: just a way to assure we process
    # all downloaded segments:
    result = runner.invoke(cli, ['process', '-d', 100*365,
                                 '-D', TEST_DOWNLOAD_CONFIG_PATH] + params +
                           [TEST_TMP_ROOT_DIR])

    assert not result.exception
    output_dir = set(os.listdir(TEST_TMP_ROOT_DIR)) - existing
    assert len(output_dir) == 1
    output_dir = join(TEST_TMP_ROOT_DIR, list(output_dir)[0])
    assert isdir(output_dir)
    hdf_file = None
    log_file = None
    for fname in os.listdir(output_dir):
        if splitext(fname)[1].lower() == '.hdf':
            hdf_file = join(output_dir, fname)
        elif splitext(fname)[1].lower() == '.log':
            log_file = join(output_dir, fname)

    assert log_file is not None
    assert hdf_file is not None

    d = pd.read_hdf(hdf_file)
    assert len(d) == 3  # noqa


def test_report():
    """test the report creation routine"""
    """test the processing routine"""
    runner = CliRunner()
    # We will infer the name of the directory we are about to create by checking which
    # new directory is created after the process finishes. To do so, we need to store
    # the content of OUTPUT_DIR beforehand:
    existing = set(os.listdir(TEST_TMP_ROOT_DIR))
    # test from today back 100 years: just a way to assure we process
    # all downloaded segments:
    result = runner.invoke(cli, ['report', TEST_PROCESSING_HDF_PATH])
    assert not result.exception
    html_file = set(os.listdir(TEST_TMP_ROOT_DIR)) - existing
    assert len(html_file) == 1
    html_file = list(html_file)[0]
    assert splitext(html_file)[1].lower() == '.html'
    # test that we wrote something:
    with open(join(TEST_TMP_ROOT_DIR, html_file), 'r') as _:
        assert len(_.read())

# @mock.patch('mecompute.run.s2s_process')
# def test_run(mock_process):
#     # dburl = yaml.safe_load(join(dirname(dirname(__file__))), 's2s_config',
#     #                        'download.private.yaml')['dburl']
#     rootdir = TMP_TEST_DATA_DIRS[0]
#     runner = CliRunner()
#     result = runner.invoke(cli, ['process', rootdir])
#     assert not result.exception
#     # check args:
#     kwargs = mock_process.call_args_list[0][1]
#     # check dburl is the url in download.private.yaml
#
#     with open(join(S2SCONFIG_DIR, 'download.private.yaml')) as _:
#         assert kwargs['dburl'] == yaml.safe_load(_)['dburl']
#     # check that python file is the file in our s2s config dir:
#     assert abspath(kwargs['pyfile']) == abspath(join(S2SCONFIG_DIR, 'process.py'))
#
#     # check that output files are all written in the same output directory
#     assert dirname(kwargs['config']) == dirname(kwargs['logfile']) == dirname(kwargs['outfile'])
#     # check that output directory end time is now:
#     _, start, end = basename(dirname(kwargs['config'])).split(ResultDir.FILENAME_SEPARATOR)
#     start, end = ResultDir.to_datetime(start), ResultDir.to_datetime(end)
#     # _, start, end = ResultDir.timebounds(basename(dirname(kwargs['config'])))
#
#     now = datetime.utcnow()
#     assert end.year == now.year and end.month == now.month and end.day == now.day
#     # check that output directory start time is now - 1 day, as in the config file:
#     then = now - timedelta(days=1)
#     assert start.year == then.year and start.month == then.month \
#            and start.day == then.day
#
#
# def test_run_real():
#     # dburl = yaml.safe_load(join(dirname(dirname(__file__))), 's2s_config',
#     #                        'download.private.yaml')['dburl']
#     rootdir = TMP_TEST_DATA_DIRS[0]
#     runner = CliRunner()
#
#     # take a single event that we inspected by issuing this:
#     # select events.time, events.id, count(segments.id)
#     # from events join segments on
#     # events.id = segments.event_id
#     # group by events.id
#     # order by events.time desc
#     #
#     # here a list of events (uncomment to choose your preferred, it should have a minimum
#     # of some segments to be tested for the HTML generation):
#     # first event: (uncomment ot use it) 331 segments, 2 processed
#     start, end = '2021-05-01T07:08:51', '2021-05-01T07:08:52'
#     # second event (~=800 events 5 processed, takes 5 minutes?):
#     # start, end = '2021-04-05T20:20:00', '2021-04-05T20:40:00'
#     # start, end = '2021-04-03 01:10:00', '2021-04-03 01:20:00'
#     result = runner.invoke(cli, ['process', rootdir, '-s', start, '-e', end])
#     assert not result.exception
#     # check that the produced hdf file exists
#     # (i.e. the method below doesn't return None):
#     assert ResultDir.get_resultfile_path(ResultDir(rootdir, start, end, mkdir=False))
#
#
# def test_report_fromreportdir():
#     rootdir = ME_TEST_DATA_ROOT
#     report_dir = 'mecomputed--2021-05-01T07:08:51--2021-05-01T07:08:52'
#     report_file = 'process--2021-05-01T07:08:51--2021-05-01T07:08:52'
#     report = join(rootdir, report_dir, report_file + '.html')
#     try:
#         if isfile(report):
#             os.remove(report)
#         assert not isfile(report)
#         runner = CliRunner()
#         result = runner.invoke(cli, ['report', rootdir])
#         assert not result.exception
#         mtime = os.stat(report).st_mtime
#         time.sleep(1)
#         result = runner.invoke(cli, ['report', rootdir])
#         assert not result.exception
#         assert os.stat(report).st_mtime == mtime
#         result = runner.invoke(cli, ['report', '-f', rootdir])
#         assert not result.exception
#         assert os.stat(report).st_mtime > mtime
#     finally:
#         pass
#         # if isfile(report):
#             # os.remove(report)


# def test_report_fromfile():
#     rootdir = dirname(TMP_TEST_DATA_DIRS[0])
#     for fname in ('process.result.singleevent.hdf', 'process.result.multievent.hdf'):
#         input = join(rootdir, fname)
#         output = input.replace('.hdf', '.html')
#
#         if isfile(output):
#             os.remove(output)
#         assert not isfile(output)
#
#         runner = CliRunner()
#
#         result = runner.invoke(cli, ['report', input])
#         assert not result.exception
#         assert isfile(output)


def test_weighter():
    """Tests scores to weight converters by assuring an array of increasing scores
    is converted in an array of non-increasing weights
    """
    import numpy as np
    scores = np.arange(0.4, 0.9, 0.1)
    for cls in (LinearScore2Weight, ParabolicScore2Weight):
        values = cls.convert(scores)
        assert (np.diff(values) <= 0).all()
