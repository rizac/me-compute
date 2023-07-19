import os
from datetime import datetime, timedelta

from os.path import dirname, join, abspath, isfile

import pandas as pd
import pytest
from click.testing import CliRunner

from mecompute.run import cli
from mecompute.event_me import ParabolicScore2Weight, LinearScore2Weight
from unittest.mock import patch

TEST_DATA_DIR = abspath(join(dirname(__file__), 'data'))
TEST_DOWNLOAD_CONFIG_PATH = join(TEST_DATA_DIR, 'download.yaml')
TEST_DB_FILE_PATH = join(TEST_DATA_DIR, 'db.sqlite')
# output dir where to store test results: the dir is git-ignored but not deleted so
# that some data (e.g. html report) can be visually inspected if needed:
TEST_TMP_ROOT_DIR = abspath(join(TEST_DATA_DIR, 'tmp'))


@patch('mecompute.run.process')
def test_proc_ess_params(mock_process, capsys):
    runner = CliRunner()
    now = datetime.utcnow().replace(microsecond=0,hour=0, minute=0, second=0)
    days = 365
    result = runner.invoke(cli, ['-f',
                                 '-d', TEST_DOWNLOAD_CONFIG_PATH, '-t', str(days),
                                 TEST_TMP_ROOT_DIR])
    assert mock_process.called
    start, end = mock_process.call_args[0][1], mock_process.call_args[0][2]
    assert now == datetime.fromisoformat(end)
    assert datetime.fromisoformat(end) - datetime.fromisoformat(start) == \
           timedelta(days=days)


@patch('mecompute.run.process')
def test_proc_no_time_bounds(mock_process, capsys):
    runner = CliRunner()
    now = datetime.utcnow().replace(microsecond=0,hour=0, minute=0, second=0)
    days = 365
    result = runner.invoke(cli, ['-f',
                                 '-d', TEST_DOWNLOAD_CONFIG_PATH,
                                 TEST_TMP_ROOT_DIR])
    assert not mock_process.called
    assert 'no time bounds specified' in result.output.lower()
    assert result.exit_code != 0


@pytest.mark.parametrize('params', [
    ['-s', '2022-05-20T09:00:00', '-e', '2022-05-20T13:00:00'],  # process all db events
])
def test_process(params, capsys):
    """test the processing routine"""
    runner = CliRunner()
    base_name = 'energy-magnitude'

    s_hdf = join(TEST_TMP_ROOT_DIR, 'station-' + base_name + '.hdf')
    s_log = join(TEST_TMP_ROOT_DIR, 'station-' + base_name + '.log')
    csv = join(TEST_TMP_ROOT_DIR, base_name + '.csv')
    log = join(TEST_TMP_ROOT_DIR, base_name + '.log')
    html = join(TEST_TMP_ROOT_DIR, base_name + '.html')
    xml = join(TEST_TMP_ROOT_DIR, 'gfz2022juqz.xml')
    # get modification times to check we overwrote those files
    # (if files do not exist, set yesterday as modification time):
    m_times = {
        f: -1. if not isfile(f) else os.stat(f).st_mtime
        for f in [s_hdf, s_log, csv, log, html, xml]
    }

    result = runner.invoke(cli, ['-f',
                                 '-d', TEST_DOWNLOAD_CONFIG_PATH] + params +
                           [TEST_TMP_ROOT_DIR])

    if result.exception:
        import traceback
        traceback.print_tb(result.exc_info[2])

    assert not result.exception
    for f, t in m_times.items():
        assert os.stat(f).st_mtime > t

    d = pd.read_hdf(s_hdf)
    assert len(d) == 3  # noqa

    d = pd.read_csv(csv)
    assert len(d) == 1  # noqa


def test_weighter():
    """Tests scores to weight converters by assuring an array of increasing scores
    is converted in an array of non-increasing weights. Weights usage is legacy code
    not used anymore but still working
    """
    import numpy as np
    scores = np.arange(0.4, 0.9, 0.1)
    for cls in (LinearScore2Weight, ParabolicScore2Weight):
        values = cls.convert(scores)
        assert (np.diff(values) <= 0).all()
