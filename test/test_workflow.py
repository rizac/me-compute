from os.path import dirname, join

from click.testing import CliRunner
from stream2segment.cli import process


def test_workflow():
    src = join(dirname(dirname(__file__)), 's2s_config')
    runner = CliRunner()
    result = runner.invoke(process(), [
        '-d', join(src, 'download.yaml').replace(),
        '-c', join(src, 'process.yaml'),
        '-p', join(src, 'process.py')
    ])