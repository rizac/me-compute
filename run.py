"""Medaily script"""
import json
import sys
from datetime import datetime, timedelta
from os.path import join, dirname, abspath, splitext, basename, isabs, isfile, isdir, \
    relpath

import click

from sqlalchemy.sql.expression import func

import yaml
from stream2segment.io.db.models import Segment, Event
from stream2segment.main import download, process
from stream2segment.process.db import get_session


# this script is supposed to be run from a directory structure like this:
# <ROOT>
#   +- stream2segment -> stream2segment (s2s) package: download + process
#   +- processing -> processing files (YAML+Python file) for s2s
#   +- download -> file (YAML file) for s2s
#   +- sdaas -> sdaas package (amplitude anomaly detector used in s2s process above)
#   +- medaily -> the directory of this package
#   +- medaily_data -> the output directory of this data (after running s2s above)

# now defined those paths here:
from stream2segment.utils.inputargs import valid_date

from create_summary import create_summary


ROOT = abspath(relpath(dirname(relpath(__file__))))  # `medaily` above
S2S_CONFIG_PATH = dirname(ROOT, 's2s_config')  # root dir
S2S_DOWNLOAD_CONFIG_PATH = abspath(join(S2S_CONFIG_PATH, "download.yaml"))
S2S_PROCESS_MODULE_PATH = abspath(join(S2S_CONFIG_PATH, "process.py"))
S2S_PROCESS_CONFIG_PATH = abspath(join(S2S_CONFIG_PATH, "process.yaml"))


assert (isfile(_) for _ in [S2S_DOWNLOAD_CONFIG_PATH, S2S_PROCESS_MODULE_PATH,
                            S2S_PROCESS_CONFIG_PATH])

# As side note, here the two s2s command lines examples (we will call them here
# but skipping command line step):
# s2s process -d postgresql://me:<password>@localhost/me_06_2020 -c ./processing/aascore.yaml -p ./processing/aascore.py -mp ./processing/aascore.hdf
# s2s process -d postgresql://me:<password>@localhost/me_06_2020 -c ./processing/comp_me.yaml -p ./processing/comp_me.py -mp ./processing/test_processing_v2.hdf


# private methods (see meprocess below)


def _yaml_load(filepath):
    try:
        with open(filepath) as _:
            return yaml.safe_load(_)
    except Exception as exc:
        raise ValueError('Could not open YAML file "%s": %s' %
                         (str(basename(filepath)), str(exc)))


def _get_last_event_datetime(dburl):
    sess = None
    date_max = None
    try:
        sess = get_session(dburl)
        # session.query(func.max(Table.column))
        # filter_expr = Event.time == func.max(Event.time)
        return sess.query(func.max(Event.time)).scalar()
    finally:
        if sess is not None:
            sess.close()


def _create_summary(hdf_path):
    html_file = join(dirname(__file__), 'template.html')
    with open(html_file, encoding='utf-8') as _:
        html_content = _.read()
    data = [_ for _ in create_summary(hdf_path)]
    html_content = html_content.replace('<data></data>', json.dumps(data))
    title = splitext(basename(hdf_path))[0]
    html_content = html_content.replace('<title></title>', title)
    dest_file = join(dirname(hdf_path), title + '.html')
    with open(dest_file, 'w', encoding='utf-8', ) as _:
        _.write(html_content)


# click is a package for building command line applications via the 'command' decorator:
# (https://click.palletsprojects.com/en/7.x/quickstart/#basic-concepts-creating-a-command)
@click.option('-c', '--configpath',
              default=abspath(relpath(join(dirname(__file__), 'config'))),
              type=click.Path(exists=True, file_okay=True, readable=True, dir_okay=False),
              show_default=True,
              help='the YAML configuration file')
@click.command()
def run(configpath):
    P_DESTDIR = 'destdir'
    try:
        config = _yaml_load(configpath)
        dburl, destdir = config['download']['dbrul'], config[P_DESTDIR]
        if not isdir(destdir):
            raise ValueError('Not a directory: %s.\nCheck % in %s' %
                             (destdir, P_DESTDIR, configpath))

        d_config = _yaml_load(S2S_DOWNLOAD_CONFIG_PATH)
        # start and end will be parsed here and not in `download` because
        # we need them to process
        start = valid_date(d_config['startime'])
        end = valid_date(d_config['endtime'])
        ret = download(config=d_config, dburl=dburl, starttime=start,
                       endtime=end)
        if ret != 0:
            raise ValueError('Error downloading data, skipping')

        start_iso = start.isoformat(sep='T')
        end_iso = end.isoformat(sep='T')
        destdir_name = "%s_to_%s" % (start_iso, end_iso)
        dest_dir = join(destdir, destdir_name)
        dest_file = join(dest_dir, 'process.hdf')

        ret = process(dburl,
                      config=S2S_PROCESS_CONFIG_PATH,
                      pyfile=S2S_PROCESS_MODULE_PATH,
                      log2file=dest_file+".log",
                      outfile=dest_file,
                      # update the parameter "segment_select" in config file
                      # (process only newly downloaded segments):
                      segment_select={'event.time': '[%s, %s]' %
                                                    (start_iso, end_iso)})

        if ret != 0 or not isfile(dest_file):
            raise ValueError('Error processing data, skipping')

        _create_summary(dest_file)

    except ValueError as exc:
        print('ERROR: %s' % str(exc))
        sys.exit(1)


if __name__ == '__main__':
    run()