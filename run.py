"""Medaily script"""
import json
import sys
from datetime import datetime, timedelta
from os.path import join, dirname, abspath, splitext, basename, isabs, isfile, isdir

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
from create_summary import create_summary


PWD = dirname(__file__)  # `medaily` above
ROOT = dirname(PWD)  # root dir
DOWNLOAD_PATH = abspath(join(ROOT, "download", "download.eida.yaml"))
PROCESS_CONFIG_PATH = abspath(join(ROOT, "processing", "medaily.yaml"))
PROCESS_PATH = abspath(join(ROOT, "processing", "medaily.py"))
OUT_DIR = abspath(join(ROOT, "mecompute_daily"))


configfilenames = ['download.yaml', 'process.yaml', 'process.py', 'mecompute.yaml']


# As side note, here the two s2s command lines examples (we will call them here
# but skipping command line step):
# s2s process -d postgresql://me:<password>@localhost/me_06_2020 -c ./processing/aascore.yaml -p ./processing/aascore.py -mp ./processing/aascore.hdf
# s2s process -d postgresql://me:<password>@localhost/me_06_2020 -c ./processing/comp_me.yaml -p ./processing/comp_me.py -mp ./processing/test_processing_v2.hdf


# private methods (see meprocess below)

def _check_config(configdir):
    """Checks the config directory and returns the absolute path of each file
     defined in the global variable `configfilenames`

    raise `ValueError` if anything is wrong (e.g., file do not exist)
    """
    if not isdir(configdir):
        raise ValueError('Config directory "%s" does not exist' % configdir)

    ret = []
    for filename in configfilenames:
        fle = join(configdir, filename)
        if not isfile(fle):
            raise ValueError('"%s" not found in %s' % (filename, configdir))
        ret.append(fle)
    return ret


def _yaml_load(filepath):
    try:
        with open(filepath) as _:
            return yaml.safe_load(_)
    except Exception as exc:
        raise ValueError('Could not open YAML file "%s": %s' %
                         (str(basename(filepath)), str(exc)))


def _download(dconf_path, dburl, dataselect_urls_list):
    dconf_dict = _yaml_load(dconf_path)

    sess = None
    date_max = None
    try:
        sess = get_session(dburl)
        # session.query(func.max(Table.column))
        # filter_expr = Event.time == func.max(Event.time)
        date_max = sess.query(func.max(Event.time)).scalar()
    finally:
        if sess is not None:
            sess.close()

    oneday = timedelta(days=1)

    # set start as date_max at 24h:00m:00s:
    start = date_max.replace(hour=0, minute=0, second=0, microsecond=0) + oneday
    # set start as today at 00h:00m:00s:
    end = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    ret = 0
    for dataws in dataselect_urls_list:
        ret = download(config=dconf_dict, start=start, end=end, dburl=dburl,
                       dataws=dataws)
        if ret !=0:
            print('Error downloading data, skipping')
            return ret

    return start, end


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
@click.option('dburl', required=True, type=str,
              help='The database URL, e.g.: '
                   'postgresql://me:<password>@localhost/me_06_2020')
@click.command()
def run(config):

    try:
        dconf_path, pconf_path, pmodule_path, globalconf_path = _check_config(config)
        global_conf = _yaml_load(globalconf_path)
        dest_root = global_conf['output_dir']
        if not isdir(dest_root):
            raise ValueError('Output directory "%s" does not exist. '
                             'Check typos in parameter "output_dir" (file %s)' %
                             (dest_root, globalconf_path))
        dburl = global_conf['download']['dburl']

        start, end = _download(dconf_path, **global_conf['download'])
        start_iso = start.isoformat(sep='T')
        end_iso = end.isoformat(sep='T')
        destdir_name = "%s_to_%s" % (start_iso, end_iso)
        dest_dir = join(dest_root, destdir_name)
        dest_file = join(dest_dir, 'process.hdf')

        ret = process(dburl,
                      config=pconf_path,
                      pyfile=pmodule_path,
                      outfile=dest_file,
                      # update the parameter "segment_select" in config file
                      # (process only newly downloaded segments):
                      segment_select={'event.time': '[%s, %s]' %
                                                    (start_iso, end_iso)})

        if ret != 0:
            raise ValueError('Error processing data, skipping')

        _create_summary(outfile)

    except ValueError as exc:
        print('ERROR: %s' % str(exc))
        sys.exit(1)


if __name__ == '__main__':
    run()