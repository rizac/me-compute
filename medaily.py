"""Medaily script"""
import json
from datetime import datetime, timedelta
from os.path import join, dirname, abspath, splitext, basename

import click

from sqlalchemy.sql.expression import func

import yaml
from stream2segment.io.db.models import Segment
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
OUT_DIR = abspath(join(ROOT, "medaily_data"))


# As side note, here the two s2s command lines examples (we will call them here
# but skipping command line step):
# s2s process -d postgresql://me:<password>@localhost/me_06_2020 -c ./processing/aascore.yaml -p ./processing/aascore.py -mp ./processing/aascore.hdf
# s2s process -d postgresql://me:<password>@localhost/me_06_2020 -c ./processing/comp_me.yaml -p ./processing/comp_me.py -mp ./processing/test_processing_v2.hdf


# private methods (see meprocess below)

def _get_download_config():
    with open(DOWNLOAD_PATH) as _:
         return yaml.safe_load(_)


def _get_download_config(config_filepath_or_dict):
    dconfig = config_filepath_or_dict
    if not isinstance(dconfig, dict):
        dconfig = _get_download_config()
            dconfig = yaml.safe_load(_)
    return dconfig['dburl']


def _download():
    with open(DOWNLOAD_PATH) as _:
        dconfig = yaml.safe_load(_)
    dburl = dconfig['dburl']
    sess = None
    date_max = None
    try:
        sess = get_session(dburl)
        # session.query(func.max(Table.column))
        filter_expr = Segment.request_start == func.max(Segment.request_start)
        date_max = sess.query(Segment).filter(filter_expr).limit(1).one()[0]
    finally:
        if sess is not None:
            sess.close()

    oneday = timedelta(days=1)

    # set start as date_max at 24h:00m:00s:
    start = date_max.replace(hour=0, minute=0, second=0, microsecond=0) + oneday
    # set start as today at 00h:00m:00s:
    end = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    ret = 0
    if end-start > oneday:
        ret = download(config=dconfig, start=start, end=end)
    else:
        ret = 0
        print('Skip download: not enough time (%s) has passed since last '
              'download (%s)' % (str(oneday), str(start)))
    if ret !=0:
        print('Error downloading data, skipping')
        return ret


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
@click.command()
def meprocess():

    dburl, start, end = _download()

    outbasename = "%s_to_%s" % (start.isoformat(sep='T'), end.isoformat(sep='T'))
    out_dir = join(OUT_DIR, outbasename)
    outfile = join(out_dir, 'process.out.hdf')

    ret = process(dburl,
                  pyfile=PROCESS_PATH,
                  config=PROCESS_CONFIG_PATH,
                  outfile=outfile,
                  # update the parameter "segment_select" in config file
                  # (process only newly downloaded segments):
                  segment_select=dict(request_start='>=%s' % start.isoformat(sep='T')))

    if ret != 0:
        print('Error processing data, skipping')
        return ret

    _create_summary(outfile)




if __name__ == '__main__':
    meprocess()