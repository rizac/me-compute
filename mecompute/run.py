"""
Command line interface (cli) of the program, type `python cli.py --help` for details
on the terminal
"""
import shutil
import json
import os
import sys
from datetime import datetime, date, timedelta
from os.path import join, dirname, isdir, basename, splitext, isfile

import click
import yaml
from jinja2 import Template
from stream2segment.io.inputvalidation import BadParam

from mecompute.stats import get_report_rows, Stats
from stream2segment.process import process as s2s_process
from mecompute.process import main as main_function

# global stuff (create config dir if non existing):
_CONFIG_DIR = join(dirname(dirname(__file__)), 'config')
try:
    shutil.copytree(join(dirname(__file__), 'base-config'), _CONFIG_DIR)
except FileExistsError:
    pass

# setup default config file paths:
DOWNLOAD_CONFIG_PATH = join(_CONFIG_DIR, 'download.yaml')
PROCESS_CONFIG_PATH = join(_CONFIG_DIR, 'process.yaml')
REPORT_TEMPLAE_PATH = join(_CONFIG_DIR, 'report.template.html')


###########################
# Me compute cli commands:
###########################


cli = click.Group()


# (https://click.palletsprojects.com/en/5.x/advanced/#invoking-other-commands)
@cli.command(context_settings=dict(max_content_width=89),)
@click.option('config', '-c', type=click.Path(exists=True),
              default=DOWNLOAD_CONFIG_PATH,
              help=f"The configuration file. Defaults to: "
                   f"{DOWNLOAD_CONFIG_PATH}")
@click.pass_context
def download(context, config):
    """Launches a download routine with Stream2segment"""
    from stream2segment.cli import download as s2s_download
    context.invoke(s2s_download, config=config)


@cli.command(context_settings=dict(max_content_width=89),)
@click.option('start', '-s', type=click.DateTime(), default=None,
              help="start time of the events to consider  (UTC ISO-formatted string). "
                   "If missing, it is set as `end` minus `duration` days")
@click.option('end', '-e', type=click.DateTime(), default=None,
              help="end time of the events to consider (UTC ISO-formatted string). "
                   "If missing, it is set as `start` plus `duration`. If `start` is "
                   "also missing, it defaults as today at midnight")
@click.option('duration', '-d', type=int, default=1,
              help="duration in days of the time window to consider. If missing, "
                   "defaults to 1. If both time bounds (start, end) are provided, it is "
                   "ignored")
@click.option('config', '-c', type=click.Path(exists=True),
              default=PROCESS_CONFIG_PATH,
              help=f"The configuration file used for processing data. "
                   f"Defaults to: {PROCESS_CONFIG_PATH}")
@click.option('dconfig', '-D', type=click.Path(exists=True),
              default=DOWNLOAD_CONFIG_PATH,
              help=f"The download configuration file employed. Only "
                   f"used to get the URL of the database where events and waveforms"
                   f"will be fetched (all other properties will be ignored). "
                   f"Defaults to: {DOWNLOAD_CONFIG_PATH}")
@click.argument('root_output_dir', required=True)
def process(start, end, duration, config, dconfig, root_output_dir):
    """
    process downloaded events computing their Magnitude energy. The output is a
    directory that will be created inside the specified ROOT_OUTPUT_DIR: the directory
    contains several files, including  a .HDF file with all waveforms processed (one row
    per waveform) and several columns, among which "me_st" represents the computed
    Magnitude energy.

    ROOT_OUTPUT_DIR: the destination root directory

    Examples. In order to process all segments of the events occurred ...

    ... yesterday:

        process ROOT_OUT_DIR

    ... in the last 2 days:

        process ROOT_OUT_DIR -d 2

    ... on January the 2nd and January the 3rd, 2016:

        process -s 2016-01-02 -d 2 ROOT_OUT_DIR
    """
    try:
        with open(dconfig) as _:
            dburl = yaml.safe_load(_)['dburl']
    except (FileNotFoundError, yaml.YAMLError, KeyError):
        print(f'Error reading "dburl" from {config}. Check that file exists '
              f'and is a well-formed YAML', file=sys.stderr)
        sys.exit(1)

    start, end = _get_timebounds(start, end, duration)

    # # in case we want to query the db (e.g., min event, legacy code not used anymore):
    # from stream2segment.process import get_session
    # sess = get_session(dburl)
    # start = sess.query(sqlmin(Event.time)).scalar()  # (raises if multiple results)
    # close_session(sess)

    base_name = _get_processing_output_dirname(start, end)

    # create output directory within destdir and assign new name:
    destdir = join(root_output_dir, base_name)
    if not isdir(destdir):
        os.makedirs(destdir)
    if not isdir(destdir):
        print(f'Not a directory: {destdir}')
        sys.exit(1)

    segments_selection = {
        'event.time': '(%s, %s]' % (start, end),
        'has_valid_data': 'true',
        'maxgap_numsamples': '(-0.5, 0.5)'
    }

    # set logfile:
    logfile = join(destdir, base_name + '.log')

    # set outfile
    outfile = join(destdir, base_name + '.hdf')

    writer_options = {
        'chunksize': 10000,
        # hdf needs a fixed length for all columns: if you write string columns
        # you need to tell in advance the size allocated with 'min_itemsize', e.g:
        'min_itemsize': {
            'network': 2,
            'station': 5,
            'location': 2,
            'channel': 3,
            'ev_mty': 2
        }
    }

    try:
        s2s_process(main_function,
                    segments_selection=segments_selection,
                    dburl=dburl,
                    config=config, logfile=logfile, outfile=outfile,
                    multi_process=True, chunksize=None, writer_options=writer_options)
    except BadParam as bpar:
        print(f'ERROR: {str(bpar)}', file=sys.stderr)
        sys.exit(1)

    # dburl = yaml_load(join(s2s_config_dir, 'download.private.yaml'))['dburl']
    # context.invoke(s2s_process, dburl=dburl, config=config, pyfile=pyfile,
    #                logfile=log, outfile=outfile)
    sys.exit(0)


def _get_timebounds(start=None, end=None, duration=1):
    """
    return the tuple start:str, end:str from the arguments. If start and
    end are None, then they will default to yesterday

    :param start: datetime or None, the start time
    :param end: datetime or None, the end time
    :param duration: int or None, the duration, in days
    """
    if end is None and start is None:
        end = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=duration)
    elif end is None:
        end = start + timedelta(days=duration)
    elif start is None:
        start = end - timedelta(days=duration)
    return _isoformat(start), _isoformat(end)


def _isoformat(time):
    if time.hour == time.minute == time.second == time.microsecond == 0:
        return date(year=time.year, month=time.month, day=time.day).isoformat()
    return time.isoformat(sep='T')


def _get_processing_output_dirname(start, end):
    sep = '_'  # file separator
    base_name_prefix = 'me-computed'
    return base_name_prefix + sep + start + sep + end


# (https://click.palletsprojects.com/en/5.x/advanced/#invoking-other-commands)
@cli.command(context_settings=dict(max_content_width=89),)
@click.option('-f', '--force-overwrite', is_flag=True,
              help='Force overwrite HTML if it already exists. Default is false '
                   '(skip process if HTML exists)')
@click.option('-t', '--html-template', type=click.Path(exists=True),
              default=REPORT_TEMPLAE_PATH,
              help=f'The HTML template file used to build the output report. This '
                   f'parameter is for users experienced with jina2 who need'
                   f'to customize the report appearance. '
                   f'Defaults to: {REPORT_TEMPLAE_PATH}')
@click.argument('input', required=False, nargs=-1)
def report(force_overwrite, html_template, input):
    """
    Create HTML report from a given HDF file generated with the process command
    for facilitating inspection and visualization of the Me computation process

    INPUT: the path or list of paths (space separated) to the HDF file(s)
        generated with the `process` command. Each report will be saved in the
        same directory by replacing the file extension with ".html"

    Each HTML report will be created with the same HDF name replacing the file
    extension with 'html'
    """
    print("%d process file(s) found" % len(input))
    # collect a dict mapping process file -> report file but only for files not existing
    # (unless force_overwrite is True):
    input = {_: splitext(_)[0] + '.html'
             for _ in input if force_overwrite or not isfile(splitext(_)[0] + '.html')}
    if not input:
        print("No report to be generated")
        sys.exit(0)
    else:
        print("%d report(s) to be generated" % len(input))

    with open(html_template) as _:
        template = Template(_.read())

    desc = Stats.as_help_dict()
    written = 0
    for process_fpath, report_fpath in input.items():
        title = splitext(basename(process_fpath))[0]
        try:
            evts, stas = [], {}
            for evid, evt_stats, stations in get_report_rows(process_fpath):
                evts.append(evt_stats)
                stas[evid] = stations
            if not evts:
                continue
            with open(report_fpath, 'w') as _:
                _.write(template.render(title=title, events=evts, description=desc,
                                        stations=json.dumps(stas, separators=(',', ':'))))
            written += 1
        except Exception as exc:
            print('ERROR: %s while generating %s: %s' % (exc.__class__.__name__,
                                                         report_fpath, str(exc)),
                  file=sys.stderr)
            # sys.exit(1)
    print("%d reports generated" % written)
    sys.exit(0)


if __name__ == '__main__':
    cli()  # noqa
