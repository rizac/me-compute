"""
Command line interface (cli) of the program, type `python cli.py --help` for details
on the terminal
"""
import shutil
import json
import os
import sys
from datetime import datetime, date, timedelta
from os.path import abspath, join, dirname, isdir, basename, splitext, isfile, relpath

import click
import yaml
from jinja2 import Template

sys.path.append(dirname(__file__))
from mecompute.stats import get_report_rows, Stats


# global stuff (create config dir if non existing):
_CONFIG_DIR = join(dirname(dirname(__file__)), 'config')
try:
    shutil.copytree(join(dirname(__file__), 'config_files'), _CONFIG_DIR)
except FileExistsError:
    pass

# setup default config file paths:
DOWNLOAD_CONFIG_PATH = join(_CONFIG_DIR, 'download.yaml')
PROCESS_CONFIG_PATH = join(_CONFIG_DIR, 'process.yaml')


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
@click.option('end', '-e', type=click.DateTime(), default=None,
              help="end time (UTC ISO-formatted) of the events to consider. If missing "
                   "it is set as `start` plus `duration` days. If `start` is also "
                   "missing, it defaults as today at midnight (00h:00m:00s)")
@click.option('duration', '-d', type=int, default=1,
              help="duration in days of the time window to consider. If missing, "
                   "defaults to 1. If both time bounds (start, end) are provided, it is "
                   "ignored, if no time bounds are provided, it will set start as "
                   "`duration` days ago. In any other case, it will set the missing "
                   "time bound")
@click.option('start', '-s', type=click.DateTime(), default=None,
              help="start time (UTC ISO-formatted) of the events to consider. If "
                   "missing, it is set as `end` minus `duration` days")
@click.option('config', '-d', type=click.Path(exists=True),
              default=PROCESS_CONFIG_PATH,
              help=f"The configuration file used for processing data. "
                   f"Defaults to: {PROCESS_CONFIG_PATH}")
@click.option('dconfig', '-d', type=click.Path(exists=True),
              default=DOWNLOAD_CONFIG_PATH,
              help=f"The download configuration file employed. The file will be only "
                   f"used to get the URL of the database where data will be extracted "
                   f"and processed (all other properties will be ignored). "
                   f"Defaults to: {DOWNLOAD_CONFIG_PATH}")
@click.argument('root_out_dir', required=False)
def process(end, duration, start, config, dconfig, root_out_dir):
    """
    process Magnitude energy data using stream2segment and saving the HDF file into
    a child directory of the given root output directory. the child directory will be
    named according the desired time window given as argument

    ROOT_OUT_DIR: the destination root directory

    Examples. In order to process all segments of the events occurred ...

    ... in the last 7 days:

        process ROOT_OUT_DIR

    ... in the last 2 days:

        process ROOT_OUT_DIR -d 2

    ... in the seven days preceding the second of January 2021:

        process -e 2021-01-02 ROOT_OUT_DIR

    ... in the seven days following the second of January 2006:

        process -s 2006-01-02 ROOT_OUT_DIR

    ... in the two days following the second of January 2016:

        process -s 2016-01-02 -d 2 ROOT_OUT_DIR
    """
    try:
        with open(dconfig) as _:
            dburl = yaml.safe_load(_)['dburl']
    except (FileNotFoundError, yaml.parser.ParseError, KeyError):
        sys.exit(f'Error reading "dburl" from {config}. Check that file exists '
                 f'and is a well-formed YAML')
    start, end = _get_timebounds(start, end, duration)

    # # in case we want to query the db (e.g., min event, legacy code not used anymore):
    # from stream2segment.process import get_session
    # sess = get_session(dburl)
    # start = sess.query(sqlmin(Event.time)).scalar()  # (raises if multiple results)
    # close_session(sess)

    sep = '--'  # file separator

    # create output directory within destdir and assign new name:
    destdir = dir_exists(root_out_dir, 'mecomputed' + sep + start +sep + end)

    segments_selection = {
        'event.time': '(%s, %s]' % (start.isoformat(), end.isoformat()),
        'has_valid_data': 'true',
        'maxgap_numsamples': '(-0.5, 0.5)'
    }

    destfile_name = 'process-result'  + sep + start +sep + end

    # set logfile:
    logfile = join(destdir, destfile_name + '.log')

    # set outfile
    outfile = join(destdir, destfile_name + '.hdf')


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

    # context.forward(test)
    from stream2segment.process import process as s2s_process
    from .process import main as main_function
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


def dir_exists(*paths, mkdirs=True):
    """Call `os.path.join` on the arguments and assures the resulting directory exists
    before returning its full path. If `mkdirs` is True (the default), an attempt to
    create the directory and all its ancestor ('s.makedirs`) is made
    """
    full_path = abspath(join(*paths))
    if not isdir(full_path):
        os.makedirs(full_path)
    if not isdir(full_path):
        raise ValueError(f'Not a directory: {full_path}')
    return full_path


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


# (https://click.palletsprojects.com/en/5.x/advanced/#invoking-other-commands)
@cli.command(context_settings=dict(max_content_width=89),)
@click.option('-f', '--force-overwrite',
              is_flag=True, help='Force overwrite HTML if already existing. '
                                 'Default is false (do not regenerate '
                                 'existing HTML)')
@click.argument('input', required=False, nargs=-1)
def report(force_overwrite, input):
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

    config_in = join(dirname(__file__), 'report.template.html')
    with open(config_in) as _:
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
