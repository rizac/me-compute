"""
Command line interface (cli) of the program, type `python cli.py --help` for details
on the terminal
"""
import os
import sys
import csv
from datetime import datetime, date, timedelta
from http.client import HTTPException
from os.path import join, dirname, isdir, basename, splitext, isfile, abspath
from urllib.error import URLError, HTTPError

import click
import pandas as pd
import yaml
from jinja2 import Template
from stream2segment.io.inputvalidation import BadParam

from mecompute.stats import get_report_rows
from stream2segment.process import imap
from mecompute.process import main as main_function


_CONFIG_DIR = join(dirname(__file__), 'base-config')
PROCESS_CONFIG_PATH = join(_CONFIG_DIR, 'process.yaml')
REPORT_TEMPLATE_PATH = join(_CONFIG_DIR, 'report.template.html')
assert isfile(PROCESS_CONFIG_PATH)
assert isfile(REPORT_TEMPLATE_PATH)


#########################
# Me compute cli command:
#########################


@click.command(context_settings=dict(max_content_width=89),)
@click.option('d_config', '-d', type=click.Path(exists=True),
              help=f"The path of the download configuration file used to download "
                   f"the data. Used get the URL of the database where events and "
                   f"waveforms will be fetched (all other properties will be ignored). "
                   f"If the output directory already exists and force_overwrite is "
                   f"False, this parameter will be ignored")
@click.option('start', '-s', type=click.DateTime(), default=None,
              help="if the database data has to be used, set the start time of the "
                   "db events to fetch (UTC ISO-formatted string). "
                   "If missing, it is set as `end` minus `duration` days")
@click.option('end', '-e', type=click.DateTime(), default=None,
              help="if the database data has to be used, set end start time of the "
                   "db events to fetch (UTC ISO-formatted string). "
                   "If missing, it is set as `start` plus `duration`. If `start` is "
                   "also missing, it defaults as today at midnight")
@click.option('time_window', '-t', type=int, default=None,
              help="if the database data has to be used, set the start time of the "
                   "db events to fetch, set the time window, in days of teh events to "
                   "fetch. If missing, it defaults to 1. If both time bounds (start, "
                   "end) are provided, it is ignored")
@click.option('-f', '--force-overwrite', is_flag=True,
              help='Force overwrite all files if it already exist. Default is false '
                   '(use existing files - if found - and do not overwrite them)')
@click.option('p_config', '-pc', type=click.Path(exists=True),
              default=None,
              help=f"The path of the configuration file used for processing the data. "
                   f"If missing, the default configuration will be used (see the file "
                   f"provided by default in the git repository for details: "
                   f"me-compute/config/process.yaml)")
@click.option('h_template', '-ht', type=click.Path(exists=True),
              default=None,
              help=f"The path of the HTML template file used to build the output "
                   f"report. This parameter is for users experienced with jina2 who "
                   f"need to customize the report appearance. If missing, "
                   f"the default template will be used (see the file provided "
                   f"by default in the git repository for details: "
                   f"me-compute/config/report.template.html)")
@click.argument('output_dir', required=True)
def cli(d_config, start, end, time_window, force_overwrite, p_config, h_template,
        output_dir):
    # if output_dir is None and not all(_ is None for _ )
    try:
        process(d_config, start, end, time_window, output_dir,
                force_overwrite=force_overwrite, p_config=p_config,
                html_template=h_template)
    except MeRoutineError as merr:
        print('Error: ' + str(merr), file=sys.stderr)
        sys.exit(1)
    except Exception as exc:  # noqa
        import traceback
        traceback.print_exception(exc, file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


class MeRoutineError(Exception):
    pass


def process(dconfig, start, end, duration, output_dir,
            force_overwrite=False,
            p_config=None, html_template=None):
    """
    process downloaded events computing their energy magnitude (Me).

    ROOT_OUTPUT_DIR: the destination root directory. NOTE: The output of this command
    is a **directory** that will be created inside ROOT_OUTPUT_DIR: the directory
    will contain several files, including a .HDF file with all waveforms processed (one
    row per waveform) and several columns

    Examples. In order to process all segments of the events occurred ...

    ... yesterday:

        process ROOT_OUT_DIR

    ... in the last 2 days:

        process ROOT_OUT_DIR -d 2

    ... on January the 2nd and January the 3rd, 2016:

        process -s 2016-01-02 -d 2 ROOT_OUT_DIR
    """
    start, end = _get_timebounds(start, end, duration)

    # # in case we want to query the db (e.g., min event, legacy code not used anymore):
    # from stream2segment.process import get_session
    # sess = get_session(dburl)
    # start = sess.query(sqlmin(Event.time)).scalar()  # (raises if multiple results)
    # close_session(sess)

    # create output directory within destdir and assign new name:
    destdir = output_dir.replace("%S%", start).replace("%E%", end)

    base_name = 'energy-magnitude'

    # set outfile
    station_me_file = join(destdir, 'station-' + base_name + '.hdf')

    if not isfile(station_me_file) or force_overwrite:

        try:
            with open(dconfig) as _:
                dburl = yaml.safe_load(_)['dburl']
        except (FileNotFoundError, yaml.YAMLError, KeyError) as exc:
            raise MeRoutineError(f'Unable to read "dburl" from {dconfig}. '
                                 f'Check that file exists and is a well-formed '
                                 f'YAML')

        if not isdir(destdir):
            os.makedirs(destdir)
        if not isdir(destdir):
            raise MeRoutineError(f'Not a directory: {destdir}')

        segments_selection = {
            'event.time': '(%s, %s]' % (start, end),
            'has_valid_data': 'true',
            'maxgap_numsamples': '(-0.5, 0.5)'
        }

        station_me_df = _compute_station_me(station_me_file, dburl, segments_selection,
                                            p_config)
    else:
        station_me_df = pd.read_hdf(station_me_file)

    if html_template is None:
        html_template = REPORT_TEMPLATE_PATH

    with open(html_template) as _:
        template = Template(_.read())

    csv_fpath = abspath(join(destdir, base_name + '.csv'))
    html_fpath = abspath(join(destdir, base_name + '.html'))

    author_uri = "https://github.com/rizac/me-compute"
    title = splitext(basename(html_fpath))[0]
    try:
        sel_event_id = None
        ev_headers = None
        csv_evts = []
        html_evts = {}
        for evt, stations in get_report_rows(station_me_df):
            csv_evts.append(evt)
            if ev_headers is None:
                ev_headers = list(csv_evts[0].keys())
            ev_catalog_url = evt['url']
            ev_catalog_id = ev_catalog_url.split('=')[-1]
            # write QuakeML:
            try:
                _write_quekeml(destdir, ev_catalog_url, ev_catalog_id,
                               evt['Me'], evt['Me_stddev'], evt['Me_waveforms_used'],
                               author_uri, force_overwrite)
            except (OSError, HTTPError, HTTPException, URLError) as exc:
                print(f'Unable to create QuakeML for "ev_catalog_id": {exc}',
                      file=sys.stderr)

            html_evts[evt['db_id']] = [[evt[h] for h in ev_headers], stations]
            if sel_event_id is None:
                sel_event_id = evt['db_id']

        if csv_evts and (not isfile(csv_fpath) or force_overwrite):
            with open(csv_fpath, 'w', newline='') as _:
                writer = csv.DictWriter(_,fieldnames=ev_headers)
                writer.writeheader()
                for evt in csv_evts:
                    writer.writerow(evt)

        if sel_event_id is not None and (not isfile(html_fpath) or force_overwrite):
            with open(html_fpath, 'w') as _:
                _.write(template.render(title=title,
                                        selected_event_id=sel_event_id,
                                        event_data=html_evts,
                                        event_headers=ev_headers))

    except Exception as exc:
        raise MeRoutineError(f'Unexpected Exception {exc.__class__.__name__}: ' 
                             f'{str(exc)}')


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


def _compute_station_me(outfile, dburl, segments_selection, p_config=None):

    # set logfile:
    logfile = splitext(abspath(outfile))[0] + '.log'

    # handle string columns:
    # store each column possible values in a dict (handle str vs categorical at
    # the end):
    categorical_columns = {  # for
        'network': {},
        'station': {},
        'location': {},
        'channel': {},
        'event_magnitude_type': {},
        'event_catalog_url': {}
    }

    if p_config is None:
        p_config = PROCESS_CONFIG_PATH

    # option to write str columns to dataframe:
    min_itemsize = {}
    processed_waveforms = []
    try:
        for res_dict in imap(main_function,
                             segments_selection=segments_selection,
                             dburl=dburl,
                             config=p_config, logfile=logfile,
                             multi_process=True, chunksize=None):
            for col in categorical_columns:
                value = res_dict[col]
                if value not in categorical_columns[col]:
                    categorical_columns[col][value] = len(categorical_columns[col])
                res_dict[col] = categorical_columns[col][value]
            processed_waveforms.append(res_dict)

        dataframe = pd.DataFrame(processed_waveforms)
        # handle str columns, convert them to str or categorical:
        for col in categorical_columns:
            dtype = 'str' if len(categorical_columns[col]) > len(dataframe) / 2 \
                else 'category'
            mapping = {v: k for k, v in categorical_columns[col].items()}
            dataframe[col] = dataframe[col].map(mapping).astype(dtype)
            if dtype == 'str':
                min_itemsize[col] = max(len(v) for v in mapping.values())

        dataframe.to_hdf(outfile, format='table', key='me_computed_waveforms_table',
                         min_itemsize=min_itemsize or None)
        return outfile

    except BadParam as bpar:
        raise MeRoutineError(str(bpar))


def _write_quekeml(dest_dir, event_url, event_id, me, me_u=None, me_stations=None,
                   author="", force_overwrite=False):
    dest_file = join(dest_dir, event_id + '.xml')
    if isfile(dest_file) and not force_overwrite:
        return dest_file

    from obspy.core.event import read_events, Magnitude, CreationInfo, QuantityError
    evt = read_events(event_url)
    if len(evt) == 1:
        mag = Magnitude()
        mag.mag = me
        mag.magnitude_type = 'Me'
        if not pd.isna(me_u):
            mag.mag_errors = QuantityError(uncertainty=me_u)
        if not pd.isna(me_stations):
            mag.station_count = me_stations
        mag.creation_info = CreationInfo()
        mag.creation_info.creation_time = datetime.utcnow()
        if author:
            mag.creation_info.author = author
        evt[0].magnitudes.append(mag)
        evt.write(dest_file, format="QUAKEML")
        return evt
    raise URLError('source QuakeML contains more than 1 event')


if __name__ == '__main__':
    cli()  # noqa
