"""
Command line interface (cli) of the program, type `python cli.py --help` for details
on the terminal
"""
import os
import sys
import csv
from datetime import datetime, date, timedelta
from http.client import HTTPException
from os.path import join, dirname, isdir, basename, splitext, isfile, abspath, isabs, \
    relpath
from urllib.error import URLError, HTTPError

import click
import pandas as pd
import yaml
from jinja2 import Template
from stream2segment.io.inputvalidation import BadParam
import logging

from mecompute.stats import get_report_rows
from stream2segment.process import imap
from mecompute.process import main as main_function

logger = logging.getLogger('me-compute')

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
                   f"the data. Used to get the URL of the database where events and "
                   f"waveforms will be fetched (all other properties will be ignored)")
@click.option('start', '-s', type=click.DateTime(), default=None,
              help="the start time of the "
                   "db events to fetch (UTC ISO-formatted string). "
                   "If missing, it is set as `end` minus `duration` days")
@click.option('end', '-e', type=click.DateTime(), default=None,
              help="the end time of the "
                   "db events to fetch (UTC ISO-formatted string). "
                   "If missing, it is set as `start` plus `duration`. If `start` is "
                   "also missing, it defaults as today at midnight")
@click.option('time_window', '-t', type=int, default=None,
              help="the time window, in days of teh events to "
                   "fetch. If missing, it defaults to 1. If both time bounds (start, "
                   "end) are provided, it is ignored")
@click.option('force_overwrite', '-f', is_flag=True,
              help='Force overwrite existing files. Default is false which will try to '
                   'preserve existing files (outdated files, if found, will be '
                   'overwritten anyway')
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
    """
    Computes the energy magnitude (Me) from a selection of events and waveforms
    previously downloaded with stream2segment and saved on a SQLite or Postgres database.

    OUTPUT_DIR: the destination root directory. You can use the special characters %S%
    and %E% that will be replaced with the start and end time in ISO format, computed
    from the given parameters. The output directory and its parents will be created if
    they do not exist

    In the output directory, the following files will be saved:
        - station-energy-magnitude.hdf A tabular files where each row represents a
          station/waveform and each column the station computed data and metadata,
          including the station energy magnitude.
          Note that the program assumes that a single channel (the vertical) is
          downloaded per station, so that 1 waveform <=> 1 station
        - energy-magnitude.csv A tabular file (one row per event) aggregating the result
          of the previous file into the final event energy magnitude. The final event Me
          is the mean of all station energy magnitudes within the 5-95 percentiles
        - energy-magnitude.html A report that can be opened in the user browser to
          visualize the computed energy magnitudes on maps and HTML tables
        - [eventid1].xml, ..., [eventid1].xml All processed events saved in QuakeMl
          format, updated with the information on their energy magnitude
        - energy-magnitude.log the log file where the info, errors and warnings
          of the routine are stored. The core energy magnitude computation at station
          level (performed via stream2segment utilities) has a separated and more
          detailed log file (see below)
        - station-energy-magnitude.log the log file where the info, errors and warnings
          of the station energy magnitude computation have been stored


    Examples. In order to process all segments of the events occurred ...

    ... yesterday:

        me-compute OUT_DIR

    ... in the last 2 days:

        me-compute -t 2 OUT_DIR

    ... on January the 2nd and January the 3rd, 2016:

        process -s 2016-01-02 -t 2 OUT_DIR
    """
    # create output directory within destdir and assign new name:
    start, end = _get_timebounds(start, end, time_window)
    dest_dir = output_dir.replace("%S%", start).replace("%E%", end)
    file_handler = logging.FileHandler(mode='w+',
                                       filename=join(dest_dir,
                                                     'energy-magnitude.log'))
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    try:
        process(d_config, start, end, dest_dir,
                force_overwrite=force_overwrite, p_config=p_config,
                html_template=h_template)
    except MeRoutineError as merr:
        logger.error(str(merr))
        sys.exit(1)
    except Exception as exc:  # noqa
        logger.exception(exc)
        sys.exit(1)
    finally:
        file_handler.close()
    sys.exit(0)


class MeRoutineError(Exception):
    pass


def process(dconfig, start, end, dest_dir,
            force_overwrite=False,
            p_config=None, html_template=None):
    """process downloaded events computing their energy magnitude (Me)"""


    # # in case we want to query the db (e.g., min event, legacy code not used anymore):
    # from stream2segment.process import get_session
    # sess = get_session(dburl)
    # start = sess.query(sqlmin(Event.time)).scalar()  # (raises if multiple results)
    # close_session(sess)

    base_name = 'energy-magnitude'

    # set outfile
    station_me_file = join(dest_dir, 'station-' + base_name + '.hdf')

    if not isfile(station_me_file) or force_overwrite:

        logger.info(f'Computing station energy magnitudes and saving data to '
                    f'{station_me_file}. See relative log file for details')

        try:
            with open(dconfig) as _:
                dburl = yaml.safe_load(_)['dburl']
                sqlite = "sqlite:///"
                if dburl.lower().startswith(sqlite):
                    dburl_ = dburl[len(sqlite):]
                    if not isabs(dburl_):
                        dburl = "sqlite:///" + abspath(join(dirname(dconfig), dburl_))
        except (FileNotFoundError, yaml.YAMLError, KeyError) as exc:
            raise MeRoutineError(f'Unable to read "dburl" from {dconfig}. '
                                 f'Check that file exists and is a well-formed '
                                 f'YAML')

        if not isdir(dest_dir):
            os.makedirs(dest_dir)
        if not isdir(dest_dir):
            raise MeRoutineError(f'Not a directory: {dest_dir}')

        segments_selection = {
            'event.time': '(%s, %s]' % (start, end),
            'has_valid_data': 'true',
            'maxgap_numsamples': '(-0.5, 0.5)'
        }
        try:
            station_me_df = _compute_station_me(station_me_file, dburl, segments_selection,
                                                p_config)
            # all next files might now be outdated so we need to force updating them:
            force_overwrite = True
        except Exception as exc:
            raise MeRoutineError('Error while computing station energy magnitude : '
                                 + str(exc))

    else:
        logger.info(f'Fetching station energy magnitudes from {station_me_file}')
        station_me_df = pd.read_hdf(station_me_file)

    if html_template is None:
        html_template = REPORT_TEMPLATE_PATH

    with open(html_template) as _:
        template = Template(_.read())

    csv_fpath = abspath(join(dest_dir, base_name + '.csv'))
    html_fpath = abspath(join(dest_dir, base_name + '.html'))

    author_uri = "https://github.com/rizac/me-compute"
    title = splitext(basename(html_fpath))[0]

    sel_event_id = None
    ev_headers = None
    csv_evts = []
    html_evts = {}
    logger.info(f'Computing events energy magnitudes')
    for evt, stations in get_report_rows(station_me_df):
        csv_evts.append(evt)
        if ev_headers is None:
            ev_headers = list(csv_evts[0].keys())
        ev_catalog_url = evt['url']
        ev_catalog_id = ev_catalog_url.split('eventid=')[-1]
        # write QuakeML:
        try:
            quakeml_file = join(dest_dir, ev_catalog_id + '.xml')
            _write_quekeml(quakeml_file, ev_catalog_url,
                           evt['Me'], evt['Me_stddev'], evt['Me_waveforms_used'],
                           author_uri, force_overwrite)
        except (OSError, HTTPError, HTTPException, URLError) as exc:
            logger.warning(f'Unable to create QuakeML for "ev_catalog_id": {exc}')

        html_evts[evt['db_id']] = [[evt[h] for h in ev_headers], stations]
        if sel_event_id is None:
            sel_event_id = evt['db_id']

    if csv_evts and (not isfile(csv_fpath) or force_overwrite):
        logger.info(f'Saving event energy magnitudes to: {csv_fpath}')
        with open(csv_fpath, 'w', newline='') as _:
            writer = csv.DictWriter(_,fieldnames=ev_headers)
            writer.writeheader()
            for evt in csv_evts:
                writer.writerow(evt)

    if sel_event_id is not None and (not isfile(html_fpath) or force_overwrite):
        logger.info(f'Saving visual report of event energy magnitudes to: '
                    f'{html_fpath}')
        with open(html_fpath, 'w') as _:
            _.write(template.render(title=title, selected_event_id=sel_event_id,
                                    event_data=html_evts, event_headers=ev_headers))


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
        'event_url': {}
    }

    if p_config is None:
        p_config = PROCESS_CONFIG_PATH

    # option to write str columns to dataframe:
    min_itemsize = {}
    processed_waveforms = []

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

    dataframe.to_hdf(outfile, format='table', key='station_energy_magnitudes',
                     min_itemsize=min_itemsize or None)
    return outfile


def _write_quekeml(dest_file, event_url, me, me_u=None, me_stations=None,
                   author="", force_overwrite=False):
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
