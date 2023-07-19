"""
Command line interface (cli) of the program, type `python cli.py --help` for details
on the terminal
"""
import os
import sys
import csv
from datetime import datetime, date, timedelta
from http.client import HTTPException
from os.path import join, dirname, isdir, basename, splitext, isfile, abspath, isabs
from urllib.error import URLError, HTTPError

import click
import pandas as pd
import yaml
from jinja2 import Template
import logging

from mecompute.stats import get_report_rows
from stream2segment.process import process as s2s_process
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

    OUTPUT_DIR is the destination directory. You can use the special characters
    `%S%` and `%E%` that will be replaced with the start and end time in ISO format,
    computed from the given parameters. The output directory and its parents will be
    created if they do not exist. START and END are the start and end time of the
    events to consider, in ISO format (e.g. "2016-03-31")

    In the output directory, the following files will be saved:

    - **station-energy-magnitude.hdf** A tabular file where each row represents a
      station(^) and each column the station computed data and metadata,
      including the station energy magnitude.

      (^) Note: technically speaking, a single HDF row represents a waveform.
      However, there is no distinction because for each station a single channel
      (the vertical component `BHZ`) is downloaded (just consider this if you
      increase the station channels to download in `download.yaml`)


    - **energy-magnitude.csv** A tabular file where each row represents a seismic
      event, aggregating the result of the previous file into the final event energy
      magnitude. The event Me is the mean of all station energy magnitudes within
      the 5-95 percentiles. Empty or non-numeric Me values indicate that the energy
      magnitude could not be computed or resulted in invalid values (NaN, null, +-inf)


    - **energy-magnitude.html** A report that can be opened in the user browser to
      visualize the computed energy magnitudes on maps and HTML tables


    - **[eventid1].xml, ..., [eventid1].xml** All processed events saved in QuakeMl
      format, updated with the information of their energy magnitude. Only events
      with valid Me will be saved


    - **energy-magnitude.log** the log file where the info, errors and warnings
      of the routine are stored. The core energy magnitude computation at station
      level (performed via `stream2segment` utilities) has a separated and more
      detailed log file (see below)


    - **station-energy-magnitude.log** the log file where the info, errors and
      warnings of the station energy magnitude computation have been stored


    Examples. In order to process all segments of the events occurred ...

    ... yesterday:

        me-compute OUT_DIR

    ... in the last 2 days:

        me-compute -t 2 OUT_DIR

    ... on January the 2nd and January the 3rd, 2016:

        process -s 2016-01-02 -t 2 OUT_DIR
    """
    if start is None and end is None and time_window is None:
        print('No time bounds specified. Please provide -s, -e or -t', file=sys.stderr)
        ret = False
    else:
        start, end = _get_timebounds(start, end, time_window)
        print(f'Computing Me for events within: [{start}, {end}]', file=sys.stderr)
        dest_dir = output_dir.replace("%S%", start).replace("%E%", end)
        ret = process(d_config, start, end, dest_dir,
                      force_overwrite=force_overwrite, p_config=p_config,
                      html_template=h_template)
    if ret:
        sys.exit(0)
    print('WARNING: the program did not complete successfully, '
          'please check log files for details',
          file=sys.stderr)
    sys.exit(1)


_REQUIRED_STATIONS_COLUMNS = [
    'event_db_id', 'station_latitude', 'station_longitude', 'network',
    'station', 'station_energy_magnitude', 'station_event_distance_deg'
]


def process(dconfig, start, end, dest_dir,
            force_overwrite=False,
            p_config=None, html_template=None):
    """process downloaded events computing their energy magnitude (Me)"""

    # # in case we want to query the db (e.g., min event, legacy code not used anymore):
    # from stream2segment.process import get_session
    # sess = get_session(dburl)
    # start = sess.query(sqlmin(Event.time)).scalar()  # (raises if multiple results)
    # close_session(sess)

    if not isdir(dest_dir):
        os.makedirs(dest_dir)
    if not isdir(dest_dir):
        raise OSError(f'Not a directory: {dest_dir}')

    base_name = 'energy-magnitude'

    # create output directory within destdir and assign new name:

    logger.setLevel(logging.INFO)
    logfile = abspath(join(dest_dir, base_name + '.log'))
    file_handler = logging.FileHandler(mode='w+',
                                       filename=logfile)
    logger.addHandler(file_handler)

    # set outfile
    station_me_file = join(dest_dir, 'station-' + base_name + '.hdf')

    try:
        with open(dconfig) as _:
            dburl = yaml.safe_load(_)['dburl']
            # make non abs-path relative to the download yaml file:
            sqlite = "sqlite:///"
            if dburl.lower().startswith(sqlite):
                dburl_ = dburl[len(sqlite):]
                if not isabs(dburl_):
                    dburl = "sqlite:///" + abspath(join(dirname(dconfig), dburl_))
    except (FileNotFoundError, yaml.YAMLError, KeyError) as exc:
        logger.error(f'Unable to read "dburl" from {dconfig}. '
                     f'Check that file exists and is a well-formed '
                     f'YAML')
        return False

    if isfile(station_me_file) and not force_overwrite:
        logger.warning('station energy magnitudes: file already exists. Delete file'
                       'or supply the force-overwrite flag is False')
        return False

    if not isfile(station_me_file) or force_overwrite:

        if p_config is None:
            p_config = PROCESS_CONFIG_PATH

        logger.info(f'Computing station energy magnitudes to file: '
                    f'{station_me_file}')

        segments_selection = {
            'event.time': '[%s, %s)' % (start, end),
            'has_valid_data': 'true',
            'maxgap_numsamples': '(-0.5, 0.5)',
            'event_distance_deg': '[20, 97.5]'
        }
        try:
            _compute_station_me(station_me_file, dburl, segments_selection, p_config)
            # all next files might now be outdated so we need to force updating them:
            force_overwrite = True
        except Exception as exc:
            logger.error('Error while computing station energy magnitude : ' + str(exc))
            return False

    else:
        logger.info(f'Fetching station energy magnitudes from {station_me_file}')

    try:
        station_me_df = pd.read_hdf(station_me_file,
                                    usecols=_REQUIRED_STATIONS_COLUMNS)
    except ValueError:
        logger.warning('Unable to read station energy magnitudes file. '
                       'This might be due to no Me computed (e.g., no segment, '
                       'all Me NaN)')
        return False

    if station_me_df.empty:  # noqa
        logger.warning('No station energy magnitude computed, check '
                       f'{basename(station_me_file)} log for details')
        return False

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
    quakeml_written = 0
    logger.info(f'Computing events energy magnitudes')
    for evt, stations in get_report_rows(station_me_df, dburl):
        csv_evts.append(evt)
        if ev_headers is None:
            ev_headers = list(csv_evts[0].keys())
        ev_catalog_url = evt['url']
        ev_catalog_id = evt['id']
        # write QuakeML:
        try:
            e_mag, e_mag_u = evt['Me'], evt['Me_stddev']
            if not pd.isna(e_mag) and not pd.isna(e_mag_u):
                quakeml_written += 1
                quakeml_file = join(dest_dir, ev_catalog_id + '.xml')
                _write_quekeml(quakeml_file, ev_catalog_url,
                               e_mag, e_mag_u, evt['Me_waveforms_used'],
                               author_uri, force_overwrite)
        except (OSError, HTTPError, HTTPException, URLError) as exc:
            logger.warning(f'Unable to create QuakeML for {ev_catalog_id}: {exc}')

        html_evts[evt['db_id']] = [[evt[h] for h in ev_headers], stations]
        if sel_event_id is None:
            sel_event_id = evt['db_id']

    if csv_evts:
        logger.info(f'Me computed for {len(csv_evts)} event(s)')
        if not quakeml_written:
            logger.warning(f'No Me computed (No QuakeML saved). Possible cause: '
                           f'station energy magnitudes all missing/NaN. Inspect station '
                           f'magnitudes file for details')
        else:
            logger.info(f'{quakeml_written} QuakeML(s) created')
    else:
        logger.warning(f'No Me computed: no events found)')

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
    return True


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

    writer_options = {
        'chunksize': 10000,
        # hdf needs a fixed length for all columns: if you write string columns
        # you need to tell in advance the size allocated with 'min_itemsize', e.g:
        'min_itemsize': {
            'network': 2,
            'station': 5,
            'location': 2,
            'channel': 3,
            # 'ev_mty': 2,
        }
    }

    s2s_process(main_function, outfile=outfile,
                segments_selection=segments_selection,
                append=False, writer_options=writer_options,
                dburl=dburl, verbose=True,
                config=p_config, logfile=logfile,
                multi_process=False, chunksize=None)


def _write_quekeml(dest_file, event_url, me, me_u=None, me_stations=None,
                   author="", force_overwrite=False):
    with pd.option_context('mode.use_inf_as_na', True):
        if pd.isna(me):
            raise URLError('Me is N/A (nan, +-inf, None)')

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
