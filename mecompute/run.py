"""
Command line interface (cli) of the program, type `python cli.py --help` for details
on the terminal
"""
import json
import os
import sys
import csv
from datetime import datetime, date, timedelta
from os.path import join, dirname, isdir, basename, splitext, isfile

import click
import pandas as pd
import yaml
from jinja2 import Template
from stream2segment.io.inputvalidation import BadParam
from stream2segment.process.db.models import WebService

from mecompute.stats import get_report_rows, Stats
from stream2segment.process import process as s2s_process, imap
from mecompute.process import main as main_function


_CONFIG_DIR = join(dirname(__file__), 'base-config')
PROCESS_CONFIG_PATH = join(_CONFIG_DIR, 'process.yaml')
REPORT_TEMPLATE_PATH = join(_CONFIG_DIR, 'report.template.html')
assert isfile(PROCESS_CONFIG_PATH)
assert isfile(REPORT_TEMPLATE_PATH)


###########################
# Me compute cli commands:
###########################


cli = click.Group()


# (https://click.palletsprojects.com/en/5.x/advanced/#invoking-other-commands)
@cli.command(context_settings=dict(max_content_width=89),)
@click.option('config', '-c', type=click.Path(exists=True),
              help=f"The path of the configuration file. You can edit and use the file "
                   f"provided by default in the git repository "
                   f"(me-compute/config/download.yaml)")
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
@click.option('duration', '-t', type=int, default=1,
              help="time window, in days. If missing, "
                   "defaults to 1. If both time bounds (start, end) are provided, it is "
                   "ignored")
@click.option('dconfig', '-d', type=click.Path(exists=True),
              help=f"The path of the download configuration file used to download "
                   f"the data. Used get the URL of the database where events and "
                   f"waveforms will be fetched (all other properties will be ignored)")
@click.option('config', '-c', type=click.Path(exists=True),
              default=PROCESS_CONFIG_PATH,
              help=f"The path of the configuration file used for processing the data. "
                   f"If missing, the default configuration will be used (see the file "
                   f"provided by default in the git repository for details: "
                   f"me-compute/config/process.yaml)")
@click.argument('root_output_dir', required=True)
def process(start, end, duration, config, dconfig, root_output_dir):
    """
    process downloaded events computing their Magnitude energy.

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
    try:
        with open(dconfig) as _:
            dburl = yaml.safe_load(_)['dburl']
    except (FileNotFoundError, yaml.YAMLError, KeyError):
        print(f'Error reading "dburl" from {dconfig}. Check that file exists '
              f'and is a well-formed YAML', file=sys.stderr)
        sys.exit(1)

    start, end = _get_timebounds(start, end, duration)

    # # in case we want to query the db (e.g., min event, legacy code not used anymore):
    # from stream2segment.process import get_session
    # sess = get_session(dburl)
    # start = sess.query(sqlmin(Event.time)).scalar()  # (raises if multiple results)
    # close_session(sess)

    base_name = _get_processing_output_dirname(start, end, dburl)

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

    # handle string columns:
    # store each column possible values in a dict (handle str vs categorical at the end):
    categorical_columns = {  # for
        'network': {},
        'station': {},
        'location': {},
        'channel': {},
        'event_magnitude_type': {},
        'event_catalog_url': {}
    }
    # option to write str columns to dataframe:
    min_itemsize = {}
    processed_waveforms = []
    try:
        for res_dict in imap(main_function,
                             segments_selection=segments_selection,
                             dburl=dburl,
                             config=config, logfile=logfile,
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

    except BadParam as bpar:
        print(f'ERROR: {str(bpar)}', file=sys.stderr)
        sys.exit(1)

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


def _get_processing_output_dirname(start, end, dburl):
    sep = '__'  # file separator
    base_name_prefix = 'me-compute'
    return base_name_prefix + sep + basename(dburl) + sep + start + sep + end


# (https://click.palletsprojects.com/en/5.x/advanced/#invoking-other-commands)
@cli.command(context_settings=dict(max_content_width=89),)
@click.option('-f', '--force-overwrite', is_flag=True,
              help='Force overwrite HTML if it already exists. Default is false '
                   '(skip process if HTML exists)')
@click.option('-t', '--html-template', type=click.Path(exists=True),
              default=REPORT_TEMPLATE_PATH,
              help=f"The path of the HTML template file used to build the output "
                   f"report. This parameter is for users experienced with jina2 who "
                   f"need to customize the report appearance. If missing, "
                   f"the default template will be used (see the file provided "
                   f"by default in the git repository for details: "
                   f"me-compute/config/report.template.html)")
@click.argument('input', required=False, nargs=-1)
def report(force_overwrite, html_template, input):
    """
    Create HTML visual report and QuakeML(s) from the output of the process command.

    INPUT: the path or list of paths (space separated) to the HDF file(s)
        generated with the `process` command. Each report file (HTML, QuakeML)
        will be saved in the same directory of the input
    """
    input_count = len(input)
    print("%d process file(s) found" % input_count)
    # collect a dict mapping process file -> report file but only for files not existing
    # (unless force_overwrite is True):
    input = [(_, splitext(_)[0] + '.html', splitext(_)[0] + '.csv')
             for _ in input if force_overwrite or not isfile(splitext(_)[0] + '.html')]
    if not input:
        print("No report to be generated")
        if input_count:
            print('If you want to overwrite existing file, use option -f')
        sys.exit(0)
    else:
        print("%d report(s) to be generated" % len(input))

    with open(html_template) as _:
        template = Template(_.read())

    desc = Stats.as_help_dict()
    written = 0
    for process_fpath, report_fpath, csv_fpath in input:
        title = splitext(basename(process_fpath))[0]
        try:
            evts, evts_sations = [], {}
            for evt, stations in get_report_rows(process_fpath):
                evid = evt['id']
                evts.append(evt)
                evts_sations[evid] = [evt['latitude'], evt['longitude'],
                                      stations]

            if not evts:
                continue
            with open(report_fpath, 'w') as _:
                events_select = {}
                events_table = {}
                events_url = {}
                sel_event_id = evts[0]['id']
                for evt in evts:
                    ev_id = evt.pop('id')
                    ev_catalog_url = evt.pop('catalog_url')
                    ev_catalog_id = ev_catalog_url.split('=')[-1]
                    # map id to the name displayed on the select:
                    events_select[ev_id] = ev_catalog_id
                    events_url[ev_id] = ev_catalog_url
                    # populate the table:
                    table = ''
                    for key, val in evt.items():
                        if key == 'time':
                            val = val.replace('T', '<br>')
                        table += f"<tr><td>{key}</td><td>{val}</td></tr>"
                    events_table[ev_id] = table

                _.write(template.render(title=title,
                                        events_url=events_url,
                                        events_select=events_select,
                                        events_table=events_table,
                                        selected_event_id=sel_event_id,
                                        description=desc,
                                        event_stations=evts_sations))
            written += 1
            with open(csv_fpath, 'w', newline='') as _:
                fieldnames = evts[0].keys()
                writer = csv.DictWriter(_, fieldnames=fieldnames)
                writer.writeheader()
                for evt in evts:
                    writer.writerow(evt)
        except Exception as exc:
            print('ERROR: %s while generating %s: %s' % (exc.__class__.__name__,
                                                         report_fpath, str(exc)),
                  file=sys.stderr)
            import traceback
            traceback.print_exception(exc)
            # sys.exit(1)
    print("%d reports generated" % written)
    sys.exit(0)


if __name__ == '__main__':
    cli()  # noqa
