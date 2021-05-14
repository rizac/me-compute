"""
Command line interface (cli) of the program, type `python cli.py --help` for details
on the terminal
"""
# import sys
# import yaml
import json
import sys
from datetime import datetime, date, timedelta
# from sqlalchemy.sql.functions import max as sqlmax, min as sqlmin
from os.path import abspath, join, dirname, isdir, basename, splitext, isfile, relpath
from os import listdir, makedirs

import click
from jinja2 import Template

# from stream2segment.process import close_session
# from stream2segment.process.db.models import Event
from stream2segment.process import yaml_load
from stream2segment.cli import process as s2s_process  # , download as s2s_download

from stats import get_report_rows, Stats

s2s_config_dir = join(dirname(__file__), "s2s_config")


DTIME_FORMATS = ('%Y-%m-%d', '%Y-%m-%dT%H:%M:%S')  # , '%Y-%m-%dT%H:%M:%S.%f')


class DateTime(click.DateTime):
    """Extends click.DateTime by supplying a better metavar ('DATETIME') and our
    custom valid formats
    """

    def __init__(self):
        super().__init__(formats=DTIME_FORMATS)

    def get_metavar(self, param):
        return "DATETIME"


class ResultDir(str):
    """String subclass denoting a result directory, it implements the start and end
    attributes (datetime) as well as the result attribute (path to the result file)
    """

    DIR_PREFIX = 'mecomputed'
    # separator for the different components of the directory basename. Its 1st char C
    # should have ord(C) < ord('T') = 84, so that a datetime in isoformat comes after
    # its date in isoformat (dir names can be approximated to dates when possible)
    FILENAME_SEPARATOR = '--'
    # safety check:
    assert FILENAME_SEPARATOR not in DIR_PREFIX, 'ResultDir.DIR_PREFIX contains ' \
                                                 '"%s"' % FILENAME_SEPARATOR
    FILE_BASENAME = 'process'

    def __new__(cls, root, start, end, *, mkdir=False):
        start, end = cls.to_datetime(start), cls.to_datetime(end)
        if start is None or end is None:
            raise ValueError('invalid time bounds for directory name')
        dir_name = cls.FILENAME_SEPARATOR.join([cls.DIR_PREFIX,
                                                cls.isoformat(start),
                                                cls.isoformat(end)])
        fullpath = join(root, dir_name)

        if mkdir and not isdir(fullpath):
            makedirs(fullpath)
            if not isdir(fullpath):
                raise ValueError('Unable to create directory "%s"' % fullpath)

        return super().__new__(cls, fullpath)

    @property
    def filename_prefix(self):
        """Returns the prefix for all filenames to be created in this directory.
        See `process` for details
        """
        return self.FILE_BASENAME + basename(self)[len(self.DIR_PREFIX):]

    @staticmethod
    def isoformat(obj, simplify=True):
        """iso-formatted string with `simpfily` flag: if True, format datetimes at
        midnight without time information

        :param obj: date or datetime
        :param simplify: if True (the default) datetimes at midnight will be formatted
            as if they were dates (removing trailing redundant zeros denoting time)
        """
        try:
            is_simple = obj.hour == obj.minute == obj.second == obj.microsecond == 0
            if simplify and is_simple:
                return date(year=obj.year, month=obj.month, day=obj.day).isoformat()
            return obj.isoformat(sep='T')
        except AttributeError:  # date object
            return obj.isoformat()

    @staticmethod
    def to_datetime(obj):
        """Convert obj to datetime. Return None if the object is invalid

        :param obj: date, datetime or string ISO formatted.
        """
        if isinstance(obj, datetime):
            return obj
        if isinstance(obj, date):
            return datetime(year=obj.year, month=obj.month, day=obj.day)
        for format_ in DTIME_FORMATS:
            try:
                return datetime.strptime(obj, format_)
            except Exception:  # noqa
                pass
        return None

    @classmethod
    def get_resultfile_path(cls, path):
        """Returns True if the given path exists denotes a ResultDir (name well
        formatted and result HDF file in it)"""
        try:
            name, start, end = basename(path).split(cls.FILENAME_SEPARATOR)
            if name != cls.DIR_PREFIX:
                return None
            resdir = ResultDir(dirname(path), start, end, mkdir=False)
            filepath = join(resdir, resdir.filename_prefix + '.hdf')
            return filepath if isfile(filepath) else None
        except Exception:
            return None


###########################
# Me cpompute cli commands:
###########################

cli = click.Group()


# (https://click.palletsprojects.com/en/5.x/advanced/#invoking-other-commands)
@cli.command(context_settings=dict(max_content_width=89),)
@click.option('end', '-e', type=DateTime(), default=None,
              help="end time (UTC ISO-formatted) of the events to consider. If missing "
                   "it is set as `start` plus `duration` days. If `start` is also "
                   "missing, it defaults as today at midnight (00h:00m:00s)")
@click.option('duration', '-d', type=int, default=7,
              help="duration in days of the time window to consider. If missing, "
                   "defaults to 7. If both time bounds (start, end) are provided, it is "
                   "ignored, if no time bounds are provided, it will set start as "
                   "`duration` days ago. In any other case, it will set the missing "
                   "time bound")
@click.option('start', '-s', type=DateTime(), default=None,
              help="start time (UTC ISO-formatted) of the events to consider. If "
                   "missing, it is set as `end` minus `duration` days")
@click.argument('root_out_dir', required=False)
@click.pass_context
def process(context, end, duration, start, root_out_dir):
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

    start, end = _get_timebounds(start, duration, end)

    # # in case we want to query the db (e.g., min event, legacy code not used anymore):
    # from stream2segment.process import get_session
    # sess = get_session(dburl)
    # start = sess.query(sqlmin(Event.time)).scalar()  # (raises if multiple results)
    # close_session(sess)

    # create output directory within destdir and assign new name:
    destdir = ResultDir(root_out_dir, start, end, mkdir=True)

    # get processing file:
    pyfile = join(s2s_config_dir, 'process.py')

    # get base config, set event time ranges in segments selection and save a new config
    # in the destination directory. Note that this is way faster than yaml load and dump:
    config_in = join(s2s_config_dir, 'process.yaml')
    with open(config_in) as _:
        template = Template(_.read())
    config = join(destdir, destdir.filename_prefix + '.yaml')
    with open(config, 'w') as _:
        evt_time_range = '(%s, %s]' % (start.isoformat(), end.isoformat())
        _.write(template.render(event_time_range=evt_time_range))

    # set logfile:
    log = join(destdir, destdir.filename_prefix + '.log')

    # set outfile
    outfile = join(destdir, destdir.filename_prefix + '.hdf')

    # context.forward(test)
    dburl = yaml_load(join(s2s_config_dir, 'download.private.yaml'))['dburl']
    context.invoke(s2s_process, dburl=dburl, config=config, pyfile=pyfile,
                   logfile=log, outfile=outfile)


def _get_timebounds(start, duration, end):
    """start, end: datetimes or None. Duration:int"""
    if end is None and start is None:
        end = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=duration)
    elif end is None:
        end = start + timedelta(days=duration)
    elif start is None:
        start = end - timedelta(days=duration)
    return start, end


# (https://click.palletsprojects.com/en/5.x/advanced/#invoking-other-commands)
@cli.command(context_settings=dict(max_content_width=89),)
@click.argument('input', required=False)
def report(input):
    """
    Create HTML report from a given HDF file generated wiuth the process command
     Magnitude energy data using stream2segment and saving the HDF file into
    a child directory of the given root output directory. the child directory will be
    named according the desired time window given as argument

    INPUT: the root directory given as argument of the `process` command
    (a report will be created for all HDFs found therein) or a single HDF file generated
    with the command.

    Each HTML report will be created with the same HDF name replacing the file
    extension with 'html'
    """
    if isfile(input):
        input = [input]
    else:
        if not isdir(input):
            print('input must be an existing file or directory')
            sys.exit(1)
        input = [ResultDir.get_resultfile_path(join(input, _)) for _ in listdir(input)]
        input = [_ for _ in input if _ is not None]  # filter out Nones
        if not input:
            print('input does not seem to be a root directory where process '
                  'result are stored')
            sys.exit(1)

    print("%d report(s) to be generated" % len(input))
    config_in = join(dirname(__file__), 'report.template.html')
    with open(config_in) as _:
        template = Template(_.read())

    desc = Stats.as_help_dict()
    ret = 1
    for fle in input:
        title = basename(fle)
        output, ext = splitext(fle)
        try:
            with open(output + '.html', 'w') as _:
                # data = [_ for _ in get_report_rows(fle)]
                evts, stas = [], []
                for evts_, stas_ in get_report_rows(fle):
                    evts.append(evts_)
                    stas.append(stas_)
                _.write(template.render(title=title, data=evts, description=desc,
                                        filename=basename(fle),
                                        stations=json.dumps(stas, separators=(',', ':'))))
                # if at least one report is generated, return 0 at the end. Thus:
                ret = 0
        except Exception as exc:
            print('ERROR generating %s: %s' % (output, str(exc)))

    sys.exit(ret)


if __name__ == '__main__':
    cli()  # noqa
