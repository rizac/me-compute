'''
This editable template shows how to generate a segment-based parametric table.
When used for visualization, this template implements a pre-processing function that
can be toggled with a checkbox, and several task-specific functions (e.g., cumulative,
synthetic Wood-Anderson) that can to be visualized as custom plots in the GUI

============================================================================================
stream2segment Python file to implement the processing/visualization subroutines: User guide
============================================================================================

This module needs to implement one or more functions which will be described in the sections below.
**All these functions must have the same signature**:
```
    def myfunction(segment, config):
```
where `segment` is the Python object representing a waveform data segment to be processed
and `config` is the Python dictionary representing the given configuration file.

After editing, this file can be invoked from the command line commands `s2s process` and `s2s show`
with the `-p` / `--pyfile` option (type `s2s process --help` or `s2s show --help` for details).
In the first case, see section 'Processing' below, otherwise see section 'Visualization (web GUI)'.


Processing
==========

When processing, the program will search for a function called "main", e.g.:
```
def main(segment, config)
```
the program will iterate over each selected segment (according to 'segment_select' parameter
in the config) and execute the function, writing its output to the given file, if given.
If you do not need to use this module for visualizing stuff, skip the section 'Visualization'
below and go to "Functions implementation".


Visualization (web GUI)
=======================

When visualizing, the program will fetch all segments (according
to 'segment_select' parameter in the config), and open a web page where the user can browse and
visualize each segment one at a time.
The page shows by default on the upper left corner a plot representing the segment trace(s).
The GUI can be customized by providing here functions decorated with
"@gui.preprocess" (pre-process function) or "@gui.plot" (plot function).

Pre-process function
--------------------

The function decorated with "@gui.preprocess", e.g.:
```
@gui.preprocess
def applybandpass(segment, config)
```
will be associated to a check-box in the GUI. By clicking the check-box,
all plots of the page will be re-calculated with the output of this function,
which **must thus return an ObsPy Stream or Trace object**.

Plot functions
--------------

The function decorated with "@gui.plot", e.g.:
```
@gui.plot
def cumulative(segment, config)
...
```
will be associated to (i.e., its output will be displayed in) the plot below the main plot.
You can also call @gui.plot with arguments, e.g.:
```
@gui.plot(position='r', xaxis={'type': 'log'}, yaxis={'type': 'log'})
def spectra(segment, config)
...
```
The 'position' argument controls where the plot will be placed in the GUI ('b' means bottom,
the default, 'r' means next to the main plot, on its right) and the other two, `xaxis` and
`yaxis`, are dict (defaulting to the empty dict {}) controlling the x and y axis of the plot
(for info, see: https://plot.ly/python/axes/).

When not given, axis types (e.g., date time vs numeric) will be inferred from the
function's returned value which *must* be a numeric sequence (y values) taken at successive
equally spaced points (x values) in any of these forms:

- ObsPy Trace object

- ObsPy Stream object

- the tuple (x0, dx, y) or (x0, dx, y, label), where

    - x0 (numeric, `datetime` or `UTCDateTime`) is the abscissa of the first point.
      For time-series abscissas, UTCDateTime is quite flexible with several input formats.
      For info see: https://docs.obspy.org/packages/autogen/obspy.core.utcdatetime.UTCDateTime.html

    - dx (numeric or `timedelta`) is the sampling period. If x0 has been given as date-time
      or UTCDateTime object and 'dx' is numeric, its unit is in seconds
      (e.g. 45.67 = 45 seconds and 670000 microseconds). If `dx` is a timedelta object and
      x0 has been given as numeric, then x0 will be converted to UtcDateTime(x0).

    - y (numpy array or numeric list) are the sequence values, numeric

    - label (string, optional) is the sequence name to be displayed on the plot legend.

- a dict of any of the above types, where the keys (string) will denote each sequence
  name to be displayed on the plot legend (and will override the 'label' argument, if provided)

Functions implementation
========================

The implementation of the functions is user-dependent. As said, all functions needed for
processing and visualization must have the same signature:
```
    def myfunction(segment, config):
```

any Exception raised will be handled this way:

* if the function is called for visualization, the exception will be caught and its message
  displayed on the plot

* if the function is called for processing, the exception will raise as usual, interrupting
  the routine, with one special case: `ValueError`s will interrupt the currently processed segment
  only (the exception message will be logged) and continue the execution to the next segment.
  This feature can also be triggered programmatically to skip the currently processed segment and
  log the error for later inspection, e.g.:
    `raise ValueError("segment sample rate too low")`
  (thus, do not issue `print` statements for debugging as it's useless, and a bad practice overall)

Conventions and suggestions
---------------------------

Handling exceptions at any point of the processing, especially when launching a very long routine,
is non trivial: you might want the execution to skip a segment and continue
smoothly if, e.g., its inventory is malformed and could not be read. But at the same time you want
the routine to stop if your code has bugs, to let you fix them.

We therefore suggest to run your code on a smaller and possibly heterogeneous dataset
first (changing temporarily the segment selection in the configuration file) in order
to check 1. bugs to fix and 2. errors to be ignored by means of `raise ValueError` statements whose
message will be logged to file. A faster solution (which we do not recommend) is to wrap all your
code into a try-except that always raises a ValueError (with the exception message), but remember
that this will hide your code bugs: you will then need to inspect the log file often, especially at
the beginning of the whole execution, and be be ready to stop it, if you see some error message
indicating a bug or unexpected result.

In any case, please spend some time on the configuration file's segment selection: you might find
that your code runs smoothly and faster by simply skipping certain segments in
the first place.

This module is designed to encourage the decoupling of code and configuration.
Avoid having e.g., several almost identical Python modules which differ only for a small set of
hard coded parameters: implement a single Python module and write different parameter sets in
several YAML configurations in case.


Functions arguments
-------------------

config (dict)
~~~~~~~~~~~~~

This is the dictionary representing the chosen configuration file (usually, via command line)
in YAML format (see documentation therein). Any property defined in the configuration file, e.g.:
```
outfile: '/home/mydir/root'
mythreshold: 5.67
```
will be accessible via `config['outfile']`, `config['mythreshold']`

segment (object)
~~~~~~~~~~~~~~~~

Technically it's like an 'SqlAlchemy` ORM instance but for the user it is enough to
consider and treat it as a normal Python object. It features special methods and
several attributes returning Python "scalars" (float, int, str, bool, datetime, bytes).
Each attribute can be considered as segment metadata: it reflects a segment column
(or an associated database table via a foreign key) and returns the relative value.

segment methods:
----------------

* segment.stream(reload=False): the `obspy.Stream` object representing the waveform data
  associated to the segment. Please remember that many ObsPy functions modify the
  stream in-place:
  ```
      stream_remresp = segment.stream().remove_response(segment.inventory())
      # any call to segment.stream() returns from now on `stream_remresp`
  ```
  For any case where you do not want to modify `segment.stream()`, copy the stream
  (or its traces) first, e.g.:
  ```
      stream_raw = segment.stream()
      stream_remresp = stream_raw.copy().remove_response(segment.inventory())
      # any call to segment.stream() will still return `stream_raw`
  ```
  You can also pass a boolean value (False by default when missing) to `stream` to force
  reloading it from the database (this is less performant as it resets the cached value):
  ```
      stream_remresp = segment.stream().remove_response(segment.inventory())
      stream_reloaded = segment.stream(True)
      # any call to segment.stream() returns from now on `stream_reloaded`
  ```
  (In visualization functions, i.e. those decorated with '@gui', any modification
  to the segment stream will NOT affect the segment's stream in other functions)

  For info see https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.copy.html

* segment.inventory(reload=False): the `obspy.core.inventory.inventory.Inventory`.
  This object is useful e.g., for removing the instrumental response from `segment.stream()`:
  note that it will be available only if the inventories in xml format were downloaded in
  the downloaded subroutine. As for `stream`, you can also pass a boolean value
  (False by default when missing) to `inventory` to force reloading it from the database.

* segment.siblings(parent=None, condition): returns an iterable of siblings of this segment.
  `parent` can be any of the following:
  - missing or None: returns all segments of the same recorded event, on the
    other channel components / orientations
  - 'stationname': returns all segments of the same station, identified by the tuple of the
    codes (newtwork, station)
  - 'networkname': returns all segments of the same network (network code)
  - 'datacenter', 'event', 'station', 'channel': returns all segments of the same datacenter, event,
    station or channel, all identified by the associated database id.
  `condition` is a dict of expression to filter the returned element. the argument
  `config['segment_select]` can be passed here to return only siblings selected for processing.
  NOTE: Use with care when providing a `parent` argument, as the amount of segments might be huge
  (up to hundreds of thousands of segments). The amount of returned segments is increasing
  (non linearly) according to the following order of the `parent` argument:
  'channel', 'station', 'stationname', 'networkname', 'event' and 'datacenter'

* segment.del_classes(*labels): Deletes the given classes of the segment. The argument is
  a comma-separated list of class labels (string). See configuration file for setting up the
  desired classes.
  E.g.: `segment.del_classes('class1')`, `segment.del_classes('class1', 'class2', 'class3')`

* segment.set_classes(*labels, annotator=None): Sets the given classes on the segment,
  deleting first all segment classes, if any. The argument is
  a comma-separated list of class labels (string). See configuration file for setting up the
  desired classes. `annotator` is a keyword argument (optional): if given (not None) denotes the
  user name that annotates the class.
  E.g.: `segment.set_classes('class1')`, `segment.set_classes('class1', 'class2', annotator='Jim')`

* segment.add_classes(*labels, annotator=None): Same as `segment.set_classes` but does not
  delete segment classes first. If a label is already assigned to the segment, it is not added
  again (regardless of whether the 'annotator' changed or not)

* segment.sds_path(root='.'): Returns the segment's file path in a seiscomp data
  structure (SDS) format:
     <root>/<event_id>/<net>/<sta>/<loc>/<cha>.D/<net>.<sta>.<loc>.<cha>.<year>.<day>
  See https://www.seiscomp3.org/doc/applications/slarchive/SDS.html for details.
  Example: to save the segment's waveform as miniSEED you can type (explicitly
  adding the file extension '.mseed' to the output path):
  ```
      segment.stream().write(segment.sds_path() + '.mseed', format='MSEED')
  ```

* segment.dbsession(): returns the database session for custom IO operations with the database.
  WARNING: this is for advanced users experienced with SQLAlchemy library. If you want to
  use it you probably want to import stream2segment in custom code. See the github documentation
  in case

segment attributes:
-------------------

===================================== ==============================================================
Segment attribute                     Python type and (optional) description
===================================== ==============================================================
segment.id                            int: segment (unique) db id
segment.event_distance_deg            float: distance between the segment's station and
                                      the event, in degrees
segment.event_distance_km             float: distance between the segment's station and
                                      the event, in km, assuming a perfectly spherical earth
                                      with a radius of 6371 km
segment.start_time                    datetime.datetime: the waveform data start time
segment.arrival_time                  datetime.datetime: the station's arrival time of the waveform.
                                      Value between 'start_time' and 'end_time'
segment.end_time                      datetime.datetime: the waveform data end time
segment.request_start                 datetime.datetime: the requested start time of the data
segment.request_end                   datetime.datetime: the requested end time of the data
segment.duration_sec                  float: the waveform data duration, in seconds
segment.missing_data_sec              float: the number of seconds of missing data, with respect
                                      to the requested time window. It might also be negative
                                      (more data received than requested). This parameter is useful
                                      when selecting segments: e.g., if we requested 5
                                      minutes of data and we want to process segments with at
                                      least 4 minutes of downloaded data, then:
                                      missing_data_sec: '< 60'
segment.missing_data_ratio            float: the portion of missing data, with respect
                                      to the request time window. It might also be negative
                                      (more data received than requested). This parameter is useful
                                      when selecting segments: e.g., if you want to process
                                      segments whose real time window is at least 90% of the
                                      requested one, then: missing_data_ratio: '< 0.1'
segment.sample_rate                   float: the waveform data sample rate.
                                      It might differ from the segment channel's sample_rate
segment.has_data                      boolean: tells if the segment has data saved (at least
                                      one byte of data). This parameter is useful when selecting
                                      segments (in most cases, almost necessary), e.g.:
                                      has_data: 'true'
segment.download_code                 int: the code reporting the segment download status. This
                                      parameter is useful to further refine the segment selection
                                      skipping beforehand segments with malformed data (code -2):
                                      has_data: 'true'
                                      download_code: '!=-2'
                                      (All other codes are generally of no interest for the user.
                                      However, for details see Table 2 in
                                      https://doi.org/10.1785/0220180314#tb2)
segment.maxgap_numsamples             float: the maximum gap or overlap found in the waveform data,
                                      in number of points. If 0, the segment has no gaps/overlaps.
                                      Otherwise, if >=1: the segment has gaps, if <=-1: the segment
                                      has overlaps. Values in (-1, 1) are difficult to interpret: a
                                      rule of thumb is to consider half a point a gap / overlap
                                      (maxgap_numsamples > 0.5 or maxgap_numsamples < -0.5).
                                      This parameter is useful when selecting segments: e.g.,
                                      to select segments with no gaps/overlaps, then:
                                      maxgap_numsamples: '(-0.5, 0.5)'
segment.seed_id                       str: the seed identifier in the typical format
                                      [Network].[Station].[Location].[Channel]. For segments
                                      with waveform data, `data_seed_id` (see below) might be
                                      faster to fetch.
segment.data_seed_id                  str: same as 'segment.seed_id', but faster to get because it
                                      reads the value stored in the waveform data. The drawback
                                      is that this value is null for segments with no waveform data
segment.has_class                     boolean: tells if the segment has (at least one) class
                                      assigned
segment.data                          bytes: the waveform (raw) data. Used by `segment.stream()`
------------------------------------- ------------------------------------------------
segment.event                         object (attributes below)
segment.event.id                      int
segment.event.event_id                str: the id returned by the web service or catalog
segment.event.time                    datetime.datetime
segment.event.latitude                float
segment.event.longitude               float
segment.event.depth_km                float
segment.event.author                  str
segment.event.catalog                 str
segment.event.contributor             str
segment.event.contributor_id          str
segment.event.mag_type                str
segment.event.magnitude               float
segment.event.mag_author              str
segment.event.event_location_name     str
------------------------------------- ------------------------------------------------
segment.channel                       object (attributes below)
segment.channel.id                    int
segment.channel.location              str
segment.channel.channel               str
segment.channel.depth                 float
segment.channel.azimuth               float
segment.channel.dip                   float
segment.channel.sensor_description    str
segment.channel.scale                 float
segment.channel.scale_freq            float
segment.channel.scale_units           str
segment.channel.sample_rate           float
segment.channel.band_code             str: the first letter of channel.channel
segment.channel.instrument_code       str: the second letter of channel.channel
segment.channel.orientation_code      str: the third letter of channel.channel
segment.channel.station               object: same as segment.station (see below)
------------------------------------- ------------------------------------------------
segment.station                       object (attributes below)
segment.station.id                    int
segment.station.network               str: the station's network code, e.g. 'AZ'
segment.station.station               str: the station code, e.g. 'NHZR'
segment.station.netsta_code           str: the network + station code, concatenated with
                                      the dot, e.g.: 'AZ.NHZR'
segment.station.latitude              float
segment.station.longitude             float
segment.station.elevation             float
segment.station.site_name             str
segment.station.start_time            datetime.datetime
segment.station.end_time              datetime.datetime
segment.station.has_inventory         boolean: tells if the segment's station inventory has
                                      data saved (at least one byte of data).
                                      This parameter is useful when selecting segments: e.g.,
                                      to select only segments with inventory downloaded:
                                      station.has_inventory: 'true'
segment.station.datacenter            object (same as segment.datacenter, see below)
------------------------------------- ------------------------------------------------
segment.datacenter                    object (attributes below)
segment.datacenter.id                 int
segment.datacenter.station_url        str
segment.datacenter.dataselect_url     str
segment.datacenter.organization_name  str
------------------------------------- ------------------------------------------------
segment.download                      object (attributes below): the download execution
segment.download.id                   int
segment.download.run_time             datetime.datetime
------------------------------------- ------------------------------------------------
segment.classes.id                    int: the id(s) of the classes assigned to the segment
segment.classes.label                 int: the label(s) of the classes assigned to the segment
segment.classes.description           int: the description(s) of the classes assigned to the
                                      segment
===================================== ================================================
'''

from __future__ import division

# make the following(s) behave like python3 counterparts if running from python2.7.x
# (http://python-future.org/imports.html#explicit-imports). UNCOMMENT or REMOVE
# if you are working in Python3 (recommended):
from builtins import (ascii, bytes, chr, dict, filter, hex, input,
                      int, map, next, oct, open, pow, range, round,
                      str, super, zip)

# From Python >= 3.6, dicts keys are returned (and thus, written to file) in the order they
# are inserted. Prior to that version, to preserve insertion order you needed to use OrderedDict:
from collections import OrderedDict
from datetime import datetime, timedelta  # always useful
from math import factorial  # for savitzky_golay function

# import numpy for efficient computation:
import numpy as np
# import obspy core classes (when working with times, use obspy UTCDateTime when possible):
from obspy import Trace, Stream, UTCDateTime
from obspy.geodetics import degrees2kilometers as d2km
# decorators needed to setup this module @gui.preprocess @gui.plot:
from stream2segment.process import gui
# strem2segment functions for processing obspy Traces. This is just a list of possible functions
# to show how to import them:
from stream2segment.process.math.traces import ampratio, bandpass, cumsumsq,\
    timeswhere, fft, maxabs, utcdatetime, ampspec, powspec, timeof, sn_split
# stream2segment function for processing numpy arrays:
from stream2segment.process.math.ndarrays import triangsmooth, snr


def main(segment, config):
    """Main processing function. The user should implement here the processing for any given
    selected segment. Useful links for functions, libraries and utilities:

    - `stream2segment.process.math.traces` (small processing library implemented in this program,
      most of its functions are imported here by default)
    - `ObpPy <https://docs.obspy.org/packages/index.html>`_
    - `ObsPy Stream object <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.html>_`
    - `ObsPy Trace object <https://docs.obspy.org/packages/autogen/obspy.core.trace.Trace.html>_`

    IMPORTANT: Any exception raised by this routine will be logged to file for inspection.
        All exceptions will interrupt the whole execution, only exceptions of type `ValueError`
        will interrupt the execution of the currently processed segment and continue to the
        next segment, as ValueErrors might not always denote critical code errors. This feature can
        also be triggered programmatically to skip the currently processed segment
        and log the message for later inspection, e.g.:
        ```
        if snr < 0.4:
            raise ValueError('SNR ratio too low')
        ```

    :param: segment (Python object): An object representing a waveform data to be processed,
        reflecting the relative database table row. See above for a detailed list
        of attributes and methods

    :param: config (Python dict): a dictionary reflecting what has been implemented in the configuration
        file. You can write there whatever you want (in yaml format, e.g. "propertyname: 6.7" ) and it
        will be accessible as usual via `config['propertyname']`

    :return: If the processing routine calling this function needs not to generate a file output,
        the returned value of this function, if given, will be ignored.
        Otherwise:

        * For CSV output, this function must return an iterable that will be written as a row of the
          resulting file (e.g. list, tuple, numpy array, dict. You must always return the same type
          of object, e.g. not lists or dicts conditionally).

          Returning None or nothing is also valid: in this case the segment will be silently skipped

          The CSV file will have a row header only if `dict`s are returned (the dict keys will be the
          CSV header columns). For Python version < 3.6, if you want to preserve in the CSV the order
          of the dict keys as the were inserted, use `OrderedDict`.

          A column with the segment database id (an integer uniquely identifying the segment)
          will be automatically inserted as first element of the iterable, before writing it to file.

          SUPPORTED TYPES as elements of the returned iterable: any Python object, but we
          suggest to use only strings or numbers: any other object will be converted to string
          via `str(object)`: if this is not what you want, convert it to the numeric or string
          representation of your choice. E.g., for Python `datetime`s you might want to set
          `datetime.isoformat()` (string), for ObsPy's `UTCDateTime`s `float(utcdatetime)` (numeric)

       * For HDF output, this function must return a dict, pandas Series or pandas DataFrame
         that will be written as a row of the resulting file (or rows, in case of DataFrame).

         Returning None or nothing is also valid: in this case the segment will be silently skipped.

         A column named 'Segment.db.id' with the segment database id (an integer uniquely identifying the segment)
         will be automatically added to the dict / Series, or to each row of the DataFrame,
         before writing it to file.

         SUPPORTED TYPES as elements of the returned dict/Series/DataFrame: all types supported
         by pandas: https://pandas.pydata.org/pandas-docs/stable/getting_started/basics.html#dtypes

         For info on hdf and the pandas library (included in the package), see:
         https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_hdf.html
         https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-hdf5
    """
    stream = segment.stream()
    assert1trace(stream)  # raise and return if stream has more than one trace
    trace = stream[0]  # work with the (surely) one trace now

    # discard saturated signals (according to the threshold set in the config file):
    amp_ratio = ampratio(trace)
    if amp_ratio >= config['amp_ratio_threshold']:
        raise ValueError('possibly saturated (amp. ratio exceeds)')

    # bandpass the trace, according to the event magnitude.
    # WARNING: this modifies the segment.stream() permanently!
    # If you want to preserve the original stream, store trace.copy() beforehand.
    # Also, use a 'try catch': sometimes Inventories are corrupted and obspy raises
    # a TypeError, which would break the WHOLE processing execution.
    # Raising a ValueError will stop the execution of the currently processed
    # segment only (logging the error message):
    try:
        trace = bandpass_remresp(segment, config)
    except TypeError as type_error:
        raise ValueError("Error in 'bandpass_remresp': %s" % str(type_error))

    spectra = signal_noise_spectra(segment, config)
    normal_f0, normal_df, normal_spe = spectra['Signal']
    noise_f0, noise_df, noise_spe = spectra['Noise']
    evt = segment.event
    fcmin = mag2freq(evt.magnitude)
    fcmax = config['preprocess']['bandpass_freq_max']  # used in bandpass_remresp
    snr_ = snr(normal_spe, noise_spe, signals_form=config['sn_spectra']['type'],
               fmin=fcmin, fmax=fcmax, delta_signal=normal_df, delta_noise=noise_df)
    snr1_ = snr(normal_spe, noise_spe, signals_form=config['sn_spectra']['type'],
                fmin=fcmin, fmax=1, delta_signal=normal_df, delta_noise=noise_df)
    snr2_ = snr(normal_spe, noise_spe, signals_form=config['sn_spectra']['type'],
                fmin=1, fmax=10, delta_signal=normal_df, delta_noise=noise_df)
    snr3_ = snr(normal_spe, noise_spe, signals_form=config['sn_spectra']['type'],
                fmin=10, fmax=fcmax, delta_signal=normal_df, delta_noise=noise_df)
    if snr_ < config['snr_threshold']:
        raise ValueError('low snr %f' % snr_)

    # calculate cumulative

    cum_labels = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    cum_trace = cumsumsq(trace, normalize=True, copy=True)  # copy=True prevent original trace from being modified
    cum_times = timeswhere(cum_trace, *cum_labels)

    # double event
    try:
        (score, t_double, tt1, tt2) = \
            get_multievent_sg(
                cum_trace, cum_times[1], cum_times[-2],
                config['savitzky_golay'], config['multievent_thresholds']
            )
    except IndexError as _ierr:
        raise ValueError("Error in 'get_multievent_sg': %s" % str(_ierr))
    if score in {1, 3}:
        raise ValueError('Double event detected %d %s %s %s' % (score, t_double, tt1, tt2))

    # calculate PGA and times of occurrence (t_PGA):
    # note: you can also provide tstart tend for slicing
    t_PGA, PGA = maxabs(trace, cum_times[1], cum_times[-2])
    trace_int = trace.copy()
    trace_int.integrate()
    t_PGV, PGV = maxabs(trace_int, cum_times[1], cum_times[-2])
    meanoff = meanslice(trace_int, 100, cum_times[-1], trace_int.stats.endtime)

    # calculates amplitudes at the frequency bins given in the config file:
    required_freqs = config['freqs_interp']
    ampspec_freqs = normal_f0 + np.arange(len(normal_spe)) * normal_df
    required_amplitudes = np.interp(np.log10(required_freqs),
                                    np.log10(ampspec_freqs), normal_spe) / segment.sample_rate

    # compute synthetic WA.
    trace_wa = synth_wood_anderson(segment, config, trace.copy())
    t_WA, maxWA = maxabs(trace_wa)

    # write stuff to csv:
    ret = OrderedDict()

    ret['snr'] = snr_
    ret['snr1'] = snr1_
    ret['snr2'] = snr2_
    ret['snr3'] = snr3_
    for cum_lbl, cum_t in zip(cum_labels[slice(1, 8, 3)], cum_times[slice(1, 8, 3)]):
        ret['cum_t%f' % cum_lbl] = float(cum_t)  # convert cum_times to float for saving

    ret['dist_deg'] = segment.event_distance_deg        # dist
    ret['dist_km'] = d2km(segment.event_distance_deg)  # dist_km
    # t_PGA is a obspy UTCDateTime. This type is not supported in HDF output, thus
    # convert it to Python datetime. Note that in CSV output, the value will be written as
    # str(t_PGA.datetime): another option might be to store it as string
    # with str(t_PGA) (returns the iso-formatted string, supported in all output formats):
    ret['t_PGA'] = t_PGA.datetime  # peak info
    ret['PGA'] = PGA
    # (for t_PGV, see note above for t_PGA)
    ret['t_PGV'] = t_PGV.datetime  # peak info
    ret['PGV'] = PGV
    # (for t_WA, see note above for t_PGA)
    ret['t_WA'] = t_WA.datetime
    ret['maxWA'] = maxWA
    ret['channel'] = segment.channel.channel
    ret['channel_component'] = segment.channel.channel[-1]
    # event metadata:
    ret['ev_id'] = segment.event.id
    ret['ev_lat'] = segment.event.latitude
    ret['ev_lon'] = segment.event.longitude
    ret['ev_dep'] = segment.event.depth_km
    ret['ev_mag'] = segment.event.magnitude
    ret['ev_mty'] = segment.event.mag_type
    # station metadata:
    ret['st_id'] = segment.station.id
    ret['st_name'] = segment.station.station
    ret['st_net'] = segment.station.network
    ret['st_lat'] = segment.station.latitude
    ret['st_lon'] = segment.station.longitude
    ret['st_ele'] = segment.station.elevation
    ret['score'] = score
    ret['d2max'] = float(tt1)
    ret['offset'] = np.abs(meanoff/PGV)
    for freq, amp in zip(required_freqs, required_amplitudes):
        ret['f_%.5f' % freq] = float(amp)

    return ret


def assert1trace(stream):
    '''asserts the stream has only one trace, raising an Exception if it's not the case,
    as this is the pre-condition for all processing functions implemented here.
    Note that, due to the way we download data, a stream with more than one trace his
    most likely due to gaps / overlaps'''
    # stream.get_gaps() is slower as it does more than checking the stream length
    if len(stream) != 1:
        raise ValueError("%d traces (probably gaps/overlaps)" % len(stream))


@gui.preprocess
def bandpass_remresp(segment, config):
    """Applies a pre-process on the given segment waveform by filtering the signal and
    removing the instrumental response. When using this function during processing
    (see e.g. the `main` function) remember that it modifies the segment stream
    in-place: further calls to `segment.stream()` will return the pre-processed stream.
    You can change this behaviour by implementing your own code, or store the
    raw stream beforehand, e.g.: `raw_trace=segment.stream().copy()`

    The filter algorithm has the following steps:
    1. Sets the max frequency to 0.9 of the Nyquist frequency (sampling rate /2)
       (slightly less than Nyquist seems to avoid artifacts)
    2. Offset removal (subtract the mean from the signal)
    3. Tapering
    4. Pad data with zeros at the END in order to accommodate the filter transient
    5. Apply bandpass filter, where the lower frequency is set according to the magnitude
    6. Remove padded elements
    7. Remove the instrumental response

    Side notes for uses with the Graphical User Interface (GUI): being decorated with
    '@gui.preprocess', this function must return either a Trace or Stream object that
    will be used as input for all visualized plots whenever the "pre-process" check-box is on.
    The GUI always passes here segments with a copy of the raw stream, so it will
    never incur in the "in-place modification" potential problems described above.

    :return: a Trace object.
    """
    stream = segment.stream()
    assert1trace(stream)  # raise and return if stream has more than one trace
    trace = stream[0]

    inventory = segment.inventory()

    # define some parameters:
    evt = segment.event
    conf = config['preprocess']
    # note: bandpass here below copied the trace! important!
    trace = bandpass(trace, mag2freq(evt.magnitude), freq_max=conf['bandpass_freq_max'],
                     max_nyquist_ratio=conf['bandpass_max_nyquist_ratio'],
                     corners=conf['bandpass_corners'], copy=False)
    trace.remove_response(inventory=inventory, output=conf['remove_response_output'],
                          water_level=conf['remove_response_water_level'])
    return trace


def mag2freq(magnitude):
    '''returns a magnitude dependent frequency (in Hz)'''
    if magnitude <= 4.5:
        freq_min = 0.4
    elif magnitude <= 5.5:
        freq_min = 0.2
    elif magnitude <= 6.5:
        freq_min = 0.1
    else:
        freq_min = 0.05
    return freq_min


def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    """Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
    The Savitzky-Golay filter removes high frequency noise from data.
    It has the advantage of preserving the original shape and
    features of the signal better than other types of filtering
    approaches, such as moving averages techniques.
    Parameters
    ----------
    y : array_like, shape (N,)
        the values of the time history of the signal.
    window_size : int
        the length of the window. Must be an odd integer number.
    order : int
        the order of the polynomial used in the filtering.
        Must be less then `window_size` - 1.
    deriv: int
        the order of the derivative to compute (default = 0 means only smoothing)
    Returns
    -------
    ys : ndarray, shape (N)
        the smoothed signal (or it's n-th derivative).
    Notes
    -----
    The Savitzky-Golay is a type of low-pass filter, particularly
    suited for smoothing noisy data. The main idea behind this
    approach is to make for each point a least-square fit with a
    polynomial of high order over a odd-sized window centered at
    the point.
    Examples
    --------
    t = np.linspace(-4, 4, 500)
    y = np.exp( -t**2 ) + np.random.normal(0, 0.05, t.shape)
    ysg = savitzky_golay(y, window_size=31, order=4)
    import matplotlib.pyplot as plt
    plt.plot(t, y, label='Noisy signal')
    plt.plot(t, np.exp(-t**2), 'k', lw=1.5, label='Original signal')
    plt.plot(t, ysg, 'r', label='Filtered signal')
    plt.legend()
    plt.show()
    References
    ----------
    .. [1] A. Savitzky, M. J. E. Golay, Smoothing and Differentiation of
       Data by Simplified Least Squares Procedures. Analytical
       Chemistry, 1964, 36 (8), pp 1627-1639.
    .. [2] Numerical Recipes 3rd Edition: The Art of Scientific Computing
       W.H. Press, S.A. Teukolsky, W.T. Vetterling, B.P. Flannery
       Cambridge University Press ISBN-13: 9780521880688
    """
    try:
        window_size = np.abs(np.int(window_size))
        order = np.abs(np.int(order))
    except ValueError:
        raise TypeError("window_size and order have to be of type int")
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")
    order_range = range(order+1)
    half_window = (window_size-1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = y[0] - np.abs(y[1:half_window+1][::-1] - y[0])
    lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
    y = np.concatenate((firstvals, y, lastvals))
    return np.convolve(m[::-1], y, mode='valid')


def get_multievent_sg(cum_trace, tmin, tmax, sg_params, multievent_thresholds):
    """
    Returns the tuple (or a list of tuples, if the first argument is a stream) of the
    values (score, UTCDateTime of arrival)
    where scores is: 0: no double event, 1: double event inside tmin_tmax,
        2: double event after tmax, 3: both double event previously defined are detected
    If score is 2 or 3, the second argument is the UTCDateTime denoting the occurrence of the
    first sample triggering the double event after tmax
    :param trace: the input obspy.core.Trace
    """
    tmin = utcdatetime(tmin)
    tmax = utcdatetime(tmax)

    # split traces between tmin and tmax and after tmax
    traces = [cum_trace.slice(tmin, tmax), cum_trace.slice(tmax, None)]

    # calculate second derivative and normalize:
    second_derivs = []
    max_ = np.nan
    for ttt in traces:
        sec_der = savitzky_golay(
            ttt.data,
            sg_params['wsize'],
            sg_params['order'],
            sg_params['deriv']
        )
        sec_der_abs = np.abs(sec_der)
        idx = np.nanargmax(sec_der_abs)
        # get max (global) for normalization:
        max_ = np.nanmax([max_, sec_der_abs[idx]])
        second_derivs.append(sec_der_abs)

    # normalize second derivatives:
    for der in second_derivs:
        der /= max_

    result = 0

    # case A: see if after tmax we exceed a threshold
    indices = np.where(second_derivs[1] >=
                       multievent_thresholds['after_tmax_inpercent'])[0]
    if len(indices):
        result = 2

    # case B: see if inside tmin tmax we exceed a threshold, and in case check the duration
    deltatime = 0
    starttime = tmin
    endtime = None
    indices = np.where(second_derivs[0] >=
                       multievent_thresholds['inside_tmin_tmax_inpercent'])[0]
    if len(indices) >= 2:
        idx0 = indices[0]
        starttime = timeof(traces[0], idx0)
        idx1 = indices[-1]
        endtime = timeof(traces[0], idx1)
        deltatime = endtime - starttime
        if deltatime >= multievent_thresholds['inside_tmin_tmax_insec']:
            result += 1

    return result, deltatime, starttime, endtime


def synth_wood_anderson(segment, config, trace):
    '''
    Low-level function to calculate the synthetic wood-anderson of `trace`.
    The dict ``config['simulate_wa']`` must be implemented
    and houses the wood-anderson configuration 'sensitivity', 'zeros', 'poles' and 'gain'.
    Modifies the trace in place.

    :param trace_input_type:
        None: trace is unprocessed and trace.remove_response(.. output="DISP"...)
            will be applied on it before applying `trace.simulate`
        'ACC': trace is already processed, e.g..
            it's the output of trace.remove_response(..output='ACC')
        'VEL': trace is already processed,
            it's the output of trace.remove_response(..output='VEL')
        'DISP': trace is already processed,
            it's the output of trace.remove_response(..output='DISP')
    '''
    trace_input_type = config['preprocess']['remove_response_output']

    conf = config['preprocess']
    config_wa = dict(config['paz_wa'])
    # parse complex string to complex numbers:
    zeros_parsed = map(complex, (c.replace(' ', '') for c in config_wa['zeros']))
    config_wa['zeros'] = list(zeros_parsed)
    poles_parsed = map(complex, (c.replace(' ', '') for c in config_wa['poles']))
    config_wa['poles'] = list(poles_parsed)
    # compute synthetic WA response. This modifies the trace in-place!

    if trace_input_type in ('VEL', 'ACC'):
        trace.integrate()
    if trace_input_type == 'ACC':
        trace.integrate()

    if trace_input_type is None:
        pre_filt = (0.005, 0.006, 40.0, 45.0)
        trace.remove_response(inventory=segment.inventory(), output="DISP",
                              pre_filt=pre_filt, water_level=conf['remove_response_water_level'])

    return trace.simulate(paz_remove=None, paz_simulate=config_wa)


def signal_noise_spectra(segment, config):
    """
    Computes the signal and noise spectra, as dict of strings mapped to tuples (x0, dx, y).
    Does not modify the segment's stream or traces in-place

    :return: a dict with two keys, 'Signal' and 'Noise', mapped respectively to the tuples
    (f0, df, frequencies)

    :raise: an Exception if `segment.stream()` is empty or has more than one trace (possible
    gaps/overlaps)
    """
    # get sn windows: PLEASE NOTE!! sn_windows might calculate the cumulative of segment.stream(),
    # thus the latter should have been preprocessed (e.g. remove response, bandpass):
    arrival_time = UTCDateTime(segment.arrival_time) + config['sn_windows']['arrival_time_shift']
    signal_trace, noise_trace = sn_split(segment.stream()[0],  # assumes stream has only one trace
                                         arrival_time, config['sn_windows']['signal_window'])
    x0_sig, df_sig, sig = _spectrum(signal_trace, config)
    x0_noi, df_noi, noi = _spectrum(noise_trace, config)
    return {'Signal': (x0_sig, df_sig, sig), 'Noise': (x0_noi, df_noi, noi)}


def _spectrum(trace, config):
    '''Calculate the spectrum of a trace. Returns the tuple (0, df, values), where
    values depends on the config dict parameters.
    Does not modify the trace in-place
    '''
    taper_max_percentage = config['sn_spectra']['taper']['max_percentage']
    taper_type = config['sn_spectra']['taper']['type']
    if config['sn_spectra']['type'] == 'pow':
        func = powspec  # copies the trace if needed
    elif config['sn_spectra']['type'] == 'amp':
        func = ampspec  # copies the trace if needed
    else:
        # raise TypeError so that if called from within main, the iteration stops
        raise TypeError("config['sn_spectra']['type'] expects either 'pow' or 'amp'")

    df_, spec_ = func(trace, taper_max_percentage=taper_max_percentage, taper_type=taper_type)

    # if you want to implement your own smoothing, change the lines below before 'return'
    # and implement your own config variables, if any
    smoothing_wlen_ratio = config['sn_spectra']['smoothing_wlen_ratio']
    if smoothing_wlen_ratio > 0:
        spec_ = triangsmooth(spec_, winlen_ratio=smoothing_wlen_ratio)

    return (0, df_, spec_)


def meanslice(trace, nptmin=100, starttime=None, endtime=None):
    """
    Returns the numpy nanmean of the trace data, optionally slicing the trace first.
    If the trace number of points is lower than `nptmin`, returns NaN (numpy.nan)
    """
    if starttime is not None or endtime is not None:
        trace = trace.slice(starttime, endtime)
    if trace.stats.npts < nptmin:
        return np.nan
    val = np.nanmean(trace.data)
    return val


######################################
# GUI functions for displaying plots #
######################################


@gui.plot
def cumulative(segment, config):
    '''Computes the cumulative of the squares of the segment's trace in the form of a Plot object.
    Modifies the segment's stream or traces in-place. Normalizes the returned trace values
    in [0,1]

    :return: an obspy.Trace

    :raise: an Exception if `segment.stream()` is empty or has more than one trace (possible
    gaps/overlaps)
    '''
    stream = segment.stream()
    assert1trace(stream)  # raise and return if stream has more than one trace
    return cumsumsq(stream[0], normalize=True, copy=False)


@gui.plot('r', xaxis={'type': 'log'}, yaxis={'type': 'log'})
def sn_spectra(segment, config):
    """
    Computes the signal and noise spectra, as dict of strings mapped to tuples (x0, dx, y).
    Does NOT modify the segment's stream or traces in-place

    :return: a dict with two keys, 'Signal' and 'Noise', mapped respectively to the tuples
    (f0, df, frequencies)

    :raise: an Exception if `segment.stream()` is empty or has more than one trace (possible
    gaps/overlaps)
    """
    stream = segment.stream()
    assert1trace(stream)  # raise and return if stream has more than one trace
    return signal_noise_spectra(segment, config)


@gui.plot
def velocity(segment, config):
    stream = segment.stream()
    assert1trace(stream)  # raise and return if stream has more than one trace
    trace = stream[0]
    trace_int = trace.copy()
    return trace_int.integrate()


@gui.plot
def derivcum2(segment, config):
    """
    compute the second derivative of the cumulative function using savitzy-golay.
    Modifies the segment's stream or traces in-place

    :return: the tuple (starttime, timedelta, values)

    :raise: an Exception if `segment.stream()` is empty or has more than one trace (possible
    gaps/overlaps)
    """
    stream = segment.stream()
    assert1trace(stream)  # raise and return if stream has more than one trace
    cum = cumsumsq(stream[0], normalize=True, copy=False)
    cfg = config['savitzky_golay']
    sec_der = savitzky_golay(cum.data, cfg['wsize'], cfg['order'], cfg['deriv'])
    sec_der_abs = np.abs(sec_der)
    sec_der_abs /= np.nanmax(sec_der_abs)
    # the stream object has surely only one trace (see 'cumulative')
    return segment.stream()[0].stats.starttime, segment.stream()[0].stats.delta, sec_der_abs


@gui.plot
def synth_wa(segment, config):
    '''compute synthetic WA. See ``synth_wood_anderson``.
    Modifies the segment's stream or traces in-place.

    :return:  an obspy Trace
    '''
    stream = segment.stream()
    assert1trace(stream)  # raise and return if stream has more than one trace
    return synth_wood_anderson(segment, config, stream[0])

