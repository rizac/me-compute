'''
This editable template shows how to save raw and pre-processed segments from the database
on the file system. Files will be saved as miniSEED in a seiscomp3-compatible directory structure
under a user-defined root directory (defined in the associated configuration file).
When used for visualization, this template implements a pre-processing function that
can be toggled with a checkbox from within the GUI, and no additional custom plot

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
# (http://python-future.org/imports.html#explicit-imports):
from builtins import (ascii, bytes, chr, dict, filter, hex, input,
                      int, map, next, oct, open, pow, range, round,
                      str, super, zip)

import os
# OrderedDict is a python dict that returns its keys in the order they are inserted
# (a normal python dict returns its keys in arbitrary order)
# Useful e.g. in  "main" if we want to control the *order* of the columns in the output csv
from collections import OrderedDict
from datetime import datetime, timedelta  # always useful
from math import factorial  # for savitzky_golay function

# import numpy for efficient computation:
import numpy as np
# import obspy core classes (when working with times, use obspy UTCDateTime when possible):
from obspy import Trace, Stream, UTCDateTime
from obspy.geodetics import degrees2kilometers as d2km
# decorators needed to setup this module @gui.sideplot, @gui.preprocess @gui.customplot:
from stream2segment.process import gui
# strem2segment functions for processing obspy Traces. This is just a list of possible functions
# to show how to import them:
from stream2segment.process.math.traces import ampratio, bandpass, cumsumsq,\
    timeswhere, fft, maxabs, utcdatetime, ampspec, powspec, timeof
# stream2segment function for processing numpy arrays:
from stream2segment.process.math.ndarrays import triangsmooth, snr


def assert1trace(stream):
    '''asserts the stream has only one trace, raising an Exception if it's not the case,
    as this is the pre-condition for all processing functions implemented here.
    Note that, due to the way we download data, a stream with more than one trace his
    most likely due to gaps / overlaps'''
    # stream.get_gaps() is slower as it does more than checking the stream length
    if len(stream) != 1:
        raise ValueError("%d traces (probably gaps/overlaps)" % len(stream))


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

    original_trace = trace.copy()  # keep a track of the original mseed

    # bandpass the trace, according to the event magnitude.
    # WARNING: this modifies the segment.stream() permanently!
    # If you want to preserve the original stream, store trace.copy() beforehand.
    # Also, use a 'try catch': sometimes Inventories are corrupted and obspy raises
    # a TypeError, which would break the WHOLE processing execution.
    # Raising a ValueError will stop the execution of the currently processed
    # segment only (logging the error message):
    try:
        processed_trace = bandpass_remresp(segment, config)
    except TypeError as type_error:
        raise ValueError("Error in 'bandpass_remresp': %s" % str(type_error))

    stream_path = segment.sds_path(config['root_dir'])
    basedir = os.path.dirname(stream_path)
    path1 = stream_path + ".s2s.raw.mseed"
    path2 = stream_path + ".s2s.processed.mseed"
    # assure directories exists, making all intermediate paths if needed:
    if not os.path.exists(basedir):
        os.makedirs(basedir)
    original_trace.write(path1, format='MSEED')
    processed_trace.write(path2, format='MSEED')


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
    '''returns a frequency in Hz from a given magnitude'''
    if magnitude <= 4.5:
        freq_min = 0.4
    elif magnitude <= 5.5:
        freq_min = 0.2
    elif magnitude <= 6.5:
        freq_min = 0.1
    else:
        freq_min = 0.05
    return freq_min
