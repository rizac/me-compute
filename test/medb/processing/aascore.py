# define main functions and dependencies:

# OrderedDict is a python dict that returns its keys in the order they are inserted
# (a normal python dict returns its keys in arbitrary order in Python < 3.7)
# Useful e.g. in  "main" if we want to control the *order* of the columns in the output csv
from collections import OrderedDict

# import numpy for efficient computation:
import numpy as np
import obspy.signal
# import obspy core classes (when working with times, use obspy UTCDateTime when possible):
from obspy import Trace, Stream, UTCDateTime, read


# decorators needed to setup this module @gui.preprocess @gui.plot:
from stream2segment.process import gui
# strem2segment functions for processing obspy Traces. This is just a list of possible functions
# to show how to import them:
from stream2segment.process.math.traces import ampratio, bandpass, cumsumsq,\
    timeswhere, fft, maxabs, utcdatetime, ampspec, powspec, timeof, sn_split
# stream2segment function for processing numpy arrays:
from stream2segment.process.math.ndarrays import triangsmooth, snr
from sdaas.core import trace_score


def main(segment, config):
    '''
    Main processing function. The user should implement here the processing for any given
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

    '''
    stream = segment.stream()
    assert1trace(stream)  # raise and return if stream has more than one trace
    trace = stream[0]  # work with the (surely) one trace now

    try:
        return {
            'aascore': trace_score(trace, segment.inventory())
        }
    except Exception as exc:
        raise ValueError(str(exc))


def assert1trace(stream):
    '''asserts the stream has only one trace, raising an Exception if it's not the case,
    as this is the pre-condition for all processing functions implemented here.
    Note that, due to the way we download data, a stream with more than one trace his
    most likely due to gaps / overlaps'''
    # stream.get_gaps() is slower as it does more than checking the stream length
    if len(stream) != 1:
        raise ValueError("%d traces (probably gaps/overlaps)" % len(stream))
