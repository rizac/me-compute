"""
===========================================================
Module computing station energy magnitude at segment level
See `main` below for details
===========================================================
"""

# from collections import OrderedDict
# from datetime import datetime, timedelta  # always useful
# from math import factorial  # for savitzky_golay function
# import numpy for efficient computation:
import numpy as np
# import pandas as pd
from scipy.interpolate import CubicSpline
from scipy.constants import pi
# from scipy import stats
# import obspy.signal
from obspy import UTCDateTime  # , read, Trace, Stream,
# from obspy.geodetics import degrees2kilometers as d2km
# from obspy.taup import TauPyModel
# from obspy.signal.konnoohmachismoothing import konno_ohmachi_smoothing

# straem2segment functions for processing obspy Traces. This is just a list of possible
# functions to show how to import them:
from stream2segment.process import SkipSegment
from stream2segment.process.funclib.traces import bandpass, cumsumsq,\
    fft, ampspec, powspec, timeof, sn_split
# stream2segment function for processing numpy arrays:
from stream2segment.process.funclib.ndarrays import triangsmooth, snr

try:
    from sdaas.core import trace_score
except ImportError:
    trace_score = lambda *a, **w: np.nan


def compute_station_me(segment, config):
    """Main processing function, called iteratively for any segment selected from `imap`
    or `process` functions of stream2segment.

    IMPORTANT: any exception raised here or from any sub-function will interrupt the
    whole processing routine with one special case: `stream2segment.process.SkipSegment`
    will resume from the next segment. Raise it to programmatically skip a segment, e.g.:
    ```
    if segment.sample_rate < 60:
        raise SkipSegment("segment sample rate too low")`
    ```
    Hint: Because handling exceptions at any point of a time-consuming processing is
    complex, we recommend to try to run your code on a smaller and possibly
    heterogeneous dataset first: change temporarily the segment selection (See section
    `if __name__ == "__main__"` at the end of the module), and inspect the logfile:
    for any exception that is not a bug and should simply be ignored, wrap only
    the part of code affected in a "try ... except" statement, and raise a `SkipSegment`.
    Also, please spend some time on refining the selection of segments: you might
    find that your code runs smoothly and faster by simply skipping certain segments in
    the first place.

    :param: segment: the object describing a downloaded waveform segment and its metadata,
        with a full set of useful attributes and methods detailed here:
        {{ THE_SEGMENT_OBJECT_WIKI_URL }}

    :param: config: a dictionary representing the configuration parameters
        accessible globally by all processed segments. The purpose of the `config`
        is to encourage decoupling of code and configuration for better and more
        maintainable code, avoiding, e.g., many similar processing functions differing
        by few hard-coded parameters (this is one of the reasons why the config is
        given as separate YAML file to be passed to the `s2s process` command)

    :return: If the processing routine calling this function needs not to generate a
        file output, the returned value of this function, if given, will be ignored.
        Otherwise:

        * For CSV output, this function must return an iterable that will be written
          as a row of the resulting file (e.g. list, tuple, numpy array, dict. You must
          always return the same type of object, e.g. not lists or dicts conditionally).

          Returning None or nothing is also valid: in this case the segment will be
          silently skipped

          The CSV file will have a row header only if `dict`s are returned (the dict
          keys will be the CSV header columns). For Python version < 3.6, if you want
          to preserve in the CSV the order of the dict keys as the were inserted, use
          `OrderedDict`.

          A column with the segment database id (an integer uniquely identifying the
          segment) will be automatically inserted as first element of the iterable,
          before writing it to file.

          SUPPORTED TYPES as elements of the returned iterable: any Python object, but
          we suggest to use only strings or numbers: any other object will be converted
          to string via `str(object)`: if this is not what you want, convert it to the
          numeric or string representation of your choice. E.g., for Python `datetime`s
          you might want to set `datetime.isoformat()` (string), for ObsPy `UTCDateTime`s
          `float(utcdatetime)` (numeric)

       * For HDF output, this function must return a dict, pandas Series or pandas
         DataFrame that will be written as a row of the resulting file (or rows, in case
         of DataFrame).

         Returning None or nothing is also valid: in this case the segment will be
         silently skipped.

         A column named '{{ SEGMENT_ID_COLNAME }}' with the segment database id (an integer
         uniquely identifying the segment) will be automatically added to the dict /
         Series, or to each row of the DataFrame, before writing it to file.

         SUPPORTED TYPES as elements of the returned dict/Series/DataFrame: all types
         supported by pandas:
         https://pandas.pydata.org/pandas-docs/stable/getting_started/basics.html#dtypes

         For info on hdf and the pandas library (included in the package), see:
         https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_hdf.html
         https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-hdf5

    """
    # bandpass the trace, according to the event magnitude.
    # WARNING: this modifies the segment.stream() permanently!
    # If you want to preserve the original stream, store trace.copy()
    # or use segment.stream(True). Note that bandpass function assures the trace is one
    # (no gaps/overlaps)
    try:
        trace = bandpass_remresp(segment, config)
    except Exception as exc:  # noqa
        raise SkipSegment('%s in bandpass_remresp' % str(exc.__class__))

    spectra = signal_noise_spectra(segment, config)
    normal_f0, normal_df, normal_spe = spectra['Signal']
    noise_f0, noise_df, noise_spe = spectra['Noise']

    # For future developments, we might use konno ohmaci:
    #  normal_spe = konno_ohmachi_smoothing(normal_spe, normal_freqs, bandwidth=40,
    #                                       count=1, enforce_no_matrix=False,
    #                                       max_memory_usage=512,
    #                                       normalize=False)

    fcmin = 0.001  # 20s, integration for Energy starts from 16s
    fcmax = config['preprocess']['bandpass_freq_max']  # used in bandpass_remresp
    snr_ = snr(normal_spe, noise_spe, signals_form=config['sn_spectra']['type'],
               fmin=fcmin, fmax=fcmax, delta_signal=normal_df, delta_noise=noise_df)

    if snr_ < config['snr_threshold']:
        raise SkipSegment('Low SNR')

    ##################
    # ME COMPUTATION #
    ##################

    normal_spe *= trace.stats.delta

    duration = get_segment_window_duration(segment, config)

    if duration == 60:
        freq_min_index = 1  # 0.015625 (see frequencies in yaml)
    else:
        freq_min_index = 0  # 0.012402 (see frequencies in yaml)

    freq_dist_table = config['freq_dist_table']
    frequencies = freq_dist_table['frequencies'][freq_min_index:]
    distances = freq_dist_table['distances']
    try:
        distances_table = freq_dist_table[duration]
    except KeyError:
        raise KeyError(f'no freq dist table implemented for {duration} seconds')

    # unnecessary asserts just used for testing (comment out):
    # assert sorted(distances) == distances
    # assert sorted(frequencies) == frequencies

    # calculate spectra with spline interpolation on given frequencies:
    normal_freqs = np.linspace(normal_f0,
                               normal_f0 + len(normal_spe) * normal_df,
                               num=len(normal_spe), endpoint=True)
    try:
        cs = CubicSpline(normal_freqs, normal_spe)
    except ValueError as verr:
        raise SkipSegment('ValueError in CubicSpline')

    seg_spectrum = cs(frequencies)

    seg_spectrum_log10 = np.log10(seg_spectrum)

    distance_deg = segment.event_distance_deg
    if distance_deg < distances[0] or distance_deg > distances[-1]:
        # just for safety (see segments_selection.yaml):
        raise SkipSegment('event distance out of bounds')

    distindex = np.searchsorted(distances, distance_deg)

    if distances[distindex] == distance_deg:
        correction_spectrum_log10 = distances_table[distindex]
    else:
        distances_table = np.array(distances_table).reshape(len(distances),
                                                            len(frequencies))
        css = [CubicSpline(distances, distances_table[:, i])
               for i in range(len(frequencies))]
        correction_spectrum_log10 = [css[freqindex](distance_deg)
                                     for freqindex in range(len(frequencies))]

    corrected_spectrum = seg_spectrum_log10 - correction_spectrum_log10

    corrected_spectrum = np.power(10, corrected_spectrum) ** 2  # convert log10A -> A^2:

    corrected_spectrum_int_vel_square = np.trapz(corrected_spectrum, frequencies)

    depth_km = segment.event.depth_km
    if depth_km < 10:
        v_dens = 2800
        v_pwave = 6500
        v_swave = 3850
    elif depth_km < 18:
        v_dens = 2920
        v_pwave = 6800
        v_swave = 3900
    else:
        v_dens = 3641
        v_pwave = 8035.5
        v_swave = 4483.9

    v_cost_p = (1. /(15. * pi * v_dens * (v_pwave ** 5)))
    v_cost_s = (1. /(10. * pi * v_dens * (v_swave ** 5)))
    # below I put a factor 2 but ... we don't know yet if it is needed
    energy = 2 * (v_cost_p + v_cost_s) * corrected_spectrum_int_vel_square
    me_st = (2./3.) * (np.log10(energy) - 4.4)

    if not np.isfinite(me_st):
        raise SkipSegment('Me NaN')

    # END OF ME COMPUTATION =============================================

    # Reload (raw) trace (raw) for the final computations (anomaly score and saturation):
    trace = segment.stream(reload=True)[0]

    ###########################
    # AMPLITUDE ANOMALY SCORE #
    ###########################

    try:
        aascore = trace_score(trace, segment.inventory())
    except Exception as exc:
        aascore = np.nan

    ##############
    # SATURATION #
    ##############

    # discard saturated signals (according to the threshold set in the config file):
    amp_ratio = np.true_divide(np.nanmax(np.abs(trace.data)), 2**23)
    flag_ratio = 1
    if amp_ratio >= config['amp_ratio_threshold']:
        flag_ratio = 0

    ################################
    # BUILD AND RETURN OUTPUT DICT #
    ################################

    return {
        'station_energy_magnitude': me_st,
        'network': segment.station.network,
        'station': segment.station.station,
        'location': segment.channel.location,
        'channel': segment.channel.channel,
        'event_db_id': segment.event_id,
        'station_event_distance_deg': distance_deg,
        'station_latitude': segment.station.latitude,
        'station_longitude': segment.station.longitude,
        'station_elevation': segment.station.elevation,
        'station_db_id': segment.station.id,
        'signal_sampling_rate': trace.stats.sampling_rate,
        'signal_to_noise_ratio': snr_,
        'signal_amplitude_anomaly_score': aascore,
        'signal_is_saturated': flag_ratio,
        'signal_corrected_spectrum_velocity_squared_integral':
            corrected_spectrum_int_vel_square
    }


# @gui.preprocess
def bandpass_remresp(segment, config):
    """Applies a pre-process on the given segment waveform by
    filtering the signal and removing the instrumental response.
    DOES modify the segment stream in-place (see below).

    The filter algorithm has the following steps:
    1. Sets the max frequency to 0.9 of the Nyquist frequency (sampling rate /2)
    (slightly less than Nyquist seems to avoid artifacts)
    2. Offset removal (subtract the mean from the signal)
    3. Tapering
    4. Pad data with zeros at the END in order to accommodate the filter transient
    5. Apply bandpass filter, where the lower frequency is set according to the magnitude
    6. Remove padded elements
    7. Remove the instrumental response

    IMPORTANT NOTES:
    - Being decorated with '@gui.preprocess', this function:
      * returns the *base* stream used by all plots whenever the relative check-box is on
      * must return either a Trace or Stream object

    - In this implementation THIS FUNCTION DOES MODIFY `segment.stream()` IN-PLACE: from
     within `main`, further calls to `segment.stream()` will return the stream returned
     by this function. However, In any case, you can use `segment.stream().copy()` before
     this call to keep the old "raw" stream

    :return: a Trace object.
    """
    stream = segment.stream()
    assert1trace(stream)  # raise and return if stream has more than one trace
    trace = stream[0]
    inventory = segment.inventory()
    conf = config['preprocess']
    # note: bandpass here below modified the trace inplace
    trace = bandpass(trace, freq_min = 0.02, freq_max=conf['bandpass_freq_max'],
                     max_nyquist_ratio=conf['bandpass_max_nyquist_ratio'],
                     corners=conf['bandpass_corners'], copy=False)
    trace.remove_response(inventory=inventory, output=conf['remove_response_output'],
                          water_level=conf['remove_response_water_level'])
    return trace


def assert1trace(stream):
    """Assert the stream has only one trace, raising an Exception if it's not the case,
    as this is the pre-condition for all processing functions implemented here.
    Note that, due to the way we download data, a stream with more than one trace his
    most likely due to gaps / overlaps"""
    # stream.get_gaps() is slower as it does more than checking the stream length
    if len(stream) != 1:
        raise SkipSegment("%d traces (probably gaps/overlaps)" % len(stream))


def signal_noise_spectra(segment, config):
    """Compute the signal and noise spectra, as dict of strings mapped to
    tuples (x0, dx, y). Does not modify the segment's stream or traces in-place

    :return: a dict with two keys, 'Signal' and 'Noise', mapped respectively to
        the tuples (f0, df, frequencies)

    :raise: an Exception if `segment.stream()` is empty or has more than one
        trace (possible gaps/overlaps)
    """
    # (this function assumes stream has only one trace)
    atime_shift = config['sn_windows']['arrival_time_shift']
    arrival_time = UTCDateTime(segment.arrival_time) + atime_shift
    duration = get_segment_window_duration(segment, config)
    signal_trace, noise_trace = sn_split(segment.stream()[0], arrival_time, duration)

    signal_trace.taper(0.05, type='cosine')
    dura_sec = signal_trace.stats.delta * (8192-1)
    signal_trace.trim(starttime=signal_trace.stats.starttime,
                      endtime=signal_trace.stats.endtime+dura_sec, pad=True,
                      fill_value=0)
    dura_sec = noise_trace.stats.delta * (8192-1)
    noise_trace.taper(0.05, type='cosine')
    noise_trace.trim(starttime=noise_trace.stats.starttime,
                     endtime=noise_trace.stats.endtime+dura_sec, pad=True,
                     fill_value=0)

    x0_sig, df_sig, sig = _spectrum(signal_trace, config)
    x0_noi, df_noi, noi = _spectrum(noise_trace, config)

    return {'Signal': (x0_sig, df_sig, sig), 'Noise': (x0_noi, df_noi, noi)}


def get_segment_window_duration(segment, config):
    magnitude = segment.event.magnitude
    magrange2duration = config['magrange2duration']
    for m in magrange2duration:
        if m[0] <= magnitude < m[1]:
            return m[2]
    return 90


def _spectrum(trace, config, starttime=None, endtime=None):
    """Calculate the spectrum of a trace. Returns the tuple (0, df, values),
    where values depends on the config dict parameters.
    Does not modify the trace in-place
    """
    taper_max_percentage = config['sn_spectra']['taper']['max_percentage']
    taper_type = config['sn_spectra']['taper']['type']
    if config['sn_spectra']['type'] == 'pow':
        func = powspec  # copies the trace if needed
    elif config['sn_spectra']['type'] == 'amp':
        func = ampspec  # copies the trace if needed
    else:
        # raise TypeError so that if called from within main, the iteration stops
        raise TypeError("config['sn_spectra']['type'] expects either 'pow' or 'amp'")

    df_, spec_ = func(trace, starttime, endtime,
                      taper_max_percentage=taper_max_percentage,
                      taper_type=taper_type)

    # if you want to implement your own smoothing, change the lines below before
    # 'return' and implement your own config variables, if any
    smoothing_wlen_ratio = config['sn_spectra']['smoothing_wlen_ratio']
    if smoothing_wlen_ratio > 0:
        spec_ = triangsmooth(spec_, winlen_ratio=smoothing_wlen_ratio)
        # normal_freqs = 0. + np.arange(len(spec_)) * df_
        # spec_ = konno_ohmachi_smoothing(spec_, normal_freqs, bandwidth=60,
        #         count=1, enforce_no_matrix=False, max_memory_usage=512,
        #         normalize=False)

    return 0, df_, spec_
