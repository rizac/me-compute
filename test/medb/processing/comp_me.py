# define main functions and dependencies:

# OrderedDict is a python dict that returns its keys in the order they are inserted
# (a normal python dict returns its keys in arbitrary order in Python < 3.7)
# Useful e.g. in  "main" if we want to control the *order* of the columns in the output csv
from collections import OrderedDict
from datetime import datetime, timedelta  # always useful
from math import factorial  # for savitzky_golay function

# import numpy for efficient computation:
import numpy as np
from scipy.interpolate import CubicSpline
from scipy.constants import pi
from scipy import stats
import obspy.signal
# import obspy core classes (when working with times, use obspy UTCDateTime when possible):
from obspy import Trace, Stream, UTCDateTime, read
from obspy.geodetics import degrees2kilometers as d2km
from obspy.taup import TauPyModel
from obspy.signal.konnoohmachismoothing import konno_ohmachi_smoothing


# decorators needed to setup this module @gui.preprocess @gui.plot:
from stream2segment.process import gui
# strem2segment functions for processing obspy Traces. This is just a list of possible functions
# to show how to import them:
from stream2segment.process.math.traces import ampratio, bandpass, cumsumsq,\
    timeswhere, fft, maxabs, utcdatetime, ampspec, powspec, timeof, sn_split
# stream2segment function for processing numpy arrays:
from stream2segment.process.math.ndarrays import triangsmooth, snr


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

    delta_t = trace.stats.delta

    # discard saturated signals (according to the threshold set in the config file):
    amp_ratio = ampratio(trace)
    flag_ratio =1
    if amp_ratio >= config['amp_ratio_threshold']:
        flag_ratio = 0
        #raise ValueError('possibly saturated (amp. ratio exceeds)')
       
    
    # bandpass the trace, according to the event magnitude.
    # WARNING: this modifies the segment.stream() permanently!
    # If you want to preserve the original stream, store trace.copy()
    try:
        trace = bandpass_remresp(segment, config)    # NOTE: change fmin mag2freq(evt.magnitude)
    except TypeError as texc:
        raise ValueError('error in bandpass_remresp: %s' % str(texc))    

    #spectra = sn_spectra(segment, config)
    spectra = signal_noise_spectra(segment, config)  #ATTENZIONEEEEE: function to be cleaned!!!
    normal_f0, normal_df, normal_spe = spectra['Signal']
    noise_f0, noise_df, noise_spe = spectra['Noise']

    # smoothing:
    normal_freqs = np.linspace(normal_f0,
                               normal_f0 + len(normal_spe) * normal_df,
                               num=len(normal_spe), endpoint=True)

    # For future developments, we might use konno ohmaci:
    #  normal_spe = konno_ohmachi_smoothing(normal_spe, normal_freqs, bandwidth=40,
    #                                       count=1, enforce_no_matrix=False, max_memory_usage=512,
    #                                       normalize=False)


    evt = segment.event
    fcmin = 0.001  #20s, integration for Energy starts from 16s
    fcmax = config['preprocess']['bandpass_freq_max']  # used in bandpass_remresp
    snr_ = snr(normal_spe, noise_spe, signals_form=config['sn_spectra']['type'],
               fmin=fcmin, fmax=fcmax, delta_signal=normal_df, delta_noise=noise_df)
    
    if snr_ < config['snr_threshold']:
        raise ValueError('low snr %f' % snr_)
    
    normal_spe *= delta_t

    duration = get_segment_window_duration(segment, config)

    if duration == 60:
        freq_min_index = 1 # 0.015625 (see frequencies in yaml)
    else:
        freq_min_index = 0 # 0.012402 (see frequencies in yaml)    

    freq_dist_table = config['freq_dist_table']
    frequencies = freq_dist_table['frequencies'][freq_min_index:]
    distances = freq_dist_table['distances']
    try:
        distances_table = freq_dist_table[duration]
    except KeyError:
        raise KeyError(f'no freq dist table implemented for {duration} seconds')

    assert sorted(distances) == distances
    assert sorted(frequencies) == frequencies

    # calculate spectra with spline interpolation on given frequencies:
    cs = CubicSpline(normal_freqs, normal_spe)
    seg_spectrum = cs(frequencies)

    seg_spectrum_log10 = np.log10(seg_spectrum)

    distance_deg = segment.event_distance_deg
    if distance_deg < distances[0] or distance_deg > distances[-1]:
        raise ValueError('Passed `distance_deg`=%f not in [%f, %f]' %
                         (distance_deg, distances[0], distances[-1]))


    distindex = np.searchsorted(distances, distance_deg)

    if distances[distindex] == distance_deg:
        correction_spectrum_log10 = distances_table[distindex]
    else:
        distances_table = np.array(distances_table).reshape(len(distances), len(frequencies))
        css = [CubicSpline(distances, distances_table[:,i]) for i in range(len(frequencies))]
        correction_spectrum_log10 = [css[freqindex](distance_deg) for freqindex in range(len(frequencies))]

    corrected_spectrum = seg_spectrum_log10 - correction_spectrum_log10

    corrected_spectrum = np.power(10, corrected_spectrum) ** 2  # convert log10A -> A^2:
    
    corrected_spectrum_int_vel_square = np.trapz(corrected_spectrum,
                                                 frequencies) 

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

    # write stuff to csv/ hdf:
    ret = {}

    ret['snr'] = snr_
    ret['satu'] = flag_ratio
    ret['dist_deg'] = distance_deg      # dist
    ret['s_r'] = trace.stats.sampling_rate
    ret['me_st'] = me_st
    ret['channel'] = segment.channel.channel
    ret['ev_id'] = segment.event.id           # event metadata
    ret['ev_time'] = segment.event.time
    ret['ev_lat'] = segment.event.latitude
    ret['ev_lon'] = segment.event.longitude
    ret['ev_dep'] = segment.event.depth_km
    ret['ev_mag'] = segment.event.magnitude
    ret['ev_mty'] = segment.event.mag_type
    ret['st_id'] = segment.station.id         # station metadata
    ret['st_net'] = segment.station.network
    ret['st_name'] = segment.station.station
    ret['st_lat'] = segment.station.latitude
    ret['st_lon'] = segment.station.longitude
    ret['st_ele'] = segment.station.elevation
    ret['integral'] = corrected_spectrum_int_vel_square 
    return ret


@gui.preprocess
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

    - In this implementation THIS FUNCTION DOES MODIFY `segment.stream()` IN-PLACE: from within
      `main`, further calls to `segment.stream()` will return the stream returned by this function.
      However, In any case, you can use `segment.stream().copy()` before this call to keep the
      old "raw" stream

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
        freq_min = 0.02  #50s
    elif magnitude <= 5.5:
        freq_min = 0.02
    elif magnitude <= 6.5:
        freq_min = 0.02
    else:
        freq_min = 0.02
    return freq_min


def assert1trace(stream):
    '''asserts the stream has only one trace, raising an Exception if it's not the case,
    as this is the pre-condition for all processing functions implemented here.
    Note that, due to the way we download data, a stream with more than one trace his
    most likely due to gaps / overlaps'''
    # stream.get_gaps() is slower as it does more than checking the stream length
    if len(stream) != 1:
        raise ValueError("%d traces (probably gaps/overlaps)" % len(stream))


def sn_spectra(segment, config):
    """
    Computes the signal and noise spectra, as dict of strings mapped to tuples (x0, dx, y).
    Does not modify the segment's stream or traces in-place

    :return: a dict with two keys, 'Signal' and 'Noise', mapped respectively to the tuples
    (f0, df, frequencies)

    :raise: an Exception if `segment.stream()` is empty or has more than one trace (possible
    gaps/overlaps)
    """
    stream = segment.stream()
    duration = get_segment_window_duration(segment, config)
    assert1trace(stream)  # raise and return if stream has more than one trace
    signal_wdw, noise_wdw = segment.sn_windows(duration,
                                               config['sn_windows']['arrival_time_shift'])
    x0_sig, df_sig, sig = _spectrum(stream[0], config, *signal_wdw)
    x0_noi, df_noi, noi = _spectrum(stream[0], config, *noise_wdw)
    return {'Signal': (x0_sig, df_sig, sig), 'Noise': (x0_noi, df_noi, noi)}


def _spectrum(trace, config, starttime=None, endtime=None):
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

    df_, spec_ = func(trace, starttime, endtime,
                      taper_max_percentage=taper_max_percentage, taper_type=taper_type)

    # if you want to implement your own smoothing, change the lines below before 'return'
    # and implement your own config variables, if any
    smoothing_wlen_ratio = config['sn_spectra']['smoothing_wlen_ratio']
    if smoothing_wlen_ratio > 0:
        spec_ = triangsmooth(spec_, winlen_ratio=smoothing_wlen_ratio)
        #normal_freqs = 0. + np.arange(len(spec_)) * df_
        #spec_ = konno_ohmachi_smoothing(spec_, normal_freqs, bandwidth=60,
        #        count=1, enforce_no_matrix=False, max_memory_usage=512,
        #        normalize=False)


    return (0, df_, spec_)


def reject_outliers(data, m=2.):
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s= d/(mdev if mdev else 1.)
    return data[s<m]


def signal_noise_spectra(segment, config):
    """
    Computes the signal and noise spectra, as dict of strings mapped to tuples (x0, dx, y).
    Does not modify the segment's stream or traces in-place

    :return: a dict with two keys, 'Signal' and 'Noise', mapped respectively to the tuples
    (f0, df, frequencies)

    :raise: an Exception if `segment.stream()` is empty or has more than one trace (possible
    gaps/overlaps)
    """
    arrival_time = UTCDateTime(segment.arrival_time) + config['sn_windows']['arrival_time_shift']
    duration = get_segment_window_duration(segment, config)
    signal_trace, noise_trace = sn_split(segment.stream()[0],  # assumes stream has only one trace
                                         arrival_time, duration)
    signal_trace.taper(0.05,type='cosine')
    dura_sec = 1*signal_trace.stats.delta * (8192-1)
    signal_trace.trim(starttime=signal_trace.stats.starttime, endtime=signal_trace.stats.endtime+dura_sec, pad=True,fill_value=0)
    dura_sec = 1*noise_trace.stats.delta * (8192-1)
    noise_trace.taper(0.05,type='cosine')
    #dura_sec = 2*signal_trace.stats.delta * (signal_trace.stats.npts-1)
    #signal_trace.trim(starttime=signal_trace.stats.starttime, endtime=signal_trace.stats.endtime+dura_sec, pad=True,fill_value=0)
    #dura_sec = 2*noise_trace.stats.delta * (noise_trace.stats.npts-1)
    noise_trace.trim(starttime=noise_trace.stats.starttime, endtime=noise_trace.stats.endtime+dura_sec, pad=True,fill_value=0)
    x0_sig, df_sig, sig = _spectrum(signal_trace, config)
    x0_noi, df_noi, noi = _spectrum(noise_trace, config)

    #signal_wdw, noise_wdw = segment.sn_windows(config['sn_windows']['signal_window'],
    #                                           config['sn_windows']['arrival_time_shift'])
    #x0_sig, df_sig, sig = _spectrum(trace, config, *signal_wdw)
    #x0_noi, df_noi, noi = _spectrum(trace, config, *noise_wdw)
    return {'Signal': (x0_sig, df_sig, sig), 'Noise': (x0_noi, df_noi, noi)}



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


def signal_smooth_spectra(segment, config):
    stream = segment.stream()
    arrival_time = UTCDateTime(segment.arrival_time) + config['sn_windows']['arrival_time_shift']
    duration = get_segment_window_duration(segment, config)
    signal_wdw, noise_wdw = sn_split(stream[0],  # assumes stream has only one trace
                                         arrival_time, duration)
    #signal_wdw, noise_wdw = segment.sn_windows(config['sn_windows']['signal_window'],
    #                                           config['sn_windows']['arrival_time_shift'])
    x0_sig, df_sig, sig = _spectrum(signal_wdw,config)
    x0_sig1, df_sig1, sig1 = _spectrumnosmooth(signal_wdw,config)
    x0_sig2, df_sig2, sig2 = _spectrumKO(signal_wdw,config)
    return {'noSmooth': (x0_sig1, df_sig1, sig1),'Triangular': (x0_sig, df_sig, sig),'Konno-Ohmachi': (x0_sig2, df_sig2, sig2)}


def _spectrumnosmooth(trace, config, starttime=None, endtime=None):
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
                      taper_max_percentage=taper_max_percentage, taper_type=taper_type)
    return (0, df_, spec_)


def _spectrumKO(trace, config, starttime=None, endtime=None):
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
                      taper_max_percentage=taper_max_percentage, taper_type=taper_type)

    # if you want to implement your own smoothing, change the lines below before 'return'
    # and implement your own config variables, if any
    smoothing_wlen_ratio = config['sn_spectra']['smoothing_wlen_ratio']
    if smoothing_wlen_ratio > 0:
        #spec_ = triangsmooth(spec_, winlen_ratio=smoothing_wlen_ratio)
        f0_ = 0
        normal_freqs = np.linspace(f0_, f0_ + len(spec_) * df_,
                               num=len(spec_), endpoint=False)

        #normal_freqs = 0. + np.arange(len(spec_)) * df_
        spec_ = konno_ohmachi_smoothing(spec_, normal_freqs, bandwidth=80,
                count=1, enforce_no_matrix=False, max_memory_usage=512,
                normalize=False)
    return (0, df_, spec_)


@gui.plot('r', xaxis={'type': 'log'}, yaxis={'type': 'log'})
def check_spe(segment,config):
    stream = segment.stream()
    assert1trace(stream)  # raise and return if stream has more than one trace
    return signal_smooth_spectra(segment, config)



def get_segment_window_duration(segment, config):
    magnitude = segment.event.magnitude
    magrange2duration = config['magrange2duration']
    for m in magrange2duration:
        if m[0] <= magnitude < m[1]:
            return m[2]
    return 90
