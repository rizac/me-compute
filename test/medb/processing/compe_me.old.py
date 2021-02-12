# define main functions and dependencies:

# OrderedDict is a python dict that returns its keys in the order they are inserted
# (a normal python dict returns its keys in arbitrary order in Python < 3.7)
# Useful e.g. in  "main" if we want to control the *order* of the columns in the output csv
from collections import OrderedDict
from datetime import datetime, timedelta  # always useful
from math import factorial  # for savitzky_golay function

# import numpy for efficient computation:
import numpy as np
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

from scipy.interpolate import CubicSpline
from scipy.constants import pi

from scipy import stats
import matplotlib.pyplot as plt
import pandas as pd

def main(segment, config):
    """Main processing function. The user should implement here the processing steps for any given
    selected segment. Useful links for functions, libraries and utilities:

    - `stream2segment.analysis.mseeds` (small processing library implemented in this program,
      most of its functions are imported here by default)
    - `obpsy <https://docs.obspy.org/packages/index.html>`_
    - `obspy Stream object <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.html>_`
    - `obspy Trace object <https://docs.obspy.org/packages/autogen/obspy.core.trace.Trace.html>_`

    IMPORTANT: Most exceptions raised by this function will continue the execution of the next
    segment(s) after writing the exception messages to a .log file (see documentation of
    `s2s process` for details), prefixing the message with the segment id for later inspection.
    This is a feature that can be trigger programmatically to skip the currently processed segment,
    e.g.:
    ```
        if snr < 0.4:
            raise Exception('SNR ratio to low')
    ```
    Note however, that some exceptions will stop the execution of the **WHOLE** processing subroutine:
    in this case, the exception message and the stack trace will be redirected as well to the log file
    (and the standard output) for debugging.
    These critical exceptions are those preventing the execution of this function and
    those that possibly indicate deeper problems or bugs. They are:
        `TypeError`, `SyntaxError`, `NameError`, `ImportError`, `AttributeError`

    :param: segment (ptyhon object): An object representing a waveform data to be processed,
    reflecting the relative database table row. See above for a detailed list
    of attributes and methods

    :param: config (python dict): a dictionary reflecting what has been implemented in the configuration
    file. You can write there whatever you want (in yaml format, e.g. "propertyname: 6.7" ) and it
    will be accessible as usual via `config['propertyname']`

    :return: If the processing routine calling this function needs not generate a file output
    (e.g., .csv file), this function does not need to return a value, and if it does, it will be
    ignored.
    Otherwise, this function must return an iterable that will be written as a row of the resulting csv
    file (e.g. list, tuple, numpy array, dict). The .csv file will have a
    row header only if `dict`s are returned: in this case, the dict keys are used as row header
    columns. If you want to preserve in the .csv the order of the dict keys as the were inserted
    in the dict, use `OrderedDict` instead of `dict` or `{}`.
    Returning None or nothing is also valid: in this case the segment will be silently skipped

    NOTES:

    1. The first column of the resulting csv will be *always* the segment id (an integer
    stored in the database uniquely identifying the segment)

    2. Pay attention to consistency: the same type of object with the same number of elements
    should be returned by all processed segments. Unexpected (non tested) result otherwise: e.g.
    when returning a list for some segments, and a dict for some others

    3. Pay attention when returning complex objects (e.g., everything neither string nor numeric) as
    elements of the iterable: the values will be most likely converted to string according
    to python `__str__` function and might be out of control for the user.
    Thus, it is suggested to convert everything to string or number. E.g., for obspy's
    `UTCDateTime`s you could return either `float(utcdatetime)` (numeric) or
    `utcdatetime.isoformat()` (string)
    """
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
    # fixme: isn't this equivalent to the above??? :
    # normal_freqs = normal_f0 + np.arange(len(normal_spe)) * normal_df

#     normal_spe = konno_ohmachi_smoothing(normal_spe, normal_freqs, bandwidth=40,
#                                          count=1, enforce_no_matrix=False, max_memory_usage=512,
#                                          normalize=False)


    evt = segment.event
    #fcmin = mag2freq(evt.magnitude)
    fcmin = 0.001  #20s, integration for Energy starts from 16s
    fcmax = config['preprocess']['bandpass_freq_max']  # used in bandpass_remresp
    snr_ = snr(normal_spe, noise_spe, signals_form=config['sn_spectra']['type'],
               fmin=fcmin, fmax=fcmax, delta_signal=normal_df, delta_noise=noise_df)
    # snr1_ = snr(normal_spe, noise_spe, signals_form=config['sn_spectra']['type'],
    #             fmin=fcmin, fmax=1, delta_signal=normal_df, delta_noise=noise_df)
    # snr2_ = snr(normal_spe, noise_spe, signals_form=config['sn_spectra']['type'],
    #             fmin=1, fmax=10, delta_signal=normal_df, delta_noise=noise_df)
    # snr3_ = snr(normal_spe, noise_spe, signals_form=config['sn_spectra']['type'],
    #             fmin=10, fmax=fcmax, delta_signal=normal_df, delta_noise=noise_df)
    
    #if snr_ < config['snr_threshold']:
    #    raise Exception('low snr %f' % snr_)
    
    normal_spe *= delta_t

    distance_deg = segment.event_distance_deg
    distances_table = config['dist_freq_table']
    distances = config['distances']
    frequencies = config['frequencies']
    
    assert sorted(distances) == distances
    assert sorted(frequencies) == frequencies


    # calculate spectra with spline interpolation on given frequencies:
    cs = CubicSpline(normal_freqs, normal_spe)
    seg_spectrum = cs(frequencies)
    
    # old implementation:
    # seg_spectrum = spline(normal_freqs, normal_spe, frequencies) # scipy.interpolate.spline(xk, yk, xnew):
    seg_spectrum_log10 = np.log10(seg_spectrum)

    if distance_deg < distances[0] or distance_deg > distances[-1]:
        raise ValueError('Passed `distance_km`=%f not in [%f, %f]' %
                         (distance_deg, distances[0], distances[-1]))

    for distindex, d in enumerate(distances):
        if d >= distance_deg:
            break

    if distances[distindex] == distance_deg:
        correction_spectrum_log10 = distances_table[distindex]
    else:
        distances_table = np.array(distances_table).reshape(len(distances), len(frequencies))
        css = [CubicSpline(distances, distances_table[:,i]) for i in range(len(frequencies))]
        correction_spectrum_log10 = [css[freqindex](distance_deg) for freqindex in range(len(frequencies))]
        #correction_spectrum_log10 = []
        #d1, d2 = distances[distindex - 1], distances[distindex]
        #for amp1, amp2 in zip(distances_table[distindex-1], distances_table[distindex]):
        #    amp_val = amp1 + (amp2 - amp1) * (distance_deg - d1) / (d2 - d1)
        #    correction_spectrum_log10.append(amp_val)
    
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
#     elif depth_km < 43:
#         dens = 3641
#         v_pwave = 8035
#         v_swave = 4483
    else:
        v_dens = 3641
        v_pwave = 8035.5
        v_swave = 4483.9
    
    v_cost_p = (1. /(15. * pi * v_dens * (v_pwave ** 5)))
    v_cost_s = (1. /(10. * pi * v_dens * (v_swave ** 5)))
    # below I put a factor 2 but ... we don't know yet if it is needed
    energy = 2 * (v_cost_p + v_cost_s) * corrected_spectrum_int_vel_square
    me_st = (2./3.) * (np.log10(energy) - 4.4)  

    # write stuff to csv:
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
        raise Exception("%d traces (probably gaps/overlaps)" % len(stream))

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
    assert1trace(stream)  # raise and return if stream has more than one trace
    signal_wdw, noise_wdw = segment.sn_windows(config['sn_windows']['signal_window'],
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
    signal_trace, noise_trace = sn_split(segment.stream()[0],  # assumes stream has only one trace
                                         arrival_time, config['sn_windows']['signal_window'])
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
################################################################################################

def signal_smooth_spectra(segment, config):
    stream = segment.stream()
    arrival_time = UTCDateTime(segment.arrival_time) + config['sn_windows']['arrival_time_shift']
    signal_wdw, noise_wdw = sn_split(stream[0],  # assumes stream has only one trace
                                         arrival_time, config['sn_windows']['signal_window'])
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

