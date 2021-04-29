from collections import OrderedDict

import pandas as pd
import numpy as np
import math


def create_summary(hdf_path):
    """Yields a series of tuples:
        (label:str, description:str, table)
    denoting different summary tables using different strategies for computing
    the Me statistics. `table` is a List of Dicts where each dict denotes
    a table row and represents a single event with the relative statistics
    (computed on all event stations)
    """
    # Group the table by event_id and calculate the event Me:
    #
    # 	1.  (mean, stdev, count) on all stations
    #
    # 	2.  (mean, stdev, count) trimmed on all stations in the range [5 95] percentiles
    #
    # 	3.  (mean, stdev, count) weighted with anomaly score (my weight)
    #
    # 	4.  (media, stdev, count) weighted with anomaly score (my weight)
    #

    # Note: we can avoid OrderedDicts as the server uses python3.6.9, which
    # already implements dicts preserving keys insertion order
    # (https://stackoverflow.com/a/39537308)
    pct = (5, 95)  # percentiles
    LABELS = {
        'Me': 'Me stats are computed on all station recordings',
        'Me_{0:d}_{1:d}_perc'.format(*pct): 'Me stats are computed '
                                            'on station recordings whose '
                                            'Me is in the [5, 95] percentiles',
        'Me_weighted_aascore_linear': 'Me stats are computed on all station '
                                      'recordings and weighted by the recording '
                                      'amplitude anomaly score (the higher the '
                                      'score the lower the weight, using a linear '
                                      'mapping)',
        'Me_weighted_aascore_nonlinear': 'Me stats are computed on all station '
                                         'recordings and weighted by the recording '
                                         'amplitude anomaly score (the higher the '
                                         'score the lower the weight, using a 2-degree '
                                         'polynomial mapping)'
    }

    # TABLES maps each label above to a list of dicts, each dict is a new
    # row of the table data to be displayed on the html
    TABLES = {k: [] for k in LABELS}
    # read computed dataframe
    # these are the columns of the hdf:
    # (medaily.py file on preocessing dir, on the server):
    #
    # 'snr' = snr_
    # 'aascore': aascore
    # 'satu' = flag_ratio
    # 'dist_deg' = distance_deg  # dist
    # 's_r' = trace.stats.sampling_rate
    # 'me_st' = me_st
    # 'channel' = segment.channel.channel
    # 'ev_id' = segment.event.id  # event metadata
    # 'ev_time' = segment.event.time
    # 'ev_lat' = segment.event.latitude
    # 'ev_lon' = segment.event.longitude
    # 'ev_dep' = segment.event.depth_km
    # 'ev_mag' = segment.event.magnitude
    # 'ev_mty' = segment.event.mag_type
    # 'st_id' = segment.station.id  # station metadata
    # 'st_net' = segment.station.network
    # 'st_name' = segment.station.station
    # 'st_lat' = segment.station.latitude
    # 'st_lon' = segment.station.longitude
    # 'st_ele' = segment.station.elevation
    # 'integral' = corrected_spectrum_int_vel_square
    dfr = pd.read_hdf(hdf_path)

    for ev_id, df_ in dfr.groupby('ev_id'):

        evt_info = {
            'event id': ev_id,
            'lat': df_.ev_lat.at[0],
            'lon': df_.ev_lon.at[0],
            'time': df_.ev_time.at[0],
            'mag': df_.ev_mag.at[0]
        }

        values = np.asarry(df_.me_st.values)
        finite_values = np.isfinite(values)
        values = values[finite_values]
        if not len(values):
            for lbl in LABELS:
                TABLES[lbl].append({**evt_info, **avg_std_count([])})
            continue

        # compute values:
        lbl = LABELS[0]
        stats = avg_std_count(values)
        TABLES[lbl].append({**evt_info, **stats})

        # compute values within [5, 95] percentiles (or whatever is given above):
        lbl = LABELS[1]
        p_low, p_high = np.nanpercentile(values, pct)
        p_filter = (values >= p_low) & (values <= p_high)
        if p_filter.any():
            p_values = values[p_filter]
            stats = avg_std_count(p_values)
        else:
            stats = avg_std_count([])
        TABLES[lbl].append({**evt_info, **stats})

        # compute weighted averages:
        values = np.asarry(df_.me_st.values)
        scores = np.asarray(df_.aascore.values)
        finite_values = np.isfinite(values) & np.isfinite(scores)
        values, scores = [], None
        if finite_values.any():
            values, scores = values[finite_values], scores[finite_values]
        for lbl, weighter in zip(LABELS[-2:], [LinearWeigher, ParabolicWeighter]):
            weights = None if scores is None else weighter.to_weights(scores)
            stats = avg_std_count(values, weights)
            TABLES[lbl].append({**evt_info, **stats})

    for label in LABELS:
        yield label, LABELS[label], TABLES[label]


def create_html(hdf_path):
    pass

class Weighter:
    # these are the min score and max score of the currently selected model
    # in sdaas:
    maxscore = 0.86059521298447561
    minscore = 0.41729107098205132

    @classmethod
    def to_weights(cls, scores):
        """converts scores to weights, returning a numpy array of values in [0, 1]
        """
        weights = cls._to_weights(np.asarray(scores), cls.minscore, cls.maxscore)
        return np.clip(weights, 0., 1.)

    @classmethod
    def _to_weights(cls, scores, minscore, maxscore):
        """converts scores to weights

        :param scores: numpy array of floats (usually but not necessarily in [0, 1])
        :param minscore: the min possible score (not necessarily `min(scores)`)
        :param maxscore: the max possible score (not necessarily `max(scores)`)

        :return: a numpy array of floats the same size of `scores`. Values
            outside [0, 1] will be clipped (limited) within that interval
        """
        raise NotImplementedError('Not implemented')


class LinearWeigher:

    def _to_weights(cls, scores, minscore, maxscore):
        """converts scores to weights

        :param scores: numpy array of floats (usually but not necessarily in [0, 1])
        :param minscore: the min possible score (not necessarily `min(scores)`)
        :param maxscore: the max possible score (not necessarily `max(scores)`)

        :return: a numpy array of floats the same size of `scores`. Values
            outside [0, 1] will be clipped (limited) within that interval
        """
        # linear weight normalized between 0 and 1 and inverted
        # (lower anomaly scores -> higher weight)
        return 1 - ((scores-minscore) / (maxscore-minscore))


class ParabolicWeighter:

    @classmethod
    def _to_weights(cls, scores, minscore, maxscore):
        """converts scores to weights

        :param scores: numpy array of floats (usually but not necessarily in [0, 1])
        :param minscore: the min possible score (not necessarily `min(scores)`)
        :param maxscore: the max possible score (not necessarily `max(scores)`)

        :return: a numpy array of floats the same size of `scores`. Values
            outside [0, 1] will be clipped (limited) within that interval
        """
        # use a parabola fitting with vertex in (0.5, 1): all scores
        # <=0.5 are converted to weight 1, all scores >0.5 have weights
        # decreasing parabolically. The fitting parabola has vertex in (0.5, 1)
        # and passes through (maxscore, 0)
        # the coefficients (calculated manually) are:
        a = 1. / (maxscore**2 - maxscore + 0.25)
        b = -a
        c = 1. + a/4.0
        return a * scores**2 + b * scores + c


def avg_std_count(values, weights=None):
    """
    Return the weighted average, standard deviation and count

    values, weights -- Numpy ndarrays with the same shape.
    """
    mean, std, count = None, None, len(values)
    if count > 0:
        average = np.average(values, weights=weights)
        # Fast and numerically precise:
        variance = np.average((values-average)**2, weights=weights)
        mean, std = float(average), math.sqrt(variance)
    return {'Me_mean': mean, 'Me_stdev': std, 'Me_count': count}


def weighted_avg_and_std(values, weights):
    """
    Return the weighted average and standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    average = np.average(values, weights=weights)
    # Fast and numerically precise:
    variance = np.average((values-average)**2, weights=weights)
    return (average, math.sqrt(variance))