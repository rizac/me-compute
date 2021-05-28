from collections import OrderedDict
from os.path import join, dirname

import pandas as pd
import numpy as np
import math
import sys


from enum import Enum

from stream2segment.download.db import get_session
from stream2segment.io import yaml_load
from stream2segment.io.db import close_session
from stream2segment.download.db.models import Event

pct = (5, 95)  # percentiles
score_th = 0.75  # anomaly score thresold
ROUND = 2  # round to be used in mean and stdev. Set to None for no rounding


class Stats(Enum):

    Me = 'Me stats are computed on all waveforms'
    Me_p = 'Me stats computed on the waveforms in the {0:d}-{1:d} ' \
           'percentiles'.format(*pct)
    Me_t = 'Me stats computed on the waveforms with anomaly score < %f' % score_th
    Me_w = 'Me stats are computed on all waveforms, weighting values with ' \
           'the inverse of the anomaly score (mapping linearly score and weights)'
    Me_w2 = 'Me stats are computed on all waveforms, weighting values with ' \
            'the inverse of the anomaly score (mapping non-linearly score and weights)'

    def compute(self, values, *options):
        values = np.asarray(values)
        weights = None

        if self is Stats.Me:
            pass
        if self is Stats.Me_p:
            p_low, p_high = np.nanpercentile(values, pct)
            values = values[(values >= p_low) & (values <= p_high)]
        elif self is Stats.Me_t:
            anomalyscores = np.asarray(options[0])
            values = values[anomalyscores < score_th]
        elif self is Stats.Me_w or self is Stats.Me_w2:
            anomalyscores = np.asarray(options[0])
            weights = (LinearScore2Weight if self is Stats.Me_w
                       else ParabolicScore2Weight).convert(anomalyscores)
        else:
            # should never happen right? for safety:
            raise ValueError('%s is not a Stats enumeration item' % str(self))

        return avg_std_count(values, weights, round=ROUND)

    @classmethod
    def as_help_dict(cls):
        return {_.name: _.value for _ in cls}
        # string = []
        # for _ in cls:
        #     string.append("<b>%s</b>: %s" % (_.name, _.value))
        # return "<p>".join(string)



def get_report_rows(hdf_path):
    """Yield a series of dicts denoting a row of the report"""
    # Note: we can avoid OrderedDicts as the server uses python3.6.9, which
    # already implements dicts preserving keys insertion order
    # (https://stackoverflow.com/a/39537308)

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
    # 'location' = segment.channel.channel
    # 'ev_id' = segment.event.id  # event metadata
    # 'ev_time' = segment.event.time
    # 'ev_lat' = segment.event.latitude
    # 'ev_lon' = segment.event.longitude
    # 'ev_dep' = segment.event.depth_km
    # 'ev_mag' = segment.event.magnitude
    # 'ev_mty' = segment.event.mag_type
    # 'st_id' = segment.station.id  # station metadata
    # 'network' = segment.station.network
    # 'station' = segment.station.station
    # 'st_lat' = segment.station.latitude
    # 'st_lon' = segment.station.longitude
    # 'st_ele' = segment.station.elevation
    # 'integral' = corrected_spectrum_int_vel_square
    dfr = pd.read_hdf(hdf_path)

    # fetch event ids from the event catalog:
    sess = None
    try:
        sess = get_session(
            yaml_load(join(dirname(__file__), 's2s_config', 'download.private.yaml'))[
                'dburl'])
        evid2catalogid = {item[0]: item[1] for item in
                          sess.query(Event.id, Event.event_id).
                              filter(Event.id.in_(dfr.ev_id.tolist()))}
    except Exception as exc:
        raise ValueError('Unable to fetch data from db (%s): %s' %
                         (exc.__class__.__name__, str(exc)))
    finally:
        close_session(sess)

    for ev_id, df_ in dfr.groupby('ev_id'):

        group_sta = df_.groupby(['network', 'station'])

        # (Convention: keys with spaces will be replaced with '<br> in HTMl template)
        row = {  # we are working in python 3.6.9+, order is preserved
            'event id': ev_id,
            'catalog id': evid2catalogid.get(ev_id, ''),
            # 'GEOFON event id': df_.ev_evid.iat[0],
            # df_ has all event related columns made of 1 unique value, so take 1st:
            'Mw': np.round(df_.ev_mag.iat[0], 2),
            'lat': np.round(df_.ev_lat.iat[0], 2),
            'lon': np.round(df_.ev_lon.iat[0], 2),
            'depth km': np.round(df_.ev_dep.iat[0], 1),
            'time': df_.ev_time.iat[0].isoformat('T'),
            'stations': group_sta.ngroups
        }

        values = np.asarray(df_.me_st.values)
        waveforms_count = np.sum(np.isfinite(values))
        if not waveforms_count:
            continue
        row['waveforms'] = waveforms_count

        anomalyscores = np.asarray(df_.aascore.values)

        row.update(dict(zip(['Me_p M', 'Me_p SD', 'Me_p #'], Stats.Me_p.compute(values))))
        row.update(dict(zip(['Me_t M', 'Me_t SD', 'Me_t #'], Stats.Me_t.compute(values, anomalyscores))))
        row.update(dict(zip(['Me_w M', 'Me_w SD'], Stats.Me_w.compute(values, anomalyscores))))
        row.update(dict(zip(['Me_w2 M', 'Me_w2 SD'], Stats.Me_w2.compute(values, anomalyscores))))

        # Stations residuals:
        me_st_mean = Stats.Me_p.compute(values)[0] # row['Me M']
        invalid_mean = me_st_mean is None or not np.isfinite(me_st_mean)
        stas = []
        for (net, sta), sta_df in group_sta:
            lat = np.round(sta_df['st_lat'].iat[0], 3)
            lon = np.round(sta_df['st_lon'].iat[0], 3)
            res = np.nan if invalid_mean else sta_df['me_st'].iat[0] - me_st_mean
            dist_deg = np.round(sta_df['dist_deg'].iat[0], 3)
            stas.append([lat if np.isfinite(lat) else None,
                         lon if np.isfinite(lon) else None,
                         net + '.' + sta,
                         res if np.isfinite(res) else None,
                         dist_deg if np.isfinite(dist_deg) else None])

        yield ev_id, {k: v for k, v in row.items()}, stas
        # yield row


class Score2Weight:
    # these are the min score and max score of the currently selected model
    # in sdaas:
    maxscore = 0.86059521298447561
    minscore = 0.41729107098205132

    @classmethod
    def convert(cls, scores):
        """converts scores to weights, returning a numpy array of values in [0, 1]
        """
        scores = np.asarray(scores)
        if not np.isfinite(scores).any():
            return None  # no weight
        weights = cls._to_weights(scores, cls.minscore, cls.maxscore)
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


class LinearScore2Weight(Score2Weight):

    @classmethod
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


class ParabolicScore2Weight(Score2Weight):

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
        b = 1. / (maxscore**2 - maxscore + 0.25)
        a = -b
        c = 1. - b/4.0
        weights = a * scores**2 + b * scores + c
        weights[scores <= 0.5] = 1.
        return weights


def avg_std_count(values, weights=None, na_repr=None, round=None):
    """Return the weighted average, standard deviation and count of values
    as dict with keys 'mean', 'stddev', 'count'

    values, weights -- Numpy ndarrays with the same shape.
    """
    # we use np.average because it supports weights, but contrarily to np.mean it
    # does not have a 'nanaverage' counterpart. So:
    indices = np.isfinite(values)
    if weights is not None:
        indices &= np.isfinite(weights)
        weights = weights[indices]
    values = values[indices]

    mean, std, count = na_repr, na_repr, len(values)
    if count > 0:
        average = np.average(values, weights=weights)
        # stdev:
        variance = np.average((values - average) ** 2, weights=weights)
        mean, std = float(average), math.sqrt(variance)
        if round is not None:
            mean = np.round(mean, round)
            std = np.round(std, round)
    return [mean, std, count]
