from collections import OrderedDict
from os.path import join, dirname

import pandas as pd
import numpy as np
import math
# import sys


from enum import Enum

from sqlalchemy.orm import load_only

# from stream2segment.download.db import get_session
# from stream2segment.io import yaml_load
# from stream2segment.io.db import close_session
# from stream2segment.download.db.models import Event

pct = (5, 95)  # percentiles
score_th = 0.75  # anomaly score thresold
ROUND = 2  # round to be used in mean and stdev. Set to None for no rounding


class Stats(Enum):

    # legacy code for docstrings (not used anymore. Also note we compute only Me_p now):
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


def get_report_rows(hdf_path_or_df, dburl):
    from stream2segment.process import get_session
    session = get_session(dburl)
    try:
        yield from _get_report_rows(hdf_path_or_df, session)
    finally:
        session.close()


def _get_report_rows(hdf_path_or_df, db_session):
    """Yield a series of dicts denoting a row of the report"""
    from stream2segment.process import Event

    # see process.py:main for a list of columns:
    dfr = hdf_path_or_df
    if not isinstance(hdf_path_or_df, pd.DataFrame):
        dfr: pd.DataFrame = pd.read_hdf(hdf_path_or_df)  # noqa

    for ev_db_id, df_ in dfr.groupby('event_db_id'):
        event = db_session.query(Event).\
            options(load_only(Event.magnitude, Event.mag_type, Event.webservice_id,
                              Event.latitude, Event.longitude, Event.depth_km,
                              Event.time, Event.event_id)).\
            filter(Event.id == ev_db_id).one()

        group_sta = df_.groupby(['network', 'station'])

        # (Convention: keys with spaces will be replaced with '<br> in HTMl template)
        event = {  # we are working in python 3.6.9+, order is preserved
            'url': event.url,
            'magnitude': event.magnitude,
            'magnitude_type': event.mag_type,
            'Me': np.nan,
            'Me_stddev': np.nan,
            'Me_waveforms_used': 0,
            'latitude': float(np.round(event.latitude, 5)),
            'longitude': float(np.round(event.longitude, 5)),
            'depth_km': float(np.round(event.depth_km, 3)),
            'time': event.time.isoformat('T'),
            'stations': int(group_sta.ngroups),
            'waveforms': 0,
            'id': str(event.event_id),
            'db_id': int(ev_db_id),  # noqa
        }

        values = np.asarray(df_['station_energy_magnitude'].values)

        waveforms_count = np.sum(np.isfinite(values))
        if not waveforms_count:
            continue
        event['waveforms'] = int(waveforms_count)

        # anomalyscores = np.asarray(df_['signal_amplitude_anomaly_score'].values)
        me, me_std, num_waveforms = Stats.Me_p.compute(values)
        with pd.option_context('mode.use_inf_as_na', True):
            invalid_me = pd.isna(me)

        event['Me'] = None if invalid_me else float(np.round(me, 2))
        event['Me_stddev'] = None if invalid_me else float(np.round(me_std, 3))
        event['Me_waveforms_used'] = int(num_waveforms)

        # Stations residuals:
        me_st_mean = Stats.Me_p.compute(values)[0]  # row['Me M']
        stas = []
        for (net, sta), sta_df in group_sta:
            lat = np.round(sta_df['station_latitude'].iat[0], 3)
            lon = np.round(sta_df['station_longitude'].iat[0], 3)
            delta_me = None
            station_me = sta_df['station_energy_magnitude'].iat[0]
            if not invalid_me:
                with pd.option_context('mode.use_inf_as_na', True):
                    if not pd.isna(station_me):
                        delta_me = station_me - me_st_mean
            # res = np.nan if invalid_me else \
            #     sta_df['station_energy_magnitude'].iat[0] - me_st_mean
            dist_deg = np.round(sta_df['station_event_distance_deg'].iat[0], 3)
            stas.append([lat if np.isfinite(lat) else None,
                         lon if np.isfinite(lon) else None,
                         net + '.' + sta,
                         delta_me,
                         dist_deg if np.isfinite(dist_deg) else None])

        yield event, stas
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
