import pandas as pd
import numpy as np
import math
import logging

from enum import Enum
from sqlalchemy.orm import load_only

from stream2segment.process import Event, get_session

logger = logging.getLogger('me-compute.event_me')

pct = (5, 95)  # percentiles
score_th = 0.75  # anomaly score threshold
ROUND = 2  # round to be used in mean and stddev. Set to None for no rounding


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


def compute_events_me(hdf_path_or_df, dburl):
    session = get_session(dburl)
    try:
        yield from _compute_events_me(hdf_path_or_df, session)
    finally:
        session.close()


def _compute_events_me(station_me: pd.DataFrame, db_session):
    """Yield a series of dicts denoting a row of the report"""
    # see process.py:main for a list of columns:
    dfr = station_me
    for ev_db_id, evt_df in dfr.groupby('event_db_id'):

        me_values = np.asarray(evt_df['station_energy_magnitude'].values)
        waveforms_count = np.sum(np.isfinite(me_values))
        if not waveforms_count:
            logger.warning(f'Event {ev_db_id} skipped: no finite Me value found')
            continue
        me, me_std, num_waveforms = Stats.Me_p.compute(me_values)
        with pd.option_context('mode.use_inf_as_na', True):
            if pd.isna(me):
                logger.warning(f'Event {ev_db_id} skipped: Me is NaN '
                               f'(e.g. not enough station Me available)')
                continue

        event = db_session.query(Event).\
            options(load_only(Event.magnitude, Event.mag_type, Event.webservice_id,
                              Event.latitude, Event.longitude, Event.depth_km,
                              Event.time, Event.event_id)).\
            filter(Event.id == ev_db_id).one()

        group_sta = evt_df.groupby(['network', 'station'])
        yield {  # we are working in python 3.6.9+, order is preserved
            'url': event.url,
            'magnitude': event.magnitude,
            'magnitude_type': event.mag_type,
            'Me': float(np.round(me, 2)),
            'Me_stddev': float(np.round(me_std, 3)),
            'Me_waveforms_used': int(num_waveforms),
            'latitude': float(np.round(event.latitude, 5)),
            'longitude': float(np.round(event.longitude, 5)),
            'depth_km': float(np.round(event.depth_km, 3)),
            'time': event.time.isoformat('T'),
            'stations': int(group_sta.ngroups),
            'waveforms': int(waveforms_count),
            'id': str(event.event_id),
            'db_id': int(ev_db_id),  # noqa
        }


def get_html_report_rows(station_me: pd.DataFrame, events: dict):
    # Stations residuals:
    for ev_db_id, evt_df in station_me.groupby('event_db_id'):
        group_sta = evt_df.groupby(['network', 'station'])
        stas = []
        for (net, sta), sta_df in group_sta:
            lat = np.round(sta_df['station_latitude'].iat[0], 3)
            lon = np.round(sta_df['station_longitude'].iat[0], 3)
            delta_me = None
            me = events.get(ev_db_id, {}).get('Me', None)
            if me is None:
                continue
            station_me = sta_df['station_energy_magnitude'].iat[0]
            with pd.option_context('mode.use_inf_as_na', True):
                if not pd.isna(station_me):
                    delta_me = float(np.round(station_me - me, 2))

            # res = np.nan if invalid_me else \
            #     sta_df['station_energy_magnitude'].iat[0] - me_st_mean
            dist_deg = np.round(sta_df['station_event_distance_deg'].iat[0], 3)
            stas.append([lat if np.isfinite(lat) else None,
                         lon if np.isfinite(lon) else None,
                         net + '.' + sta,
                         delta_me,
                         dist_deg if np.isfinite(dist_deg) else None])

        yield events[ev_db_id], stas


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
