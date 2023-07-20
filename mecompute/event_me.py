import pandas as pd
import numpy as np
import math
import logging

from enum import Enum
from sqlalchemy.orm import load_only

from stream2segment.process import Event, get_session

logger = logging.getLogger('me-compute.event_me')


def compute_events_me(station_me: pd.DataFrame, dburl):
    """Yield Events in form of dicts from the given station_me and dburl"""
    session = get_session(dburl)
    try:
        yield from _compute_events_me(station_me, session)
    finally:
        session.close()


def _compute_events_me(station_me: pd.DataFrame, db_session):
    """Yield Events in form of dicts from the given station_me and db session"""
    # see process.py:main for a list of columns:
    dfr = station_me
    for ev_db_id, evt_df in dfr.groupby('event_db_id'):

        me_values = np.asarray(evt_df['station_energy_magnitude'].values)
        waveforms_count = np.sum(np.isfinite(me_values))
        if not waveforms_count:
            logger.warning(f'Event {ev_db_id} skipped: no finite Me value found')
            continue
        me, me_std, num_waveforms = avg_std_count_within_percentiles(me_values)
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


def avg_std_count(values, weights=None, na_repr=None, round=None):
    """Return the weighted average, standard deviation and count of finite values
    from the arguments
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
        # stddev:
        variance = np.average((values - average) ** 2, weights=weights)
        mean, std = float(average), math.sqrt(variance)
        if round is not None:
            mean = np.round(mean, round)
            std = np.round(std, round)
    return [mean, std, count]


def avg_std_count_within_percentiles(values, plow=5, phight=95, round=2):
    """Return the average, stddev and count of the given values within the
    given percentiles (5-95 by default)
    See avg_std_count for details
    """
    p_low, p_high = np.nanpercentile(values, [plow, phight])
    values = values[(values >= p_low) & (values <= p_high)]
    return avg_std_count(values, None, round=round)


# legacy code used in the test phase (ignore):

def avg_std_count_anomalyscore_th(values, anomalyscores, score_th=.75, round=2):
    """Return the average, stddev and count of the given values, discarding
    values whose anomaly score is higher than the specified threshold (.75)
    See avg_std_count for details
    """

    values = values[anomalyscores < score_th]
    return avg_std_count(values, None, round=round)


def avg_std_count_anomalyscore_weight(values, anomalyscores, round=2):
    """Return the average, stddev and count of the given values, with weights
    mapped linearly from the inverse of the given anomaly scores
    See avg_std_count for details
    """
    maxscore = 0.86059521298447561
    minscore = 0.41729107098205132
    weights = 1 - ((anomalyscores-minscore) / (maxscore-minscore))
    #            |
    #          1 +  ooo
    #  weight    |       o
    #          0 +          ooo
    #            +----+-----+-----
    #                .4    .8
    #              anomalyscore
    return avg_std_count(values, np.clip(weights, 0., 1.), None, round=round)


def avg_std_count_anomalyscore_weight2(values, anomalyscores, round=2):
    """Return the average, stddev and count of the given values, with weights
    mapped non-linearly from the inverse of the given anomaly scores
    See avg_std_count for details
    """
    maxscore = 0.86059521298447561
    minscore = 0.41729107098205132
    # use a parabola fitting with vertex in (0.5, 1): all scores
    # <=0.5 are converted to weight 1, all scores >0.5 have weights
    # decreasing parabolically:
    #            |
    #          1 +  ooo
    #            |       o
    #  weight    |        o
    #          0 +         ooo0
    #            +----+----+-----
    #                .5   .8
    #              anomalyscore
    #
    # The fitting parabola has vertex in (0.5, 1)
    # and passes through (maxscore, 0)
    # the coefficients (calculated manually) are:
    b = 1. / (maxscore ** 2 - maxscore + 0.25)
    a = -b
    c = 1. - b / 4.0
    weights = a * anomalyscores ** 2 + b * anomalyscores + c
    weights[anomalyscores <= 0.5] = 1.
    return avg_std_count(values, np.clip(weights, 0., 1.), None, round=round)
