#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 26.08.17
#
# Created for moist_airflow
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@studium.uni-hamburg.de
#
#    Copyright (C) {2017}  {Tobias Sebastian Finn}
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# System modules
import logging
import datetime

# External modules
import numpy as np

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec

# Internal modules


logger = logging.getLogger(__name__)

last_n_hours = 6
dpi = 72


def large(measured, forecast, location, date):
    size = (8.3, 5.25)
    measured = measured.copy()
    forecast = forecast[
               date-datetime.timedelta(hours=last_n_hours):
               date+datetime.timedelta(hours=36)]
    measured = measured[
               date-datetime.timedelta(hours=last_n_hours):
               date+datetime.timedelta(hours=36)]
    measured = measured.dropna()
    forecast.index = forecast.index.tz_convert('Europe/Berlin').tz_localize(
        None)
    measured.index = measured.index.tz_convert('Europe/Berlin').tz_localize(
        None)
    unit = "°C"
    ylim = [int(np.floor(min(forecast['mean'].min(), measured.min())-0.5)),
            int(np.floor(max(forecast['mean'].max(), measured.max())+1))]
    ylim[1] += int(np.round((ylim[1]-ylim[0])/10))
    fig = plt.figure(figsize=size, dpi=dpi)
    gs1 = gridspec.GridSpec(1, 1)
    gs1.update(left=0.085, right=0.95, top=0.85, bottom=0.15)
    ax1 = plt.subplot(gs1[:, :])
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.spines['bottom'].set_color('0.5')
    ax1.spines['left'].set_color('0.5')
    ax1.get_xaxis().tick_bottom()
    ax1.get_yaxis().tick_left()
    time_axis = forecast.index[forecast.index.hour % 6 == 0]
    value_axis = range(ylim[0], ylim[1])
    for time in time_axis:
        if time > forecast.index.min()+pd.Timedelta('1 hour'):
            ax1.vlines(time, ylim[0], ylim[1],
                       linestyles="-", color="0.8")
    for val in value_axis[1:]:
        ax1.hlines(val, forecast.index.min(),
                   forecast.index.max() + dt.timedelta(minutes=120),
                   linestyles="-", color="0.8")
    if ylim[0] < 0:
        ax1.hlines(0, forecast.index.min(),
                   forecast.index.max()+dt.timedelta(minutes=120),
                   linestyles="-", linewidth="2", color="0.5")

    plt_forecast = forecast[forecast.index.hour % 3 == 0]
    ax1.errorbar(plt_forecast.index, plt_forecast['mean'],
                 yerr=[plt_forecast['q25'], plt_forecast['q75']],
                 color="#CC5F0A", linestyle=' ', marker="x", mew=2,
                 markersize=8)
    ax1.plot(measured.index, measured,
             color="#008000", lw=2, label="Messung")

    for time in time_axis:
        ax1.text(time, ylim[1],
                 "%0.1f %s" % (forecast['mean'][time], unit),
                 horizontalalignment='center',
                 verticalalignment='bottom', color='#CC5F0A',
                 size="11")
        try:
            ax1.text(time, ylim[1]+(ylim[1]-ylim[0])/20,
                     "%0.1f %s" % (measured[time], unit),
                     horizontalalignment='center',
                     verticalalignment='bottom', color='#008000',
                     size="11")
        except KeyError:
            pass

    hoursFmt = mdates.DateFormatter('%H:%M')
    ax1.xaxis.set_major_formatter(hoursFmt)
    ax1.set_xlim(forecast.index.min(), forecast.index.max() +
                 datetime.timedelta(minutes=120))
    ax1.set_xticks(time_axis)
    for tic in ax1.xaxis.get_major_ticks():
        tic.tick1On = tic.tick2On = False
    [t.set_color('0.2') for t in ax1.xaxis.get_ticklabels()]

    ax1.set_ylabel('2 Meter Temperatur [°C]', color='0.2')
    ax1.set_ylim(ylim[0], ylim[1])
    ax1.set_yticks(range(ylim[0]+1, ylim[1], 1))
    for tic in ax1.yaxis.get_major_ticks():
        tic.tick1On = tic.tick2On = False
    [t.set_color('0.2') for t in ax1.yaxis.get_ticklabels()]

    gs2 = gridspec.GridSpec(1, 1)
    gs2.update(left=0.085, right=0.95, top=0.050000001, bottom=0.05)
    ax2 = plt.subplot(gs2[:, :])
    for tic in ax2.xaxis.get_major_ticks():
        tic.tick1On = tic.tick2On = False
    for tic in ax2.yaxis.get_major_ticks():
        tic.tick1On = tic.tick2On = False
    dates = pd.date_range(forecast.index.min(),
                          forecast.index.max()+datetime.timedelta(minutes=120),
                          freq="H")
    axis2dates = []
    hours, offset = 0, 0
    for date in dates:
        if date.hour % 24 == 0:
            if hours > 6:
                axis2dates[int(offset+hours/2)] = \
                    (date - datetime.timedelta(hours=1)).strftime("%d.%m.%Y")
            axis2dates.append("|")
            offset = offset+hours+1
            hours = 0
        else:
            axis2dates.append("")
            hours += 1
    if hours > 6:
        axis2dates[int(offset+hours/2)] = \
            (date - datetime.timedelta(hours=1)).strftime("%d.%m.%Y")
    ax2.set_xlim(dates[0], dates[-1])
    ax2.xaxis.set_label_position('bottom')
    ax2.xaxis.tick_top()
    ax2.set_xticks(dates, minor=False)
    ax2.set_xticklabels(axis2dates, minor=False)
    [t.set_color('0.2') for t in ax2.xaxis.get_ticklabels()]
    for tic in ax2.xaxis.get_major_ticks():
        tic.tick1On = tic.tick2On = False
    ax2.set_frame_on(False)
    ax2.get_yaxis().set_visible(False)
    ax2.spines["bottom"].set_visible(False)
    plt.suptitle(
        "Init {0:s} Uhr, alle Zeiten in Lokalzeit".format(
            forecast.index[0].strftime("%d.%m.%Y %H:%M")),
         y=0, x=0.98, horizontalalignment='right', verticalalignment='bottom',
        fontsize="8", color="0.2")
    gs3 = gridspec.GridSpec(1, 1)
    gs3.update(left=0.085, right=0.95, top=0.930001, bottom=0.93)
    ax3 = plt.subplot(gs3[:, :])
    for tic in ax3.xaxis.get_major_ticks():
        tic.tick1On = tic.tick2On = False
    for tic in ax3.yaxis.get_major_ticks():
        tic.tick1On = tic.tick2On = False
    ax3.set_frame_on(False)
    ax3.get_yaxis().set_visible(False)
    ax3.get_xaxis().set_visible(False)
    ax3.set_title("Temperaturvorhersage")
    plt.savefig(location, size=size, dpi=dpi)
