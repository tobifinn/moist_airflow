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
import os
import datetime

# External modules
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults

import pandas as pd

import pymepps

# Internal modules


logger = logging.getLogger(__name__)


def df_update_another(input_path, input_template, another_path,
                      another_template, time_bound=None, *args, **kwargs):
    """
    Update a pandas.dataframe based on another pandas.dataframe.

    Parameters
    ----------
    input_path : str
        The folder where the original wettermast file is saved.
    another_path : str
        The folder where the json file should be saved.
    input_template : str
        The filename template of the input file. The filename template is
        used within the strftime of contexts execution_date. So please see
        within the datetime documentation for format options
    another_template : str
        The filename template of the output file. The filename template is
        used within the strftime of contexts execution_date. So please see
        within the datetime documentation for format options.
    time_bound : datetime.timedelta or None, optional
        The time bound is used to limit the index of the resulting updated
        dataframe. This could be used if the dataframe is used as database. If
        time_bound is None this limitation is skipped. Default is None
    """
    execution_date = kwargs['execution_date']
    another_filename = execution_date.strftime(another_template)
    another_file_path = os.path.join(another_path, another_filename)
    try:
        another_df = pd.read_json(another_file_path, orient='split',
                                  typ='frame')
    except ValueError:
        another_df = pd.read_json(another_file_path, orient='split',
                                  typ='series')
    if isinstance(another_df.index, pd.DatetimeIndex):
        another_df.index = another_df.index.tz_localize('UTC')
    input_filename = execution_date.strftime(input_template)
    input_file_path = os.path.join(input_path, input_filename)
    if os.path.isfile(input_file_path):
        try:
            input_df = pd.read_json(input_file_path, orient='split',
                                      typ='frame')
        except ValueError:
            input_df = pd.read_json(input_file_path, orient='split',
                                      typ='series')
        if isinstance(input_df.index, pd.DatetimeIndex):
            input_df.index = input_df.index.tz_localize('UTC')
        resulting_df = input_df.pp.update(another_df)
    else:
        resulting_df = another_df
    if isinstance(time_bound, datetime.timedelta):
        resulting_df = resulting_df.loc[
            resulting_df.index>execution_date-time_bound]
    resulting_df.to_json(input_file_path, orient='split', date_format='iso')
