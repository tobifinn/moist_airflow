#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 27.08.17
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

# External modules
import pandas as pd

# Internal modules


logger = logging.getLogger(__name__)


def df_extract_columns(input_path, input_template, output_path,
                       output_template, column_names, *args, **kwargs):
    """
    Extract one or several columns from a pandas.dataframe and save it to a
    given path.

    Parameters
    ----------
    input_path : str
        The folder where the original wettermast file is saved.
    output_path : str
        The folder where the json file should be saved.
    input_template : str
        The filename template of the input file. The filename template is
        used within the strftime of contexts execution_date. So please see
        within the datetime documentation for format options
    output_template : str
        The filename template of the output file. The filename template is
        used within the strftime of contexts execution_date. So please see
        within the datetime documentation for format options.
    column_names : str or iterable
        The name(s) of column(s) which should be extracted.

    Notes
    -----
    Internally this is a wrapper around pandas.dataframe.loc
    """
    input_filename = kwargs['execution_date'].strftime(input_template)
    input_file_path = os.path.join(input_path, input_filename)
    input_df = pd.read_json(input_file_path, orient='split',
                            typ='frame')
    if isinstance(input_df.index, pd.DatetimeIndex):
        input_df.index = input_df.index.tz_localize('UTC')
    output_df = input_df.loc[:, column_names]
    output_filename = kwargs['execution_date'].strftime(output_template)
    output_file_path = os.path.join(output_path, output_filename)
    output_df.to_json(output_file_path, orient='split', date_format='iso')
