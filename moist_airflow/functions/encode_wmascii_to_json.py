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

import pymepps

# External modules

# Internal modules
import moist_airflow.functions.utiltities as utils

logger = logging.getLogger(__name__)


def encode_wmascii_to_json(input_path, output_path,
                           input_template='%G_W%V_MASTER_M10.txt',
                           output_template='wettermast_%Y%m%d%H%M.json',
                           *args,
                           **kwargs):
    """
    Encode an ascii Wettermast-conform file into a pandas.dataframe conform
    json file. The json is saved under output_path/output_template

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
    """
    input_file_path = utils.compose_address(
        kwargs['execution_date'], input_path, input_template)
    output_file_path = utils.compose_address(
        kwargs['execution_date'], output_path, output_template)
    ds = pymepps.open_station_dataset(input_file_path, 'wm', checking=False)
    df = ds.select_ds()
    df.to_json(output_file_path, orient='split', date_format='iso')
