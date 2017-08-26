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

# External modules
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults

import pandas as pd

import pymepps

# Internal modules
from .wm_ftp_sensor import get_filename


logger = logging.getLogger(__name__)


class WMEncodeTemp(BaseOperator):
    @apply_defaults
    def __init__(self, disk_path, temp_path, *args, **kwargs):
        """
        WMEncodeTemp is used to encode a Wettermast Ascii to a temporary json
        file.

        Parameters
        ----------
        disk_path : str
            Disk path where the Wettermast files are archived.
        temp_path : str
            Temporary path where the json file should be saved.
        """
        super().__init__(*args, **kwargs)
        self.disk_path = disk_path
        self.temp_path = temp_path

    def execute(self, context):
        wm_filename = get_filename(context['execution_date'])
        disk_file_path = os.path.join(self.disk_path, wm_filename)
        temp_filename = 'wettermast_{0:s}.json'.format(
            context['execution_date'].strftime('%Y%m%d%H%M'))
        temp_file_path = os.path.join(self.temp_path, temp_filename)
        wm_ds = pymepps.open_station_dataset(disk_file_path, 'wm',
                                             checking=False)
        wm_df = wm_ds.select_ds()
        wm_df.pp.save(temp_file_path)


