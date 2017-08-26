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

# External modules
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults

import pandas as pd
import pymepps

# Internal modules


logger = logging.getLogger(__name__)


class PlotOperator(BaseOperator):
    @apply_defaults
    def __init__(self, plot_function, obs_path, fcst_path, plot_file,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.plot_function = plot_function
        self.obs_path = obs_path
        self.fcst_path = fcst_path
        self.plot_file = plot_file

    @property
    def observation(self):
        obs_df = pd.DataFrame.pp.load(self.obs_path)
        return obs_df

    @property
    def forecast(self):
        fcst_df = pd.DataFrame.pp.load(self.fcst_path)
        return fcst_df

    def execute(self, context):
        self.plot_function(self.observation, self.forecast, self.plot_file,
                           context['execution_date'])
