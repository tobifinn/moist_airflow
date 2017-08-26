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


class WMTemp2DB(BaseOperator):
    @apply_defaults
    def __init__(self, temp_path, db_file, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.temp_path = temp_path
        self.db_file = db_file

    def execute(self, context):
        temp_filename = 'wettermast_{0:s}.json'.format(
            context['execution_date'].strftime('%Y%m%d%H%M'))
        temp_file_path = os.path.join(self.temp_path, temp_filename)
        temp_df = pd.DataFrame.pp.load(temp_file_path)
        try:
            db_df = pd.DataFrame.pp.load(self.db_file)
            db_df = db_df.pp.update(temp_df)
        except FileNotFoundError:
            db_df = temp_df
        time_bound = context['execution_date'] - datetime.timedelta(days=7)
        db_df = db_df.loc[
            (db_df.index <= context['execution_date']) &
            (db_df.index > time_bound)]
        db_df.pp.save(self.db_file)
