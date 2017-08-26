#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 23.08.17
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

# Internal modules
from .wm_ftp_sensor import get_filename


logger = logging.getLogger(__name__)


class WMFileAvailable(BaseOperator):
    @apply_defaults
    def __init__(self, disk_path, *args, **kwargs):
        """
        WMFileAvailable is used to check if a file is available.

        Parameters
        ----------
        disk_path : str
            Disk path where the Wettermast files are archived.
        """
        super().__init__(*args, **kwargs)
        self._disk_path = None
        self.disk_path = disk_path

    @property
    def disk_path(self):
        return self._disk_path

    @disk_path.setter
    def disk_path(self, path):
        if not isinstance(path, str):
            raise TypeError('The given disk path is not a string!')
        elif os.path.isfile(path):
            raise TypeError('The given path is already a file!')
        elif not os.path.isdir(path):
            os.makedirs(path)
        else:
            self._disk_path = path

    def execute(self, context):
        wm_filename = get_filename(context['execution_date'])
        disk_file_path = os.path.join(self.disk_path, wm_filename)
        if not os.path.isfile(disk_file_path):
            raise FileNotFoundError(
                'The given file path {0:s} couldn\'t found'.format(
                    disk_file_path))
