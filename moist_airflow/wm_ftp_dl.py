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
from airflow.contrib.hooks.ftp_hook import FTPHook

# Internal modules
from .wm_ftp_sensor import get_filename


logger = logging.getLogger(__name__)


class WMFTPDownloader(BaseOperator):
    @apply_defaults
    def __init__(self, disk_path, ftp_conn_id, *args, **kwargs):
        """
        The WMFTPDownloader is used to download new Wettermast content.

        Parameters
        ----------
        disk_path : str
            Disk path where the Wettermast files are archived.
        ftp_conn_id : str
            The connection id for the ftp connection. The connection is searched
            within the defined airflow connections.
        """
        super().__init__(*args, **kwargs)
        self._ftp_conn_id = None
        self._disk_path = None
        self.disk_path = disk_path
        self.ftp_conn_id = ftp_conn_id

    @property
    def ftp_conn_id(self):
        return self._ftp_conn_id

    @ftp_conn_id.setter
    def ftp_conn_id(self, id):
        if not isinstance(id, str):
            raise TypeError('The given connection id is not a string!')
        else:
            self._ftp_conn_id = id

    @property
    def disk_path(self):
        return self._disk_path

    @disk_path.setter
    def disk_path(self, path):
        if not isinstance(path, str):
            raise TypeError('The given disk path is not a string!')
        elif os.path.isfile(path):
            raise TypeError('The given path is a file!')
        elif not os.path.isdir(path):
            os.makedirs(path)
        else:
            self._disk_path = path

    def _create_hook(self):
        """
        Create a ftp hook based on the ftp connection id.

        Returns
        -------
        hook : FTPHook
            The created ftp hook.
        """
        return FTPHook(self.ftp_conn_id)

    def execute(self, context):
        wm_filename = get_filename(context['execution_date'])
        disk_file_path = os.path.join(self.disk_path, wm_filename)
        with self._create_hook() as hook:
            hook.retrieve_file(wm_filename, disk_file_path)
