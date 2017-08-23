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
import os
import logging
import ftplib

# External modules
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.ftp_hook import FTPHook


# Internal modules


logger = logging.getLogger(__name__)

FILENAME_TEMPLATE = '%Y_W%W_MASTER_M10.txt'


def get_filename(date):
    """
    Get the filename for given date.

    Parameters
    ----------
    date : datetime.datetime
        The filename is generated for this datetime

    Returns
    -------
    filename : str
        The generated filename.
    """
    return date.strftime(FILENAME_TEMPLATE)


class WettermastFTPSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, disk_path, ftp_conn_id, *args, **kwargs):
        """
        The WettermastFTPSensor is used to check if new measurement content is
        available, based on a Wettermast ascii file.

        Parameters
        ----------
        disk_path : str
            Disk path where the Wettermast files are archived.
        ftp_conn_id : str
            The connection id for the ftp connection. The connection is searched
            within the defined airflow connections.

        Notes
        -----
        Partially this method is based on airflow/contrib/sensors/ftp_sensor.py
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
            raise TypeError('The given path is already a file!')
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

    def poke(self, context):
        """
        Compares disk file size and ftp file size for given generated file name
        based on the execution date.

        Parameters
        ----------
        context : dict
            The airflow task instance context.

        Returns
        -------
        bool
            If the ftp file size is bigger than the disk file size.
        """
        wm_filename = get_filename(context['execution_date'])
        disk_file_path = os.path.join(self.disk_path, wm_filename)
        try:
            disk_file_size = os.path.getsize(disk_file_path)
        except FileNotFoundError:
            disk_file_size = -1
        ftp_size = -2
        with self._create_hook() as hook:
            try:
                hook.get_mod_time(wm_filename)
            except ftplib.error_perm as e:
                error = str(e).split(None, 1)
                if error[1] != "{0:s}: No such file or directory".format(
                        wm_filename):
                    raise e
                return False
            try:
                conn = hook.get_conn()
                conn.sendcmd("TYPE i")
                ftp_size = conn.size(wm_filename)
            except ftplib.error_perm as e:
                raise e
        if ftp_size > disk_file_size:
            return True
        return False
