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
import datetime

# External modules
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.ftp_hook import FTPHook

# Internal modules


logger = logging.getLogger(__name__)


class FTPSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, filename_template, ftp_conn_id, disk_path='/',
                 *args, **kwargs):
        """
        The ftp sensor is used to check if a given file is available on ftp
        server and differs from disk.

        Parameters
        ----------
        filename_template : str
            The filename template is used to generated the filename based on the
            execution date given by the airflow context. The filename template
            is passed to strftime and should be conform to the datetime format.
        ftp_conn_id : str
            The connection id for the ftp connection. The connection is searched
            within the defined airflow connections.
        disk_path : str, optional
            The path where the files are archived. If the file is not found
            within the disk path the sensor checks only if the file is available
            within the ftp server. Default is '/', where the existence
            possibility is low.

        Notes
        -----
        Partially this method is based on airflow/contrib/sensors/ftp_sensor.py
        """
        super().__init__(*args, **kwargs)
        self.disk_path = disk_path
        self.ftp_conn_id = ftp_conn_id
        self.filename_template = filename_template

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
        filename = context['execution_date'].strftime(self.filename_template)
        disk_file_path = os.path.join(self.disk_path, filename)
        ftp_file_size = -1
        disk_file_size = -1
        with self._create_hook() as hook:
            try:
                hook.get_mod_time(filename)
                logger.info('The file {0:s} is available for given connection '
                            '{1:s}'.format(filename, self.ftp_conn_id))
            except ftplib.error_perm as e:
                error = str(e).split(None, 1)
                logger.error('The ftp connection has an error: {0}'.format(
                    error))
                if error[1] != "{0:s}: No such file or directory".format(
                        filename):
                    raise e
                return False
            try:
                disk_file_size = os.path.getsize(disk_file_path)
                logger.info(
                    'The file {0:s} is available with size {1:.2f}'.format(
                        disk_file_path, disk_file_size
                    ))
            except FileNotFoundError:
                logger.info('The file {0:s} is not available'.format(
                    disk_file_path))
                return True
            try:
                conn = hook.get_conn()
                conn.sendcmd("TYPE i")
                ftp_file_size = conn.size(filename)
                logger.info('The file {0:s} has a ftp file size of '
                            '{1:.2f}'.format(filename, ftp_file_size))
            except ftplib.error_perm as e:
                logger.error('The file couldn\'t downloaded: {0}'.format(
                    e))
                raise e
        if ftp_file_size != disk_file_size:
            logger.info('The ftp file size differs from disk file size')
            return True
        else:
            return False
