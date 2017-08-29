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
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults

# Internal modules

logger = logging.getLogger(__name__)


class FTPDownloader(BaseOperator):
    @apply_defaults
    def __init__(self, filename_template, ftp_conn_id, disk_path, *args,
                 **kwargs):
        """
        The FTP could be used to download ftp content into a given path.

        Parameters
        ----------
        filename_template : str
            The filename template is used to generated the filename based on the
            execution date given by the airflow context. The filename template
            is passed to strftime and should be conform to the datetime format.
        ftp_conn_id : str
            The connection id for the ftp connection. The connection is searched
            within the defined airflow connections.
        disk_path : str
            The path where the file should be archived. If the disk path doesn't
            exist it will be created.
        """
        super().__init__(*args, **kwargs)
        self._ftp_conn_id = None
        self._disk_path = None
        self.disk_path = disk_path
        self.ftp_conn_id = ftp_conn_id
        self.filename_template = filename_template

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
        filename = context['execution_date'].strftime(self.filename_template)
        disk_file_path = os.path.join(self.disk_path, filename)
        with self._create_hook() as hook:
            hook.retrieve_file(filename, disk_file_path)
