#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 29.08.17
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
import datetime

# External modules
import netCDF4 as nc

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

# Internal modules
from moist_airflow.functions import utilities as utils


logger = logging.getLogger(__name__)


class OpenDapSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, server_static_path, server_template=None,
                 dt_rounding=None, dt_offset=None,
                 *args, **kwargs):
        """
        The OpenDapSensor is used to check if new data is available on an
        OpenDap server.

        Parameters
        ----------
        server_static_path : str
            The static part of the server address. This part is used for a
            non-changing part of the server address. The resulting address will
            be: server_static_path/server_template
        server_template : str or None, optional
            The template for the non-static part of the server address. This
            part is used to generate the non-static part based on the execution
            date given by the airflow context. The filename template is passed
            to strftime and have to be conform to the datetime format. If
            server_template is None, the server address will be only the static
            part. Default is None.
        dt_rounding : datetime.timedelta or None, optional
            The exeuction date will be rounded to this timedelta instance. If
            the rounding is None there is no rounding. Default is None.
        dt_offset : datetime.timedelta or None, optional
            This timedelta offset will be added to the rounded execution date.
            If the offset is None there is no offset. Default is None.
        """
        super().__init__(*args, **kwargs)
        self.server_static_path = server_static_path
        self.server_template = server_template
        self.dt_rounding = dt_rounding
        self.dt_offset = dt_offset

    def poke(self, context):
        """
        Modify the execution date of the context and check if the server address
        is a valid and available opendap address.

        Parameters
        ----------
        context : dict
            The airflow task instance context.

        Returns
        -------
        bool
            If the opendap server address is valid and available.
        """
        server_date = utils.modify_date(
            context['execution_date'], self.dt_rounding, self.dt_offset)
        server_address = utils.compose_address(
            server_date, self.server_static_path, self.server_template)
        try:
            _ = nc.Dataset(server_address)
            logger.info('{0:s} is available'.format(server_address))
            return True
        except OSError:
            logger.warning('{0:s} is not available'.format(server_address))
            return False
