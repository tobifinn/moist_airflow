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

# Internal modules


logger = logging.getLogger(__name__)


def compose_address(input_date, static_path, path_template=None):
    """
    Compose address based on given date

    Parameters
    ----------
    input_date : datetime.datetime
        The path template will be modified by this date.
    static_path : str
        The static path of the address.
    path_template : str
        This template is used to generate the non-static part based on the
        given date. If this is None no variable path is set.

    Returns
    -------
    address : str
        The address based on given date. The address will be:
        static_part/modified_template.
    """
    address = static_path
    if path_template is not None:
        additional_path = input_date.strftime(path_template)
        address = '{0:s}/{1:s}'.format(address, additional_path)
    return address


def modify_date(input_date, rounding_td=None, offset_td=None):
    """
    Round and offset the given date.

    Parameters
    ----------
    input_date : datetime.datetime
        This date will be rounded and offset.
    rounding_td : datetime.timedelta
        The input date will be rounded to this timedelta instance. If it is None
        the input date isn't rounded. Default is None.
    offset_td : datetime.timedelta
        This offset is added after the rounding to the date. If this is None
        there will be no offset. Default is None.

    Returns
    -------
    modified_date : datetime.datetime
        The rounded and offset datetime instance.
    """
    modified_date = input_date
    if isinstance(rounding_td, datetime.timedelta):
        execution_seconds = int(
            (modified_date-datetime.datetime.min.replace(
                tzinfo=modified_date.tzinfo)).total_seconds()
        )
        remainder = datetime.timedelta(
            seconds=execution_seconds % rounding_td.total_seconds(),
            microseconds=modified_date.microsecond,
        )
        modified_date -= remainder
    if isinstance(offset_td, datetime.timedelta):
        modified_date += offset_td
    return modified_date
