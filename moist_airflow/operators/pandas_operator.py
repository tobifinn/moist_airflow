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
import os

# External modules
import pandas as pd

# Internal modules
from .inout_operator import InOutOperator


logger = logging.getLogger(__name__)


class PandasOperator(InOutOperator):
    """
    The PandasOperator is an operator for standardized pandas input and output.
    The input and output have to be json files with a split orient and date in
    iso-format.

    Parameters
    ----------
    python_callable : python callable
        A reference to a python object that is callable. The callable should
        have a ds argument to get the loaded data. To save the data the
        callable should return the modified ds as first return value.
    input_static_path : str
        The static part of the input file.
    input_template : str or None, optional
        The template for the dynamic part of the input path. This
        part is used to generate the non-static part based on the execution
        date given by the airflow context. The template is passed
        to strftime and have to be conform to the datetime format. If
        the template is None, the input path will have only the static
        part. Default is None.
    output_static_path : str or None, optional
        The static part of the output file. If this is None the output
        static path is set to the input static path. Default is None.
    output_template : str or None, optional
        The template for the dynamic part of the output path. This
        part is used to generate the dynamic part based on the execution
        date given by the airflow context. The template is passed
        to strftime and have to be conform to the datetime format. If
        the template is None, the output path will have only the static
        part. Default is None.
    rounding_td : datetime.timedelta or None, optional
        The exeuction date will be rounded to this timedelta instance. If
        the rounding is None there is no rounding. Default is None.
    offset_td : datetime.timedelta or None, optional
        This timedelta offset will be added to the rounded execution date.
        If the offset is None there is no offset. Default is None.
    op_args : list or None, optional
        A list of positional arguments that will get unpacked when calling
        your callable. Default is None.
    op_kwargs : dict or None, optional
        A dictionary of keyword arguments that will get unpacked in your
        function. Default is None.
    provide_context : boolean, optional
        If set to true, Airflow will pass a set of keyword arguments tha
        can be used in your function. This set of kwargs correspond exactly
        to what you can use in your jinja templates. For this to work, you
        need to define `**kwargs` in your function header.
    """
    def load_input(self, input_path, *args, **kwargs):
        logger.info('Read the dataset from: {0:s}'.format(input_path))
        try:
            loaded_data = pd.read_json(input_path, orient='split', typ='frame')
        except ValueError:
            loaded_data = pd.read_json(input_path, orient='split', typ='series')
        if isinstance(loaded_data.index, pd.DatetimeIndex):
            loaded_data.index = loaded_data.index.tz_localize('UTC')
        return loaded_data

    def save_output(self, ds, output_path, *args, **kwargs):
        logger.info('Save the dataset to: {0:s}'.format(output_path))
        if 'input_path' in kwargs and kwargs['input_path'] == output_path:
            tmp_path = output_path+'.tmp'
            ds.to_json(tmp_path, orient='split', date_format='iso')
            os.remove(kwargs['input_path'])
            os.rename(tmp_path, output_path)
        else:
            self._create_parent(output_path)
            ds.to_json(output_path, orient='split', date_format='iso')
