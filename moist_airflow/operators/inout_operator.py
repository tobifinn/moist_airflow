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
import abc

# External modules
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults

# Internal modules
import moist_airflow.functions.utiltities as utils


logger = logging.getLogger(__name__)


class InOutOperator(BaseOperator):
    @apply_defaults
    def __init__(self, python_callable, input_static_path, input_template=None,
                 output_static_path=None, output_template=None,
                 rounding_td=None, offset_td=None, op_args=None, op_kwargs=None,
                 provide_context=False, *args, **kwargs):
        """
        The InOutOperator is an operator for standardized input and output.

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

        Notes
        -----
        This operator is based on the standard PythonOperator.
        """
        super().__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context
        self.input_static_path = input_static_path
        self.input_template = input_template
        self.output_static_path = output_static_path
        self.output_template = output_template
        self.rounding_td = rounding_td
        self.offset_td = offset_td
        self.ds = None

    @abc.abstractmethod
    def load_input(self, input_path):
        """
        Load the input data.

        Parameters
        ----------
        input_path : str
            The path to the input data.

        Returns
        -------
        loaded_input : python Obj
            The loaded input data.
        """
        pass

    @abc.abstractmethod
    def save_output(self, ds, output_path):
        """
        Save the output data.
        Parameters
        ----------
        ds : python Obj
            The data which should be saved.
        output_path : str
            The output path where the data should be stored.
        """
        pass

    def input_path(self, input_date):
        return utils.compose_address(input_date, self.input_static_path,
                                     self.input_template
        )

    def output_path(self, output_date):
        if not self.output_static_path and not self.output_template:
            return self.input_path(output_date)
        elif not self.output_static_path:
            return utils.compose_address(
                output_date, self.input_static_path, self.output_template
            )
        else:
            return utils.compose_address(
                output_date, self.output_static_path, self.output_template
            )

    def execute(self, context):
        modified_date = utils.modify_date(
            context['execution_date'], self.rounding_td, self.offset_td
        )
        ds = self.load_input(self.input_path(modified_date))
        if self.provide_context:
            context.update(self.op_kwargs)
            context['ds'] = ds
            self.op_kwargs = context
        ds, return_value = self.python_callable(*self.op_args, **self.op_kwargs)
        self.save_output(ds, self.output_path(modified_date))
        logging.info("Done. Returned value was: " + str(return_value))
        return return_value
