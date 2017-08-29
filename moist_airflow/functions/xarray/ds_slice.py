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
import xarray as xr

# Internal modules
import moist_airflow.functions.utiltities as utils


logger = logging.getLogger(__name__)


def dataset_slice_data(input_static_path, input_template=None,
                       output_static_path=None, output_template=None,
                       dt_rounding=None, dt_offset=None, variables=None,
                       isel=None, sel=None, *args, **kwargs):
    """
    Function to slice data from a xarray.Dataset. This function could be used to
    download opendap data to netcdf.

    Parameters
    ----------
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
        The static part of the output file. If this is None the output static
        path is set to the input static path. Default is None.
    output_template : str or None, optional
        The template for the dynamic part of the output path. This
        part is used to generate the dynamic part based on the execution
        date given by the airflow context. The template is passed
        to strftime and have to be conform to the datetime format. If
        the template is None, the output path will have only the static
        part. Default is None.
    dt_rounding : datetime.timedelta or None, optional
        The exeuction date will be rounded to this timedelta instance. If
        the rounding is None there is no rounding. Default is None.
    dt_offset : datetime.timedelta or None, optional
        This timedelta offset will be added to the rounded execution date.
        If the offset is None there is no offset. Default is None.
    variables : str, iterable or None, optional
        These variables are selected within the dataset. If this is None, all
        variables are selected. Default is None.
    isel : dict(str, any) or None, optional
        This dictionary is used to slice the dataset with positional indexing.
        If this is None, the dataset is not sliced. Default is None.
    sel : dict(str, any) or None, optional
        This dictionary is used to slice the dataset with value indexing. If
        this is None the dataset is not sliced. Default is None.
    """
    dataset_date = utils.modify_date(
        kwargs['execution_date'], dt_rounding, dt_offset)
    input_file_path = utils.compose_address(
        dataset_date, input_static_path, input_template)
    if isinstance(output_static_path, str):
        output_file_path = utils.compose_address(
            dataset_date, output_static_path, output_template)
    else:
        output_file_path = utils.compose_address(
            dataset_date, input_static_path, output_template)
    logger.info('The input file is: {0:s}'.format(input_file_path))
    logger.info('The output file is: {0:s}'.format(output_file_path))
    ds = xr.open_dataset(input_file_path)
    if variables is not None:
        ds = ds[variables]
    if isinstance(isel, dict):
        ds = ds.isel(**isel)
    if isinstance(sel, dict):
        ds = ds.sel(**sel)
    if output_file_path != input_file_path:
        dirname = os.path.dirname(os.path.abspath(output_file_path))
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        ds.to_netcdf(output_file_path)
        ds.close()
    else:
        tmp_file_path = output_file_path+'.tmp'
        ds.to_netcdf(tmp_file_path)
        ds.close()
        os.remove(input_file_path)
        os.rename(tmp_file_path, output_file_path)
