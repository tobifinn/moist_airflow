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

# External modules

# Internal modules


logger = logging.getLogger(__name__)


def dataset_slice_data(ds, variables=None, isel=None, sel=None, *args,
                       **kwargs):
    """
    Function to slice data from a xarray.Dataset. This function could be used to
    download opendap data to netcdf.

    Parameters
    ----------
    ds : xarray.Dataset
        The input dataset which should be sliced.
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
    if variables is not None:
        ds = ds[variables]
    if isinstance(isel, dict):
        ds = ds.isel(**isel)
    if isinstance(sel, dict):
        ds = ds.sel(**sel)
    return ds, None
