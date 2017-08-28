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

# Internal modules


logger = logging.getLogger(__name__)


class FileAvailableOperator(BaseOperator):
    @apply_defaults
    def __init__(self, parent_dir, filename_template, *args, **kwargs):
        """
        FileAvailableOperator is used to check if the given file is available.

        Parameters
        ----------
        parent_dir : str
            The path to the parent directory of the file.
        filename_template : str
            The filename template is used to generated the filename based on the
            execution date given by the airflow context. The filename template
            is passed to strftime and should be conform to the datetime format.
        """
        super().__init__(*args, **kwargs)
        self._parent_dir = None
        self.parent_dir = parent_dir
        self.filename_template = filename_template

    @property
    def parent_dir(self):
        return self._parent_dir

    @parent_dir.setter
    def parent_dir(self, path):
        if not isinstance(path, str):
            raise TypeError('The given parent dir is not a string!')
        elif os.path.isfile(path):
            raise TypeError('The given path is already a file!')
        elif not os.path.isdir(path):
            os.makedirs(path)
        else:
            self._parent_dir = path

    def execute(self, context):
        filename = context['execution_date'].strftime(self.filename_template)
        file_path = os.path.join(self.parent_dir, filename)
        if not os.path.isfile(file_path):
            raise FileNotFoundError(
                'The given file path {0:s} couldn\'t found'.format(file_path))
