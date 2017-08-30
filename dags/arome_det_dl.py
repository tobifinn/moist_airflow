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
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Internal modules
from moist_airflow.operators.opendap_sensor import OpenDapSensor
from moist_airflow.functions.xarray.ds_slice import dataset_slice_data
from moist_airflow.operators.check_file_available import FileAvailableOperator


logger = logging.getLogger(__name__)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2017, 9, 1, 5),
    'email': ['tfinn@live.com', ],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

FILE_PATH = '/home/tfinn/Data/test/model/metno/det'

dag = DAG('extract_metno_det', default_args=default_args,
          schedule_interval=datetime.timedelta(hours=6),
          orientation='TB')

rt_sensor = OpenDapSensor(
    server_static_path='http://thredds.met.no/thredds/dodsC/meps25files',
    server_template='meps_det_extracted_2_5km_%Y%m%dT%HZ.nc',
    dt_rounding=datetime.timedelta(hours=6),
    dt_offset=None,
    task_id='sensor_realtime',
    timeout=60*60,
    poke_interval=60,
    dag=dag)

rt_dl_t2m = PythonOperator(
    python_callable=dataset_slice_data,
    op_kwargs=dict(
        input_static_path='http://thredds.met.no/thredds/dodsC/meps25files',
        input_template='meps_det_extracted_2_5km_%Y%m%dT%HZ.nc',
        dt_rounding=datetime.timedelta(hours=6),
        dt_offset=None,
        output_static_path=FILE_PATH,
        output_template='%Y%m%d_%H%M/t2m.nc',
        variables='air_temperature_2m',
        isel=dict(y=slice(0, 90), x=slice(210, 270))
    ),
    provide_context=True,
    task_id='realtime_t2m_download',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

rt_dl_extracted = PythonOperator(
    python_callable=dataset_slice_data,
    op_kwargs=dict(
        input_static_path='http://thredds.met.no/thredds/dodsC/meps25files',
        input_template='meps_det_extracted_2_5km_%Y%m%dT%HZ.nc',
        dt_rounding=datetime.timedelta(hours=6),
        dt_offset=None,
        output_static_path=FILE_PATH,
        output_template='%Y%m%d_%H%M/extracted.nc',
        variables=None,
        isel=dict(y=slice(0, 90), x=slice(210, 270))
    ),
    provide_context=True,
    task_id='realtime_extracted_download',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

archive_sensor = OpenDapSensor(
    server_static_path='http://thredds.met.no/thredds/dodsC/meps25epsarchive',
    server_template='%Y/%m/%d/meps_extracted_2_5km_%Y%m%dT%HZ.nc',
    dt_rounding=datetime.timedelta(hours=6),
    dt_offset=None,
    task_id='sensor_archive',
    timeout=1,
    poke_interval=1,
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag)

archive_dl_t2m = PythonOperator(
    python_callable=dataset_slice_data,
    op_kwargs=dict(
        input_static_path='http://thredds.met.no/thredds/dodsC'
                           '/meps25epsarchive',
        input_template='%Y/%m/%d/meps_extracted_2_5km_%Y%m%dT%HZ.nc',
        dt_rounding=datetime.timedelta(hours=6),
        dt_offset=None,
        output_static_path=FILE_PATH,
        output_template='%Y%m%d_%H%M/t2m.nc',
        variables='air_temperature_2m',
        isel=dict(y=slice(0, 90), x=slice(210, 270)),
        sel=dict(ensemble_member=0)
    ),
    provide_context=True,
    task_id='archive_t2m_download',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

archive_dl_extracted = PythonOperator(
    python_callable=dataset_slice_data,
    op_kwargs=dict(
        input_static_path='http://thredds.met.no/thredds/dodsC'
                           '/meps25epsarchive',
        input_template='%Y/%m/%d/meps_extracted_2_5km_%Y%m%dT%HZ.nc',
        dt_rounding=datetime.timedelta(hours=6),
        dt_offset=None,
        output_static_path=FILE_PATH,
        output_template='%Y%m%d_%H%M/extracted.nc',
        variables=None,
        isel=dict(y=slice(0, 90), x=slice(210, 270)),
        sel=dict(ensemble_member=0)
    ),
    provide_context=True,
    task_id='archive_extracted_download',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

available_t2m = FileAvailableOperator(
    parent_dir=FILE_PATH,
    filename_template='%Y%m%d_%H%M/t2m.nc',
    task_id='realtime_t2m_checker',
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag)

available_extracted = FileAvailableOperator(
    parent_dir=FILE_PATH,
    filename_template='%Y%m%d_%H%M/extracted.nc',
    task_id='realtime_extracted_checker',
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag)

rt_dl_t2m.set_upstream(rt_sensor)
rt_dl_extracted.set_upstream(rt_sensor)
archive_sensor.set_upstream(rt_sensor)

archive_dl_t2m.set_upstream(archive_sensor)
archive_dl_extracted.set_upstream(archive_sensor)
available_t2m.set_upstream(archive_sensor)
available_extracted.set_upstream(archive_sensor)
