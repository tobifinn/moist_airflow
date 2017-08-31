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
import os

# External modules
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.hooks.fs_hook import FSHook

import pandas as pd
import xarray as xr

import pymepps

# Internal modules
from moist_airflow.operators import OpenDapSensor
from moist_airflow.operators import XarrayOperator
from moist_airflow.operators import Xr2PdOperator
from moist_airflow.operators import FileAvailableOperator
from moist_airflow.functions.xarray.ds_slice import dataset_slice_data


logger = logging.getLogger(__name__)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2017, 8, 30, 21),
    'email': ['tfinn@live.com', ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}


METNO_DET_HOOK = FSHook('metno_det_data')
DB_HOOK = FSHook('db_data')

dag = DAG('extract_metno_det_v0.3', default_args=default_args,
          schedule_interval=datetime.timedelta(hours=6),
          orientation='TB')

rt_sensor = OpenDapSensor(
    server_static_path='http://thredds.met.no/thredds/dodsC/meps25files',
    server_template='meps_det_extracted_2_5km_%Y%m%dT%HZ.nc',
    dt_rounding=datetime.timedelta(hours=6),
    dt_offset=None,
    task_id='sensor_realtime',
    timeout=60*60*6,
    poke_interval=60,
    pool='sensor_pool',
    dag=dag)

rt_dl_t2m = XarrayOperator(
    python_callable=dataset_slice_data,
    input_static_path='http://thredds.met.no/thredds/dodsC/meps25files',
    input_template='meps_det_extracted_2_5km_%Y%m%dT%HZ.nc',
    rounding_td=datetime.timedelta(hours=6),
    output_static_path=METNO_DET_HOOK.get_path(),
    output_template='%Y%m%d_%H%M/t2m.nc',
    op_kwargs=dict(
        variables='air_temperature_2m',
        isel=dict(y=slice(0, 90), x=slice(210, 270))
    ),
    provide_context=True,
    task_id='realtime_t2m_download',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)
rt_dl_t2m.set_upstream(rt_sensor)

rt_dl_extracted = XarrayOperator(
    python_callable=dataset_slice_data,
    input_static_path='http://thredds.met.no/thredds/dodsC/meps25files',
    input_template='meps_det_extracted_2_5km_%Y%m%dT%HZ.nc',
    rounding_td=datetime.timedelta(hours=6),
    output_static_path=METNO_DET_HOOK.get_path(),
    output_template='%Y%m%d_%H%M/extracted.nc',
    op_kwargs=dict(
        variables=None,
        isel=dict(y=slice(0, 90), x=slice(210, 270))
    ),
    provide_context=True,
    task_id='realtime_extracted_download',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    pool='download_pool',
    dag=dag
)
rt_dl_extracted.set_upstream(rt_sensor)

archive_sensor = OpenDapSensor(
    server_static_path='http://thredds.met.no/thredds/dodsC/meps25epsarchive',
    server_template='%Y/%m/%d/meps_extracted_2_5km_%Y%m%dT%HZ.nc',
    dt_rounding=datetime.timedelta(hours=6),
    dt_offset=None,
    task_id='sensor_archive',
    timeout=1,
    poke_interval=1,
    trigger_rule=TriggerRule.ALL_FAILED,
    pool='sensor_pool',
    dag=dag)
archive_sensor.set_upstream(rt_sensor)

archive_dl_t2m = XarrayOperator(
    python_callable=dataset_slice_data,
    input_static_path='http://thredds.met.no/thredds/dodsC'
                      '/meps25epsarchive',
    input_template='%Y/%m/%d/meps_extracted_2_5km_%Y%m%dT%HZ.nc',
    rounding_td=datetime.timedelta(hours=6),
    output_static_path=METNO_DET_HOOK.get_path(),
    output_template='%Y%m%d_%H%M/t2m.nc',
    op_kwargs=dict(
        variables='air_temperature_2m',
        isel=dict(y=slice(0, 90), x=slice(210, 270)),
        sel=dict(ensemble_member=0)
    ),
    provide_context=True,
    task_id='archive_t2m_download',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)
archive_dl_t2m.set_upstream(archive_sensor)

archive_dl_extracted = XarrayOperator(
    python_callable=dataset_slice_data,
    input_static_path='http://thredds.met.no/thredds/dodsC'
                      '/meps25epsarchive',
    input_template='%Y/%m/%d/meps_extracted_2_5km_%Y%m%dT%HZ.nc',
    rounding_td=datetime.timedelta(hours=6),
    output_static_path=METNO_DET_HOOK.get_path(),
    output_template='%Y%m%d_%H%M/extracted.nc',
    op_kwargs=dict(
        variables=None,
        isel=dict(y=slice(0, 90), x=slice(210, 270)),
        sel=dict(ensemble_member=0)
    ),
    provide_context=True,
    task_id='archive_extracted_download',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    pool='download_pool',
    dag=dag
)
archive_dl_extracted.set_upstream(archive_sensor)

available_t2m = FileAvailableOperator(
    parent_dir=METNO_DET_HOOK.get_path(),
    filename_template='%Y%m%d_%H%M/t2m.nc',
    task_id='realtime_t2m_checker',
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag)
available_t2m.set_upstream(archive_sensor)

available_extracted = FileAvailableOperator(
    parent_dir=METNO_DET_HOOK.get_path(),
    filename_template='%Y%m%d_%H%M/extracted.nc',
    task_id='realtime_extracted_checker',
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag)
available_extracted.set_upstream(archive_sensor)


def extract_t2m_wm(ds, *args, **kwargs):
    ds = ds['air_temperature_2m']
    # Extracted grid from arome metno files
    grid_dir = FSHook('grid_data')
    grid_file = os.path.join(grid_dir.get_path(), 'metno_grid')
    grid_builder = pymepps.GridBuilder(grid_file)

    ds.pp.grid = grid_builder.build_grid()

    # Extract the nearest point to the Wettermast
    pd_extracted = ds.pp.to_pandas((10.105139, 53.519917))

    # Transform to degree celsius
    pd_extracted -= 273.15
    pd_extracted.index -= pd_extracted.index[0]
    pd_extracted.columns = [kwargs['run_date'], ]
    logger.info('The extracted T2m is:\n{0}'.format(str(pd_extracted)))

    # Update the database
    try:
        loaded_data = pd.DataFrame.pp.load(kwargs['output_path'])
        loaded_data.index = pd.TimedeltaIndex(loaded_data.index.values)
        return loaded_data.pp.update(pd_extracted)
    except FileNotFoundError:
        return pd_extracted

make_t2m_wm_fcst = Xr2PdOperator(
    python_callable=extract_t2m_wm,
    input_static_path=METNO_DET_HOOK.get_path(),
    input_template='%Y%m%d_%H%M/t2m.nc',
    rounding_td=datetime.timedelta(hours=6),
    output_static_path=DB_HOOK.get_path(),
    output_template='arome_metno_det.json',
    provide_context=False,
    task_id='extract_t2m_wm',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)
make_t2m_wm_fcst.set_upstream(rt_dl_t2m)
make_t2m_wm_fcst.set_upstream(archive_dl_t2m)
make_t2m_wm_fcst.set_upstream(available_t2m)


def slice_wettermast(ds, *args, **kwargs):
    logger.info(ds)
    grid_dir = FSHook('grid_data')
    grid_file = os.path.join(grid_dir.get_path(), 'metno_grid')
    grid_builder = pymepps.GridBuilder(grid_file)
    grid = grid_builder.build_grid()
    nn = grid.nearest_point((53.519917, 10.105139))
    logger.info('Select point: {0}'.format(nn))
    ds = ds.isel(y=nn[0], x=nn[1])
    return ds

slice_wm_extracted = XarrayOperator(
    python_callable=slice_wettermast,
    input_static_path=METNO_DET_HOOK.get_path(),
    input_template='%Y%m%d_%H%M/extracted.nc',
    rounding_td=datetime.timedelta(hours=6),
    output_static_path=METNO_DET_HOOK.get_path(),
    output_template='%Y%m%d_%H%M/extracted_wm.nc',
    task_id='slice_wettermast_extracted',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    pool='processing_pool',
    dag=dag
)
slice_wm_extracted.set_upstream(rt_dl_extracted)
slice_wm_extracted.set_upstream(archive_dl_extracted)
slice_wm_extracted.set_upstream(available_extracted)
