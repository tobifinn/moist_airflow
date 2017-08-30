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
import datetime

# External modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.hooks.fs_hook import FSHook

# Internal modules
from moist_airflow.functions.pandas.df_update_db import df_update_another
from moist_airflow.functions.encode_wmascii_to_json import \
    encode_wmascii_to_json
from moist_airflow.operators import FileAvailableOperator
from moist_airflow.operators import FTPDownloader
from moist_airflow.operators import FTPSensor
from moist_airflow.operators import PandasOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2017, 8, 30, 16, 45),
    'email': ['tfinn@live.com', ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

DATA_HOOK = FSHook('wettermast_data')
DB_HOOK = FSHook('db_data')
TMP_HOOK = FSHook('temporary_data')


dag = DAG('extract_wettermast', default_args=default_args,
          schedule_interval=datetime.timedelta(minutes=15),
          orientation='TB')

wm_sensor_task = FTPSensor(filename_template='%G_W%V_MASTER_M10.txt',
                           ftp_conn_id='ftp_wettermast',
                           disk_path=DATA_HOOK.get_path(),
                           task_id='sensor_ftp',
                           timeout=120,
                           poke_interval=10,
                           dag=dag)

dl_task = FTPDownloader(filename_template='%G_W%V_MASTER_M10.txt',
                        ftp_conn_id='ftp_wettermast',
                        disk_path=DATA_HOOK.get_path(),
                        task_id='downloader_ftp',
                        trigger_rule=TriggerRule.ALL_SUCCESS,
                        dag=dag)

already_dl_task = FileAvailableOperator(
    parent_dir=DATA_HOOK.get_path(),
    filename_template='%G_W%V_MASTER_M10.txt',
    task_id='file_checker',
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag)

encode_wm = PythonOperator(
    python_callable=encode_wmascii_to_json,
    op_kwargs=dict(
        input_path=DATA_HOOK.get_path(),
        output_path=TMP_HOOK.get_path()
    ),
    task_id='encoder_temp',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
    provide_context=True
)

todb_wm = PandasOperator(
    python_callable=df_update_another,
    input_static_path=DB_HOOK.get_path(),
    input_template='wm_test.json',
    output_static_path=DB_HOOK,
    output_template='wm_test.json',
    op_kwargs=dict(
        another_path=TMP_HOOK.get_path(),
        another_template='wettermast_%Y%m%d%H%M.json',
        time_bound=datetime.timedelta(days=7)
    ),
    provide_context=True,
    task_id='add_to_db',
    dag=dag
)


def extract_columns(ds, column_names, *args, **kwargs):
    return ds.loc[:, column_names]

prepare_plot = PandasOperator(
    python_callable=extract_columns,
    input_static_path=DB_HOOK.get_path(),
    input_template='wm.json',
    output_static_path=TMP_HOOK.get_path(),
    output_template='plot_obs.json',
    op_kwargs=dict(
        column_names='TT002_M10'
    ),
    provide_context=True,
    task_id='extract_tt002',
    dag=dag
)

dl_task.set_upstream(wm_sensor_task)
already_dl_task.set_upstream(wm_sensor_task)
encode_wm.set_upstream(dl_task)
encode_wm.set_upstream(already_dl_task)
todb_wm.set_upstream(encode_wm)
prepare_plot.set_upstream(todb_wm)
